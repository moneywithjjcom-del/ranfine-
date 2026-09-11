"""Watch n8n workflows from outside n8n, and complain when they go quiet.

n8n tells you when a run errors. It does not tell you when a run finishes
successfully having done nothing, and it cannot tell you anything at all once
the instance is down or a worker has been OOM-killed.

Three questions n8n will not answer:

    1. Has this workflow ever run at all since you published it?
    2. Has it checked in recently enough?
    3. Did the last run produce roughly what it usually produces?

Question 3 is the one that matters and the one everybody gets wrong. A zero-row
check is easy and nearly useless: the run that quietly returns 60% of its normal
output passes every emptiness assertion you can write. So the default here is
deviation from a rolling median of recent runs, not a fixed floor.

Read-only. It never writes to your n8n.

Usage:
    export N8N_URL=https://n8n.example.com
    export N8N_API_KEY=...

    python monitor.py --scan      # check active workflows, no config needed
    python monitor.py --scan 100  # ... more of them; the default stops at 25
    python monitor.py --report watch.json [days]   # the month, in your units

Config keys worth knowing: expected_items (a blessed count), expect_present
(values that must appear in EVERY run) and expect_within_days (values that need
not be in every run but must turn up inside a window). Counts answer "how much";
the other two answer "which ones", and no count answers that.
    python monitor.py watch.json  # check the ones you have chosen to watch

Start with --scan. It needs nothing but the two variables above, and it prints a
watch.json at the end if you want to keep going.

`watch.json`:

    {
      "slack_webhook": "https://hooks.slack.com/...",
      "workflows": [
        {"id": "17", "name": "invoice-sync",  "every_minutes": 15},
        {"id": "22", "name": "nightly-export", "every_minutes": 1440,
         "watch_output": true},
        {"id": "31", "name": "billing-run", "every_minutes": 1440,
         "watch_output": true, "min_items": 50}
      ]
    }

`watch_output` turns on deviation detection. `min_items` adds a hard floor on
top of it, for the cases where you genuinely know the number.

Set HEARTBEAT_URL to a ping URL from Healthchecks.io, Cronitor or similar and
this tool will hit it after every completed run. That is how you find out the
CHECK stopped, which is a failure shaped exactly like good news.

NOTE ON SCOPE: the heartbeat below is the commoditised part -- Healthchecks.io
and Cronitor already do dead-man's switches, and do them well. What they cannot
do is look *inside* an n8n execution. The output checks are the part worth
having, and the n8n-specific checks listed at the bottom of this file are the
part worth building next.
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

# A late run is not a dead run: a 15-minute job firing at 15m01s is fine. Alert
# only once it has missed by half its own period again.
GRACE = 1.5

# How far below its own normal a run has to fall before we care. 0.4 means
# "alert under 60% of the median", which is the case that passes every
# zero-check ever written.
DEVIATION = 0.4

# Fewer runs than this and there is no meaningful "normal" yet. Alerting on a
# baseline of one or two samples is how a monitor earns itself a mute rule.
MIN_HISTORY = 3

# Same-weekday samples needed before the calendar baseline is trusted over the
# flat one. Three Mondays is thin but it beats comparing Monday to Sunday.
WEEKDAY_MIN_HISTORY = 3

# How many recent executions to pull per workflow. Deep enough that a daily job
# has several samples of each weekday -- ten runs would give barely one.
HISTORY = 30

# Scan mode pulls full run data, which is the expensive part of this whole tool:
# every execution carries every node's output. Thirty runs each across eighty
# workflows is megabytes of payload and eighty requests against an instance whose
# owner has not met us yet. A first run that hammers somebody's production n8n
# would disprove "read-only and harmless" more convincingly than any claim proves
# it. So scan pulls a shorter history than a configured watch does, and covers a
# bounded number of workflows unless asked for more.
SCAN_HISTORY = 12
SCAN_LIMIT = 25

# A scan that flags most of an instance is more likely a broken check than a
# broken instance. Below SCAN_HIT_FLOOR workflows the proportion means nothing,
# so the warning stays quiet rather than firing on a two-workflow scan.
SCAN_HIT_FLOOR = 5
SCAN_HIT_RATE = 0.5

# The monthly report pulls run data for a whole window, which is the one place
# this tool is allowed to be expensive - it runs once a month, on purpose, by a
# person. Still bounded, and the bound is named in the output when it bites.
REPORT_LIMIT = 250
REPORT_DAYS = 30

SCHEDULE_TRIGGERS = ("n8n-nodes-base.scheduleTrigger",
                     "n8n-nodes-base.cron",
                     "n8n-nodes-base.interval")

# Node types the static pass needs to recognise. Kept narrow on purpose: a
# guess here becomes a false finding in a report a stranger is reading.
HTTP_TYPES = ("n8n-nodes-base.httpRequest",)
CODE_TYPES = ("n8n-nodes-base.code", "n8n-nodes-base.function",
              "n8n-nodes-base.functionItem")

# Enough to turn a trigger rule into "how often should this have checked in".
MINUTES_PER = {"minutes": 1.0, "hours": 60.0, "days": 1440.0, "weeks": 10080.0}

DAY_NAMES = ("Monday", "Tuesday", "Wednesday", "Thursday",
             "Friday", "Saturday", "Sunday")


# --- pure logic (no network; this is the part worth testing) ----------------

def parse_time(value):
    """n8n returns ISO-8601, sometimes with a trailing Z. Tolerate both, and
    tolerate junk rather than exploding on one odd execution."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def median(values):
    ordered = sorted(values)
    n = len(ordered)
    if not n:
        return None
    mid = n // 2
    if n % 2:
        return float(ordered[mid])
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def _terminal_runs(execution):
    """The run records of the node named by lastNodeExecuted, or None.

    One place knows this shape. item_count and terminal_items both read it, so
    a change in n8n's execution format breaks one function rather than two.
    """
    result = ((execution or {}).get("data") or {}).get("resultData") or {}
    run_data = result.get("runData")
    if not isinstance(run_data, dict) or not run_data:
        return None
    last = result.get("lastNodeExecuted")
    runs = run_data.get(last) if last else None
    if not isinstance(runs, list) or not runs:
        return None
    return runs


def item_count(execution):
    """How many items the LAST node to run emitted.

    Not the sum across every node. Verified against a live n8n 2.36.7: summing
    counted the Schedule Trigger's own item too, so a workflow emitting 5 rows
    reported 6. Worse than an off-by-one -- a three-node chain passing 100 rows
    would report 300, and the figure would move every time someone added a node,
    silently invalidating the baseline it is compared against.

    ``resultData.lastNodeExecuted`` names the terminal node. Returns None when
    the run data is absent or the terminal node cannot be identified: None means
    "unknown", which is deliberately different from 0, and we never alert on
    unknown.
    """
    runs = _terminal_runs(execution)
    if runs is None:
        return None

    total = 0
    seen_any = False
    for run in runs:
        main = ((run or {}).get("data") or {}).get("main")
        if not isinstance(main, list):
            continue
        seen_any = True
        for branch in main:
            if isinstance(branch, list):
                total += len(branch)
    return total if seen_any else None


def terminal_items(execution):
    """The json payloads the terminal node emitted, or None if unreadable.

    None means "we cannot see the output", which is deliberately different from
    an empty list. We never alert on unknown.
    """
    runs = _terminal_runs(execution)
    if runs is None:
        return None

    items, seen_any = [], False
    for run in runs:
        main = ((run or {}).get("data") or {}).get("main")
        if not isinstance(main, list):
            continue
        seen_any = True
        for branch in main:
            if not isinstance(branch, list):
                continue
            for item in branch:
                if isinstance(item, dict):
                    payload = item.get("json")
                    items.append(payload if isinstance(payload, dict) else {})
    return items if seen_any else None


def _searchable(item, field=None):
    """The text of one item that a blessed value could appear in."""
    if field is not None:
        value = item.get(field)
        return "" if value is None else str(value)
    parts = []
    for value in item.values():
        if isinstance(value, bool):
            continue
        if isinstance(value, (str, int, float)):
            parts.append(str(value))
    return "\n".join(parts)


def missing_regulars(items, expected, field=None):
    """Which blessed values are absent from this run. -> (missing, field_gone)

    Reported by an agency owner running a bank-statement pull on r/n8n
    (2026-09-04): "a statement has the same regulars every month, rent,
    salaries, two or three subscriptions. If they are not in the pull then
    something got cut no matter what the total says."

    That is a different question from how many rows arrived, and no count
    answers it. A pull can lose the rent line and gain two others, and every
    total, median and blessed count still agrees with itself.

    Matching is case-insensitive substring, because real descriptions are dirty:
    "RENT PAYMENT 4421" has to satisfy "Rent". Returns no opinion at all when
    the output is unreadable or empty -- emptiness belongs to the count checks,
    and unknown belongs to nobody.
    """
    if items is None or not items or not expected:
        return [], False

    if field is not None and not any(field in item for item in items):
        # The column itself is gone. Naming every blessed value as missing
        # would be noise stacked on a different failure.
        return [], True

    hay = "\n".join(_searchable(item, field) for item in items).casefold()
    missing = [v for v in expected
               if str(v).strip().casefold() and str(v).strip().casefold() not in hay]
    return missing, False


def regular_history(executions, expected, field=None, limit=HISTORY):
    """How many readable runs contained each blessed value. -> (counts, readable)

    A run is "readable" only if we can see its items at all, and only if the
    named field is present in them. Pruned history is not evidence of absence,
    so it is not counted either way.
    """
    counts = dict((v, 0) for v in expected)
    readable = 0
    for e in executions[:limit]:
        items = terminal_items(e)
        if items is None or not items:
            continue
        if field is not None and not any(field in item for item in items):
            continue
        readable += 1
        hay = "\n".join(_searchable(item, field) for item in items).casefold()
        for v in expected:
            needle = str(v).strip().casefold()
            if needle and needle in hay:
                counts[v] += 1
    return counts, readable


def value_seen_within(executions, value, days, now, field=None, limit=HISTORY):
    """Did this value appear in any readable run inside the window?

    Returns (seen, covered). `covered` is False when the readable history does
    not reach back to the start of the window, and then `seen` carries no
    information at all: not finding a monthly line in thirty hours of runs says
    nothing about the month. Callers must not alert on an uncovered window.
    """
    needle = str(value).strip().casefold()
    if not needle or not days:
        return True, False

    since = now - timedelta(days=float(days))
    seen = False
    oldest = None

    for e in executions[:limit]:
        items = terminal_items(e)
        if items is None or not items:
            continue
        if field is not None and not any(field in item for item in items):
            continue
        when = parse_time(e.get("stoppedAt") or e.get("startedAt"))
        if when is None:
            continue
        if oldest is None or when < oldest:
            oldest = when
        if when < since:
            continue
        hay = "\n".join(_searchable(item, field) for item in items).casefold()
        if needle in hay:
            seen = True

    covered = oldest is not None and oldest <= since
    return seen, covered


def nodes_that_ran(execution):
    """Names of the nodes present in this execution's run data.

    Measured on n8n 2.36.7, and it contradicts the obvious guess: a node on a
    branch whose condition no longer matches is NOT recorded with
    executionStatus="skipped". It is absent from runData entirely, while the
    execution as a whole still reports status="success". Checking
    executionStatus therefore finds nothing at all on the exact failure it
    would be written for.
    """
    run_data = (((execution or {}).get("data") or {})
                .get("resultData") or {}).get("runData")
    if not isinstance(run_data, dict):
        return set()
    return set(run_data)


def nodes_that_stopped_running(executions, declared=None, threshold=0.6):
    """Nodes that used to run on most runs and did not run on the latest one.

    Absence on its own is not a fault -- the untaken side of an IF is absent on
    every healthy run, and alerting on that would fire constantly. What matters
    is a node that *used* to run and has quietly stopped: the condition that
    used to match no longer does, and the work silently is not happening while
    the run still reports success.

    ``declared`` is the set of node names the workflow currently contains. Pass
    it and a node the client deliberately deleted or renamed stops being a
    finding, because it is no longer declared -- that is an edit, not a
    failure. Without it, every legitimate change to a workflow produces false
    positives until the history ages out, which is the failure mode that makes
    a monitor get muted. Correctly identified as this approach's weak point by
    an n8n operator on 2026-08-30.

    Needs MIN_HISTORY prior runs to have an opinion. Returns [] otherwise.
    """
    if len(executions) < MIN_HISTORY + 1:
        return []
    latest = nodes_that_ran(executions[0])
    if not latest:
        return []
    history = [nodes_that_ran(e) for e in executions[1:]]
    history = [h for h in history if h]
    if len(history) < MIN_HISTORY:
        return []
    usual = []
    for node in set().union(*history):
        if declared is not None and node not in declared:
            continue  # deleted or renamed: an edit, not a silent failure
        seen = sum(1 for h in history if node in h)
        if seen / len(history) >= threshold and node not in latest:
            usual.append((node, seen, len(history)))
    return sorted(usual)


def _has_value(value):
    """Whether a field carries anything. "" counts as empty, deliberately.

    A field that normally holds content and now holds an empty string is the
    reported failure, not a healthy variation: the r/n8n case below handed the
    next node exactly that. Zero and False are values, not emptiness.
    """
    return value is not None and value != "" and value != [] and value != {}


def workflow_version_of(execution):
    """The workflow version this run executed, or None if not recorded.

    A per-node baseline only means anything while the workflow is unchanged.
    Edit a node and "what it normally emits" is a claim about a workflow that no
    longer exists. n8n stamps a versionId on the workflow and carries it on the
    execution's embedded workflowData, so a version change is the honest place
    to throw the baseline away.

    The cost is real and worth stating rather than hiding: a workflow edited
    every few days never accumulates enough same-version history to have an
    opinion about its own output. That is correct -- a baseline built across an
    edit is worse than no baseline -- but it does mean actively maintained
    workflows are covered by the check-in and step checks rather than this one.
    """
    if not isinstance(execution, dict):
        return None
    direct = execution.get("workflowVersionId")
    if direct:
        return direct
    data = execution.get("workflowData")
    if isinstance(data, dict):
        return data.get("versionId")
    return None


def node_output_shapes(execution):
    """Per node: how many items it emitted, and which keys carried a value.

    Two numbers rather than one, because they fail differently.

    The case this exists for was described by an n8n operator on 2026-09-01: an
    embedding call whose quota had run out returned HTTP 200 with an empty body.
    The node executed, so it landed in runData with a success status, handed an
    empty string to the next node, and the run finished clean. Retrieval was
    dead for days and every execution reported success.

    Absence detection cannot see that, because the node is present. The
    workflow-level item count cannot see it either, because the workflow still
    produced an answer -- just one built on nothing retrieved. Only the node's
    own output shape shows it.
    """
    run_data = (((execution or {}).get("data") or {})
                .get("resultData") or {}).get("runData")
    if not isinstance(run_data, dict):
        return {}
    shapes = {}
    for node, runs in run_data.items():
        if not isinstance(runs, list):
            continue
        items = 0
        keys = set()
        seen_any = False
        for run in runs:
            main = ((run or {}).get("data") or {}).get("main")
            if not isinstance(main, list):
                continue
            seen_any = True
            for branch in main:
                if not isinstance(branch, list):
                    continue
                items += len(branch)
                for item in branch:
                    payload = (item or {}).get("json")
                    if not isinstance(payload, dict):
                        continue
                    for key, value in payload.items():
                        if _has_value(value):
                            keys.add(key)
        if seen_any:
            shapes[node] = (items, frozenset(keys))
    return shapes


def _type_name(value):
    """A coarse shape name for a value, or None when it carries nothing.

    Deliberately coarse. 1 and 1.0 are the same shape for this purpose, and
    distinguishing them would alarm on every numeric field that happened to
    round. Booleans are checked before numbers because bool is a subclass of
    int in Python, so isinstance(True, int) is True and the obvious ordering
    silently reports every boolean as a number.
    """
    if not _has_value(value):
        return None
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "text"
    if isinstance(value, list):
        return "list"
    if isinstance(value, dict):
        return "object"
    return "other"


def node_key_types(execution):
    """Per node, the shape of each key that carried a value: {node: {key: type}}.

    Reported by an n8n operator on 2026-09-03, describing the case the presence
    check misses: "It may be blank or it could be in the wrong data type like an
    array when it should be a string, so nothing errors."

    A blank field is caught by node_output_shapes, because the key comes back
    empty. An array where a string belongs is not: the key is present, it holds
    a value, and every presence test passes. Only the type shows it.

    Where a key appears more than once with conflicting types in the same run,
    the first wins. Mixed types within one run are their own smell, but calling
    that a fault would fire on any node that legitimately emits heterogeneous
    rows.
    """
    run_data = (((execution or {}).get("data") or {})
                .get("resultData") or {}).get("runData")
    if not isinstance(run_data, dict):
        return {}
    types = {}
    for node, runs in run_data.items():
        if not isinstance(runs, list):
            continue
        seen = {}
        for run in runs:
            main = ((run or {}).get("data") or {}).get("main")
            if not isinstance(main, list):
                continue
            for branch in main:
                if not isinstance(branch, list):
                    continue
                for item in branch:
                    payload = (item or {}).get("json")
                    if not isinstance(payload, dict):
                        continue
                    for key, value in payload.items():
                        name = _type_name(value)
                        if name and key not in seen:
                            seen[key] = name
        if seen:
            types[node] = seen
    return types


def nodes_emitting_nothing(executions, declared=None, threshold=0.6):
    """Nodes that ran, reported success, and stopped carrying what they carry.

    Returns (node, kind, evidence) triples. Two kinds, deliberately unequal:

      "keys_lost"  fields the node normally populates came back empty. The
                   stronger signal, because a shape change is hard to explain
                   away as a quiet day.
      "type_changed"  a field the node normally fills with one shape came back
                   as another - a list where every prior run held text. Strong,
                   for the same reason: the shape changed, not the volume. Only
                   counted where that key held one settled type historically.
      "no_items"   the node emitted nothing at all -- and only when it has
                   emitted something on every prior run. Weaker on its own: a
                   search step returning zero results some days is correct, not
                   broken, so a bare zero is treated as evidence only where zero
                   has never happened before.

    Only runs of the same workflow version are compared, so an edit resets the
    baseline rather than alerting on itself. `declared` drops nodes the client
    has since removed, for the same reason it does in nodes_that_stopped_running.
    """
    if len(executions) < MIN_HISTORY + 1:
        return []
    latest = executions[0]

    history = executions[1:]
    version = workflow_version_of(latest)
    if version is not None:
        history = [e for e in history if workflow_version_of(e) == version]
    if len(history) < MIN_HISTORY:
        return []

    latest_shapes = node_output_shapes(latest)
    if not latest_shapes:
        return []
    latest_types = node_key_types(latest)
    history_pairs = [(sh, node_key_types(e))
                     for sh, e in ((node_output_shapes(e), e) for e in history)
                     if sh]
    if len(history_pairs) < MIN_HISTORY:
        return []
    history_shapes = [sh for sh, _ in history_pairs]

    findings = []
    for node, (items, keys) in sorted(latest_shapes.items()):
        if declared is not None and node not in declared:
            continue
        samples = [s[node] for s in history_shapes if node in s]
        if len(samples) < MIN_HISTORY:
            continue
        if len(samples) / float(len(history_shapes)) < threshold:
            continue  # not a node that reliably runs; absence is its normal

        every_key = set()
        for _, sample_keys in samples:
            every_key |= set(sample_keys)
        usual = {k for k in every_key
                 if sum(1 for _, ks in samples if k in ks) / float(len(samples))
                 >= threshold}
        # Only meaningful while the node still emitted something. A node that
        # emitted nothing has trivially lost every key, which is the same single
        # failure counted twice -- and it would defeat the guard below that lets
        # a legitimately-empty node stay quiet.
        lost = sorted(usual - set(keys)) if items > 0 else []
        if lost:
            findings.append((node, "keys_lost", lost))

        # Only keys the node still populates AND normally populates. Disjoint
        # from `lost` by construction, so one failure cannot report twice - the
        # mistake the zero-items case made before the tests caught it.
        node_types = latest_types.get(node) or {}
        type_samples = [t[node] for _, t in history_pairs if node in t]
        changed = []
        for key in sorted(set(keys) & usual):
            seen = [ts[key] for ts in type_samples if ts.get(key)]
            if len(seen) < MIN_HISTORY:
                continue
            settled = seen[0]
            if any(t != settled for t in seen):
                continue  # this key legitimately varies; it promises nothing
            # Unanimity, deliberately stricter than the threshold used for
            # presence. A key that has ever legitimately held another type is
            # not making a promise, and a false alarm costs more trust than a
            # missed alert. Same reasoning as no_items requiring every prior
            # run to be non-empty.
            now = node_types.get(key)
            if now and now != settled:
                changed.append((key, settled, now))
        if changed:
            findings.append((node, "type_changed", changed))

        counts = [c for c, _ in samples]
        if items == 0 and all(c > 0 for c in counts):
            findings.append((node, "no_items", median(counts)))
    return findings


def weekday_of(execution):
    """Which day of the week this run belongs to, or None if unreadable."""
    when = parse_time((execution or {}).get("stoppedAt")
                      or (execution or {}).get("startedAt"))
    return when.weekday() if when else None


def baseline_for(latest, history):
    """Return (expected_count, description) for comparing `latest` against.

    A baseline is an expectation with a calendar attached. A flat median across
    recent runs quietly assumes every day is the same shape, so a client whose
    Monday is legitimately ten times its Tuesday either alerts every Monday or,
    worse, hides a collapsed Monday behind the week's average. Where there are
    enough samples of the same weekday, compare like with like.

    Falls back to the flat median when the calendar view is too thin -- which
    is most of the first month of any workflow's life.
    """
    day = weekday_of(latest)
    if day is not None:
        same_day = [c for e in history
                    for c in [item_count(e)]
                    if c is not None and weekday_of(e) == day]
        if len(same_day) >= WEEKDAY_MIN_HISTORY:
            return median(same_day), "%s median of %d" % (DAY_NAMES[day], len(same_day))

    every_day = [c for c in (item_count(e) for e in history) if c is not None]
    if len(every_day) >= MIN_HISTORY:
        return median(every_day), "median of the last %d runs" % len(every_day)
    return None, None


def schedule_minutes(workflow):
    """How often a workflow's schedule says it should run, in minutes.

    Returns None when it cannot be read with confidence: a cron expression, an
    unfamiliar field, or no schedule trigger at all. None is the honest answer
    and it has a consequence worth stating -- the check-in test simply has no
    opinion about that workflow. Guessing an interval would produce a lateness
    alert on something that was never late, and one false alarm costs more trust
    than one missed alert.

    Where a workflow has several rules, the shortest wins: something due every
    hour and every day is late once the hour passes.
    """
    best = None
    for node in (workflow or {}).get("nodes") or []:
        if node.get("type") not in SCHEDULE_TRIGGERS:
            continue
        rule = (node.get("parameters") or {}).get("rule") or {}
        for entry in rule.get("interval") or []:
            field = (entry or {}).get("field")
            if field not in MINUTES_PER:
                continue  # cronExpression and seconds are not worth guessing at
            try:
                count = float(entry.get("%sInterval" % field) or 1)
            except (TypeError, ValueError):
                continue
            minutes = MINUTES_PER[field] * count
            if minutes > 0 and (best is None or minutes < best):
                best = minutes
    return best


def _downstream_of(workflow, types):
    """Names of every node reachable from any node of the given types.

    Follows main connections forward. Used to answer "is this Code node
    reading an item that an HTTP call has already replaced", which is only a
    defect when the HTTP node is upstream.
    """
    nodes = (workflow or {}).get("nodes") or []
    edges = {}
    for source, outputs in ((workflow or {}).get("connections") or {}).items():
        targets = edges.setdefault(source, set())
        for branch in (outputs or {}).get("main") or []:
            for link in branch or []:
                if isinstance(link, dict) and link.get("node"):
                    targets.add(link["node"])

    frontier = [n.get("name") for n in nodes if n.get("type") in types]
    seen = set()
    while frontier:
        current = frontier.pop()
        for target in edges.get(current, ()):
            if target not in seen:
                seen.add(target)
                frontier.append(target)
    return seen


def _code_of(node):
    params = node.get("parameters") or {}
    parts = [params.get(key) for key in ("jsCode", "pythonCode", "code")]
    return "\n".join(p for p in parts if isinstance(p, str))


def static_findings(workflow):
    """Defects readable in the workflow itself, with no execution history.

    Every other check here compares a run against that workflow's own past,
    which makes them change detectors: a workflow that has been wrong since its
    first execution has no healthy baseline to differ from and is invisible to
    them by construction. These three read the node graph instead, so they see
    a defect that has never once produced a bad-looking run.

    All three are failure modes enzosoftware measured as his own misses on
    community.n8n.io/t/308708, handed over with an invitation to test them.
    """
    findings = []
    name = (workflow or {}).get("name") or str((workflow or {}).get("id") or "")
    after_http = _downstream_of(workflow, HTTP_TYPES)

    for node in (workflow or {}).get("nodes") or []:
        node_name = node.get("name") or "?"
        params = node.get("parameters") or {}

        if node.get("type") in CODE_TYPES:
            code = _code_of(node)

            # After an HTTP node the item IS the API response, so anything
            # reaching back for the original record gets fields that are not
            # there. The row still gets written and the count is still right.
            if node_name in after_http and "$input.item" in code:
                findings.append({
                    "workflow": name,
                    "kind": "input_item_after_http",
                    "detail": "%s reads $input.item downstream of an HTTP call, "
                              "where the item is the API response rather than "
                              "the original record" % node_name,
                })

            # One try/catch decides whether a bad model reply is an error or a
            # silently malformed row.
            if "JSON.parse(" in code and "try" not in code:
                findings.append({
                    "workflow": name,
                    "kind": "unguarded_json_parse",
                    "detail": "%s calls JSON.parse with no try/catch, so a reply "
                              "that is not JSON becomes a failed run or a bad row"
                              % node_name,
                })

        # Raw JSON put in the wrong field is sent as nothing at all: the call
        # succeeds, the remote ignores it, and the run is green.
        if node.get("type") in HTTP_TYPES and params.get("sendBody"):
            if params.get("specifyBody") == "json" and not params.get("jsonBody"):
                findings.append({
                    "workflow": name,
                    "kind": "body_in_wrong_field",
                    "detail": "%s is set to send raw JSON but jsonBody is empty; "
                              "a payload in any other field is not sent"
                              % node_name,
                })

    return findings


def spec_for(workflow):
    """A watch spec for a workflow nobody has configured by hand.

    Every check on, because the point of scan mode is to answer "do I have this
    problem" before anyone has decided which workflows they care about. Checks
    that need history stay silent until they have it, so turning them all on
    costs nothing on a fresh instance.
    """
    spec = {"id": str(workflow.get("id")),
            "name": workflow.get("name") or str(workflow.get("id")),
            "watch_output": True,
            "watch_steps": True,
            "watch_node_output": True}
    # The node list as it stands today. Saving the file blesses it, and the
    # monthly report then names anything added or removed since - which is how
    # a client renaming a field without telling anyone becomes one line in
    # a report instead of a surprise.
    names = sorted(n.get("name") for n in workflow.get("nodes") or [] if n.get("name"))
    if names:
        spec["nodes"] = names
    every = schedule_minutes(workflow)
    if every:
        spec["every_minutes"] = every
    return spec


def propose_expectation(runs):
    """What history suggests this workflow normally produces, or None.

    The rolling median has a known failure: an outage longer than half the
    window becomes the baseline, and the recovery then pages as the anomaly.
    Named by an n8n operator on 2026-08-27 -- "a rolling learner will happily
    learn a two-week outage as the new normal" -- and the answer another gave is
    better than anything unsupervised: history PROPOSES an expectation, a human
    BLESSES it, and a blessed expectation never drifts without another bless.

    This is the proposing half. --scan writes it into the emitted watch.json as
    `expected_items`; whoever saves that file is doing the blessing. Returns a
    whole number, since nobody expects 4.5 rows.
    """
    # Every run counts, newest included. baseline_for deliberately leaves the
    # newest out because it is the run under comparison; here nothing is being
    # compared, so leaving it out would just discard the freshest sample. And a
    # flat median rather than the weekday view: a blessed number is one number.
    counts = [c for c in (item_count(e) for e in runs) if c is not None]
    if len(counts) < MIN_HISTORY:
        return None
    proposed = median(counts)
    if proposed <= 0:
        return None
    return int(round(proposed))


def summarise(spec, successes, failures, declared, since, limit=REPORT_LIMIT):
    """One workflow's month, in the client's units.

    Reported by an agency owner on r/n8n (2026-08-28): the first client he
    bundled monitoring into cancelled in month two, "because from where he sat
    he was paying for nothing to happen." What fixed it was one line a month
    with the numbers in it - "412 orders processed and 3 that needed a human,
    plus one line about anything that changed on their side." Uptime percentages
    got zero reaction. Monitoring that works is indistinguishable from nothing
    happening, so this is what makes it visible.

    Counts are in the workflow's own units - the items its terminal node
    emitted - not in ours. "Needed a human" is executions that ended in error.
    "Changed" is the declared node set against the one blessed in watch.json,
    which is how a client renaming a field without telling anyone shows up.
    """
    ok = [e for e in successes
          if (parse_time(e.get("stoppedAt") or e.get("startedAt")) or since) >= since]
    bad = [e for e in failures
           if (parse_time(e.get("stoppedAt") or e.get("startedAt")) or since) >= since]

    counts = [item_count(e) for e in ok]
    known = [c for c in counts if c is not None]

    truncated_at = None
    if len(successes) >= limit and successes:
        oldest = parse_time(successes[-1].get("stoppedAt")
                            or successes[-1].get("startedAt"))
        if oldest is not None and oldest > since:
            truncated_at = oldest

    changed = None
    blessed = spec.get("nodes")
    if isinstance(blessed, list) and declared is not None:
        before, after = set(blessed), set(declared)
        changed = {"added": sorted(after - before),
                   "removed": sorted(before - after)}

    return {
        "workflow": spec.get("name") or spec.get("id"),
        "runs_ok": len(ok),
        "runs_failed": len(bad),
        "items": sum(known),
        "items_known_runs": len(known),
        "truncated_at": truncated_at,
        "changed": changed,
    }


def format_summary(rows, days):
    """The report itself. One line per workflow, no percentages.

    412 orders and 3 that needed a human is a sentence a client can act on.
    99.8% is not, because nobody knows what the missing 0.2 cost them.
    """
    lines = ["Last %d days, in your units:" % days]
    for r in rows:
        bits = ["%d runs" % r["runs_ok"]]
        if r["items_known_runs"]:
            item_bit = "{:,} items".format(r["items"])
            if r["items_known_runs"] < r["runs_ok"]:
                item_bit += " (from %d of %d runs; n8n had pruned the rest)" % (
                    r["items_known_runs"], r["runs_ok"])
            bits.append(item_bit)
        bits.append("%d needed a human" % r["runs_failed"])
        line = "  - %s: %s." % (r["workflow"], ", ".join(bits))
        ch = r.get("changed")
        if ch and (ch["added"] or ch["removed"]):
            parts = []
            if ch["added"]:
                parts.append("added " + ", ".join("'%s'" % n for n in ch["added"]))
            if ch["removed"]:
                parts.append("removed " + ", ".join("'%s'" % n for n in ch["removed"]))
            line += " Changed since you last blessed it: %s." % "; ".join(parts)
        if r.get("truncated_at"):
            line += (" (Last %d runs only, back to %s - the window is longer than "
                     "that; raise REPORT_LIMIT for all of it.)"
                     % (REPORT_LIMIT, r["truncated_at"].date().isoformat()))
        lines.append(line)
    return "\n".join(lines)


def human(delta):
    total = int(delta.total_seconds())
    if total < 3600:
        return "%dm" % (total // 60)
    if total < 86400:
        return "%dh %dm" % (total // 3600, (total % 3600) // 60)
    return "%dd %dh" % (total // 86400, (total % 86400) // 3600)


def check_workflow(spec, executions, now, declared=None, workflow=None):
    """Alerts for one workflow. `executions` is newest-first, successful only.

    Empty list means it has never succeeded -- which is its own alarm, not a
    staleness one. Staleness needs a baseline to compare against, and "never
    ran" has no baseline: a schedule trigger that was never going to fire looks
    identical to a healthy workflow that simply has not been due yet.

    `workflow` is the definition as the API returns it. Pass it and the static
    pass runs too, which is the only way to see a defect that has been there
    since the first execution: every other check here needs a healthy past to
    compare against, and that kind of fault never had one.
    """
    name = spec.get("name") or spec.get("id")
    alerts = []

    # Runs first and unconditionally. A workflow that has never executed can
    # still be visibly broken, and that is precisely the case history cannot
    # reach.
    for finding in (static_findings(workflow) if workflow else []):
        finding["workflow"] = name  # the spec's name is what every alert uses
        alerts.append(finding)

    if not executions:
        return alerts + [{
            "workflow": name,
            "kind": "never_ran",
            "detail": "no successful execution on record -- if this was just "
                      "published, check the trigger actually fires on its own",
        }]

    latest = executions[0]

    # --- 1. has it checked in? ---
    stopped = parse_time(latest.get("stoppedAt") or latest.get("startedAt"))
    every = spec.get("every_minutes")
    if stopped is not None and every:
        if now > stopped + timedelta(minutes=every * GRACE):
            alerts.append({
                "workflow": name,
                "kind": "no_check_in",
                "detail": "last succeeded %s ago; expected every %s minutes"
                          % (human(now - stopped), every),
            })

    # --- 2. did it produce what it usually produces? ---
    if spec.get("watch_output") or spec.get("min_items") is not None:
        latest_count = item_count(latest)
        if latest_count is not None:
            floor = spec.get("min_items")
            if floor is not None and latest_count < floor:
                alerts.append({
                    "workflow": name,
                    "kind": "below_floor",
                    "detail": "produced %d item(s), floor is %d"
                              % (latest_count, floor),
                })
            elif spec.get("watch_output"):
                blessed = spec.get("expected_items")
                if isinstance(blessed, (int, float)) and blessed > 0:
                    # A number a person set on purpose. It does not learn, so
                    # an outage cannot become normal and a recovery cannot
                    # page. It also does not follow a legitimate change until
                    # someone changes it -- that is the trade, and it is the
                    # right one for anything that pays the bills.
                    baseline, described = float(blessed), "the expected count you set"
                else:
                    baseline, described = baseline_for(latest, executions[1:])
                if baseline:
                    # Inclusive: a run at exactly the threshold is the case
                    # this exists for, not a near miss.
                    if latest_count <= baseline * (1 - DEVIATION):
                        alerts.append({
                            "workflow": name,
                            "kind": "output_deviation",
                            "detail": "produced %d item(s); %s is %g "
                                      "(%.0f%% of normal)"
                                      % (latest_count, described, baseline,
                                         100.0 * latest_count / baseline),
                        })
                elif latest_count == 0:
                    # No baseline yet, but zero is worth saying out loud.
                    alerts.append({
                        "workflow": name,
                        "kind": "empty_output",
                        "detail": "produced 0 items and there is not enough "
                                  "history yet to know what normal looks like",
                    })

    # --- 2b. are the rows that must always be there, actually there? ---
    regulars = spec.get("expect_present")
    if isinstance(regulars, list) and regulars:
        missing, field_gone = missing_regulars(
            terminal_items(latest), regulars, spec.get("expect_present_field"))
        if field_gone:
            alerts.append({
                "workflow": name,
                "kind": "expected_field_absent",
                "detail": "no item has a '%s' field, so the regulars you listed "
                          "cannot be checked" % spec.get("expect_present_field"),
            })
        elif missing:
            # A value missing from THIS run and from every run before it was
            # almost certainly renamed, not lost. Saying "missing" every day
            # about a thing that is never coming back is how a monitor teaches
            # you to ignore it. Raised by u/No-Hold-6217, who muted his own.
            counts, readable = regular_history(
                executions[1:], missing, spec.get("expect_present_field"))

            if readable >= MIN_HISTORY:
                stale = [m for m in missing if counts[m] == 0]
                rare = [m for m in missing
                        if 0 < counts[m] * 2 < readable]
                gone = [m for m in missing if m not in stale and m not in rare]
            else:
                # Not enough history to claim anything about why. Report the
                # plain fact and stay quiet about the cause.
                stale, rare, gone = [], [], missing

            if stale:
                alerts.append({
                    "workflow": name,
                    "kind": "regulars_stale",
                    "detail": "%s never appeared in any of the last %d runs, so "
                              "it was probably renamed or retired. Update the "
                              "list - this will not fix itself and will repeat "
                              "every run until you do"
                              % (", ".join("'%s'" % m for m in stale), readable),
                })
            if rare:
                alerts.append({
                    "workflow": name,
                    "kind": "regulars_irregular",
                    "detail": "%s appears in only some runs (%s of the last %d), "
                              "so it is not something every run produces and "
                              "probably does not belong in expect_present"
                              % (", ".join("'%s'" % m for m in rare),
                                 ", ".join(str(counts[m]) for m in rare),
                                 readable),
                })
            if gone:
                seen = ""
                if readable >= MIN_HISTORY:
                    seen = " (present in %s of the last %d)" % (
                        ", ".join(str(counts[m]) for m in gone), readable)
                alerts.append({
                    "workflow": name,
                    "kind": "regulars_missing",
                    "detail": "expected in every run but absent from this one: "
                              "%s%s" % (", ".join("'%s'" % m for m in gone), seen),
                })

    # --- 2c. things that need not be in every run, but must turn up ---
    # His second list. A monthly line in an hourly pull belongs here, not above.
    windows = spec.get("expect_within_days")
    if isinstance(windows, dict) and windows:
        field = spec.get("expect_present_field")
        overdue, blind = [], []
        for value, days in sorted(windows.items()):
            seen, covered = value_seen_within(executions, value, days, now, field)
            if seen:
                continue
            if covered:
                overdue.append((value, days))
            else:
                blind.append((value, days))
        if overdue:
            alerts.append({
                "workflow": name,
                "kind": "window_value_overdue",
                "detail": "not seen inside its window: %s"
                          % ", ".join("'%s' (%g days)" % (v, float(d))
                                      for v, d in overdue),
            })
        if blind:
            # Saying nothing here would be the failure this tool exists to
            # catch: a check that quietly is not running.
            alerts.append({
                "workflow": name,
                "kind": "window_not_covered",
                "detail": "cannot check %s - the last %d runs do not reach back "
                          "that far. Shorten the window or raise HISTORY"
                          % (", ".join("'%s' (%g days)" % (v, float(d))
                                       for v, d in blind), HISTORY),
            })

    # --- 3. did a step that normally runs quietly stop running? ---
    if spec.get("watch_steps"):
        for node, seen, total in nodes_that_stopped_running(executions, declared):
            alerts.append({
                "workflow": name,
                "kind": "step_stopped",
                "detail": "step '%s' ran on %d of the last %d runs but not on "
                          "the latest one; the run still reports success"
                          % (node, seen, total),
            })

    # --- 4. did a step run, report success, and carry nothing? ---
    if spec.get("watch_node_output"):
        for node, kind, evidence in nodes_emitting_nothing(executions, declared):
            if kind == "keys_lost":
                alerts.append({
                    "workflow": name,
                    "kind": "node_keys_lost",
                    "detail": "step '%s' ran and reported success but returned "
                              "nothing under %s, which it normally populates"
                              % (node, ", ".join("'%s'" % k for k in evidence)),
                })
            elif kind == "type_changed":
                alerts.append({
                    "workflow": name,
                    "kind": "node_type_changed",
                    "detail": "step '%s' ran and reported success but returned "
                              "%s" % (node, "; ".join(
                                  "'%s' as %s where it is normally %s"
                                  % (k, now, was) for k, was, now in evidence)),
                })
            else:
                alerts.append({
                    "workflow": name,
                    "kind": "node_no_items",
                    "detail": "step '%s' ran and reported success but emitted "
                              "0 items, having emitted items on every prior "
                              "run (median %g)" % (node, evidence),
                })

    return alerts


def format_report(alerts):
    if not alerts:
        return "All watched workflows checked in."
    lines = ["%d workflow(s) need attention:" % len(alerts)]
    for a in alerts:
        lines.append("  - %s: %s" % (a["workflow"], a["detail"]))
    return "\n".join(lines)


# --- the network edge ------------------------------------------------------

def get_json(url, api_key, timeout=20):
    request = urllib.request.Request(url, headers={
        "X-N8N-API-KEY": api_key,
        "Accept": "application/json",
    })
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def recent_executions(base, api_key, workflow_id, status="success",
                      limit=HISTORY, include_data=True):
    """Most recent executions of one status for one workflow, newest first.

    Failures here are reported, never swallowed: a monitor that goes quiet when
    it breaks is the exact thing this exists to prevent.
    """
    url = ("%s/api/v1/executions?status=%s&includeData=%s&limit=%d&workflowId=%s"
           % (base.rstrip("/"), status, "true" if include_data else "false",
              limit, workflow_id))
    payload = get_json(url, api_key)
    return (payload or {}).get("data") or []


def recent_successful_executions(base, api_key, workflow_id, limit=HISTORY):
    """Kept so every existing caller stays untouched."""
    return recent_executions(base, api_key, workflow_id, "success", limit, True)


def active_workflows(base, api_key, limit=250):
    """Every active workflow, with its nodes, in one call.

    The nodes come back on this payload, so scan mode gets the declared-node set
    for free and does not need a second request per workflow.
    """
    url = "%s/api/v1/workflows?active=true&limit=%d" % (base.rstrip("/"), limit)
    return (get_json(url, api_key) or {}).get("data") or []


def declared_nodes(base, api_key, workflow_id):
    """The node names the workflow currently contains.

    Read fresh each run rather than cached, so an edit is reflected on the very
    next check instead of after a stale baseline has already fired.
    """
    url = "%s/api/v1/workflows/%s" % (base.rstrip("/"), workflow_id)
    payload = get_json(url, api_key)
    return {n.get("name") for n in (payload or {}).get("nodes") or [] if n.get("name")}


def post_slack(webhook, text):
    body = json.dumps({"text": text}).encode("utf-8")
    request = urllib.request.Request(
        webhook, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=20) as response:
        return response.status


def load_env_file(path=".env"):
    """Read KEY=value lines into the environment if the file exists.

    Cron runs with almost no environment, so relying on an exported variable is
    how a monitor silently stops monitoring. Existing environment variables win,
    so an explicit export still overrides the file.
    """
    try:
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    except OSError:
        pass  # no file is a normal, supported case


def hit_rate_warning(alerts, scanned, floor=SCAN_HIT_FLOOR,
                     threshold=SCAN_HIT_RATE):
    """Say so when the scan flags so much of an instance that it suspects itself.

    Raised by enzosoftware on community.n8n.io, who pointed a static scanner at
    3,138 published templates and had 58 of the first 83 come back positive.
    Every one was a fault in the check, not in the workflow. His conclusion is
    the useful part: a hit rate is itself a diagnostic, and a check firing on
    most of a population you believe is mostly healthy is telling you about
    itself.

    Counts WORKFLOWS, not alerts. One badly broken workflow can raise five
    findings, and calling that a five-workflow hit rate would fire this warning
    on exactly the instance where the tool is working correctly.

    Deliberately not a verdict. A single instance really can be mostly broken,
    which is not true of a corpus of other people's templates, so this names the
    two ways to tell the cases apart and leaves the judgement with the operator.
    Returns None when there is nothing worth saying.
    """
    if scanned < floor:
        return None
    flagged = len({a.get("workflow") for a in alerts if a.get("workflow")})
    if flagged < threshold * scanned:
        return None
    return (
        "%d of the %d workflows scanned came back with a finding (%d%%).\n"
        "That is a high enough share to suspect this check before believing it.\n"
        "Two ways to tell which: confirm one finding by hand against that\n"
        "workflow's execution list, and include a workflow you know is healthy\n"
        "in the next scan. If the healthy one is flagged too, the fault is here."
        % (flagged, scanned, round(100.0 * flagged / scanned)))


def scan(base, api_key, now, limit=SCAN_LIMIT):
    """Check active workflows with sensible defaults and no config file.

    The config file is the barrier to finding out whether you have this problem
    at all: you cannot write one without already knowing your workflow IDs, and
    nobody looks those up on the strength of a stranger's claim. Scan mode
    answers the question in one command, then prints a config for anyone who
    decides they want to keep watching.

    Bounded on purpose. `limit` caps how many workflows get the expensive
    run-data fetch, and anything skipped is named rather than quietly dropped --
    a truncated scan that reads as "you are fine" is the same lie this tool
    exists to catch.
    """
    workflows = active_workflows(base, api_key)
    if not workflows:
        print("No active workflows found. Nothing to check.")
        return [], []

    skipped = workflows[limit:]
    workflows = workflows[:limit]
    print("Checking %d active workflow(s), %d recent runs each. "
          "This reads execution data, so give it a moment."
          % (len(workflows), SCAN_HISTORY))

    alerts, specs = [], []
    for workflow in workflows:
        spec = spec_for(workflow)
        specs.append(spec)
        declared = {n.get("name") for n in workflow.get("nodes") or []
                    if n.get("name")}
        try:
            runs = recent_successful_executions(base, api_key, spec["id"],
                                                limit=SCAN_HISTORY)
        except (urllib.error.URLError, OSError, ValueError) as exc:
            alerts.append({"workflow": spec["name"], "kind": "unreachable",
                           "detail": "could not query n8n: %s" % exc})
            # The history call failed, but the definition is already in hand and
            # the static pass needs nothing else. Report what can still be read.
            alerts.extend(static_findings(workflow))
            continue
        alerts.extend(check_workflow(spec, runs, now, declared,
                                     workflow=workflow))
        proposed = propose_expectation(runs)
        if proposed is not None:
            spec["expected_items"] = proposed

    print("")
    print(format_report(alerts))
    suspect = hit_rate_warning(alerts, len(workflows))
    if suspect:
        print("\n" + suspect)
    if skipped:
        print("\nNot checked (%d over the limit of %d): %s"
              % (len(skipped), limit,
                 ", ".join(w.get("name") or str(w.get("id")) for w in skipped[:10])
                 + (", ..." if len(skipped) > 10 else "")))
        print("Run `python monitor.py --scan %d` to include them."
              % (len(skipped) + limit))
    return alerts, specs


def report(base, api_key, config, now, days=REPORT_DAYS):
    """Build the month's one-liners for every watched workflow."""
    since = now - timedelta(days=days)
    rows = []
    for spec in config.get("workflows", []):
        try:
            ok = recent_executions(base, api_key, spec["id"], "success",
                                   REPORT_LIMIT, True)
            bad = recent_executions(base, api_key, spec["id"], "error",
                                    REPORT_LIMIT, False)
        except (urllib.error.URLError, OSError, ValueError) as exc:
            rows.append({"workflow": spec.get("name") or spec.get("id"),
                         "runs_ok": 0, "runs_failed": 0, "items": 0,
                         "items_known_runs": 0, "truncated_at": None,
                         "changed": None,
                         "error": "could not query n8n: %s" % exc})
            continue
        declared = None
        if isinstance(spec.get("nodes"), list):
            try:
                declared = declared_nodes(base, api_key, spec["id"])
            except (urllib.error.URLError, OSError, ValueError):
                declared = None
        rows.append(summarise(spec, ok, bad, declared, since))
    return rows


def report_own_liveness(url=None, opener=urllib.request.urlopen):
    """Tell an outside service this check ran, so its silence is detectable.

    Raised by DuskWatch on community.n8n.io as the state underneath the three
    this tool already distinguishes: the check did not execute at all. A check
    that stops produces nothing, and nothing is exactly what all-clear looks
    like. Their words, and they are right: a muted alarm at least still emits a
    line somebody chose to ignore.

    This does NOT implement a dead-man's switch. Healthchecks.io and Cronitor
    already do that well, which is said elsewhere in this file, so the correct
    move is to be watched by one rather than to build another. Set HEARTBEAT_URL
    to a ping URL from whichever you use.

    Called only after a run completes. A crashed run must not ping, because the
    whole value is that silence means something.

    A failed ping is printed, never raised, and never changes the exit code: an
    alert about your workflows must not be lost because a third-party ping
    endpoint was briefly down. But it is printed rather than swallowed, because
    a heartbeat failing quietly is the same disease one layer further out.
    """
    url = url if url is not None else os.environ.get("HEARTBEAT_URL", "").strip()
    if not url:
        return False
    try:
        opener(url, timeout=10).close()
        return True
    except (urllib.error.URLError, OSError, ValueError) as exc:
        print("heartbeat ping failed (%s): this run completed, but nothing "
              "outside it knows that" % exc, file=sys.stderr)
        return False


def main(argv):
    if len(argv) < 2:
        print(__doc__)
        return 2

    scanning = argv[1] in ("--scan", "-s")
    reporting = argv[1] in ("--report", "-r")
    if reporting and len(argv) < 3:
        print("usage: python monitor.py --report watch.json [days]", file=sys.stderr)
        return 2
    config_path = argv[2] if reporting else argv[1]
    load_env_file(os.path.join(
        os.path.dirname(os.path.abspath(argv[0] if scanning else config_path)), ".env"))
    base = os.environ.get("N8N_URL", "").strip()
    api_key = os.environ.get("N8N_API_KEY", "").strip()
    if not base or not api_key:
        print("N8N_URL and N8N_API_KEY must both be set", file=sys.stderr)
        return 2

    now = datetime.now(timezone.utc)

    if scanning:
        try:
            limit = int(argv[2]) if len(argv) > 2 else SCAN_LIMIT
        except ValueError:
            print("usage: python monitor.py --scan [how-many-workflows]",
                  file=sys.stderr)
            return 2
        try:
            alerts, specs = scan(base, api_key, now, limit)
        except (urllib.error.URLError, OSError, ValueError) as exc:
            print("could not reach n8n: %s" % exc, file=sys.stderr)
            return 2
        if specs:
            print("\nTo keep watching these, save the following as watch.json "
                  "and run: python monitor.py watch.json")
            if any("expected_items" in sp for sp in specs):
                print("expected_items is what recent history suggests each "
                      "workflow normally produces. Check the numbers before you "
                      "save the file: once set they do not drift, which is the "
                      "point, so a wrong one stays wrong until you change it.")
            print(json.dumps({"workflows": specs}, indent=2))
        report_own_liveness()
        return 1 if alerts else 0

    config = json.loads(open(config_path, encoding="utf-8").read())

    if reporting:
        try:
            days = int(argv[3]) if len(argv) > 3 else REPORT_DAYS
        except ValueError:
            print("usage: python monitor.py --report watch.json [days]",
                  file=sys.stderr)
            return 2
        rows = report(base, api_key, config, now, days)
        print(format_summary(rows, days))
        for r in rows:
            if r.get("error"):
                print("  ! %s: %s" % (r["workflow"], r["error"]), file=sys.stderr)
        report_own_liveness()
        return 1 if any(r.get("error") for r in rows) else 0

    alerts = []
    for spec in config.get("workflows", []):
        try:
            runs = recent_successful_executions(base, api_key, spec["id"])
        except (urllib.error.URLError, OSError, ValueError) as exc:
            # Reaching n8n at all is itself a signal. If the instance is down,
            # that is the most important thing to say.
            alerts.append({
                "workflow": spec.get("name") or spec.get("id"),
                "kind": "unreachable",
                "detail": "could not query n8n: %s" % exc,
            })
            continue
        declared = None
        if spec.get("watch_steps"):
            try:
                declared = declared_nodes(base, api_key, spec["id"])
            except (urllib.error.URLError, OSError, ValueError):
                # Unknown is safer than stale: without the declared set the
                # step check simply has no opinion this run.
                declared = None
        # No workflow= here, deliberately. Watch is the recurring mode, and a
        # static defect does not change between runs: it would fire every few
        # minutes, for ever, saying the same thing, until somebody muted the
        # channel and stopped reading the alerts that do change. The same data
        # is a finding in `scan` -- asked for once, by a person, read once --
        # and a flood here. Whether it is noise depends on whether somebody
        # asked, which is the whole difference between an audit and a monitor.
        alerts.extend(check_workflow(spec, runs, now, declared))

    report = format_report(alerts)
    print(report)

    webhook = config.get("slack_webhook")
    if alerts and webhook:
        try:
            post_slack(webhook, report)
        except (urllib.error.URLError, OSError) as exc:
            print("could not post to Slack: %s" % exc, file=sys.stderr)
            return 1

    report_own_liveness()
    return 1 if alerts else 0


# --- what to build next, and why -------------------------------------------
#
# Everything above can, in principle, be approximated by a generic monitoring
# service. The following cannot, because they require reading inside an n8n
# execution, and they are the reason this exists rather than a Healthchecks.io
# subscription. Each needs verifying against a real instance before it ships --
# the JSON shapes are not guessable and guessing produces false alarms, which
# is the one failure this tool cannot afford.
#
#   - Output that is present, plausible and wrong. node_output_shapes reads
#     shape, never meaning: a node returning last week's data in the right
#     shape passes every check here. Needs semantic assertions per workflow,
#     which is a different product.
#   - A per-node baseline for workflows edited often. Version-scoped history
#     means a weekly-edited workflow never accumulates one. A diff of what
#     actually changed between versions would let unaffected nodes keep theirs.
#   - Credential expiry that fails before the request leaves ("Unable to sign
#     without access token"), so there is no HTTP status for error handling
#     built around status codes to catch.
#   - Queue-mode executions that hang forever, ignoring both the per-workflow
#     Timeout and EXECUTIONS_TIMEOUT (n8n-io/n8n#36343, still open). They
#     neither complete nor error, so they are invisible to both error handling
#     and to any check that only asks "did it run".
#   - Schedule triggers that never fire at all -- e.g. a weeks-interval trigger
#     missing weeksInterval in the workflow JSON. Reported measured on 2.31.5;
#     the "never_ran" alert above is the outside-in half of catching it.
#   - A node left "disabled": true, which passes activation validation and
#     passes its input straight through, so output is wrong and nothing flags.


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
