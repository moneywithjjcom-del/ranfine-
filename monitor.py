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

    python monitor.py --scan      # check every active workflow, no config needed
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

SCHEDULE_TRIGGERS = ("n8n-nodes-base.scheduleTrigger",
                     "n8n-nodes-base.cron",
                     "n8n-nodes-base.interval")

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
    result = ((execution or {}).get("data") or {}).get("resultData") or {}
    run_data = result.get("runData")
    if not isinstance(run_data, dict) or not run_data:
        return None

    last = result.get("lastNodeExecuted")
    runs = run_data.get(last) if last else None
    if not isinstance(runs, list) or not runs:
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


def nodes_emitting_nothing(executions, declared=None, threshold=0.6):
    """Nodes that ran, reported success, and stopped carrying what they carry.

    Returns (node, kind, evidence) triples. Two kinds, deliberately unequal:

      "keys_lost"  fields the node normally populates came back empty. The
                   stronger signal, because a shape change is hard to explain
                   away as a quiet day.
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
    history_shapes = [s for s in (node_output_shapes(e) for e in history) if s]
    if len(history_shapes) < MIN_HISTORY:
        return []

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
    every = schedule_minutes(workflow)
    if every:
        spec["every_minutes"] = every
    return spec


def human(delta):
    total = int(delta.total_seconds())
    if total < 3600:
        return "%dm" % (total // 60)
    if total < 86400:
        return "%dh %dm" % (total // 3600, (total % 3600) // 60)
    return "%dd %dh" % (total // 86400, (total % 86400) // 3600)


def check_workflow(spec, executions, now, declared=None):
    """Alerts for one workflow. `executions` is newest-first, successful only.

    Empty list means it has never succeeded -- which is its own alarm, not a
    staleness one. Staleness needs a baseline to compare against, and "never
    ran" has no baseline: a schedule trigger that was never going to fire looks
    identical to a healthy workflow that simply has not been due yet.
    """
    name = spec.get("name") or spec.get("id")
    alerts = []

    if not executions:
        return [{
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


def recent_successful_executions(base, api_key, workflow_id, limit=HISTORY):
    """Most recent successful executions for one workflow, newest first.

    Failures here are reported, never swallowed: a monitor that goes quiet when
    it breaks is the exact thing this exists to prevent.
    """
    url = ("%s/api/v1/executions?status=success&includeData=true"
           "&limit=%d&workflowId=%s" % (base.rstrip("/"), limit, workflow_id))
    payload = get_json(url, api_key)
    return (payload or {}).get("data") or []


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


def scan(base, api_key, now):
    """Check every active workflow with sensible defaults and no config file.

    The config file is the barrier to finding out whether you have this problem
    at all: you cannot write one without already knowing your workflow IDs, and
    nobody looks those up on the strength of a stranger's claim. Scan mode
    answers the question in one command, then prints a config for anyone who
    decides they want to keep watching.
    """
    workflows = active_workflows(base, api_key)
    if not workflows:
        print("No active workflows found. Nothing to check.")
        return [], []

    alerts, specs = [], []
    for workflow in workflows:
        spec = spec_for(workflow)
        specs.append(spec)
        declared = {n.get("name") for n in workflow.get("nodes") or []
                    if n.get("name")}
        try:
            runs = recent_successful_executions(base, api_key, spec["id"])
        except (urllib.error.URLError, OSError, ValueError) as exc:
            alerts.append({"workflow": spec["name"], "kind": "unreachable",
                           "detail": "could not query n8n: %s" % exc})
            continue
        alerts.extend(check_workflow(spec, runs, now, declared))

    print("Checked %d active workflow(s).\n" % len(workflows))
    print(format_report(alerts))
    return alerts, specs


def main(argv):
    if len(argv) < 2:
        print(__doc__)
        return 2

    scanning = argv[1] in ("--scan", "-s")
    load_env_file(os.path.join(
        os.path.dirname(os.path.abspath(argv[0] if scanning else argv[1])), ".env"))
    base = os.environ.get("N8N_URL", "").strip()
    api_key = os.environ.get("N8N_API_KEY", "").strip()
    if not base or not api_key:
        print("N8N_URL and N8N_API_KEY must both be set", file=sys.stderr)
        return 2

    now = datetime.now(timezone.utc)

    if scanning:
        try:
            alerts, specs = scan(base, api_key, now)
        except (urllib.error.URLError, OSError, ValueError) as exc:
            print("could not reach n8n: %s" % exc, file=sys.stderr)
            return 2
        if specs:
            print("\nTo keep watching these, save the following as watch.json "
                  "and run: python monitor.py watch.json")
            print(json.dumps({"workflows": specs}, indent=2))
        return 1 if alerts else 0

    config = json.loads(open(argv[1], encoding="utf-8").read())
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
