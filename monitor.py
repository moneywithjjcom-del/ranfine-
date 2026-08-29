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
    python monitor.py watch.json

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


def nodes_that_stopped_running(executions, threshold=0.6):
    """Nodes that used to run on most runs and did not run on the latest one.

    Absence on its own is not a fault -- the untaken side of an IF is absent on
    every healthy run, and alerting on that would fire constantly. What matters
    is a node that *used* to run and has quietly stopped: the condition that
    used to match no longer does, and the work silently is not happening while
    the run still reports success.

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
        seen = sum(1 for h in history if node in h)
        if seen / len(history) >= threshold and node not in latest:
            usual.append((node, seen, len(history)))
    return sorted(usual)


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


def human(delta):
    total = int(delta.total_seconds())
    if total < 3600:
        return "%dm" % (total // 60)
    if total < 86400:
        return "%dh %dm" % (total // 3600, (total % 3600) // 60)
    return "%dd %dh" % (total // 86400, (total % 86400) // 3600)


def check_workflow(spec, executions, now):
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
        for node, seen, total in nodes_that_stopped_running(executions):
            alerts.append({
                "workflow": name,
                "kind": "step_stopped",
                "detail": "step '%s' ran on %d of the last %d runs but not on "
                          "the latest one; the run still reports success"
                          % (node, seen, total),
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


def main(argv):
    if len(argv) < 2:
        print(__doc__)
        return 2

    load_env_file(os.path.join(os.path.dirname(os.path.abspath(argv[1])), ".env"))
    config = json.loads(open(argv[1], encoding="utf-8").read())
    base = os.environ.get("N8N_URL", "").strip()
    api_key = os.environ.get("N8N_API_KEY", "").strip()
    if not base or not api_key:
        print("N8N_URL and N8N_API_KEY must both be set", file=sys.stderr)
        return 2

    now = datetime.now(timezone.utc)
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
        alerts.extend(check_workflow(spec, runs, now))

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
#   - Branch marked SKIPPED inside a run marked COMPLETED. The work silently
#     did not happen and the run still reports success.
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
