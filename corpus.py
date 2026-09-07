"""A labelled corpus: what this tool catches, and what it does not.

Every number a checker prints is a floor until someone measures it against
cases where the answer is already known. Raised by enzosoftware on
community.n8n.io, who had 23 workflows known broken and 23 known fixed, found
precision perfect and recall 52%, and had never measured it in eight rounds of
tuning.

THE RULE THAT MAKES THIS WORTH RUNNING: every broken case below is a failure
mode named by an operator in a public thread, not one taken from this tool's
feature list. A corpus built from your own detectors measures nothing -- it
tells you that you detect what you set out to detect. Each case carries the
person and thread it came from, so the list can be argued with.

Run:  python corpus.py

It prints precision, recall, and names every miss. A miss here is not a bug to
be silenced; several are honest limits of reading execution history from
outside, and those are worth stating plainly rather than hiding.
"""
from datetime import datetime, timedelta, timezone

import monitor

NOW = datetime(2026, 9, 7, 12, 0, 0, tzinfo=timezone.utc)


def run(minutes_ago, items=None, nodes=None, fields=None):
    """One successful execution as n8n returns it.

    items=None means no run data at all, which is different from zero items:
    n8n prunes run data, and unknown must never be read as empty.
    """
    ex = {"id": "1", "status": "success",
          "stoppedAt": (NOW - timedelta(minutes=minutes_ago)).isoformat()}
    if items is None and nodes is None and fields is None:
        return ex
    if nodes is None:
        payload = [{"json": f} for f in (fields or [{}] * items)]
        nodes = {
            "Trigger": [{"executionStatus": "success",
                         "data": {"main": [[{"json": {}}]]}}],
            "Last Node": [{"executionStatus": "success",
                           "data": {"main": [payload]}}],
        }
    ex["data"] = {"resultData": {"lastNodeExecuted": "Last Node",
                                 "runData": nodes}}
    return ex


def hourly(counts, start=30):
    return [run(start + i * 60, items=c) for i, c in enumerate(counts)]


def daily(counts, start=60):
    return [run(start + i * 1440, items=c) for i, c in enumerate(counts)]


def statement(minutes_ago, *descriptions):
    return run(minutes_ago,
               fields=[{"description": d} for d in descriptions])


HOURLY = {"name": "pull", "every_minutes": 60, "watch_output": True}

CASES = [
    # ---- broken: the failure modes operators actually named ---------------
    dict(label="broken", name="branch stops matching, node vanishes from runData",
         source="us, verified on 2.36.7 (307194)",
         spec={"name": "pull", "watch_output": True, "watch_steps": True},
         runs=[run(30, nodes={
             "Trigger": [{"executionStatus": "success",
                          "data": {"main": [[{"json": {}}]]}}],
             "Last Node": [{"executionStatus": "success",
                            "data": {"main": [[{"json": {}}]]}}]})]
              + [run(30 + i * 60, nodes={
                  "Trigger": [{"executionStatus": "success",
                               "data": {"main": [[{"json": {}}]]}}],
                  "Enrich": [{"executionStatus": "success",
                              "data": {"main": [[{"json": {}}]]}}],
                  "Last Node": [{"executionStatus": "success",
                                 "data": {"main": [[{"json": {}}]]}}]})
                  for i in range(1, 8)]),

    dict(label="broken", name="node returns 200 with an empty body",
         source="pirateprentice (307194) and the selfhost-ai embedding case",
         spec=HOURLY, runs=hourly([0] + [12] * 8)),

    dict(label="broken", name="schedule trigger stopped firing entirely",
         source="KSD, ASHIM_DOLEY, rodri.galloswork (307194, 308708)",
         spec={"name": "pull", "every_minutes": 60},
         runs=[run(600), run(660), run(720)]),

    dict(label="broken", name="workflow active but never ran once",
         source="jayson-odoo sorento issue 77, scheduler dead after crash loop",
         spec={"name": "pull", "every_minutes": 60}, runs=[]),

    dict(label="broken", name="gradual deviation, 60 percent of normal volume",
         source="themineworks (307194): the day that got past the count check",
         spec=HOURLY, runs=hourly([6] + [15] * 8)),

    dict(label="broken", name="crashed runs, so no new successful execution",
         source="anon45958619 (307194): Error Trigger never fires on a crash",
         spec={"name": "pull", "every_minutes": 60},
         runs=[run(400, items=10), run(460, items=10)]),

    dict(label="broken", name="right row count, the rent line is missing",
         source="No-Hold-6217 (r/n8n): the 60 percent day looked fine on count",
         spec={"name": "bank", "every_minutes": 60,
               "expect_present_field": "description",
               "expect_present": ["Rent", "AWS"]},
         runs=[statement(30, "AWS", "Payroll")]
              + [statement(30 + i * 60, "AWS", "Rent") for i in range(1, 8)]),

    dict(label="broken", name="monthly line absent from a long window",
         source="No-Hold-6217 (r/n8n): rent monthly, insurance quarterly",
         spec={"name": "bank", "every_minutes": 1440,
               "expect_present_field": "description",
               "expect_within_days": {"Rent": 35}},
         runs=[statement(60 + i * 1440, "AWS") for i in range(0, 40)]),

    dict(label="broken", name="named value renamed upstream, list now stale",
         source="No-Hold-6217 (r/n8n): the check cried until he stopped reading",
         spec={"name": "bank", "every_minutes": 60,
               "expect_present_field": "description",
               "expect_present": ["Rnt"]},
         runs=[statement(30 + i * 60, "AWS", "Rent") for i in range(0, 8)]),

    dict(label="broken", name="webhook stopped receiving, no cadence declared",
         source="KSD (307194): webhook stops receiving events",
         spec={"name": "intake", "watch_output": True},
         runs=[run(4000, items=9), run(4100, items=9)]),

    dict(label="broken", name="downstream never received the write",
         source="RomeoApps (308708) receipt health; KSD downstream check",
         spec=HOURLY, runs=hourly([12] * 9)),

    # ---- defect classes enzosoftware measured as HIS misses ---------------
    # Handed over on 308708 with an explicit invitation to test them. He found
    # these by shipping five products broken and keeping the archives, which is
    # a corpus nobody would design on purpose. All three are STATIC defects in
    # the workflow, and every check here reads execution history instead, so
    # they test the seam between the two approaches rather than our detectors.

    dict(label="broken", name="payload in bodyParameters, API wanted raw body",
         source="enzosoftware (308708): six of his eleven misses, one class",
         # Broken since the first run: there is no healthy baseline to deviate
         # from, which is the shape a static defect always takes in production.
         spec=HOURLY, runs=hourly([0] * 9)),

    dict(label="broken", name="$input.item.json read after an HTTP call",
         source="enzosoftware (308708): four misses; the row written holds the "
                "API response, not the original record",
         # The count is right and stays right. Only the contents are wrong, and
         # they have been wrong since day one.
         spec=HOURLY,
         runs=[run(30 + i * 60, fields=[{"status": "ok"}] * 12)
               for i in range(0, 9)]),

    dict(label="broken", name="fragile JSON parse of a model reply",
         source="enzosoftware (308708): his one miss he believes undetectable",
         # Intermittent: most runs fine, this one wrote a short batch.
         spec=HOURLY, runs=hourly([11, 12, 12, 11, 12, 12, 13, 12])),

    # ---- healthy: these must stay silent ----------------------------------
    dict(label="healthy", name="ordinary run, stable volume",
         spec=HOURLY, runs=hourly([12, 11, 12, 13, 12, 12, 11, 12])),

    dict(label="healthy", name="a branch that is untaken on every run",
         source="the untaken side of an IF is absent on healthy runs too",
         spec={"name": "pull", "watch_output": True, "watch_steps": True},
         runs=[run(30 + i * 60, nodes={
             "Trigger": [{"executionStatus": "success",
                          "data": {"main": [[{"json": {}}]]}}],
             "Last Node": [{"executionStatus": "success",
                            "data": {"main": [[{"json": {}}]]}}]})
             for i in range(0, 8)]),

    dict(label="healthy", name="brand new workflow, one run of history",
         spec=HOURLY, runs=hourly([12])),

    dict(label="healthy", name="run data pruned, counts unknown",
         source="EXECUTIONS_DATA_MAX_AGE: unknown must not read as empty",
         spec=HOURLY, runs=[run(30), run(90), run(150), run(210)]),

    dict(label="healthy", name="monthly line present inside its window",
         spec={"name": "bank", "every_minutes": 1440,
               "expect_present_field": "description",
               "expect_within_days": {"Rent": 35}},
         runs=[statement(60 + i * 1440, "AWS") for i in range(0, 12)]
              + [statement(60 + 12 * 1440, "AWS", "Rent")]
              + [statement(60 + i * 1440, "AWS") for i in range(13, 40)]),
]


def main():
    tp = fp = fn = tn = 0
    misses, false_alarms = [], []
    for case in CASES:
        alerts = monitor.check_workflow(case["spec"], case["runs"], NOW)
        fired = bool(alerts)
        broken = case["label"] == "broken"
        if broken and fired:
            tp += 1
        elif broken and not fired:
            fn += 1
            misses.append(case)
        elif not broken and fired:
            fp += 1
            false_alarms.append((case, alerts))
        else:
            tn += 1

    broken_n = tp + fn
    healthy_n = tn + fp
    print("Corpus: %d cases, %d broken, %d healthy."
          % (len(CASES), broken_n, healthy_n))
    print("Every broken case is a failure mode named by an operator in public.")
    print("")
    print("Recall     %d/%d  (%d%%)  - of the real failures, how many we catch"
          % (tp, broken_n, round(100.0 * tp / broken_n)))
    print("Precision  %d/%d  (%d%%)  - of our alerts, how many were real"
          % (tp, tp + fp, round(100.0 * tp / (tp + fp))) if tp + fp else
          "Precision  n/a")
    print("Silence    %d/%d  (%d%%)  - healthy cases we correctly said nothing about"
          % (tn, healthy_n, round(100.0 * tn / healthy_n)))

    if misses:
        print("\nMISSED (%d). Stated plainly rather than hidden:" % len(misses))
        for c in misses:
            print("  - %s" % c["name"])
            print("      raised by: %s" % c.get("source", "unattributed"))
    if false_alarms:
        print("\nFALSE ALARMS (%d). Each one is a check that would get muted:"
              % len(false_alarms))
        for c, alerts in false_alarms:
            print("  - %s -> %s"
                  % (c["name"], ", ".join(a["kind"] for a in alerts)))
    print("\nRecall is a floor, not an estimate. It measures this corpus only,"
          "\nand the corpus is only as good as the failure modes people have"
          "\ntold us about.")


if __name__ == "__main__":
    main()
