"""Tests for the alert logic. No network: every case is a fixture.

The rule these all serve: alert on real trouble, never on missing information.
A monitor that cries wolf gets muted, and a muted monitor is worse than none.
"""
from datetime import datetime, timedelta, timezone

import monitor

NOW = datetime(2026, 8, 26, 12, 0, 0, tzinfo=timezone.utc)


def execution(minutes_ago, items=None):
    """A successful execution that stopped `minutes_ago`.

    items=None means n8n returned no run data at all (it prunes it, and only
    includes it when asked) -- deliberately different from items=0.
    """
    ex = {
        "id": "1",
        "status": "success",
        "stoppedAt": (NOW - timedelta(minutes=minutes_ago)).isoformat(),
    }
    if items is not None:
        # Shape verified against a live n8n 2.36.7: a trigger node emits its own
        # item, so a fixture with only the terminal node would have hidden the
        # bug where item_count summed every node.
        ex["data"] = {"resultData": {
            "lastNodeExecuted": "Last Node",
            "runData": {
                "Trigger": [{"executionStatus": "success",
                             "data": {"main": [[{"json": {}}]]}}],
                "Last Node": [{"executionStatus": "success",
                               "data": {"main": [[{"json": {}} for _ in range(items)]]}}],
            }}}
    return ex


def history(counts, spacing=1440):
    """Executions newest-first, one per day, with the given item counts."""
    return [execution((i + 1) * spacing - spacing + 30, items=c)
            for i, c in enumerate(counts)]


# --- never ran -------------------------------------------------------------

def test_a_workflow_that_has_never_succeeded_gets_its_own_alarm():
    """Not a staleness alert. Staleness needs a baseline, and "never ran" has
    none -- a schedule trigger that was never going to fire looks exactly like
    a healthy workflow that isn't due yet."""
    alerts = monitor.check_workflow({"name": "nightly", "every_minutes": 1440}, [], NOW)
    assert [a["kind"] for a in alerts] == ["never_ran"]
    assert "trigger actually fires" in alerts[0]["detail"]


# --- check-in --------------------------------------------------------------

def test_a_workflow_that_just_ran_is_quiet():
    spec = {"name": "invoice-sync", "every_minutes": 15}
    assert monitor.check_workflow(spec, [execution(3)], NOW) == []


def test_a_slightly_late_workflow_does_not_page_anyone():
    """A 15-minute job seen 20 minutes ago is late, not dead. GRACE=1.5 means
    nothing fires until 22.5 minutes."""
    spec = {"name": "invoice-sync", "every_minutes": 15}
    assert monitor.check_workflow(spec, [execution(20)], NOW) == []


def test_a_workflow_that_stopped_checking_in_alerts():
    spec = {"name": "lead-router", "every_minutes": 5}
    alerts = monitor.check_workflow(spec, [execution(41)], NOW)
    assert [a["kind"] for a in alerts] == ["no_check_in"]
    assert "41m" in alerts[0]["detail"]


def test_a_workflow_with_no_cadence_is_not_checked_for_lateness():
    """Event-driven workflows have no schedule; watching them for lateness
    would alert forever."""
    assert monitor.check_workflow({"name": "webhook-intake"}, [execution(9999)], NOW) == []


# --- output deviation: the case a zero-check misses -------------------------

def test_a_run_at_sixty_percent_of_normal_alerts():
    """The failure that matters. It passes every emptiness assertion you can
    write, which is exactly why a fixed floor is the wrong tool."""
    runs = history([60, 100, 100, 100, 100])
    spec = {"name": "nightly-export", "watch_output": True}
    alerts = monitor.check_workflow(spec, runs, NOW)
    assert [a["kind"] for a in alerts] == ["output_deviation"]
    assert "60% of normal" in alerts[0]["detail"]


def test_a_run_at_full_volume_is_quiet():
    runs = history([98, 100, 102, 99, 101])
    assert monitor.check_workflow({"name": "x", "watch_output": True}, runs, NOW) == []


def test_normal_fluctuation_does_not_alert():
    """80% of normal is a quiet day, not an incident."""
    runs = history([80, 100, 100, 100, 100])
    assert monitor.check_workflow({"name": "x", "watch_output": True}, runs, NOW) == []


def test_the_baseline_is_a_median_so_one_freak_run_does_not_move_it():
    """A single 1000-item day must not raise the bar and make every normal day
    look like a failure afterwards. That is why it is a median, not a mean."""
    runs = history([100, 100, 1000, 100, 100])
    assert monitor.check_workflow({"name": "x", "watch_output": True}, runs, NOW) == []


def test_too_little_history_means_no_deviation_alert():
    """Alerting off a baseline of two samples is how a monitor gets muted."""
    runs = history([10, 100])
    assert monitor.check_workflow({"name": "x", "watch_output": True}, runs, NOW) == []


def test_zero_output_still_speaks_up_without_a_baseline():
    """No history yet, but zero is worth saying out loud regardless."""
    runs = history([0, 100])
    alerts = monitor.check_workflow({"name": "x", "watch_output": True}, runs, NOW)
    assert [a["kind"] for a in alerts] == ["empty_output"]


def test_missing_run_data_is_never_treated_as_zero():
    """n8n prunes execution data on a retention schedule. Absent data must read
    as 'unknown', not 'produced nothing' -- otherwise every workflow alerts as
    soon as its history ages out."""
    runs = [execution(30, items=None)] + history([100, 100, 100, 100])
    assert monitor.check_workflow({"name": "x", "watch_output": True}, runs, NOW) == []


def test_output_is_not_checked_unless_asked():
    runs = history([0, 100, 100, 100, 100])
    assert monitor.check_workflow({"name": "x"}, runs, NOW) == []


# --- explicit floor, for when you actually know the number ------------------

def test_a_hard_floor_alerts_when_you_know_the_expected_volume():
    runs = history([7, 100, 100, 100])
    spec = {"name": "billing", "min_items": 50}
    alerts = monitor.check_workflow(spec, runs, NOW)
    assert [a["kind"] for a in alerts] == ["below_floor"]


def test_the_floor_takes_precedence_over_deviation():
    """Two alerts for one problem is noise. The floor is the more specific
    statement, so it wins."""
    runs = history([7, 100, 100, 100])
    spec = {"name": "billing", "min_items": 50, "watch_output": True}
    assert [a["kind"] for a in monitor.check_workflow(spec, runs, NOW)] == ["below_floor"]


def test_both_a_stale_check_in_and_bad_output_can_fire_together():
    runs = [execution(90, items=0)] + history([100, 100, 100, 100])
    spec = {"name": "lead-router", "every_minutes": 5, "watch_output": True}
    kinds = [a["kind"] for a in monitor.check_workflow(spec, runs, NOW)]
    assert kinds == ["no_check_in", "output_deviation"]


# --- parsing ---------------------------------------------------------------

def test_median_of_an_even_number_of_runs():
    assert monitor.median([1, 2, 3, 4]) == 2.5
    assert monitor.median([3, 1]) == 2.0


def test_median_of_nothing_is_unknown():
    assert monitor.median([]) is None


def test_item_count_sums_the_branches_of_the_terminal_node():
    ex = {"data": {"resultData": {"lastNodeExecuted": "Split", "runData": {
        "Split": [{"data": {"main": [
            [{"json": {}}, {"json": {}}],
            [{"json": {}}],
        ]}}]
    }}}}
    assert monitor.item_count(ex) == 3


def test_item_count_ignores_upstream_nodes():
    """The bug a live instance caught: summing every node counted the Schedule
    Trigger's own item, so a workflow emitting 5 rows reported 6."""
    ex = {"data": {"resultData": {"lastNodeExecuted": "Emit", "runData": {
        "Every minute": [{"data": {"main": [[{"json": {}}]]}}],
        "Emit": [{"data": {"main": [[{"json": {}} for _ in range(5)]]}}],
    }}}}
    assert monitor.item_count(ex) == 5


def test_item_count_is_unknown_when_the_terminal_node_cannot_be_identified():
    ex = {"data": {"resultData": {"runData": {"A": [{"data": {"main": [[{"json": {}}]]}}]}}}}
    assert monitor.item_count(ex) is None


def run_with_nodes(*names):
    return {"data": {"resultData": {"runData": {n: [{"data": {"main": [[]]}}] for n in names}}}}


def test_nodes_that_ran_reads_the_rundata_keys():
    assert monitor.nodes_that_ran(run_with_nodes("A", "B")) == {"A", "B"}
    assert monitor.nodes_that_ran({}) == set()


def test_a_step_that_used_to_run_and_stopped_is_caught():
    """Measured on n8n 2.36.7: a node on a branch that no longer matches is
    ABSENT from runData -- it is not marked executionStatus="skipped". The run
    still reports success. Absence against a baseline is the only signal."""
    runs = [run_with_nodes("Trigger", "Check")] + [
        run_with_nodes("Trigger", "Check", "Send invoice") for _ in range(4)]
    stopped = monitor.nodes_that_stopped_running(runs)
    assert [n for n, _, _ in stopped] == ["Send invoice"]


def test_a_branch_that_never_runs_is_not_an_alert():
    """The untaken side of an IF is absent on every healthy run. Alerting on
    that would fire constantly and get the tool muted."""
    runs = [run_with_nodes("Trigger", "Check") for _ in range(5)]
    assert monitor.nodes_that_stopped_running(runs) == []


def test_no_opinion_on_steps_without_enough_history():
    runs = [run_with_nodes("Trigger"), run_with_nodes("Trigger", "Send")]
    assert monitor.nodes_that_stopped_running(runs) == []


def test_step_watching_is_off_unless_asked():
    runs = [run_with_nodes("Trigger", "Check")] + [
        run_with_nodes("Trigger", "Check", "Send") for _ in range(4)]
    assert monitor.check_workflow({"name": "x"}, runs, NOW) == []


def test_step_watching_reports_the_stopped_step():
    runs = [run_with_nodes("Trigger", "Check")] + [
        run_with_nodes("Trigger", "Check", "Send") for _ in range(4)]
    alerts = monitor.check_workflow({"name": "x", "watch_steps": True}, runs, NOW)
    assert [a["kind"] for a in alerts] == ["step_stopped"]
    assert "Send" in alerts[0]["detail"]


def test_item_count_of_an_execution_with_no_data_is_unknown():
    assert monitor.item_count({"id": "1"}) is None
    assert monitor.item_count({}) is None
    assert monitor.item_count(None) is None


def test_trailing_z_timestamps_parse():
    assert monitor.parse_time("2026-08-26T12:00:00.000Z") == NOW


def test_an_unparseable_timestamp_does_not_crash_the_run():
    assert monitor.parse_time("not a date") is None
    assert monitor.parse_time(None) is None


def test_a_workflow_whose_timestamp_is_junk_is_skipped_not_crashed():
    spec = {"name": "odd", "every_minutes": 5}
    assert monitor.check_workflow(spec, [{"stoppedAt": "???"}], NOW) == []


# --- reporting -------------------------------------------------------------

def test_the_all_clear_reads_as_an_all_clear():
    assert monitor.format_report([]) == "All watched workflows checked in."


def test_the_report_names_every_workflow_in_trouble():
    report = monitor.format_report([
        {"workflow": "lead-router", "kind": "no_check_in", "detail": "41m ago"},
        {"workflow": "nightly-export", "kind": "output_deviation", "detail": "60% of normal"},
    ])
    assert "2 workflow(s)" in report
    assert "lead-router" in report
    assert "nightly-export" in report


def test_human_readable_durations():
    assert monitor.human(timedelta(minutes=41)) == "41m"
    assert monitor.human(timedelta(hours=3, minutes=5)) == "3h 5m"
    assert monitor.human(timedelta(days=2, hours=4)) == "2d 4h"


# --- calendar baselines ----------------------------------------------------
#
# "A baseline is an expectation with a calendar attached... Averages hide
# exactly the days you care about." -- AleksGorbatov, n8n forum, 2026-08-25.
# A flat median assumes every day is the same shape. These two tests are the
# pair that matters: each one has the flat baseline giving the wrong answer.

def at(days_ago, items):
    """An execution that stopped `days_ago` days before NOW."""
    ex = {"id": str(days_ago), "status": "success",
          "stoppedAt": (NOW - timedelta(days=days_ago)).isoformat()}
    ex["data"] = {"resultData": {
        "lastNodeExecuted": "Last Node",
        "runData": {"Last Node": [
            {"executionStatus": "success",
             "data": {"main": [[{"json": {}} for _ in range(items)]]}}]}}}
    return ex


def test_a_collapsed_spike_day_is_caught_even_though_it_beats_the_weekly_median():
    """The client whose Monday is legitimately 10x its Tuesday. A Monday at 400
    is well above the week's median of 100, so a flat baseline sees nothing --
    but it is 40% of what this weekday normally produces."""
    latest = at(0, 400)
    hist = [at(7, 1000), at(14, 1000), at(21, 1000)] + [at(d, 100) for d in range(1, 7)]
    runs = [latest] + hist

    flat = monitor.median([monitor.item_count(e) for e in hist])
    assert flat == 100, "the flat baseline would see 400 as four times normal"

    alerts = monitor.check_workflow({"name": "x", "watch_output": True}, runs, NOW)
    assert [a["kind"] for a in alerts] == ["output_deviation"]
    assert "40% of normal" in alerts[0]["detail"]


def test_a_normal_quiet_day_does_not_alert_just_because_other_days_are_busy():
    """The inverse, and the one that would get the tool muted: a perfectly
    normal quiet day looks like a 90% collapse against a week dominated by
    busy days."""
    latest = at(0, 100)
    hist = [at(7, 100), at(14, 100), at(21, 100)] + [at(d, 1000) for d in range(1, 7)]
    runs = [latest] + hist

    flat = monitor.median([monitor.item_count(e) for e in hist])
    assert flat == 1000, "the flat baseline would call this a 90% drop"

    assert monitor.check_workflow({"name": "x", "watch_output": True}, runs, NOW) == []


def test_the_alert_says_which_baseline_it_used():
    latest = at(0, 10)
    runs = [latest] + [at(7, 100), at(14, 100), at(21, 100)]
    alerts = monitor.check_workflow({"name": "x", "watch_output": True}, runs, NOW)
    detail = alerts[0]["detail"]
    assert "median of 3" in detail
    assert any(day in detail for day in monitor.DAY_NAMES), detail


def test_it_falls_back_to_the_flat_median_without_enough_same_day_history():
    """Most of a workflow's first month has one sample per weekday. Falling
    back is correct; refusing to have an opinion would be worse."""
    latest = at(0, 10)
    runs = [latest] + [at(d, 100) for d in range(1, 8)]
    _, described = monitor.baseline_for(latest, runs[1:])
    assert "median of the last" in described
    assert [a["kind"] for a in monitor.check_workflow(
        {"name": "x", "watch_output": True}, runs, NOW)] == ["output_deviation"]


def test_two_samples_of_a_weekday_are_not_enough_to_trust_it():
    latest = at(0, 500)
    runs = [latest] + [at(7, 1000), at(14, 1000)] + [at(d, 100) for d in range(1, 7)]
    _, described = monitor.baseline_for(latest, runs[1:])
    assert "median of the last" in described, "two Mondays is not a baseline"


def test_weekday_of_tolerates_junk():
    assert monitor.weekday_of({"stoppedAt": "???"}) is None
    assert monitor.weekday_of({}) is None
    assert monitor.weekday_of(None) is None


# --- baseline drift ---------------------------------------------------------
#
# "someone legitimately edits a workflow and your expected node set goes stale,
# suddenly you're swimming in false positives until you re-baseline every
# changed workflow" -- n8n operator, 2026-08-30. That is exactly right about
# the version without `declared`, and it is the failure that gets a monitor
# muted. A node the client deleted is an edit, not a silent failure.

def test_a_deleted_node_is_an_edit_not_a_failure():
    runs = [run_with_nodes("Trigger", "Check")] + [
        run_with_nodes("Trigger", "Check", "Send invoice") for _ in range(4)]

    # without the declared set, the removal looks exactly like a silent failure
    assert [n for n, _, _ in monitor.nodes_that_stopped_running(runs)] == ["Send invoice"]

    # with it, the node is simply gone from the workflow and nothing fires
    declared = {"Trigger", "Check"}
    assert monitor.nodes_that_stopped_running(runs, declared) == []


def test_a_renamed_node_does_not_alert_under_its_old_name():
    """A rename is a delete plus an add. The old name stops appearing, but it
    is no longer declared, so it is not a finding. The new name has no history
    yet, so it is not one either - the baseline just rebuilds quietly."""
    runs = [run_with_nodes("Trigger", "Send invoices")] + [
        run_with_nodes("Trigger", "Send invoice") for _ in range(4)]
    declared = {"Trigger", "Send invoices"}
    assert monitor.nodes_that_stopped_running(runs, declared) == []


def test_a_node_that_is_still_declared_but_stopped_running_still_alerts():
    """The fix must not swallow the real case: the node is still in the
    workflow, it just stopped being reached."""
    runs = [run_with_nodes("Trigger", "Check")] + [
        run_with_nodes("Trigger", "Check", "Send invoice") for _ in range(4)]
    declared = {"Trigger", "Check", "Send invoice"}
    assert [n for n, _, _ in monitor.nodes_that_stopped_running(runs, declared)] == ["Send invoice"]


def test_check_workflow_passes_the_declared_set_through():
    runs = [run_with_nodes("Trigger", "Check")] + [
        run_with_nodes("Trigger", "Check", "Send") for _ in range(4)]
    spec = {"name": "x", "watch_steps": True}
    assert monitor.check_workflow(spec, runs, NOW, {"Trigger", "Check"}) == []
    assert [a["kind"] for a in monitor.check_workflow(
        spec, runs, NOW, {"Trigger", "Check", "Send"})] == ["step_stopped"]


def test_no_declared_set_keeps_the_old_behaviour():
    """If the workflow could not be read this run, the check still works -
    unknown must not mean silent."""
    runs = [run_with_nodes("Trigger", "Check")] + [
        run_with_nodes("Trigger", "Check", "Send") for _ in range(4)]
    assert [a["kind"] for a in monitor.check_workflow(
        {"name": "x", "watch_steps": True}, runs, NOW, None)] == ["step_stopped"]


# --- per-node output shape -------------------------------------------------
#
# The failure these serve, reported by an n8n operator on 2026-09-01: a node
# ran, returned HTTP 200 with an empty body, went green, and handed an empty
# string downstream. Present in runData, successful execution, produced nothing.


def shape_run(nodes, version="v1", minutes_ago=0):
    """An execution whose runData carries the given per-node item payloads.

    `nodes` maps node name to a list of json dicts, so a node emitting nothing
    is [] and a node emitting two rows is [{...}, {...}].
    """
    run_data = {}
    for name, items in nodes.items():
        run_data[name] = [{"data": {"main": [[{"json": i} for i in items]]}}]
    return {
        "id": "1",
        "status": "success",
        "stoppedAt": (NOW - timedelta(minutes=minutes_ago)).isoformat(),
        "workflowData": {"versionId": version},
        "data": {"resultData": {
            "lastNodeExecuted": list(nodes)[-1] if nodes else None,
            "runData": run_data,
        }},
    }


def healthy_history(n=5):
    """Runs where Embed carries real vectors. Counts vary, as real ones do -
    a constant series would hide any dependence on variance."""
    counts = [4, 6, 5, 7, 5, 6, 4]
    return [shape_run({
        "Fetch": [{"id": i} for i in range(counts[k % len(counts)])],
        "Embed": [{"vector": [0.1, 0.2], "text": "chunk %d" % i}
                  for i in range(counts[k % len(counts)])],
    }, minutes_ago=60 * (k + 1)) for k in range(n)]


def test_node_output_shapes_counts_items_and_populated_keys():
    shapes = monitor.node_output_shapes(
        shape_run({"Embed": [{"vector": [0.1], "text": "a"}, {"vector": [0.2], "text": "b"}]}))
    assert shapes["Embed"] == (2, frozenset({"vector", "text"}))


def test_empty_string_is_not_a_value():
    """The reported failure exactly: 200 with an empty body, node goes green."""
    shapes = monitor.node_output_shapes(
        shape_run({"Embed": [{"vector": [], "text": ""}]}))
    assert shapes["Embed"] == (1, frozenset())


def test_zero_and_false_are_values_not_emptiness():
    shapes = monitor.node_output_shapes(
        shape_run({"Count": [{"total": 0, "ok": False}]}))
    assert shapes["Count"] == (1, frozenset({"total", "ok"}))


def test_a_node_that_ran_but_returned_empty_fields_is_reported():
    latest = shape_run({
        "Fetch": [{"id": 1}, {"id": 2}, {"id": 3}],
        "Embed": [{"vector": [], "text": ""}, {"vector": [], "text": ""}],
    })
    found = monitor.nodes_emitting_nothing([latest] + healthy_history())
    assert found == [("Embed", "keys_lost", ["text", "vector"])]


def test_a_node_that_emitted_nothing_is_reported_when_zero_is_unprecedented():
    latest = shape_run({"Fetch": [{"id": 1}], "Embed": []})
    kinds = [k for _, k, _ in monitor.nodes_emitting_nothing(
        [latest] + healthy_history())]
    assert kinds == ["no_items"]


def test_zero_is_not_reported_for_a_node_that_is_sometimes_legitimately_empty():
    """A search step returning no results some days is correct, not broken."""
    history = healthy_history(3) + [shape_run({
        "Fetch": [{"id": 1}], "Embed": []}, minutes_ago=600)]
    latest = shape_run({"Fetch": [{"id": 1}], "Embed": []})
    assert monitor.nodes_emitting_nothing([latest] + history) == []


def test_editing_the_workflow_resets_the_baseline():
    """History from before an edit describes a workflow that no longer exists."""
    latest = shape_run({"Fetch": [{"id": 1}], "Embed": []}, version="v2")
    assert monitor.nodes_emitting_nothing([latest] + healthy_history()) == []


def test_a_removed_node_is_an_edit_not_a_failure():
    latest = shape_run({
        "Fetch": [{"id": 1}, {"id": 2}],
        "Embed": [{"vector": [], "text": ""}],
    })
    runs = [latest] + healthy_history()
    assert monitor.nodes_emitting_nothing(runs, {"Fetch", "Embed"})
    assert monitor.nodes_emitting_nothing(runs, {"Fetch"}) == []


def test_thin_history_says_nothing():
    latest = shape_run({"Fetch": [{"id": 1}], "Embed": []})
    assert monitor.nodes_emitting_nothing([latest] + healthy_history(2)) == []


def test_a_node_that_only_sometimes_runs_is_not_held_to_a_baseline():
    """Absence is normal for it, so its output shape is not a promise."""
    history = healthy_history(5)
    history[0] = shape_run({"Fetch": [{"id": 1}], "Embed": [{"vector": [1]}],
                            "Rare": [{"x": 1}]}, minutes_ago=60)
    latest = shape_run({"Fetch": [{"id": 1}],
                        "Embed": [{"vector": [1], "text": "a"}],
                        "Rare": [{"x": None}]})
    assert [n for n, _, _ in monitor.nodes_emitting_nothing([latest] + history)] == []


def test_check_workflow_reports_node_output_only_when_asked():
    latest = shape_run({
        "Fetch": [{"id": 1}, {"id": 2}],
        "Embed": [{"vector": [], "text": ""}],
    })
    runs = [latest] + healthy_history()
    assert monitor.check_workflow({"name": "x"}, runs, NOW) == []
    kinds = [a["kind"] for a in monitor.check_workflow(
        {"name": "x", "watch_node_output": True}, runs, NOW)]
    assert kinds == ["node_keys_lost"]
