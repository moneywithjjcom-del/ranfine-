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


# --- scan mode -------------------------------------------------------------
#
# The rule: an interval we cannot read confidently must produce None, never a
# guess. A wrong interval means a lateness alert on a workflow that was never
# late, and one false alarm costs more trust than one missed alert.


def workflow(*trigger_params, **kwargs):
    """A workflow whose schedule trigger carries the given interval rules."""
    nodes = [{"name": "Trigger", "type": "n8n-nodes-base.scheduleTrigger",
              "parameters": {"rule": {"interval": list(trigger_params)}}}]
    nodes += [{"name": n, "type": "n8n-nodes-base.set"}
              for n in kwargs.get("also", [])]
    return {"id": kwargs.get("id", "17"), "name": kwargs.get("name", "wf"),
            "nodes": nodes}


def test_reads_a_plain_hourly_trigger():
    assert monitor.schedule_minutes(workflow({"field": "hours"})) == 60


def test_reads_an_explicit_interval_count():
    assert monitor.schedule_minutes(
        workflow({"field": "minutes", "minutesInterval": 5})) == 5
    assert monitor.schedule_minutes(
        workflow({"field": "days", "daysInterval": 2})) == 2880


def test_the_shortest_rule_wins():
    """Due hourly and daily means late once the hour passes."""
    assert monitor.schedule_minutes(
        workflow({"field": "days"}, {"field": "hours"})) == 60


def test_a_cron_expression_is_not_guessed_at():
    assert monitor.schedule_minutes(
        workflow({"field": "cronExpression", "expression": "0 */3 * * *"})) is None


def test_a_workflow_with_no_schedule_has_no_interval():
    assert monitor.schedule_minutes(
        {"nodes": [{"name": "Hook", "type": "n8n-nodes-base.webhook"}]}) is None
    assert monitor.schedule_minutes({}) is None
    assert monitor.schedule_minutes(None) is None


def test_a_nonsense_interval_count_is_ignored_not_crashed():
    assert monitor.schedule_minutes(
        workflow({"field": "hours", "hoursInterval": "soon"})) is None


def test_spec_turns_every_check_on():
    spec = monitor.spec_for(workflow({"field": "hours"}, id=9, name="sync"))
    assert spec["id"] == "9" and spec["name"] == "sync"
    assert spec["every_minutes"] == 60
    assert spec["watch_output"] and spec["watch_steps"] and spec["watch_node_output"]


def test_spec_omits_the_interval_it_could_not_read():
    """No key at all, so check_workflow's lateness test simply never fires."""
    spec = monitor.spec_for(workflow({"field": "cronExpression"}))
    assert "every_minutes" not in spec


# --- wrong type, not missing value -----------------------------------------
#
# Reported by an n8n operator on 2026-09-03: "It may be blank or it could be in
# the wrong data type like an array when it should be a string, so nothing
# errors." A blank field is caught by presence. An array where a string belongs
# is a value, so presence passes and only the type shows it.


def typed_history(n=5, value="chunk"):
    """Runs where Embed emits `text` as a string. Counts vary, as real ones do."""
    counts = [4, 6, 5, 7, 5, 6, 4]
    return [shape_run({
        "Fetch": [{"id": i} for i in range(counts[k % len(counts)])],
        "Embed": [{"text": "%s %d" % (value, i), "score": 0.5}
                  for i in range(counts[k % len(counts)])],
    }, minutes_ago=60 * (k + 1)) for k in range(n)]


def test_type_names_are_coarse_and_booleans_are_not_numbers():
    """bool is a subclass of int, so the obvious ordering reports True as a number."""
    assert monitor._type_name(True) == "boolean"
    assert monitor._type_name(1) == "number"
    assert monitor._type_name(1.5) == "number"
    assert monitor._type_name("a") == "text"
    assert monitor._type_name([1]) == "list"
    assert monitor._type_name({"a": 1}) == "object"


def test_empty_values_have_no_type():
    for empty in (None, "", [], {}):
        assert monitor._type_name(empty) is None


def test_node_key_types_reads_the_shape_of_each_populated_key():
    types = monitor.node_key_types(
        shape_run({"Embed": [{"text": "a", "score": 1, "ok": True, "tags": []}]}))
    assert types["Embed"] == {"text": "text", "score": "number", "ok": "boolean"}


def test_a_field_that_changes_type_is_reported():
    """An array where a string belongs: present, non-empty, and wrong."""
    latest = shape_run({
        "Fetch": [{"id": 1}, {"id": 2}],
        "Embed": [{"text": ["a", "b"], "score": 0.5}],
    })
    found = monitor.nodes_emitting_nothing([latest] + typed_history())
    assert found == [("Embed", "type_changed", [("text", "text", "list")])]


def test_a_key_that_legitimately_varies_in_type_promises_nothing():
    history = typed_history(3) + [shape_run({
        "Fetch": [{"id": 1}],
        "Embed": [{"text": ["a"], "score": 0.5}]}, minutes_ago=500)] + [shape_run({
        "Fetch": [{"id": 1}],
        "Embed": [{"text": ["b"], "score": 0.5}]}, minutes_ago=600)]
    latest = shape_run({"Fetch": [{"id": 1}],
                        "Embed": [{"text": ["c"], "score": 0.5}]})
    assert monitor.nodes_emitting_nothing([latest] + history) == []


def test_an_emptied_field_reports_once_not_twice():
    """keys_lost and type_changed must be disjoint - the zero-items bug again."""
    latest = shape_run({
        "Fetch": [{"id": 1}, {"id": 2}],
        "Embed": [{"text": "", "score": 0.5}],
    })
    kinds = [k for _, k, _ in monitor.nodes_emitting_nothing(
        [latest] + typed_history())]
    assert kinds == ["keys_lost"]


def test_a_number_staying_a_number_is_not_a_change():
    """1 and 1.0 are the same shape; distinguishing them would alarm constantly."""
    latest = shape_run({"Fetch": [{"id": 1}],
                        "Embed": [{"text": "a", "score": 2}]})
    assert monitor.nodes_emitting_nothing([latest] + typed_history()) == []


def test_check_workflow_reports_a_type_change():
    latest = shape_run({
        "Fetch": [{"id": 1}, {"id": 2}],
        "Embed": [{"text": ["a"], "score": 0.5}],
    })
    alerts = monitor.check_workflow(
        {"name": "x", "watch_node_output": True},
        [latest] + typed_history(), NOW)
    assert [a["kind"] for a in alerts] == ["node_type_changed"]
    assert "normally text" in alerts[0]["detail"]


# --- history proposes, a human blesses ------------------------------------
#
# The rolling median learns an outage as the new normal once the outage is
# longer than half the window; the recovery then pages as the anomaly. A
# blessed number does not learn. That is the whole point of it.


def outage_history(good=3, dead=5):
    """Three healthy runs, then five that produced nothing - newest first.

    Counts vary on the healthy runs; a constant series would hide any
    dependence on the median's behaviour.
    """
    dead_runs = [execution(60 * (k + 1), items=0) for k in range(dead)]
    good_runs = [execution(60 * (dead + k + 1), items=[48, 55, 51][k])
                 for k in range(good)]
    return dead_runs + good_runs


def test_the_rolling_median_learns_the_outage():
    """The failure being fixed, pinned so the fix is measurable."""
    runs = [execution(30, items=52)] + outage_history()
    baseline, _ = monitor.baseline_for(runs[0], runs[1:])
    assert baseline == 0  # five zeros out of eight: the median is dead


def test_a_blessed_expectation_does_not_learn_the_outage():
    runs = [execution(30, items=0)] + outage_history()
    spec = {"name": "x", "watch_output": True, "expected_items": 50}
    kinds = [a["kind"] for a in monitor.check_workflow(spec, runs, NOW)]
    assert kinds == ["output_deviation"]


def test_a_recovery_against_a_blessed_number_is_not_an_anomaly():
    runs = [execution(30, items=52)] + outage_history()
    spec = {"name": "x", "watch_output": True, "expected_items": 50}
    assert monitor.check_workflow(spec, runs, NOW) == []


def test_blessed_beats_thin_history():
    """One prior run is not enough for a median but a blessed number needs none."""
    runs = [execution(30, items=2), execution(90, items=50)]
    spec = {"name": "x", "watch_output": True, "expected_items": 50}
    kinds = [a["kind"] for a in monitor.check_workflow(spec, runs, NOW)]
    assert kinds == ["output_deviation"]


def test_the_floor_still_wins_over_the_blessed_number():
    """min_items is a hard rule; one problem produces one alert."""
    runs = [execution(30, items=3)] + outage_history()
    spec = {"name": "x", "watch_output": True, "expected_items": 50, "min_items": 10}
    kinds = [a["kind"] for a in monitor.check_workflow(spec, runs, NOW)]
    assert kinds == ["below_floor"]


def test_a_nonsense_blessed_value_is_ignored_not_crashed():
    runs = [execution(30, items=52)] + outage_history()
    for bad in (0, -5, "fifty", None):
        spec = {"name": "x", "watch_output": True, "expected_items": bad}
        monitor.check_workflow(spec, runs, NOW)  # must not raise or divide by zero


def test_history_proposes_a_whole_number():
    runs = [execution(30, items=52), execution(90, items=48),
            execution(150, items=55), execution(210, items=51)]
    assert monitor.propose_expectation(runs) == 52


def test_nothing_is_proposed_from_thin_or_empty_history():
    assert monitor.propose_expectation([]) is None
    assert monitor.propose_expectation([execution(30, items=52)]) is None
    assert monitor.propose_expectation(
        [execution(30, items=52), execution(90, items=48)]) is None


# --- the monthly one-liner ---------------------------------------------------
#
# "412 orders processed and 3 that needed a human, plus one line about anything
# that changed on their side." Counts in the client's units, never percentages.


SINCE = NOW - timedelta(days=30)


def failed(minutes_ago):
    return {"id": "9", "status": "error",
            "stoppedAt": (NOW - timedelta(minutes=minutes_ago)).isoformat()}


def test_summary_counts_runs_items_and_failures_inside_the_window():
    ok = [execution(60, items=40), execution(120, items=50), execution(180, items=45)]
    bad = [failed(90)]
    r = monitor.summarise({"name": "orders"}, ok, bad, None, SINCE)
    assert r["runs_ok"] == 3 and r["items"] == 135 and r["runs_failed"] == 1
    assert r["items_known_runs"] == 3 and r["truncated_at"] is None


def test_runs_outside_the_window_are_not_counted():
    ok = [execution(60, items=40), execution(60 * 24 * 40, items=999)]
    bad = [failed(60 * 24 * 45)]
    r = monitor.summarise({"name": "orders"}, ok, bad, None, SINCE)
    assert r["runs_ok"] == 1 and r["items"] == 40 and r["runs_failed"] == 0


def test_pruned_run_data_is_counted_as_unknown_not_zero():
    ok = [execution(60, items=40), execution(120), execution(180, items=45)]
    r = monitor.summarise({"name": "orders"}, ok, [], None, SINCE)
    assert r["runs_ok"] == 3 and r["items"] == 85 and r["items_known_runs"] == 2


def test_a_capped_fetch_that_does_not_reach_the_window_start_is_named():
    """250 runs of a ten-minute workflow is under two days, not a month."""
    ok = [execution(10 * (k + 1), items=5) for k in range(250)]
    r = monitor.summarise({"name": "fast"}, ok, [], None, SINCE, limit=250)
    assert r["truncated_at"] is not None
    assert r["truncated_at"].date() < NOW.date()


def test_a_full_fetch_that_covers_the_window_is_not_flagged():
    ok = [execution(60 * 24 * k + 30, items=5) for k in range(10)]
    r = monitor.summarise({"name": "daily"}, ok, [], None, SINCE, limit=250)
    assert r["truncated_at"] is None


def test_changed_nodes_are_named_against_the_blessed_list():
    spec = {"name": "x", "nodes": ["Trigger", "Check", "Send"]}
    r = monitor.summarise(spec, [], [], {"Trigger", "Check", "Notify"}, SINCE)
    assert r["changed"] == {"added": ["Notify"], "removed": ["Send"]}


def test_no_blessed_nodes_means_no_change_line_at_all():
    r = monitor.summarise({"name": "x"}, [], [], {"Trigger"}, SINCE)
    assert r["changed"] is None


def test_the_report_reads_like_a_sentence_and_has_no_percentages():
    rows = [monitor.summarise(
        {"name": "orders", "nodes": ["Trigger", "Send"]},
        [execution(60, items=400), execution(120, items=12)],
        [failed(90), failed(200), failed(300)],
        {"Trigger", "Send", "Slack"}, SINCE)]
    text = monitor.format_summary(rows, 30)
    assert "Last 30 days, in your units:" in text
    assert "orders: 2 runs, 412 items, 3 needed a human." in text
    assert "added 'Slack'" in text
    assert "%" not in text


def test_the_report_says_when_it_could_not_see_the_whole_window():
    ok = [execution(10 * (k + 1), items=1) for k in range(250)]
    rows = [monitor.summarise({"name": "fast"}, ok, [], None, SINCE, limit=250)]
    assert "Last 250 runs only" in monitor.format_summary(rows, 30)


def test_scan_spec_blesses_the_node_list():
    spec = monitor.spec_for({"id": 4, "name": "wf", "nodes": [
        {"name": "Trigger", "type": "n8n-nodes-base.scheduleTrigger",
         "parameters": {"rule": {"interval": [{"field": "hours"}]}}},
        {"name": "Send", "type": "n8n-nodes-base.set"}]})
    assert spec["nodes"] == ["Send", "Trigger"]


# --- the regulars: which rows are missing, not how many ----------------------
#
# "a statement has the same regulars every month, rent, salaries, two or three
# subscriptions. if they are not in the pull then something got cut no matter
# what the total says." The 60% day passed his row count; missing names caught it.


def rows(*payloads):
    """A successful run whose terminal node emitted these json payloads."""
    return {
        "id": "1", "status": "success",
        "stoppedAt": (NOW - timedelta(minutes=30)).isoformat(),
        "data": {"resultData": {
            "lastNodeExecuted": "Last Node",
            "runData": {
                "Schedule Trigger": [{"data": {"main": [[{"json": {"t": 1}}]]}}],
                "Last Node": [{"data": {"main": [
                    [{"json": p} for p in payloads]]}}],
            }}},
    }


STATEMENT = ["Rent", "Salaries", "AWS"]


def test_terminal_items_returns_the_payloads_not_the_trigger():
    got = monitor.terminal_items(rows({"description": "Rent"}, {"description": "AWS"}))
    assert got == [{"description": "Rent"}, {"description": "AWS"}]


def test_pruned_run_data_gives_no_opinion():
    assert monitor.terminal_items(execution(30)) is None
    assert monitor.missing_regulars(None, STATEMENT) == ([], False)


def test_all_regulars_present_is_silent():
    items = monitor.terminal_items(rows(
        {"description": "Rent"}, {"description": "Salaries"}, {"description": "AWS"}))
    assert monitor.missing_regulars(items, STATEMENT) == ([], False)


def test_a_missing_regular_is_named():
    items = monitor.terminal_items(rows(
        {"description": "Rent"}, {"description": "AWS"}))
    missing, gone = monitor.missing_regulars(items, STATEMENT)
    assert missing == ["Salaries"] and gone is False


def test_matching_is_case_insensitive_and_substring():
    """Real descriptions are dirty. RENT PAYMENT 4421 has to satisfy Rent."""
    items = monitor.terminal_items(rows(
        {"description": "RENT PAYMENT 4421"},
        {"description": "  monthly salaries  "},
        {"description": "aws emea billing"}))
    assert monitor.missing_regulars(items, STATEMENT) == ([], False)


def test_the_count_can_be_perfectly_normal_and_a_regular_still_missing():
    """The whole point. Three rows in, three rows out, one of them wrong."""
    spec = {"name": "bank-feed", "watch_output": True, "expected_items": 3,
            "expect_present": STATEMENT, "expect_present_field": "description"}
    run = rows({"description": "Rent"}, {"description": "AWS"},
               {"description": "Some new merchant"})
    alerts = monitor.check_workflow(spec, [run], NOW)
    kinds = [a["kind"] for a in alerts]
    assert "output_deviation" not in kinds
    assert "regulars_missing" in kinds
    assert "Salaries" in [a for a in alerts if a["kind"] == "regulars_missing"][0]["detail"]


def test_the_named_field_vanishing_is_its_own_alert_not_three_missing_values():
    items = monitor.terminal_items(rows({"desc": "Rent"}, {"desc": "Salaries"}))
    missing, gone = monitor.missing_regulars(items, STATEMENT, field="description")
    assert gone is True and missing == []


def test_searching_a_named_field_ignores_a_match_in_another_column():
    """'Rent' appearing in a memo column is not the rent line arriving."""
    items = monitor.terminal_items(rows(
        {"description": "AWS", "memo": "Rent was paid last month"},
        {"description": "Salaries", "memo": ""}))
    missing, _ = monitor.missing_regulars(items, STATEMENT, field="description")
    assert missing == ["Rent"]


def test_with_no_field_named_any_column_counts():
    items = monitor.terminal_items(rows(
        {"payee": "Rent"}, {"note": "Salaries"}, {"x": "AWS"}))
    assert monitor.missing_regulars(items, STATEMENT) == ([], False)


def test_an_empty_run_belongs_to_the_count_checks_not_this_one():
    """Zero rows is already reported once. Do not report it three more times."""
    empty = monitor.terminal_items(rows())
    assert monitor.missing_regulars(empty, STATEMENT) == ([], False)


def test_no_blessed_list_means_no_check():
    run = rows({"description": "anything"})
    alerts = monitor.check_workflow({"name": "x", "watch_output": True}, [run], NOW)
    assert [a for a in alerts if a["kind"] == "regulars_missing"] == []


# --- a stale list must not cry wolf ------------------------------------------
#
# "watch out for the list going stale though. one of mine renamed itself and
# the check cried every morning until i stopped reading it." - u/No-Hold-6217,
# one day after expect_present shipped. A monitor that repeats an alert nobody
# can action is worse than no monitor, because it hides the real ones too.


def rows_at(minutes_ago, *payloads):
    """A successful run at a given age whose terminal node emitted these rows."""
    return {
        "id": "1", "status": "success",
        "stoppedAt": (NOW - timedelta(minutes=minutes_ago)).isoformat(),
        "data": {"resultData": {
            "lastNodeExecuted": "Last Node",
            "runData": {
                "Schedule Trigger": [{"data": {"main": [[{"json": {"t": 1}}]]}}],
                "Last Node": [{"data": {"main": [
                    [{"json": pl} for pl in payloads]]}}],
            }}},
    }


def statement(minutes_ago, *names):
    return rows_at(minutes_ago, *[{"description": n} for n in names])


BANK = {"name": "bank-feed", "expect_present": ["Rent", "Salaries", "AWS"],
        "expect_present_field": "description"}


def kinds_of(spec, runs):
    return [a["kind"] for a in monitor.check_workflow(spec, runs, NOW)]


def detail_of(spec, runs, kind):
    return [a["detail"] for a in monitor.check_workflow(spec, runs, NOW)
            if a["kind"] == kind][0]


def test_a_renamed_line_is_reported_as_stale_not_missing():
    """Absent from this run AND from every run before it. Fix the list."""
    runs = [statement(30 * (k + 1), "Rent", "Salaries") for k in range(6)]
    assert "regulars_stale" in kinds_of(BANK, runs)
    assert "regulars_missing" not in kinds_of(BANK, runs)


def test_the_stale_alert_says_it_will_repeat_until_the_list_is_fixed():
    runs = [statement(30 * (k + 1), "Rent", "Salaries") for k in range(6)]
    d = detail_of(BANK, runs, "regulars_stale")
    assert "'AWS'" in d and "renamed" in d and "Update the list" in d


def test_a_line_that_was_there_yesterday_and_is_gone_today_still_alarms():
    runs = [statement(30, "Rent", "AWS")] + [
        statement(30 * (k + 2), "Rent", "Salaries", "AWS") for k in range(5)]
    assert "regulars_missing" in kinds_of(BANK, runs)
    assert "regulars_stale" not in kinds_of(BANK, runs)
    assert "'Salaries'" in detail_of(BANK, runs, "regulars_missing")


def test_the_real_alarm_shows_how_often_it_used_to_appear():
    runs = [statement(30, "Rent", "AWS")] + [
        statement(30 * (k + 2), "Rent", "Salaries", "AWS") for k in range(5)]
    assert "present in 5 of the last 5" in detail_of(BANK, runs, "regulars_missing")


def test_a_value_that_only_appears_sometimes_is_called_out_as_not_a_regular():
    """A monthly line in an hourly pull. We shipped this hole yesterday."""
    runs = [statement(30, "Rent", "AWS")] + \
           [statement(60, "Rent", "Salaries", "AWS")] + \
           [statement(30 * (k + 3), "Rent", "AWS") for k in range(6)]
    assert "regulars_irregular" in kinds_of(BANK, runs)
    d = detail_of(BANK, runs, "regulars_irregular")
    assert "'Salaries'" in d and "does not belong in expect_present" in d


def test_too_little_readable_history_claims_nothing_about_the_cause():
    runs = [statement(30, "Rent", "AWS"), statement(60, "Rent", "AWS")]
    ks = kinds_of(BANK, runs)
    assert "regulars_missing" in ks
    assert "regulars_stale" not in ks and "regulars_irregular" not in ks
    assert "of the last" not in detail_of(BANK, runs, "regulars_missing")


def test_pruned_runs_are_not_counted_as_absence():
    """n8n pruning history must never be read as the value disappearing."""
    runs = [statement(30, "Rent", "AWS")] + [execution(30 * (k + 2)) for k in range(8)]
    ks = kinds_of(BANK, runs)
    assert "regulars_stale" not in ks
    assert "regulars_missing" in ks


def test_history_counting_ignores_runs_whose_field_is_gone():
    hist = [rows_at(60, {"desc": "Rent"}), rows_at(90, {"description": "Rent"})]
    counts, readable = monitor.regular_history(hist, ["Rent"], field="description")
    assert readable == 1 and counts["Rent"] == 1


def test_history_counting_matches_the_same_way_the_check_does():
    hist = [statement(60, "RENT PAYMENT 4421"), statement(90, "rent")]
    counts, readable = monitor.regular_history(hist, ["Rent"], field="description")
    assert readable == 2 and counts["Rent"] == 2


def test_stale_and_genuine_missing_can_both_be_reported_at_once():
    """One renamed line and one real disappearance are different problems."""
    spec = {"name": "bank", "expect_present": ["Rent", "Salaries", "AWS"],
            "expect_present_field": "description"}
    runs = [statement(30, "Rent")] + [
        statement(30 * (k + 2), "Rent", "Salaries") for k in range(5)]
    ks = kinds_of(spec, runs)
    assert "regulars_stale" in ks and "regulars_missing" in ks
    assert "'AWS'" in detail_of(spec, runs, "regulars_stale")
    assert "'Salaries'" in detail_of(spec, runs, "regulars_missing")


# --- the second list: values that need not be in every run -------------------
#
# "i stopped keeping one list and keep two now. things that must be in every
# run, and things that must show up at least once inside a window."
#   - u/No-Hold-6217, r/n8n, 2026-09-05


def statement_hours_ago(hours, *names):
    return rows_at(hours * 60, *[{"description": n} for n in names])


MONTHLY = {"name": "bank-feed", "expect_present_field": "description",
           "expect_within_days": {"Rent": 35}}


def test_a_monthly_line_seen_inside_the_window_is_silent():
    runs = [statement_hours_ago(h, "AWS") for h in range(1, 20)]
    runs.append(statement_hours_ago(24 * 10, "Rent"))
    runs.append(statement_hours_ago(24 * 40, "AWS"))     # reaches past 35 days
    assert monitor.value_seen_within(runs, "Rent", 35, NOW, "description") == (True, True)
    assert "window_value_overdue" not in kinds_of(MONTHLY, runs)


def test_a_monthly_line_absent_across_a_covered_window_alarms():
    runs = [statement_hours_ago(24 * d, "AWS") for d in range(0, 45, 3)]
    seen, covered = monitor.value_seen_within(runs, "Rent", 35, NOW, "description")
    assert seen is False and covered is True
    assert "window_value_overdue" in kinds_of(MONTHLY, runs)
    assert "'Rent'" in detail_of(MONTHLY, runs, "window_value_overdue")


def test_history_that_cannot_reach_the_window_gives_no_verdict():
    """Thirty hours of an hourly workflow says nothing about a month."""
    runs = [statement_hours_ago(h, "AWS") for h in range(1, 25)]
    seen, covered = monitor.value_seen_within(runs, "Rent", 35, NOW, "description")
    assert seen is False and covered is False
    ks = kinds_of(MONTHLY, runs)
    assert "window_value_overdue" not in ks
    assert "window_not_covered" in ks


def test_the_uncovered_message_says_what_to_do_about_it():
    runs = [statement_hours_ago(h, "AWS") for h in range(1, 25)]
    d = detail_of(MONTHLY, runs, "window_not_covered")
    assert "do not reach back" in d and "Shorten the window" in d


def test_seen_just_outside_the_window_does_not_count_as_seen():
    runs = [statement_hours_ago(24 * d, "AWS") for d in range(0, 45, 3)]
    runs.append(statement_hours_ago(24 * 36, "Rent"))
    seen, covered = monitor.value_seen_within(runs, "Rent", 35, NOW, "description")
    assert seen is False and covered is True


def test_pruned_runs_never_extend_coverage():
    """A run we cannot read proves nothing, including about how far back we see."""
    runs = [statement_hours_ago(h, "AWS") for h in range(1, 10)]
    runs += [execution(24 * 60 * d) for d in range(30, 50)]
    seen, covered = monitor.value_seen_within(runs, "Rent", 35, NOW, "description")
    assert covered is False


def test_the_window_check_respects_the_named_field():
    runs = [rows_at(24 * 60 * d, {"description": "AWS", "memo": "Rent paid"})
            for d in range(0, 45, 3)]
    seen, _ = monitor.value_seen_within(runs, "Rent", 35, NOW, "description")
    assert seen is False


def test_no_window_config_means_no_window_check():
    runs = [statement_hours_ago(h, "AWS") for h in range(1, 25)]
    ks = kinds_of({"name": "x", "expect_present_field": "description"}, runs)
    assert "window_value_overdue" not in ks and "window_not_covered" not in ks


def test_the_two_lists_are_independent():
    """A value in the window list must not be required in every run."""
    spec = {"name": "bank", "expect_present_field": "description",
            "expect_present": ["AWS"], "expect_within_days": {"Rent": 35}}
    runs = [statement_hours_ago(24 * d, "AWS") for d in range(0, 45, 3)]
    runs.insert(1, statement_hours_ago(24 * 5, "AWS", "Rent"))
    ks = kinds_of(spec, runs)
    assert "regulars_missing" not in ks and "regulars_stale" not in ks
    assert "window_value_overdue" not in ks


# --- the check that suspects itself ---------------------------------------
# Raised by enzosoftware on community.n8n.io: a hit rate is itself a diagnostic.
# Four of these five are silence assertions, which is his other point - the
# quiet cases are the ones that catch the next false positive.

def flags(*names):
    return [{"workflow": n, "kind": "x", "detail": "y"} for n in names]


def test_a_scan_flagging_most_of_the_instance_says_so():
    warning = monitor.hit_rate_warning(flags("a", "b", "c"), 5)
    assert warning is not None
    assert "3 of the 5" in warning and "60%" in warning


def test_a_normal_hit_rate_stays_quiet():
    assert monitor.hit_rate_warning(flags("a"), 5) is None


def test_a_clean_scan_stays_quiet():
    assert monitor.hit_rate_warning([], 9) is None


def test_a_small_scan_stays_quiet_however_bad_it_looks():
    """Two of two is 100% and means nothing. Do not cry wolf on a tiny sample."""
    assert monitor.hit_rate_warning(flags("a", "b"), 2) is None


def test_many_findings_on_one_workflow_are_not_a_hit_rate():
    """The count is workflows, not alerts.

    One thoroughly broken workflow raising five findings out of six scanned is
    the tool working, not the tool failing. Counting alerts here would fire the
    warning on exactly the run we most want believed.
    """
    alerts = [{"workflow": "a", "kind": k, "detail": "y"}
              for k in ("stalled", "empty", "shape", "regulars_missing", "drift")]
    assert monitor.hit_rate_warning(alerts, 6) is None


# --- telling the outside world this check ran ------------------------------
# Raised by DuskWatch on community.n8n.io: underneath the three states above
# sits a fourth, the check did not run at all, and its silence is shaped
# exactly like all-clear.

def collector():
    seen = []

    def opener(url, timeout=None):
        seen.append(url)

        class R:
            def close(self):
                pass
        return R()
    return seen, opener


def test_no_heartbeat_url_means_no_ping():
    """Silence must stay opt-in. Nobody gets pinged by installing this."""
    seen, opener = collector()
    assert monitor.report_own_liveness("", opener) is False
    assert seen == []


def test_a_configured_heartbeat_is_pinged_once():
    seen, opener = collector()
    assert monitor.report_own_liveness("https://hc.example/abc", opener) is True
    assert seen == ["https://hc.example/abc"]


def test_a_failed_ping_never_raises():
    """An alert about your workflows must not be lost because a third-party
    ping endpoint was down for a minute."""
    def boom(url, timeout=None):
        raise OSError("connection refused")
    assert monitor.report_own_liveness("https://hc.example/abc", boom) is False
