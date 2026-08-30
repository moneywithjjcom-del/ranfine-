# Ran Fine

Watches n8n workflows **from outside n8n** and complains when they go quiet or
quietly stop doing their job.

Three questions n8n won't answer:

1. **Has this workflow ever run at all since you published it?**
2. **Has it checked in recently enough?**
3. **Did the last run produce roughly what it usually produces?**

Read-only. It never writes to your n8n.

## What this is, and what it isn't

**The heartbeat is not the point.** [Healthchecks.io](https://healthchecks.io)
and [Cronitor](https://cronitor.io) already do dead-man's switches, and do them
well. If all you need is "tell me when the cron stops," use one of those.

What a generic pinger cannot do is look *inside* an n8n execution. That's what
question 3 is, and it's the reason this exists.

## The output check, and why a zero-check isn't enough

Zero rows is the easy failure and nearly a solved one — any non-empty assertion
catches it. The expensive failure is the run that quietly returns 60% of its
normal output, because that passes every emptiness check you can write.

So the default is **deviation from a median of comparable runs**, not a fixed
threshold:

- **The baseline has a calendar attached.** Where there are at least 3 samples
  of the same weekday, the comparison is Monday against Mondays. A flat median
  across recent runs quietly assumes every day is the same shape, so a client
  whose Monday is legitimately 10× its Tuesday either alerts every Monday or
  hides a collapsed Monday behind the week's average. Falls back to the flat
  median when the calendar view is too thin, which is most of a workflow's
  first month.
- Median, not mean, so one freak 10× day doesn't raise the bar and make every
  normal day afterwards look like a failure.
- No alert until there are at least 3 prior runs to compare against. Alerting
  off a baseline of two samples is how a monitor earns itself a mute rule.
- Absent run data reads as **unknown**, never as zero. n8n prunes execution
  data on a retention schedule; without this rule every workflow would alert
  the moment its history aged out.

The calendar point came from AleksGorbatov on the n8n forum, who maintains
integrations for a dozen-plus clients: *"a baseline is an expectation with a
calendar attached... averages hide exactly the days you care about."*

## Edits are not failures

The obvious version of "which steps normally run" breaks the first time someone
edits a workflow: a deleted node stops appearing, looks like a silent failure,
and you get false positives until the history ages out. That is how a monitor
earns itself a mute rule.

So the step check reads the workflow's **currently declared** nodes each run and
ignores anything no longer in it. A deleted or renamed node is an edit. A node
that is still declared and stopped being reached is a finding. If the workflow
can't be read that run, the check keeps its old behaviour rather than going
quiet — unknown must not mean silent.

Raised by an n8n operator on 2026-08-30, who was right about the version that
shipped before it.

## "Never ran" is its own alarm

Staleness detection needs a baseline. *Never ran* has none — a schedule trigger
that was never going to fire looks identical to a healthy workflow that simply
isn't due yet. So zero successful executions is reported separately rather than
folded into "stopped running."

This matters more than it sounds: a Schedule Trigger missing `weeksInterval` in
its workflow JSON never fires at all, and manual execution skips the recurrence
check entirely — so "I tested it and it ran fine" tells you nothing about
whether it will ever fire on its own.

## Install

No dependencies beyond the standard library. Python 3.9+.

```bash
git clone https://github.com/moneywithjjcom-del/ranfine-.git
cd ranfine
cp watch.example.json watch.json   # then edit it
```

## Use

```bash
export N8N_URL=https://n8n.example.com
export N8N_API_KEY=...
python monitor.py watch.json
```

Exits `0` when healthy and `1` when something needs attention, so cron and CI
can act on it:

```
*/10 * * * * cd /opt/dms && python monitor.py watch.json
```

## Config

```json
{
  "slack_webhook": "https://hooks.slack.com/services/...",
  "workflows": [
    {"id": "17", "name": "invoice-sync",   "every_minutes": 15},
    {"id": "22", "name": "nightly-export", "every_minutes": 1440, "watch_output": true},
    {"id": "31", "name": "billing-run",    "every_minutes": 1440, "watch_output": true, "min_items": 50}
  ]
}
```

- `every_minutes` — how often you expect success. Omit for event-driven
  workflows with no schedule; they won't be checked for lateness.
- `watch_output` — turn on deviation detection against the rolling median.
- `min_items` — a hard floor, for when you genuinely know the number. Takes
  precedence over deviation, so one problem produces one alert.
- `slack_webhook` — optional; without it, output goes to stdout only.

Tuning constants live at the top of `monitor.py`: `GRACE` (how late is late),
`DEVIATION` (how far below normal is a problem), `MIN_HISTORY`, `HISTORY`.

## Roadmap — the part that actually differentiates

Everything above could in principle be approximated by a generic monitoring
service. These cannot, because they require reading inside an n8n execution:

- **Branch marked SKIPPED inside a run marked COMPLETED** — the work silently
  didn't happen and the run still reports success.
- **Credential expiry that fails before the request leaves** — no HTTP status
  at all, so error handling built around status codes never sees it.
- **Queue-mode executions that hang forever**, ignoring both the per-workflow
  Timeout and `EXECUTIONS_TIMEOUT`
  ([n8n#36343](https://github.com/n8n-io/n8n/issues/36343), still open). They
  neither complete nor error.
- **Disabled nodes** — a node left `"disabled": true` passes activation
  validation and passes its input straight through, so the output is wrong and
  nothing flags it.

Each needs verifying against a real instance before it ships. The JSON shapes
aren't guessable, and guessing produces false alarms — the one failure this
tool can't afford.

## Tests

Every alert case is a fixture; no network involved.

```bash
python -m pytest -q
```

45 tests. They encode one rule: **alert on real trouble, never on missing
information.** A monitor that cries wolf gets muted, and a muted monitor is
worse than none.

## Status

Early and honest about it. The alert logic is tested. The HTTP layer is written
against n8n's documented API and wants validating against more real instances.
Edge cases you've hit are genuinely welcome — the failure modes above came from
people describing them on the n8n forum, and there are certainly more.

## Why this exists

I do fixed-fee reliability audits for n8n and Make automations. This is the
monitoring half, given away, because the heartbeat was never the valuable part.

Every failure mode above came from someone describing it on the n8n forum or
from running this against a real instance. If you have one this misses, an
issue is genuinely welcome.

MIT licensed. Use it, fork it, sell services with it.
