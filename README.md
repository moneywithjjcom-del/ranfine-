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

## Which rows are missing, not how many

The strongest objection to every count-based check came from the person whose
count-based check had already failed him. He pulls a bank statement, and his fix
after the empty-pull incident was to alert on zero rows:

> the 60 percent day is the one that got past me, count looked fine. what i watch
> now is not how many rows but which ones are missing. a statement has the same
> regulars every month, rent, salaries, two or three subscriptions. if they are not
> in the pull then something got cut no matter what the total says. percentages
> never caught that, missing names did.

He is right, and no count fixes it. A pull can drop the rent line, pick up two new
merchants, and land on exactly the normal total. The median agrees. A blessed
`expected_items` agrees. Every number in this tool agrees, and the statement is
wrong.

So name the regulars:

```json
{
  "name": "bank-feed",
  "expect_present_field": "description",
  "expect_present": ["Rent", "Salaries", "AWS"]
}
```

Matching is case-insensitive substring, because real descriptions are dirty and
`RENT PAYMENT 4421` has to satisfy `Rent`. Naming the field matters: *Rent*
appearing in a memo column is not the rent line arriving, and the check says so.

Two deliberate silences. If the field itself has vanished from every item, you get
one alert saying that, rather than one per blessed value on top of a failure you
already have. And an empty run produces nothing here at all, because zero rows is
already reported by the count checks and saying it twice trains people to mute both.

### A stale list is worse than no list

The first person to use this told us what breaks it, one day after it shipped:

> watch out for the list going stale though. one of mine renamed itself and the
> check cried every morning until i stopped reading it.

That is the failure that kills monitors. A renamed row is absent from every run
from now on, so a naive check repeats the same alert daily until the person mutes
it, and a muted check hides the real failures too.

So the tool looks at how often each value appeared in the runs before this one,
and says something different depending on the answer:

- **Never appeared in any run we can read.** It was renamed or retired. You get
  one line telling you to update the list, saying plainly that it will repeat
  until you do. That is an instruction you can act on, not an alarm.
- **Appeared in only some runs.** It is not something every run produces, so it
  does not belong in a list of values expected in every run. This catches a hole
  we shipped without noticing: a monthly line in an hourly pull would otherwise
  have alerted on nearly every run.
- **Appeared in the runs before this one and is absent now.** That is the real
  alarm, and it tells you how many of the recent runs had it.

Pruned history is never counted as absence, and with too little readable history
the check reports the bare fact and claims nothing about the cause.

The honest cost that remains: cancel a subscription and this tells you the list is
stale until you edit it. That is the right kind of nagging, because the fix is one
line and it is yours to make. There is no `--scan` proposal for this one. Only you
know which lines are load-bearing, and a tool that guessed would bless noise.

## Monitoring that works looks like nothing happening

An agency owner on r/n8n put it exactly: the first client he bundled monitoring
into cancelled in month two, *"because from where he sat he was paying for nothing
to happen."* What fixed it was one line a month with the numbers in it — *"412
orders processed and 3 that needed a human, plus one line about anything that
changed on their side."* Uptime percentages got no reaction at all; 99.8% means
nothing to someone who has no idea what the missing 0.2 cost them.

`--report` is that line:

```
Last 30 days, in your units:
  - invoice-sync: 412 runs, 18,340 items, 3 needed a human.
  - lead-router: 8,640 runs, 8,640 items, 0 needed a human. Changed since you
    last blessed it: added 'Slack alert'; removed 'Send email'.
```

Counts are in the workflow's own units — the items its last node emitted — not
ours. *Needed a human* is executions that ended in error. *Changed* is the current
node list against the one `--scan` blessed into `watch.json`, which is how a client
renaming a field without telling anyone becomes a line in a report instead of a
surprise three weeks later.

It is bounded at 250 runs per workflow, and when that is shorter than the window
the line says so rather than quietly reporting a partial month as a whole one.

## History proposes. You bless.

The rolling median has a failure an n8n operator named precisely: *"a rolling
learner will happily learn a two-week outage as the new normal."* Once an outage
is longer than half the window, the dead runs *are* the median, and the recovery
pages as the anomaly. The monitor was quietly wrong in exactly this way.

The answer another operator gave is better than anything unsupervised: history
**proposes** an expectation, a human **blesses** it, and a blessed expectation never
drifts without another bless.

`--scan` does the proposing. When it has enough history to have an opinion, the
`watch.json` it prints carries `expected_items` for each workflow — the median of
recent runs, rounded. Saving that file is the blessing. From then on deviation is
measured against your number, not against whatever the last fortnight looked like.

The trade is deliberate: a blessed number also does not follow a legitimate change
until you change it. Run `--scan` again after editing a workflow and it re-proposes.

## Absence is a signal. So is presence.

`watch_steps` catches a step that stopped running. It cannot catch the opposite,
which an n8n operator described on 2026-09-01 and which is more common:

> An embedding call whose quota had run out came back 200 with an empty body.
> The node executed, so it lands in runData with a happy status, and the next
> node got an empty string and carried on. Retrieval was dead for days and every
> execution reported success.

The node is present, so absence detection sees nothing. The workflow still
produced an answer — just one built on nothing retrieved — so a workflow-level
item count sees nothing either. Both of the checks above miss it.

`watch_node_output` compares each node against what that node normally emits,
with two rules of deliberately unequal weight:

- **Fields that normally carry a value came back empty.** The strong signal. A
  shape change is hard to explain away as a quiet day, and an empty string in a
  field that normally holds content counts as empty on purpose.
- **A field came back as the wrong type** — a list where every prior run held
  text. Also strong, and for the same reason: the shape changed, not the volume.
  Reported by an operator on 2026-09-03, describing what presence checking
  misses: *"It may be blank or it could be in the wrong data type like an array
  when it should be a string, so nothing errors."* A blank field is caught by the
  rule above, because the key comes back empty. An array where a string belongs
  is a value — key present, not empty, every presence test passes. Only the type
  shows it. Counted only where that key held one type on *every* prior run;
  a field that has ever legitimately varied is making no promise.
- **The node emitted nothing at all**, and only where that node has emitted
  something on *every* prior run. Weak on its own: a search step returning zero
  results some days is correct, not broken, so a bare zero is evidence only
  where zero has never happened before.

Comparisons are made only across runs of the same workflow version. Edit the
workflow and the baseline resets rather than alerting on itself — which means an
actively-edited workflow rarely accumulates enough same-version history for this
check to have an opinion. That is the honest cost: a baseline built across an
edit is worse than no baseline.

What it still does not catch: output that is present, plausible, and wrong.
Nothing here reads meaning, only shape. The sharpest form of that, from an
agency owner on r/n8n who scrapes prices: a site-wide change moves every row
together, so your own history agrees with the wrong answer. Anything that
compares a run against its own past has that blind spot, this included. The
only thing that catches it is a known answer from outside the run - he keeps
five URLs whose prices he checks by hand once a month - and that expected value
has to come from a person, because a number you can compute drifts with the
same change.

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
python monitor.py watch.json              # the checks
python monitor.py --report watch.json 30  # the month, in your units
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
- `watch_steps` — alert when a step that normally runs stops running.
- `watch_node_output` — alert when a step *runs*, reports success, and carries
  nothing. See below; this is the one that catches the green-but-empty node.
- `expected_items` — a blessed baseline. When set, deviation is measured against
  this number instead of the rolling median. See below for why you want one.
- `nodes` — the blessed node list. `--scan` writes it; `--report` names anything
  added or removed since.
- `expect_present` — values that must appear in every run, and
  `expect_present_field` to say which column to look in. Counts answer *how
  many*. This answers *which ones*.
- `min_items` — a hard floor, for when you genuinely know the number. Takes
  precedence over everything else, so one problem produces one alert.
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
