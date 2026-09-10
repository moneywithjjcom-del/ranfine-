# Templates

Workflows you can import into n8n directly. They answer a narrower question than
`monitor.py` and need nothing installed.

## silent-failure-check.json

Finds workflows that are **active**, have a **schedule trigger**, and have quietly
stopped running. n8n raises no error for this: the error workflow never fires, because
nothing failed — nothing ran.

Ten nodes. Two read-only GETs against the n8n public API, a comparison in a Code node,
an alert, and a heartbeat. Nothing is written to the instance and no workflow is
modified.

**Setup.** In the `Settings` node set `n8nBaseUrl`, `silentAfterMinutes`, and
`heartbeatUrl` (a dead man's switch you control — Healthchecks.io, Cronitor, or your
own endpoint). Add an n8n API key to both HTTP Request nodes as Header Auth with the
header name `X-N8N-API-KEY`. Replace the two Gmail nodes with whatever your team reads.

### Two exclusions, both deliberate

**Webhook-only workflows are skipped.** They may legitimately sit idle for days, so
silence there carries no information. Alerting on them produces findings that are all
false.

**A workflow with no execution history at all is skipped, not alerted.** n8n prunes
execution data on a retention schedule (`EXECUTIONS_DATA_MAX_AGE`, 336 hours by
default). Treating pruned history as "never ran" fires on everything the moment history
ages out, which is how a check teaches you to ignore it.

### It handles its own failure

Each API call retries three times, five seconds apart. If it still fails, the node's
error output routes to `Check could not run` rather than the workflow dying quietly — a
check that fails silently is indistinguishable from a clean one, which is the fault this
workflow exists to detect.

Both endings reach `Heartbeat`, so a crashed run pings nothing and an external dead
man's switch notices that the watcher stopped watching. The heartbeat continues on
error: a ping endpoint being briefly down must never cost you a real finding.

### What it does not catch

The run that completes, reports success, and produced nothing. A timestamp check
confirms a workflow ran, not that the run did anything. That needs output compared
against a baseline — which is what `monitor.py` in the root of this repository does.
