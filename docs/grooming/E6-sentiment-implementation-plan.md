# F1 sentiment: implementation and verification plan

Revision: 2026-09-11. Canonical shared copy: https://linear.app/buddy-bot/issue/BUD-188
Planning: Astra only. Implementation, implementation tests, and independent QA: GPT-5.6 Terra, alias `codex-terra`. Runtime sentiment: the evaluated local model served by Unsloth, NOT Terra or Astra.

## Objective and observed baseline

Populate the approved sentiment-first website with traceable race-specific opinions and evidence-linked summaries, while keeping personal predictions separate. Treat f1.brettswift.com / namespace f1-predictor as staging despite its legacy prod name. Do not touch the F1 dev account/environment. Future production may be AWS or DigitalOcean; choose no cloud provider now.

Checks on 2026-09-11: the namespace has driver-refresh and race-manager CronJobs, no sentiment collector; main/deployed files expose sentiment styling but not an operational collector. BUD-136/137 and 185–191 are Backlog; race-card PR #86 is open. Unsloth's wrapper listens on 127.0.0.1:8888, its child inference port is ephemeral. Source access is not proven. Recheck before implementation; never infer deployment from a Done label or code merge.

## Architecture decision: ordinary worker, no agent/tool loop

Run collection and analysis on Buddy, beside Unsloth. A plain scheduler starts portable worker commands; collection writes a durable local spool, analysis consumes it, calls the loopback Unsloth API, validates JSON, and uploads structured records to the website's authenticated ingest API. The model receives text and returns data; it gets no tools, credentials, network access, or authority to execute source instructions. OpenClaw is an optional external observer, not a pipeline dependency.

Flow: permitted Reddit / editorial feeds → collector on Buddy → persistent JSONL + ledger → race/driver attribution → Unsloth classification and summary → validated idempotent HTTPS ingest → website database → rolling snapshot → existing UI.

This avoids exposing Unsloth to another machine and avoids shared network filesystems. Existing source stories mentioning k8s collection are superseded for the initial deployment: deploy collection beside analysis on Buddy. Keep commands/container images orchestrator-independent. If collection moves to the cloud later, introduce an authenticated pending-record download endpoint/object store and let Buddy pull outward; do not publish a raw model port or mount the website's SQLite file across hosts.

## Processes, scheduling, and network

Keep implementation in the f1-predictor repository, with independently invocable collect/analyse/summarise/publish commands and a small scheduler wrapper. Use a systemd timer on Buddy for initial operation (cron invoking the same command is acceptable). Container packaging must support persistent volume mounts; do not assume container localhost is the host. Initial host worker may call 127.0.0.1:8888/v1 directly. If containerized, explicitly configure and test loopback access (Linux host networking or an authenticated host adapter); never globally bind the Unsloth service merely to make containers work.

Collection: default 3-hour baseline, configurable 30-minute cadence near canonical sessions, subject to verified source rate limits. A timer may tick every 15 minutes and exit successfully when not due. Analysis: bounded batches every 15 minutes; summaries only on a changed input snapshot. Forbid overlapping runs with an OS lock/lease. One inference request at a time, coordinated with other local-model workloads; finite HTTP/model timeouts and bounded retries. Do not unload or replace an interactive user's model automatically. Discover wrapper health, loaded model ID, context limit and capacity; do not hardcode the child port.

Buddy only needs outbound approved source access and HTTPS to staging; future cloud production uses the same outbound pattern. Use separate environment-specific credentials and URLs. Configuration includes source allowlist, raw directory, canonical race endpoint, model base URL/ID, batch limits, timeout, ingest URL, protected credential references, collection cadence, retention, and publish feature flag. Never store secrets in Git, command arguments or logs.

## Source collection contract — BUD-189, BUD-136

Verify official Reddit API application eligibility and a real authenticated thread/comment read for r/formula1 and r/F1Technical; record scopes, expiration/renewal, rate-limit handling and permitted retention/display. A token name is not evidence of access. Missing external approvals are explicit blockers, not successful empty polls. No scraping bypass or new paid account without authority. Verify at least one permitted F1 RSS/news source separately. Editorial sources may supply attributed news summaries, never the fan sentiment denominator.

Retain the established RAW_SENTIMENT_DIR/{subreddit}/{UTC-run}.jsonl layout for Reddit; add source-specific directories for other feeds. Each record includes schema_version, source_kind, stable source_id, revision/content hash, parent/thread identity, source URL, source_created_at, collected_at, text and minimal required metadata. Omit unnecessary author profiles. Each run writes temp files, fsyncs and atomically renames before advancing the collection checkpoint. Persist original timestamps and source-specific pagination cursors; hot/top listings are not chronological, so a single last_seen_id is insufficient. Use overlapping scans plus durable ID/revision deduplication. Document bounded lookback and backfill limitations.

Maintain separate collection and consumption checkpoints. A successful no-new-items run differs from auth failure, partial pagination and outage. Honor Retry-After and reset headers, exponential backoff with jitter; exhausted runs exit nonzero. Source edits/deletions create revisions/tombstones that invalidate derived active output. Retention obeys verified provider rules; default to minimum needed, never assume indefinite raw storage. Do not delete unacknowledged input merely because a daily retention timer ran.

## Attribution, model contract, and quality — BUD-190

Use canonical season/race/session and current driver IDs, explicit thread/event metadata plus contextual evidence; timestamps alone cannot establish race relevance. General discussion and ambiguous race/driver references remain unassigned. Support multiple drivers in one source, but at most one active class per source revision + race + driver + classifier version, preventing sentence/chunk double counting.

Send bounded batches with stable record IDs and surrounding context. Require schema-validated per-record output: source_id, source_revision, race_id or null, driver_id, class (positive/neutral/negative/abstain), evidence offsets/excerpt, abstention reason and optional confidence. Validate IDs and evidence against input; the worker supplies trusted timestamps and model/prompt versions. Confidence alone is not a quality guarantee. Strip reasoning traces from stored/public results. Unknown or malformed output is quarantined/retried, never forced neutral. Source text is untrusted data; prompt-injection samples must not change output schema or trigger actions.

Evaluate >=100 permitted, manually labelled excerpts split into tuning and held-out sets. Include sarcasm, mixed attitudes, multi-driver text, wrong-race/general discussion, multilingual/unsupported cases, news and adversarial content. Gate publication on race/driver attribution precision >=90% and three-class macro-F1 >=0.80, plus reported abstention/coverage. Publish confusion matrix, sample counts, timing and failures; model-generated labels are not ground truth. Do not silently lower thresholds. Re-evaluate after model, prompt, attribution or quantization changes.

## Reliable structured ingest and storage — BUD-137

Implement the local worker and authenticated bounded POST /api/sentiment/ingest. Use scoped protected credentials over verified TLS; constant-time key checks or equivalent supported auth. Validate payload size, schema, canonical IDs, UTC timestamps and URLs. Reject invalid batches without partial writes. Return explicit accepted/replayed/rejected IDs. The server computes counts; never trust model-supplied percentages or volume.

Use a durable local SQLite ledger/outbox (not the website DB), with pending, processing lease, retry, accepted and quarantined states. Mark accepted only after server acknowledgment. A crash after server commit but before local acknowledgment must retry safely. Server unique identity includes source kind/ID/revision, race, driver and classifier version. Exact replay is a no-op; same identity with different content is a conflict. Track active revisions and explicitly selected classifier versions so reanalysis does not count twice. Preserve immutable processing history but exclude superseded/tombstoned records from active aggregates.

Migrations add source/mention/summary/run metadata without altering historical votes or scoring. Back up staging DB before migration, test on a copy, verify indexes and rollback compatibility. Separate ingestion endpoints from public reads; raw exports require authorization. Finite disk usage, queue-age metrics, backpressure and quarantine inspection are mandatory. No shared SQLite across hosts.

## Read model and UI — BUD-185

Compute positive/neutral/negative counts over source-created UTC in exact [start,end) 24h/48h windows for the selected race. Exclude abstentions, tombstones and superseded versions but report their coverage. Shares are counts divided by classified mentions; zero denominator is unavailable, not 0% or neutral. Previous equal-length nonoverlapping window supplies percentage-point change. Do not average batch percentages or convert scalar scores into shares.

Rank by positive share with configured minimum 500 classified mentions, ties by volume then stable driver ID. Insufficient eligible drivers leave podium slots empty. A small initial real-data sample must visibly remain low-sample; do not fabricate a populated podium to pass demo. Separate editorial coverage, fan sentiment and player prediction popularity in data and presentation. Preserve purple sentiment versus amber personal race card. Expose collected/generated/source-window times, source coverage, stale/empty/low-sample/unavailable states. Publish snapshots atomically so a reader cannot mix versions/windows.

## Grounded summaries — BUD-186

Produce summaries locally from eligible source/classification records for a race/window. Store generated time, model/prompt versions, input snapshot ID and source references per claim. Represent fan opinions as opinions and editorial reports as attributed reports. Validate referenced IDs/URLs against collected sources, escape text/HTML, reject unsupported quotes/facts and missing evidence. Automated citation checks do not prove semantic support: QA must inspect groundedness. Revisions/deletions invalidate affected summaries. Never display model reasoning, raw private metadata, or synthetic fixture text in the live feed.

## Separate readiness tests and operating evidence — BUD-191

Gate A — collection: manually run each enabled source using real permitted access; record timestamps, stable IDs, counts, checkpoint progress and sanitized evidence. Repeat with unchanged input (no duplicate active records), simulate auth expiry, 429, partial failure and restart. Confirm the installed timer is enabled and capture at least two actual scheduled executions, not just a successful manual command. State Reddit readiness independently of RSS readiness; news-only is not fan sentiment.

Gate B — inference/delivery: call the actual Unsloth wrapper from the deployed worker context, identify the loaded model, process a fixed batch, validate schema/evidence, upload, replay and verify zero duplicates. Test model unavailable/busy/timeout, malformed JSON, ingest unavailable/401, disk pressure and interruption after server acknowledgment. Recover without loss. Gate B can use labelled fixtures offline while Gate A awaits approval, but only real permitted data may populate staging.

Gate C — end to end: selected real race with trace from raw record to attribution/class, stored row, exact denominator, snapshot and rendered UI. QA reproduces values and several summary claims independently. Test mobile/desktop with real long text; prediction reads/writes remain independent throughout sentiment failure tests. Historical data is marked replay, not fresh live. Record deployment/image or commit version, scheduler history, last success/attempt, queue lag, latency, model version and rollback procedure. BUD-164 remains sole external notifier; pipeline emits structured health and exit status only.

## Story ownership and dependency order

1. BUD-189: prove source access, allowed feed/retention policy, protected setup. External-access gate.
2. BUD-136: portable collector, raw spool/checkpoints and configured Buddy schedule. Depends on 189 for real-source proof.
3. BUD-190: race/driver attribution and evaluated Unsloth contract. Depends on 136 for real benchmark inputs.
4. BUD-137: Buddy worker/outbox, local API calls, authenticated ingest and migrations. Depends on 136 and 190.
5. BUD-185 and BUD-186: independent read/summary deliverables after 137.
6. BUD-191: timer operation, failure recovery and real-data staging demonstration after 185/186.
7. BUD-187: independent race-card hardening. Resume existing PR #86 at QA after verifying current code/CI; do not recreate it or merge without review.
8. BUD-188: this parent plan and completion checklist; never dispatch as a duplicate monolithic implementation. Close only after all applicable child gates pass.

Preserve native blocking relations and other story labels. Each story must link this canonical plan and include its relevant section/gates. The plan supersedes old Kimi/Sonnet routing, cloud inference suggestions, duplicate-row acceptance, simplistic hot/top watermarks, and k8s-first collection instructions. Preserve all nonconflicting existing acceptance criteria.

## Model routing and review controls

Recommendation: GPT-5.6 Terra, the official intelligence/cost-balanced model, already configured as codex-terra. Reference: https://developers.openai.com/api/docs/models.md . Use model-dev:codex-terra and model-qa:codex-terra, both resolving to openai/gpt-5.6-terra. Remove conflicting role labels. Dev owns implementation/tests; a fresh Terra QA session independently checks ACs, tests, actual diff, CI and deployment evidence. Same model is explicitly requested; fresh reviewer context avoids simply accepting the developer's report.

Astra does not implement this pipeline. Do not silently escalate QA to Opus/Astra or fall back to a different model after failures; preserve the requested Terra routing and report runtime problems. Inspect existing generic escalation machinery before resuming automated dispatch. Validate live story labels through the actual _model_for resolver for Ready, In Progress and AI Review, then verify the OpenClaw alias/runtime resolution. A configured alias is not proof a child execution succeeded; capture resolved model/runtime on first dispatch and halt on mismatch. No global default model change is needed.

## Completion report

Report separately: code merged; deployed version; source access verified per source; collection timer verified; Unsloth quality passed; delivery/replay passed; actual-data UI verified. Explicitly name any human-only credential/application approval. Planning and label assignment do not constitute implementation, successful scheduling or access approval.
