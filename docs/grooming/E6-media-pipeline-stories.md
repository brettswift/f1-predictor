# E6 — Media pipeline stories

**Written 2026-09-12.** Replaces the cancelled Reddit-era E6 stories (BUD-136,
137, 185, 186, 188, 189, 190, 191). Design: `../MEDIA_PIPELINE.md`.

## How these stories are written

Every story ends in a **functional outcome** — a sentence describing what is
observably true after the story ships, checkable against the database or a
command's exit code. Not "the UI looks right."

Rules applied throughout:

- **One source adapter per story.** Adding a feed is never "also add
  formula1.com" inside another story. Each source has its own quirks (date
  formats, guid stability, encoding) and needs its own tests.
- **Tests read the database, not the screen.** Run the job against a source,
  then assert on rows.
- **Portable code, k8s deployment.** Pipeline code is a plain Python module
  runnable as `python -m media.fetch` with no Kubernetes, cloud, or scheduler
  coupling. Config comes from env vars. We deploy it as a k8s CronJob now; it
  must be liftable to Lambda, DigitalOcean, or a plain cron box without code
  changes. **No story may import a k8s or cloud SDK into pipeline code.**
- **Never break the game.** No story in this epic may alter prediction
  submission, scoring, or locking.

Sizing: each should be a day or less. If one grows past that, split it.

**Model: Sonnet 5 (Dev) → Sonnet 4.6 (QA)** for all 15 stories. QA must
empirically verify the database outcome, not just review the code — every story
here is written so that is possible.

The dispatcher reads this from Linear **labels**, not from story text:
`model-dev:sonnet-5` and `model-qa:sonnet-4-6`. The alias is `sonnet-4-6` with
hyphens — `sonnet-4.6` does not resolve. Labels are authoritative; anything
written in a description is documentation only.

**Deployment target:** merging to `f1-predictor@main` is the only deploy path.
There is no dev environment; `f1-dev` does not exist on the remote. See
`../FEATURE_STATE.md` § How deployment actually works before writing M-14.

---

## Phase 1 — Fetch

### M-01 · Article storage schema and dedup guarantee

Create `media_articles` per `MEDIA_PIPELINE.md`, via a migration following the
existing `init_db()` convention.

Includes a source-agnostic `store_article()` that upserts on `guid`, plus a
`fetch_runs` row recording each run's source, counts and outcome.

**AC**
- Migration creates `media_articles` with `guid` UNIQUE; running it twice is a no-op.
- `store_article()` called twice with the same `guid` leaves exactly one row and
  does not alter `fetched_at` on the second call.
- Storing an article with a `guid` that exists but changed `title` updates the
  title and leaves `fetched_at` untouched.
- Missing `published_at` is stored as NULL, not `now()` — we never fake a date.

**Functional outcome:** the database can hold articles from any source and
physically cannot contain the same `guid` twice.

---

### M-02 · Generic RSS fetch runner

A runnable module, `python -m media.fetch --source <name>`, that loads the named
adapter, fetches, and writes via `store_article()`. Adapters register by name;
the runner knows nothing about any specific site.

No adapters ship in this story — M-03 is the first.

**AC**
- `python -m media.fetch --source nonexistent` exits nonzero with a clear error
  and writes no rows.
- The runner imports no k8s, AWS, or scheduler library.
- Network timeout is configurable by env var, defaults to 30s, and is applied.
- A fetch that raises exits nonzero and records a `fetch_runs` row with the failure.
- Run against a local fixture server: articles land in `media_articles`.

**Functional outcome:** one command runs any registered source; adding a source
never means editing the runner.

---

### M-03 · Formula1.com source adapter

First real adapter. `https://www.formula1.com/en/latest/all.xml`.

Establishes the adapter contract: parse the feed, map to article fields, extract
a stable guid, parse that site's date format to UTC.

**AC**
- Adapter parses a committed real-response fixture into article dicts with
  non-empty `guid`, `url`, `title`.
- Date parsing produces timezone-aware UTC datetimes; a malformed date yields
  NULL and a warning, never a crash or a wrong date.
- Running the fetch twice against the fixture leaves the same row count.
- A live run against the real feed stores ≥1 article. Marked as a network test
  so CI can skip it.
- Malformed XML mid-feed keeps the articles parsed before the break.

**Functional outcome:** after `python -m media.fetch --source formula1`, F1.com
articles are queryable in `media_articles`, each with a working URL, and a second
run adds no duplicates.

---

### M-04 · Motorsport.com source adapter

Same contract. `https://www.motorsport.com/rss/f1/news/`.

Verified 2026-09-12 to carry a stable numeric `<guid>` and RFC-822 `<pubDate>`.

**AC**
- Same five criteria as M-03, against a Motorsport.com fixture.
- The numeric `<guid>` is used as the dedup key, not the URL — URLs may carry
  changing query strings.
- CDATA-wrapped titles/descriptions are unwrapped, not stored with markers.

**Functional outcome:** `--source motorsport` stores articles keyed on the
feed's numeric guid; re-running adds nothing.

---

### M-05 · Autosport.com source adapter

Same contract. `https://www.autosport.com/rss/f1/news/`.

**AC**
- Same five criteria as M-03, against an Autosport fixture.
- If this feed's guid proves unstable across fetches, the adapter falls back to
  a documented deterministic hash of the canonical URL — decided in this story
  and written down, not left implicit.

**Functional outcome:** `--source autosport` stores articles with no duplicates
across repeated runs.

---

### M-06 · Race-weekend-aware scheduling

Pick the polling cadence by proximity to the next race: frequent Fri–Sun, sparse
midweek. Pure function over the races table — no scheduler coupling.

**AC**
- `should_fetch_now(now, next_race_date)` returns the documented cadence for:
  mid-session, race day, weekend, 3 days out, no upcoming race.
- Function is pure and unit-tested with injected times — no `datetime.now()`
  inside it.
- Runner honours it unless `--force` is passed.

**Functional outcome:** the fetch job self-throttles midweek and tightens over a
race weekend without a schedule change.

---

## Phase 2 — Analyse

### M-07 · Insight schema and JSON contract validation

Create `media_insights` and `media_driver_sentiment`, plus a validator for the
stage-2 schema in `MEDIA_PIPELINE.md`.

Validation only — no model call yet.

**AC**
- Migration creates both tables; re-running is a no-op.
- Validator rejects: missing `summary`, `sentiment` outside the three values,
  `strength` outside 0–1, absent `evidence`, unknown `driver_id`.
- A rejected payload writes nothing to either table — no partial writes.
- A valid payload writes one `media_insights` row and one
  `media_driver_sentiment` row per driver.
- `driver_id` not in `drivers` is dropped with a warning; other drivers in the
  same payload still persist.

**Functional outcome:** invalid model output can never reach the database, and a
valid payload lands in both tables atomically.

---

### M-08 · Evidence quote verification

Every driver tint carries a verbatim quote from the article. Verify it actually
appears in the source text; reject the tint if not.

**AC**
- Tint whose `evidence` appears in the article body is accepted.
- Tint whose `evidence` does not appear is rejected, with the rest of the
  payload still stored.
- Matching tolerates whitespace and typographic-quote differences but not
  reworded text.
- Rejections are logged with article id and driver id.

**Functional outcome:** every row in `media_driver_sentiment` has a quote
findable in its article — a hallucinated tint cannot be stored.

---

### M-09 · Local model analysis worker

`python -m media.analyse` claims `status='pending'` articles, calls the local
model, validates via M-07/M-08, writes results, flips status.

The model call sits behind one swappable adapter function — not inlined.

**AC**
- Picks up only `pending` articles, oldest first, honouring `--limit`.
- Success → `media_insights` row + status `analysed`.
- Invalid JSON → status `failed`, raw output retained for debugging.
- Timeout → article stays `pending` and is retried on the next run.
- Killing the worker mid-article leaves that article `pending`, never half-written.
- Every insight records `model_id` and `prompt_version`.
- Model invocation is isolated to one function; a fake adapter runs the whole
  worker in tests with no model present.

**Functional outcome:** after a run, every processed article is `analysed` or
`failed` — never silently skipped — and re-running processes only what's left.

---

### M-10 · Race attribution

Bind articles to a canonical race, or explicitly to none.

**AC**
- An article naming a current-season GP resolves to that `race_id`.
- General F1 news with no race resolves to NULL, not a guess.
- Driver aliases ("Max", "Verstappen", "VER") resolve via the `drivers` table.
- Ambiguous references resolve to NULL and are logged.
- Attribution accuracy is measured against ≥30 hand-labelled real articles and
  the result is recorded in the story.

**Functional outcome:** `SELECT * FROM media_insights WHERE race_id = ?` returns
that weekend's coverage, and unattributed news is excluded rather than
misfiled.

---

## Phase 3 — Roll up

### M-11 · Media digest generation

Stage 3a. One digest per race weekend from that weekend's insights, with
citations.

**AC**
- Migration creates `media_digests` with `UNIQUE (race_id, version)`.
- Generating a digest writes one row with non-empty `summary` and a `citations`
  JSON array.
- Every cited URL matches a real `media_articles` row for that race.
- Regenerating creates version n+1; the previous row is retained.
- A failed generation leaves the prior version as the newest.
- Zero insights for a race produces no digest row at all, not an empty one.

**Functional outcome:** each race weekend has at most one current digest, every
citation traces to a stored article, and a failure never destroys the last good one.

---

### M-12 · Wire comparison computation

Stage 3b, computation half. Pure functions deriving the media-versus-players
relationship. **No model.**

**AC**
- `compute_media_leaning()` aggregates tints weighted by `strength` into a
  per-driver ranking.
- `compute_comparison()` returns media leader, crowd leader, crowd share,
  divergence, and `alignment` of `aligned`/`mixed`/`contrasting`.
- `alignment` thresholds are documented and unit-tested at each boundary.
- Fewer than the configured minimum predictions returns `insufficient_data`,
  never a comparison.
- No media insights returns `no_media`, never a fabricated comparison.
- All functions are pure, with injected inputs.

**Functional outcome:** given a race, the app computes a true statement about
media-versus-crowd agreement, or explicitly reports it cannot.

---

### M-13 · Wire comparison phrasing and storage

Hand M-12's computed facts to the model for prose only. Store facts alongside.

**AC**
- Migration creates `wire_comparisons` with `UNIQUE (race_id, version)`.
- The model receives only computed facts — never raw articles or the pick table.
- Stored row contains both `facts` JSON and `summary` prose.
- `alignment` is copied from M-12's computation, never from model output.
- A generation whose prose names a different leading driver than `facts` is
  rejected and logged as a bug.
- `insufficient_data` or `no_media` writes no row.
- Regeneration versions; previous rows retained.

**Functional outcome:** the stored comparison always carries the arithmetic that
produced it, so any claim on screen is checkable after the fact.

---

## Phase 4 — Run it

### M-14 · Kubernetes CronJobs for the three stages

Three CronJob manifests in `base/`, added to `kustomization.yaml`, following the
existing `race-manager-cronjob.yaml` pattern.

Manifests only — no pipeline code changes. If a code change is needed here, the
portability rule was broken upstream.

**AC**
- Three CronJobs exist: fetch, analyse, digest+comparison.
- Each runs the same image with a different command; no new image.
- All config is env vars in the manifest — no values baked into code.
- Concurrency policy prevents overlapping runs of the same stage.
- Failed jobs are retained for log inspection.
- `test_cronjob_deployment.py` is extended to cover all three.
- `kubectl apply --dry-run=server` validates.

**Functional outcome:** the pipeline runs unattended on the cluster, and each
stage's failure is visible in job status without affecting the others.

---

### M-15 · End-to-end verification against real sources

Prove the whole pipeline on one real race weekend.

**AC**
- A documented command sequence runs all three stages against live feeds.
- After the run, for one real race: `media_articles` has rows from all three
  sources, `media_insights` covers them, `media_digests` has a digest whose
  citations resolve, `wire_comparisons` has a comparison whose `facts` match
  what M-12 recomputes.
- Every displayed claim traces back to an article URL by SQL alone.
- Re-running the whole pipeline creates no duplicate articles or insights.
- Interrupting each stage and resuming loses no data and double-counts nothing.
- Results are written up in the repo.

**Functional outcome:** one query path proves a rendered sentence traces to real
fetched articles — and the pipeline survives being run twice and interrupted.

---

## Not in this epic

- **UI rendering** of either panel. These stories end at the read model; the
  panels are separate front-end work.
- **BUD-187** (race-card hardening) continues independently — it is prediction
  side, unaffected by this design.
- **Event-driven triggering.** Polling now. The stage transitions are explicit
  state changes, so swapping in events later needs no redesign.
- **Retiring `src/source_access.py`** and its Reddit-only tests. Worth a
  separate cleanup story.
