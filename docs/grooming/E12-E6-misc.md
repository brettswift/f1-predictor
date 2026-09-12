## F1-110: Build the season replay harness on the real scoring path

**Tier:** T1 — **Dev:** Sonnet 5 → **QA:** Opus 5
**Epic:** E12 — Season Simulation & Synthetic Cohort [MVP] — Phase 0→1
**Depends on:** F1-10, F1-11, F1-12, F1-13, F1-14 (E2 accounts) — lands together with F1-111

### Description

Epic: E12 — Season Simulation & Synthetic Cohort (Phase 0→1)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* One command (e.g. `python3 -m scripts.replay_season --season 2026 --seed <N>`) produces a populated global leaderboard with ≥20 synthetic users across ≥10 already-scored rounds of the real 2026 season.
* The replay MUST call the production `calculate_score(prediction, result)` function (`src/app.py:823`) for every synthetic prediction. Directly `INSERT INTO scores` without routing through `calculate_score` fails review — verify by code inspection that no insert bypasses it, and empirically by corrupting one stored result and confirming the replay's score output changes exactly the way a live scoring run would.
* Synthetic predictions are written through the same `predictions` table/schema the live app uses (same shape as the insert in `predict()`, `src/app.py:972`), not a parallel synthetic-only table.
* `--seed <N>` produces byte-identical leaderboard output (same users, same picks, same scores) across two runs with the same seed; a different seed produces a different pick distribution.
* A teardown command purges every row the replay created (users, predictions, scores) and leaves the DB at its pre-replay row counts, verified by a before/after count diff.
* Replay consumes real historical race + result data already available via the existing fetch pipeline (`fetch_race_results_from_api`, `src/app.py:449`) for rounds that have actually happened in the 2026 season — no invented results.

## Notes

* Real scoring function: `calculate_score()` at `src/app.py:823` — exact P1/P2/P3 worth 10/6/4, +1 per correctly-picked driver in the wrong slot. Any replay implementation must call this, not reimplement the math, per BACKLOG.md's explicit "no bypassing of the production scoring code (a replay that mints scores directly proves nothing)."
* Current schema (`init_db()`, `src/app.py:118`) has `predictions`, `results`, `scores` keyed by `race_id` / `user_id` (`session_id`). There is no `users.email` or `is_synthetic` column yet — those arrive with E2 (F1-10) and F1-111 respectively.
* **Depends on:** E2 accounts (F1-10–F1-14), per BACKLOG.md §E12 sequencing: "F1-110/111/112 land immediately after E2 (accounts), because the replay harness is what makes F1-19's global leaderboard testable as a leaderboard rather than a list of one." This story also assumes F1-111's `is_synthetic` column exists at the point synthetic users are created (see F1-111's "ships with the first persona, never retrofitted" rule) — build them together.
* **Open question:** the backlog doesn't say whether F1-110 and F1-111 should be one PR or two coordinated ones. Given F1-111's "never retrofitted" language, recommend landing them together rather than sequencing F1-110 to completion first.

---

## F1-111: Flag and isolate synthetic users at creation

**Tier:** T1 — **Dev:** Sonnet 5 → **QA:** Opus 5
**Epic:** E12 — Season Simulation & Synthetic Cohort [MVP] — Phase 0→1
**Depends on:** F1-10 (E2 accounts, real email to guarantee against); lands together with F1-110

### Description

Epic: E12 — Season Simulation & Synthetic Cohort (Phase 0→1)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* The `users` table (or its E2 successor) gains an `is_synthetic` column, set at row-creation time for every replay/persona-created user — never set retroactively via `UPDATE`. Verify by inspecting the insert path used by F1-110/F1-112 persona creation.
* Analytics (F1-90) excludes `is_synthetic = 1` rows — verify the analytics query/event pipeline (once F1-90 exists) carries a `WHERE is_synthetic = 0` clause or equivalent filter.
* Ad-eligibility traffic counts (F1-81) exclude synthetic users — same verification pattern against F1-81's traffic-count query.
* Public leaderboards (F1-19) exclude synthetic users by default; an internal/admin view may show them for QA, but the default user-facing leaderboard query must filter them out.
* One command purges all `is_synthetic = 1` rows (users, predictions, scores) — this should be the same purge path F1-110 already needs, not a second one.
* **Hard guarantee: no email is ever sent to a synthetic user.** Verify by inspecting every email-sending call site (magic-link auth F1-10, pre-lock reminders F1-33) for an `is_synthetic` check before send, AND by a test that creates a synthetic user, triggers the reminder-email code path with the mail sender mocked, and asserts the mock was never invoked for that user.
* This ships in the same story batch as the first persona (F1-110/F1-112) — a PR that introduces synthetic users without this flag already in place fails review.

## Notes

* BACKLOG.md §7 Risks is explicit: "the failure mode is silent, so the flag ships with the first persona, never retrofitted." Treat this as a hard sequencing gate, not a preference.
* No `is_synthetic` column exists in the current schema (checked `init_db()`, `src/app.py:118`); add it via `_apply_migrations()` (`src/app.py:233`), following the existing migration pattern rather than a fresh `CREATE TABLE`.
* Purge should run again before public launch per BACKLOG.md §E12 "Retirement" note (F1-104's window) — out of scope here, just worth the cross-reference.
* **Depends on:** E2 (F1-10) for the email surface this story guarantees against; co-lands with F1-110.

---

## F1-112: Implement the seven persona archetypes with AC traceability

**Tier:** T3 — **Dev:** DeepSeek Flash → **QA:** Kimi K2.7
**Epic:** E12 — Season Simulation & Synthetic Cohort [MVP] — Phase 0→1
**Depends on:** F1-110, F1-111

### Description

Epic: E12 — Season Simulation & Synthetic Cohort (Phase 0→1)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* All seven archetypes from BACKLOG.md §E12 are implemented, matching the list exactly: **chalk-picker** (always picks championship order), **homer** (always favors one driver), **contrarian**, **lapsed user** (misses ~40% of races), **latecomer** (joins at round 15), **solo-only** (never joins a league), **multi-league member**.
* Each archetype's pick-generation is deterministic given the harness's `--seed` (F1-110), so persona behavior is reproducible run to run.
* A literal traceability table (in the story or a linked doc, not asserted informally) maps each archetype to the specific E3/E5 multi-user ACs it exercises — e.g. latecomer → F1-28 ("joined at round X" context), multi-league member → F1-25, lapsed user → F1-23's "minimum 2 scored races" Average-ranking rule, solo-only → F1-19's "no empty states for non-league users."
* Every multi-user AC listed in BACKLOG.md §E3 (F1-19, F1-22, F1-23, F1-25, F1-27, F1-28) and §E5 (F1-41) is covered by at least one archetype in that table — an uncovered AC is a failed AC for this story, not a nice-to-have gap.
* All archetype-created users are created with `is_synthetic = 1` (F1-111) from the moment they're created.

## Notes

* Traceability requirement is a direct quote from BACKLOG.md §E12: "Every multi-user AC in E3 and E5 is exercised by at least one archetype — traceability documented in the story."
* **Depends on:** F1-110 (the harness personas run through) and F1-111 (the synthetic flag must exist before any persona user is created).

---

## F1-116: Fast-forward a full race weekend in under 5 minutes

**Tier:** T2 — **Dev:** Kimi K2.7 → **QA:** Sonnet 5
**Epic:** E12 — Season Simulation & Synthetic Cohort [MVP] — Phase 0→1
**Depends on:** none — independent, pull forward ahead of E2/E3

### Description

Epic: E12 — Season Simulation & Synthetic Cohort (Phase 0→1)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* One command runs a full simulated race weekend — lock → results ingest → score → leaderboard update — end to end in under 5 minutes wall-clock.
* State transitions go through `f1-mock-api`'s real admin endpoints, not hand-written fixtures: `POST /admin/race/<id>/start` (sets `start_override`), `POST /admin/race/<id>/podium` (sets P1/P2/P3), `POST /admin/race/<id>/finish` (flips `has_results`), and `POST /admin/reseed` for a clean slate (`f1-mock-api/src/app.py:456-519`).
* Time progression uses `tests/utils/time_control.py`'s `TimeController` — `freeze()` / `advance()` / the `frozen()` context manager, which patches `app._now_utc` — to fast-forward through lock time and the results-check delay, not real `sleep()`.
* At the end of the run, the race's `status` is `completed`, a `scores` row exists per participating user for that race (via `calculate_score()`, `src/app.py:823`), and the leaderboard reflects the new totals — inspectable via direct DB query or the `/leaderboard` route.
* Lives alongside the existing suite layout (`tests/integration/`, `tests/e2e/`) and is runnable repeatedly (idempotent or self-cleaning between runs).

## Notes

* This is mostly composition of infrastructure that already exists: `tests/utils/time_control.py` (freeze/advance/jump_to, patches `app._now_utc`) and `f1-mock-api/src/app.py`'s admin routes do the two hard parts already; this story wires them into one command.
* BACKLOG.md §E12 calls this out as worth pulling forward specifically because "it pays for itself the first time it catches a lock bug on a Tuesday instead of a Sunday," and because it turns F1-04/F1-05's real-race-weekend ACs into "confirmation rather than discovery." Recommend scheduling this early even though it's filed under E12.
* No hard dependency chain — the backlog explicitly frames it as independent of the F1-110/111/112 → F1-113/114/115 sequencing.

---

## F1-50: Open F1-news RSS ingestion cron for race-weekend-aware sentiment sourcing

**Tier:** T2 — **Dev:** Kimi K2.7 → **QA:** Sonnet 5
**Epic:** E6 — Sentiment Dashboard [MVP] — Phase 2
**Depends on:** none (feeds F1-51)

### Description

Epic: E6 — Sentiment Dashboard (Phase 2)
**Revised 2026-09-12 per Brett: no Reddit, no Twitter/X. News-only, open (free) sources only — no paid news API.**

Live-verified 2026-09-12 against the real Madrid/Spanish GP race weekend — all three feeds are free, public RSS, no auth, no scraping:

* `https://www.formula1.com/en/latest/all.xml`
* `https://www.motorsport.com/rss/f1/news/`
* `https://www.autosport.com/rss/f1/news/`

Sample pulled live from Motorsport.com during Madrid GP quali weekend: dated, driver-attributable items ("F1 Spanish GP: Lando Norris takes stunning pole at Madring", "Lewis Hamilton under fire after ignoring Ferrari calls..."), each with a stable numeric `<guid>` and an RFC-822 `<pubDate>`.

## Acceptance Criteria

* A cron job polls the three RSS feeds above on a 2–3 hour cycle.
* The cycle is race-weekend aware: polling frequency increases near sessions (practice/quali/race) relative to the off-weekend baseline — verify via a config or scheduling condition tied to the race calendar, not a flat fixed interval.
* Each article is stored with: feed source, article `<guid>` (or link if a feed lacks one), title, `<pubDate>`, and enough raw text (full article or RSS-provided summary/description) for downstream driver attribution and classification.
* **Dedup keys off the stable per-article `guid`/link, not the date** — `pubDate` is used for freshness/staleness display only, never as a uniqueness key (a feed can carry multiple dated items in one poll cycle; re-polling an unchanged feed must not re-ingest already-seen guids).
* A fetch failure (feed unreachable, malformed XML, timeout) logs/alerts but does not crash the cron and does not block prediction/scoring functionality — sentiment degrades gracefully rather than taking the site down with it.

## Notes

* No existing news-ingestion code in `~/src/f1-predictor` — this is net-new. Follow the existing cron pattern under `cron/` (e.g. `cron/race_manager.py`) and the k8s CronJob style already used in `base/*-cronjob.yaml` for consistency.
* Superseded direction: this story previously specified Reddit's official API (r/formula1, r/F1Technical). Brett ruled out Reddit and Twitter/X entirely on 2026-09-12 — replaced with the open RSS sources above.
* Feed list can grow (more open F1 RSS sources) without re-architecting — treat the three URLs as config, not hardcoded.

---

## F1-51: Extract per-driver news sentiment with a local model, blended 50/50 with prediction-vote sentiment

**Tier:** T2 — **Dev:** Kimi K2.7 → **QA:** Sonnet 5
**Epic:** E6 — Sentiment Dashboard [MVP] — Phase 2
**Depends on:** F1-50

### Description

Epic: E6 — Sentiment Dashboard (Phase 2)
**Revised 2026-09-12 per Brett: sentiment is a 50/50 blend of (a) news-article classification and (b) the app's own podium + safety-car prediction votes — not a Reddit-derived score.** "App voting" is not a new feature — it's the existing prediction picks (podium order + safety-car call) already collected by the core game; F1-53's "N% of predictors agree" crowd module *is* the voting half of this blend, so this story wires the news half to combine with it, not stand alone.

## Acceptance Criteria

* For each (race, driver) pair, a structured JSON record is produced containing: a news-sentiment sub-score (from local-model classification of that race weekend's ingested articles mentioning the driver), a vote-sentiment sub-score (from the existing podium/safety-car prediction distribution for that driver, per F1-53's crowd-consensus data), and a blended score computed as a straight 50/50 average of the two sub-scores.
* A driver with zero news mentions in a cycle but nonzero prediction votes still gets a valid blended score (news sub-score treated as neutral/absent, not a crash or a dropped record) — and vice versa for a driver with news coverage but low pick volume.
* Records are stored per (race, driver, timestamp) — appended, not overwritten — so a sentiment trend over time can be reconstructed later (feeds F1-52).
* Local-model classification runs on-box (existing local inference, per project convention — no per-run cloud cost to track; the prior $5/mo DeepSeek-cloud budget guardrail no longer applies since this doesn't call a paid API).
* Output is valid, parseable JSON that downstream code (F1-52, F1-53) can consume directly, without ad hoc string-scraping of model output.

## Notes

* Superseded direction: this story previously specified DeepSeek v4 Flash (paid cloud) with a <$5/mo guardrail, scoring Reddit text only. Brett revised 2026-09-12: local model only, no cloud sentiment-extraction spend, blended with existing vote data rather than replacing it.
* **Depends on:** F1-50 for the news half's source-traceable raw text; depends on the existing podium/safety-car prediction pipeline (F1-53's data) for the vote half — confirm F1-53's per-driver vote aggregation is queryable before starting, don't re-derive it.
* The exact local model/runtime is discovered at implementation time per the pipeline plan's "Runtime/deployment" section (`docs/f1-sentiment-pipeline-plan.md`) — not hardcoded here.

---

## F1-02a: Prototype the remaining Pit Wall screens for a cold visitor

**Tier:** T3 — **Dev:** DeepSeek Flash → **QA:** Kimi K2.7
**Epic:** E0 — Design Prototype [MVP] — Phase 0
**Depends on:** F1-00b (done — Pit Wall direction decided)

### Description

Epic: E0 — Design Prototype (Phase 0)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* Static HTML prototypes exist for four screens: sign-up/magic-link, league invite landing (cold-visitor view — not signed in, doesn't know anyone in the league), post-race results + share card, and empty states (pre-season, no picks yet, brand-new/empty league).
* All new screens reuse the Pit Wall CSS/JS base from `/tmp/f1-mock-a` (per BACKLOG.md's note that F1-00c's sentiment sandbox "shares A's exact CSS/JS base") rather than introducing a new visual language.
* No build step, no dependencies — self-contained HTML/JS per screen, matching the existing `/tmp/f1-mock-a` / `/tmp/f1-mock-b` structure.
* Published via `python3 ~/.openclaw/workspace/scripts/publish_ephemeral.py <slug> <dir> --ttl-days 90`, reachable at `lab.home.brettswift.com/<slug>/` (LAN); if external reachability is needed, call the backend's `/public/<slug>` endpoint directly — the CLI has no `--public` flag yet (see Notes).
* Verbatim backlog AC: "a stranger could click through the whole product" — verify by clicking invite-landing → sign-up → mock first pick → results/share-card without a dead link or an unstyled page.

## Notes

* Direction is locked: BACKLOG.md §E0, "F1-00b DONE... Pit Wall wins." This story is not a re-litigation of aesthetic direction, only new screens in that established language.
* `/tmp/f1-mock-a` is the **locked reference — no further edits** per BACKLOG.md; build this story's screens in a new/sibling directory rather than editing it in place.
* Known infra gap (not this story's job to fix): `~/.openclaw/workspace/scripts/publish_ephemeral.py` has no `--public`/`--unpublic` flags despite `skill/ephemeral-publish/SKILL.md` documenting them; the backend `/public/<slug>` endpoint works when called directly. BACKLOG.md flags this as "small fix, not urgent" — mention it if the workaround is needed, don't scope-creep into fixing it here.
* Sentiment placement decision (Brett, §E0): sentiment appears on home/race and predict pages, never on league pages. Not central to this story's screen list, but keep any incidental sentiment UI consistent with that if it appears.

---

## F1-31: Safety-car bonus call, scored from F1-07 data

**Tier:** T2 — **Dev:** Kimi K2.7 → **QA:** Sonnet 5
**Epic:** E4 — Prediction Experience v2 [MVP] — Phase 3 (design in Phase 2)
**Depends on:** F1-07 (safety-car ingestion, tracked in Linear as BUD-129)

### Description

Epic: E4 — Prediction Experience v2 (Phase 3, design in Phase 2)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* The prediction UI shows a distinct "bonus call" element (yes/no safety car, optional "how many" tiebreaker) visually separated from the podium picker — not rendered as another dropdown/row in the main form.
* Scoring reads from the safety-car field ingested by F1-07 (`race_control`-derived, stored per race per BACKLOG.md §E1) — verify the scoring code queries that stored value rather than re-deriving it from raw OpenF1 data.
* A user can submit the safety-car prediction independently of (before or alongside) their podium prediction, and it locks at the same lock time as the podium pick for that race.
* Safety-car scoring is additive and tracked as a separate line item wherever race-by-race scores are shown — it does not modify or get folded into `calculate_score()` (`src/app.py:823`), which remains podium-only.

## Notes

* **Depends on:** F1-07 (already tracked in Linear as BUD-129) — the safety-car occurrence/count field is this story's only data input. Don't start until BUD-129 has shipped and produced data for at least one completed race.
* BACKLOG.md's UI framing — "shown as a distinct 'bonus call' in the UI, not another form row" — is a direct product decision from the relaunch brief; don't default to appending a plain form field.
* **Open question:** BACKLOG.md doesn't specify point values for correct safety-car yes/no or the "how many" tiebreaker. This likely belongs in F1-40 (scoring v2, documented and versioned) — get that decision made there before finalizing this story's scoring code.

---

## F1-33: Opt-in pre-lock reminder emails, ~24h before lock

**Tier:** T2 — **Dev:** Kimi K2.7 → **QA:** Sonnet 5
**Epic:** E4 — Prediction Experience v2 [MVP] — Phase 3 (design in Phase 2)
**Depends on:** F1-10 (E2 email/magic-link accounts)

### Description

Epic: E4 — Prediction Experience v2 (Phase 3, design in Phase 2)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* Reminder emails are opt-in, default off — verify a stored per-user preference flag, not a global toggle.
* Exactly one email is sent per race, approximately 24h before that race's lock time, derived from the same lock-time source `auto_lock_races()` (`src/app.py:44`) uses — not a separately hardcoded schedule.
* The email contains a one-tap deep link landing the user directly on that race's predict page, authenticated (or magic-link-authenticated) — not a generic homepage link.
* Unsubscribing (email footer or account settings) is honored: a test that opts a user out confirms no further reminder is sent for a subsequent race.
* Cross-cutting with F1-111: this send path checks `is_synthetic` before sending and never emails a synthetic user — this is one of the call sites F1-111's hard guarantee requires.

## Notes

* **Depends on:** F1-10 (E2 email/magic-link auth) — there's no address to send to until real accounts exist.
* BACKLOG.md §E12 notes a side benefit: F1-114 (persona race-calendar trigger, ~26h before lock) "validates F1-33's pre-lock reminder timing for free" — worth sequencing so both use the same calendar-derived lock-time calculation to actually get that validation.

---

## F1-41: Permanent season-stats profile page

**Tier:** T2 — **Dev:** Kimi K2.7 → **QA:** Sonnet 5
**Epic:** E5 — Scoring, Stats & Streaks [MVP] — Phase 1
**Depends on:** F1-40 (scoring v2), F1-10 (E2 accounts)

### Description

Epic: E5 — Scoring, Stats & Streaks (Phase 1)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* Every signed-in user has a permanent season-stats page showing at minimum: overall accuracy, best race (highest single-race score), and rank history (global rank over time, race by race).
* Rank history and accuracy are derived from the same `scores`/leaderboard data source as the global leaderboard (BACKLOG.md §3.5 "one game, many views" — no separate/forked stats computation) — verify the profile query reuses the leaderboard's scoring source rather than recomputing independently.
* The page persists across devices/sessions — tied to the real account (F1-10), not a session cookie, per BACKLOG.md's E2 rationale that "clear cookies = lose your history."
* The page is reachable and populated for a solo user with no league membership, consistent with F1-19's "solo users are first-class" rule — no empty/broken state for a non-league user.

## Notes

* **Depends on:** F1-40 (scoring v2 must be documented/versioned before "accuracy" has a stable, citable definition) and F1-10 (real accounts, for cross-device permanence).
* This is also one of F1-112's traceability targets — the lapsed-user and latecomer archetypes exist specifically to exercise non-trivial, gap-y "rank history" and "best race" data. Coordinate with F1-112's traceability table once both are in flight.

---
