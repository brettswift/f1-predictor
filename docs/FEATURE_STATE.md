# Feature state — where the app actually is

**Written 2026-09-12.** Snapshot of what is *shipped in code* versus what is
*planned in stories*, because those two had drifted far enough apart to cause
real confusion.

Rule for this doc, same as `SCORING_RULES.md`: **if this doc and the code
disagree, the code is right — fix the doc in the same change.**

---

## TL;DR

The app is a working F1 prediction game with two scoring pools, real accounts,
leagues, and automated result ingestion from OpenF1. **There is no sentiment
pipeline.** Not shipped, not stubbed, not started. The homepage has a
sentiment-shaped UI panel that renders an honest "awaiting data" empty state,
and a separate crowd-picks chart fed entirely by the app's own users'
predictions.

The word "sentiment" in this repo currently means two different things, and
conflating them is the single biggest source of confusion:

| Term | What it actually is | Status |
|---|---|---|
| **Crowd picks / "The Wire"** | Aggregate of *this app's own users'* podium predictions | **Shipped and live** |
| **Sentiment** | External signal from outside the app (news, fans) | **Does not exist** |

The shipped code goes out of its way to keep these apart. `src/app.py:1746`
carries the comment *"This is prediction data, not external sentiment"*, and
`src/templates/home.html:195` tells users *"Your race-card votes never
contribute to this chart."* There is a unit test, `test_sentiment_desk.py`,
whose whole job is asserting sentiment is never fabricated.

---

## Shipped and working

### Data & ingestion
- **OpenF1 is the only F1 data source** (`src/openf1.py`). Fully migrated off
  Ergast/Jolpica. Retry with backoff, then falls back to a last-known-good
  `api_cache` SQLite table; raises only if both upstream and cache fail.
  `OPENF1_OFFLINE` forces cache-only for tests.
- **`cron/race_manager.py`** — the one real ingestion job. State machine
  `watching → locked → polling → completed`, running every 5 min Fri–Mon.
  Locks races 6 min before start, polls for results, writes `results` (including
  safety-car counts), and scores every prediction.
- **`cron/refresh_drivers.py`** — weekly driver-roster refresh (prod overlay only).
- **`fetch_attempts` table** — observability log of every fetch outcome.

### The game
- **Podium predictions** — pick P1/P2/P3 (`POST /predict/<race_id>`). One
  submission per user per race; resubmits are *rejected*, not upserted.
- **Safety-car conviction pool** — optional bet from −100..+100. Sign is the
  call, magnitude is the stake. Multiplier is priced against crowd consensus at
  vote time and **frozen**. This one *does* allow re-voting.
- **Two-pool scoring** — podium points (10/6/4, +1 wrong-slot, max 20) and the
  SC pool (starts at 100 chips, floors at 0) are computed independently and only
  summed for display. Documented in `SCORING_RULES.md`.
- **Accounts** — magic-link email, Google OAuth, legacy anonymous-user migration.
- **Leagues** — a pure display filter over one global game, not separate scoring
  universes. Create/invite/join, multi-league membership, joined-at-round context.
- **Leaderboards** — global standings, dual ranking (total/average), head-to-head,
  per-race winner, season stats.
- **Synthetic personas** — 7 archetypes (`src/personas.py`) + season replay harness.

### Frontend
Server-rendered Jinja2, 26 templates, no JS framework. Main surfaces: home
(race desk), predict, races, race detail, live leaderboard, leaderboard, h2h,
leagues, profile, stats, plus admin/sim pages.

### Tests
50 files, ~13.8k lines: unit, integration, and Playwright e2e. Coverage is
genuinely good across scoring, locking, leagues, auth, and ingestion.

---

## Not shipped

- **Any sentiment ingestion.** Zero. No Reddit client, no RSS reader, no
  classifier, no NLP dependency, no `sentiment_batches` table. Grep for
  `reddit|praw|nlp|classifier` returns only comments *disclaiming* sentiment and
  test names guarding against fabricating it.
- **F1-53 "fans vs crowd"** — specified as fan-sentiment *beside* predictor
  agreement. Only the predictor half exists. The feature as scoped is not real.
- **Talking points** (BUD-186) — UI placeholder only.
- **Sentiment read API** (BUD-185) — not started.
- **`/admin/fast-forward` and `/admin/reset-season`** — flash a success message
  and change nothing. UI-only stubs (`src/app.py:3729-3741`).

### Dead code still in the tree
`cron/lock_races.py`, `cron/fetch_race_results.py`, `cron/scheduler.py` are all
superseded by `race_manager.py` and referenced by no active kustomization.
Candidates for deletion.

---

## The sentiment direction change (2026-09-12)

Brett's decision, superseding all prior E6 design:

> **No Reddit. No Twitter/X. No paid APIs.** Open F1 news RSS only, classified
> per driver by a local model.

**The 50/50 blend was considered and dropped the same day.** Blending press
coverage into The Wire produces a number nobody can verify. Final shape is two
separate panels: The Wire stays 100% player predictions, and a new Media panel
shows sourced press coverage with per-article driver tints. Full design in
**`MEDIA_PIPELINE.md`** — that is the authoritative spec for this work.

Three free, no-auth RSS feeds were live-verified against the Madrid/Spanish GP
weekend on 2026-09-12:

- `https://www.formula1.com/en/latest/all.xml`
- `https://www.motorsport.com/rss/f1/news/`
- `https://www.autosport.com/rss/f1/news/`

Items carry a stable `<guid>` and an RFC-822 `<pubDate>`. **Dedup keys off the
`guid`, not the date** — `pubDate` is freshness and ordering only.

Because classification is local-inference only, the previous $5/mo cloud-model
guardrail no longer applies. Because RSS needs no credentials, the Reddit
OAuth access gate is moot.

Full plan: `~/.openclaw/workspace/docs/f1-sentiment-pipeline-plan.md`
(workspace, not this repo).

### Conflicts this direction change creates

These must be resolved before E6 work restarts:

1. **`docs/SOURCE_ACCESS_GATE.md` (on unmerged branch
   `feat/BUD-189-source-access-gate`) should be retired.** Its rule that
   *"editorial coverage is never included in the fan-sentiment denominator"* is
   satisfied by the two-panel design, which has no fan-sentiment denominator at
   all — press coverage is labelled as press coverage. Site copy still needs to
   stop saying "fan sentiment" where it now means press.
2. **`src/source_access.py` and `tests/unit/test_source_access.py` are
   Reddit-only** and have no RSS equivalent. Both need replacing.
3. **Linear stories BUD-137, BUD-189, and the parent plan BUD-188 still describe
   the Reddit design.** Only BUD-136 was updated. BUD-189's entire premise
   (proving Reddit API access) is obsolete.

---

## How deployment actually works

Verified 2026-09-12 against the remotes and workflow files. Two repos are
involved and the branch names collide confusingly, so read this before assuming.

| Repo | Branch | Role |
|---|---|---|
| `f1-predictor` | `main` | The only deploy trigger. Merging here builds and ships. |
| `k8s_nas` | `live` | Default branch. ArgoCD watches it. CI writes the image digest here. |

**There is one environment.** Pushing to `f1-predictor@main` builds the image,
tags it `:live` plus a semver and a sha, then clones `k8s_nas`, pins the new
**digest** into `apps/f1-predictor/overlays/prod/kustomization.yaml`, and pushes
to `k8s_nas@live`. ArgoCD picks it up from there.

Two traps:

- **`live` is a branch of `k8s_nas`, not of `f1-predictor`.** "Deploy to live"
  is accurate, but the branch is in the infra repo. There is no `live` branch in
  this repo, and creating one would do nothing.
- **`f1-dev` does not exist on the remote and we do not deploy to dev.**
  `.github/workflows/build-f1-predictor-dev.yml` triggers on `push: [f1-dev]`,
  so it has never run. The `overlays/dev` kustomization exists but nothing
  deploys it. Treat dev as not a thing until someone deliberately revives it.

The "prod" naming inside `k8s_nas` paths and tags is historical — the workflow's
own header comment admits it deploys what is really a QA environment at
`f1.brettswift.com`. Don't read `overlays/prod` as meaning a separate production
tier exists.

---

## Known doc drift (unrelated to sentiment)

- **Deployment docs are stale.** `README.md`, `WORKFLOW.md`, and
  `DEPLOYMENT.md` describe an `f1-dev`/`live` two-branch GitOps flow with an
  ArgoCD PostSync polling hook. See the verified flow below; the polling hook is
  moot when CI pins the digest.
- **`DEPLOYMENT.md` still claims data comes from "Ergast/Jolpica."** It comes
  from OpenF1.
- **`docs/BACKLOG.md` is stranded** on the unmerged `docs/2026-relaunch-brief`
  branch and carries no DONE markers, despite most of E1/E2/E3/E5/E12 having
  shipped. Anyone reading `main` never sees it.
- **The safety-car pool mechanism has no prose documentation** anywhere.
  `SCORING_RULES.md` deliberately covers only podium scoring. The pool is
  inferable from code alone.
- **Sentiment docs exist only on an unmerged branch**, so "what does the design
  say" depends on which worktree you happen to be in.
- **Scoring is duplicated in 3 places** (`src/app.py`, `cron/race_manager.py`,
  and the dead `cron/fetch_race_results.py`) and must be kept in sync by hand.

---

## Worktree map

All of these are the same repo (`brettswift/f1-predictor`), different branches:

| Path | Branch |
|---|---|
| `~/src/f1-predictor` | `feat/BUD-187-race-card-hardening` |
| `~/src/f1-sentiment-production` | `feat/BUD-189-source-access-gate` |
| `~/src/f1-undercut` | older UI rebuild work |
| `~/src/f1-wire` | Wire chart work |
