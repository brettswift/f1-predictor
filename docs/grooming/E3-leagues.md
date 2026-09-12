## F1-19: Global leaderboard as a first-class destination

**Tier:** T2 — **Dev:** Kimi K2.7 → **QA:** Sonnet 5
**Epic:** E3 — Leagues as views over the global game [MVP] — Phase 1
**Depends on:** F1-10

### Description

Epic: E3 — Leagues as views over the global game (Phase 1)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* A signed-in user with zero league memberships can reach the global leaderboard from primary navigation (not a fallback/error state) and see their global rank; test: create a solo user, GET the leaderboard route, assert HTTP 200 with a rank row for that user.
* No page reachable by a solo (non-league) user contains copy gating them on league membership (e.g. "join a league to see your rank" / "join a league to continue"); test scans rendered output of the leaderboard, home, and live pages for a solo user against a denylist of such phrases.
* A regression test locks in the invariant for the rest of the epic: once F1-20/F1-21/F1-25 add league tables, a solo user's global `total_score` on the leaderboard is unchanged (same integer) before and after those features exist in the schema.
* Global standings continue to be computed by summing the single `scores` table (no per-league computation path introduced by this story) — verified by inspection that the query is still `SUM(points) ... GROUP BY user` with no `league` join.

## Notes

* Existing route: `src/app.py:1040` (`/leaderboard`), template `templates/leaderboard.html`. It already computes global standings from `scores` joined to `users`/`races` and is currently ungated — the risk this story guards against is regression once league tables and routes land later in this epic.
* Add the non-regression test somewhere under `tests/` (check `tests/conftest.py` for existing fixtures) so it runs in CI as later E3 stories merge — this is the epic's anchor/backstop, everything else layers on top of it.
* This story does not implement the Average ranking mode — that toggle mechanism (and the >=2-scored-races rule) is F1-23's scope, applied to this surface and to league standings (F1-22) together.
* Depends only on F1-10 (E2, real accounts) for a stable authenticated identity — not on any other league story, since it's the baseline the rest of the epic must not break.

---

## F1-20: Create league (name, emoji/color, scoring window)

**Tier:** T2 — **Dev:** Kimi K2.7 → **QA:** Sonnet 5
**Epic:** E3 — Leagues as views over the global game [MVP] — Phase 1
**Depends on:** F1-10

### Description

Epic: E3 — Leagues as views over the global game (Phase 1)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* Any signed-in user can create a league by submitting a name (required) and an emoji-or-color value (required); a new league record is created with that user recorded as admin.
* Creating a league inserts zero rows into `predictions` or `scores` — test snapshots row counts of both tables immediately before and after league creation and asserts no change.
* When no scoring window is explicitly chosen, the league resolves to "current round forward": test creates a league with no window param, then asserts the stored start round equals the round of the current open/next race at creation time (per the same status logic `get_next_open_race`/`get_races_with_computed_status` in `src/app.py`).
* Choosing "whole season" explicitly is a distinct, testable option (stored as a sentinel, e.g. `NULL` start round or a `whole_season` flag) distinguishable from a league whose current-round-forward window happens to start at round 1.
* The creating user is a member of the league immediately (visible via a membership lookup) with no separate self-invite step required.

## Notes

* No `leagues` table exists yet in `src/app.py`'s schema (`init_db`, `src/app.py:118-227`) — this is net new. Suggested shape: `leagues(id INTEGER PK, name TEXT NOT NULL, emoji_or_color TEXT, start_round INTEGER, whole_season BOOLEAN DEFAULT 0, admin_user_id TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)`.
* `start_round` should reference `races.round` (`src/app.py:158`, an integer, season-relative) rather than `races.id`, since §3.5's scoring window is described in round terms.
* Reuse the existing "what's the current round" logic (`get_next_open_race`, `src/app.py:879`; `get_races_with_computed_status`, `src/app.py:864`) rather than re-deriving race status.
* Depends on F1-10 for a stable `admin_user_id`. Under the current legacy scheme, identity is `users.session_id` (`src/app.py:124-129`); confirm against F1-10's actual output column before wiring the FK.
* **Open question:** whether emoji and color are one free-form field or two separate columns is a design-system detail not decided in the backlog — left to implementer/E7 discretion; the AC only requires the value(s) round-trip.

---

## F1-21: Invite link with join flow optimized for cold users

**Tier:** T2 — **Dev:** Kimi K2.7 → **QA:** Sonnet 5
**Epic:** E3 — Leagues as views over the global game [MVP] — Phase 1
**Depends on:** F1-10, F1-20

### Description

Epic: E3 — Leagues as views over the global game (Phase 1)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* A league admin can generate an invite link/token scoped to exactly that league.
* Opening the invite link as a logged-out stranger renders the league name, current member list, and a path into making a first prediction, without a login/signup wall appearing first — test: unauthenticated GET on the invite URL returns 200 with league name + member list + a predict-flow CTA in the body, not a redirect to a login route.
* Accepting the invite while authenticated adds the user to the league's membership exactly once, even if the invite link is opened and accepted twice — test hits the accept action twice and asserts the membership row count for that user+league is 1, not 2.
* A user who views but never accepts an invite still has a fully working global-game account: test asserts such a user's `/leaderboard` (F1-19) entry renders correctly with no league-related errors.
* A given invite token is scoped to its one league: accepting it never creates a membership row in any other league.
* At accept time, the member's join round (current round at that moment, per the same resolution used in F1-20) is recorded — this is the data F1-28 reads.

## Notes

* Net new: an invite mechanism, e.g. `league_invites(token TEXT PRIMARY KEY, league_id INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)`, plus the membership table `league_members(league_id INTEGER, user_id TEXT, joined_at_round INTEGER, is_admin BOOLEAN DEFAULT 0, joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (league_id, user_id))`.
* `joined_at_round` should use the identical "current round" resolution introduced in F1-20 so the two stories don't disagree on what round a join happened in.
* Depends on F1-20 for a league to invite into, and on F1-10 for account creation to be possible mid-flow for a cold/unauthenticated visitor.
* **Open question:** invite token expiry (none vs. TTL) isn't specified in the backlog. Default to non-expiring unless told otherwise; note the choice in the PR.

---

## F1-22: League standings as a filtered view of global scores

**Tier:** T1 — **Dev:** Sonnet 5 → **QA:** Opus 5
**Epic:** E3 — Leagues as views over the global game [MVP] — Phase 1
**Depends on:** F1-10, F1-20, F1-21

### Description

Epic: E3 — Leagues as views over the global game (Phase 1)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* League standings are produced by filtering the same `scores` table the global leaderboard reads (e.g. `WHERE user_id IN (SELECT user_id FROM league_members WHERE league_id = ?)`), with no separate scoring computation, write path, or cache introduced for leagues.
* For a fixture with >=2 leagues sharing >=1 common member and >=2 completed races, a test asserts that member's per-race points shown in every league's standings they belong to are byte-identical (same value) to the points shown on the global leaderboard for the same race, for every race in the comparison.
* League standings respect the league's scoring window: with a league whose `start_round` is round 5 and a member scored on rounds 1–10, the league total equals the sum of rounds 5–10 while that same member's global total (F1-19) equals the sum of rounds 1–10, in the same test run.
* A "whole season" league's total for a member equals their global total for the season (no round floor applied), proven in the same fixture style.
* Recomputing a score once (e.g. a results correction via the existing `enter_results` admin path, `src/app.py:1496`) updates the value identically in the global view and in every league view on the next read — no view holds a stale cached copy.
* Inspection/grep confirms no write path inserts into `scores` (or an equivalent) keyed by `league_id` — scoring writes remain `(user_id, race_id, points)` only, league-agnostic.

## Notes

* This is the load-bearing story for §3.5 ("one game, many views"). It must read the existing `scores` table as-is (`src/app.py:193-202`: columns `user_id, race_id, points`, `UNIQUE(user_id, race_id)`) — do not add a `league_id` column to `scores`, and do not introduce a parallel per-league scores table.
* Mirror the existing global aggregation pattern rather than inventing a new query shape: `src/app.py:1071-1090` (`/leaderboard`) does `LEFT JOIN scores s ON u.session_id = s.user_id ... LEFT JOIN races r ON s.race_id = r.id ... GROUP BY u.session_id`. The league version is the same shape plus `WHERE u.session_id IN (league members)` and, when not whole-season, `AND r.round >= league.start_round`.
* Scoring window comparisons are round-based (`races.round`, `src/app.py:158`), not calendar/year-based like the existing season filter (`strftime('%Y', r.date)`, `src/app.py:1067`) — don't reuse the year-string filter pattern here.
* Per `docs/f1-model-assignment.md`'s standing rule 2, this story gets the automatic T1 tier bump: it's directly on the scoring/aggregation path, and a global/league divergence would be a silently-wrong-answer failure, not a crash.
* Depends on F1-20 (league + `start_round` must exist) and F1-21 (need >=2 real members in a league for "filtered view" to mean anything testable).

---

## F1-23: Dual ranking modes everywhere (global + league): Total and Average

**Tier:** T2 — **Dev:** Kimi K2.7 → **QA:** Sonnet 5
**Epic:** E3 — Leagues as views over the global game [MVP] — Phase 1
**Depends on:** F1-10, F1-19, F1-22

### Description

Epic: E3 — Leagues as views over the global game (Phase 1)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* Both the global leaderboard (F1-19) and every league standings view (F1-22) expose two ranking modes: Total (sum of in-window points) and Average (points per scored race, in-window).
* The selected mode persists per user across page loads without relying solely on a query-string parameter: test sets the mode, reloads the page with no mode param, and asserts the previous mode is still applied.
* A user with fewer than 2 scored races in the relevant window (global or a given league's window) does not appear in the ranked Average list, while still appearing normally in Total mode — test fixture: a user with exactly 1 scored race.
* A user with exactly 2 scored races does appear ranked in Average mode — boundary test at the >=2 threshold (not >2).
* Average is computed only over races within the same window used for that view's Total (global = all races; league = that league's `start_round` onward) — a league's narrower window does not change what counts toward that member's *global* Average.

## Notes

* No ranking-mode toggle exists today; the current `/leaderboard` (`src/app.py:1040`) computes only `COALESCE(SUM(s.points), 0)`. This story adds an average/count-gated query variant against the same `scores`/`races` join, plus a persisted per-user preference (e.g. a new `users` column, or an account-tied cookie — implementer's choice, but it must survive a fresh load per the AC).
* The >=2-races rule is evaluated per window, so a member's global Average and their League-X Average can have different qualifying race counts when League X's window is narrower.
* Depends on F1-19 (global surface to attach the toggle to) and F1-22 (league surface to attach the toggle to) — this story wires the same mechanism into both, it doesn't invent either surface.

---

## F1-24: Per-race winner surfaced on every race, globally and per league

**Tier:** T3 — **Dev:** DeepSeek Flash → **QA:** Kimi K2.7
**Epic:** E3 — Leagues as views over the global game [MVP] — Phase 1
**Depends on:** F1-10, F1-19, F1-22

### Description

Epic: E3 — Leagues as views over the global game (Phase 1)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* Every completed race shows a "Race winner: <user>" line for the global scope, computed as the user(s) with `MAX(points)` for that `race_id` in `scores`.
* Each league a race applies to shows the same per-race-winner computation filtered to that league's members, reusing the membership filter from F1-22 rather than a separate winner-calculation path.
* A tie for max points on a race is handled without crashing the page (e.g. all tied users listed) — tested with a fixture where two users share the max points for one race.
* A user who joined a league yesterday with only one scored race can still be shown as that race's winner if their points for that race are the max — the winner calculation is not gated by league tenure beyond the normal in-window filter.

## Notes

* Net new UI element; natural homes are `templates/race_detail.html` and/or `templates/leaderboard.html`. The existing per-race score lookup pattern already exists at `src/app.py:1105-1109` and `src/app.py:1292` (`SELECT user_id, points FROM scores WHERE race_id = ?`) — the winner is just `MAX(points)` over that same query, optionally with the league membership filter added.
* Depends on F1-19 (global scope) and F1-22 (league-filtered scope) since this runs in both.

---

## F1-25: Multi-league membership

**Tier:** T2 — **Dev:** Kimi K2.7 → **QA:** Sonnet 5
**Epic:** E3 — Leagues as views over the global game [MVP] — Phase 1
**Depends on:** F1-10, F1-21, F1-22

### Description

Epic: E3 — Leagues as views over the global game (Phase 1)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* A user can hold membership rows in N>=2 leagues at once: test creates 3 leagues and joins one user to all 3, then asserts 3 membership rows exist for that user.
* A single prediction (one `predictions` row for a given user+race) contributes to every league that user belongs to for that race, with no re-entry of picks and no duplicate `predictions`/`scores` rows created per league: test has a user in 3 leagues submit one prediction, results get entered, and asserts all 3 leagues' standings plus the global leaderboard show the identical point value for that race, sourced from a single `scores` row.
* A league switcher lists all of a user's leagues, and navigating between them requires no re-authentication and no re-prediction.
* Leaving or being removed from one league (F1-26) does not change the user's standing or membership in their other leagues: test removes the user from league A and asserts league B and the global leaderboard are unchanged.

## Notes

* This is the direct proof of the §3.5 invariant "you can belong to many leagues at once ... one pick counts everywhere." Reuse the same fixture as F1-22's cross-view equality test, extended to N=3 leagues, rather than building a second fixture from scratch.
* Depends on F1-21 (a join flow that can add a user to more than one league) and F1-22 (per-league standings must already be correct before "multi" is meaningfully verifiable).

---

## F1-26: League admin basics: rename, remove member, transfer admin, delete, leave league

**Tier:** T2 — **Dev:** Kimi K2.7 → **QA:** Sonnet 5
**Epic:** E3 — Leagues as views over the global game [MVP] — Phase 1
**Depends on:** F1-10, F1-20

### Description

Epic: E3 — Leagues as views over the global game (Phase 1)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* League admin can rename the league; membership, scores, and predictions rows are untouched by the rename (row counts unchanged before/after).
* League admin can remove a member: the member's row is deleted from league membership, but their rows in `predictions` and `scores` are untouched and their global leaderboard total (F1-19) is unchanged — test asserts unchanged `predictions`/`scores` row counts and unchanged `/leaderboard` total for that user.
* League admin can transfer admin status to another member; after transfer, the ex-admin's admin-only actions (rename/remove/delete) are rejected, and the new admin's are accepted — tested both ways.
* League admin can delete the league: this removes the league's own rows (league + membership + invites) but a test explicitly asserts zero rows are removed from `predictions`, `scores`, `races`, or `results`, and every former member's global leaderboard total is unchanged before/after deletion.
* Any member, including the admin, can leave a league voluntarily. If the sole admin leaves, the story defines and tests one specific behavior (e.g. auto-transfer to another member, or block-until-transferred) — the league is never left in an admin-less state with members still present.
* All actions above are self-serve via UI/API; no route or workflow requires manual (Brett-side) database intervention.

## Notes

* Deletion safety is the highest-risk part of this story per §3.5 ("deleting a league never deletes predictions or global history") — write that test first. Scope any cascade deletes to `leagues`/`league_members`/`league_invites` only; do not add `ON DELETE CASCADE` from `predictions` or `scores` toward any league table.
* **Open question:** sole-admin-leaves behavior isn't decided in the backlog. Pick one approach, document it in the PR description, and cover it with a test rather than leaving it unhandled.
* Depends on F1-20 (a league and its admin concept must already exist).

---

## F1-27: Head-to-head view: any two users' season accuracy compared

**Tier:** T2 — **Dev:** Kimi K2.7 → **QA:** Sonnet 5
**Epic:** E3 — Leagues as views over the global game [MVP] — Phase 1
**Depends on:** F1-10, F1-19

### Description

Epic: E3 — Leagues as views over the global game (Phase 1)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* Given any two user identifiers, a head-to-head page shows both users' season stats (e.g. total points, average points per race, count of races each out-scored the other) computed from the shared global `scores` table.
* The view works for two users who have never shared a league (no common `league_members` row) — test asserts the route still returns a valid comparison for such a pair.
* The page is linkable: a stable URL (e.g. `/h2h/<user_a>/<user_b>`) returns equivalent content on repeat GETs with no required prior navigation state.
* Per-race points in the comparison match each user's individual per-race points exactly as shown on the global leaderboard's score matrix (same values as the `src/app.py:1101-1109` pattern) for the same race — no separate scoring computation.

## Notes

* Net new route/template. Reuse the per-user, per-race score lookups already patterned in `src/app.py` (`score_matrix` construction, lines 1101-1109) rather than writing a new aggregation.
* Depends only on F1-19/global scores existing — deliberately not on any league story, per the backlog ("works globally, not just within a league").
* **Open question:** discovery UX (link generated from a league roster vs. a user-search entry point) isn't specified in the backlog; the AC only requires the URL-addressable comparison to work, not a particular entry point.

---

## F1-28: "Joined at round X" context in league tables

**Tier:** T3 — **Dev:** DeepSeek Flash → **QA:** Kimi K2.7
**Epic:** E3 — Leagues as views over the global game [MVP] — Phase 1
**Depends on:** F1-10, F1-21, F1-22

### Description

Epic: E3 — Leagues as views over the global game (Phase 1)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* Every league standings row for a member displays the round at which they joined that league, sourced from the `joined_at_round` value captured at accept-time in F1-21.
* A late joiner (joined after the league's start round) is visibly distinguished in the standings markup from members present since the start round — test asserts the joined-round text renders for a late joiner's row.
* When a member's scored-race count is well below the league's median member race count, the standings view surfaces a suggestion to switch to Average ranking (F1-23) for that member's row specifically — tested with a fixture league containing one significantly-behind member and asserting the suggestion appears only for that row, not for at-or-above-median rows.
* This story adds annotation only: a test confirms the underlying point/rank values from F1-22 are unchanged before and after this story's UI addition.

## Notes

* Depends on F1-21, which must persist `joined_at_round` at join time (coordinate the exact column name/shape with that story), and on F1-22, the standings surface this annotates.
* **Open question:** the exact "well below median" threshold for suggesting Average mode isn't quantified in the backlog. Pick a reasonable default (e.g. less than half the median race count), document it as adjustable, and don't treat the specific number as load-bearing.

---
