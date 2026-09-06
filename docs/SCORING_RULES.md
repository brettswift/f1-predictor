# Scoring Rules

Versioned record of the scoring rules the app **actually implements**. The code is the
source of truth: every value and condition in this document was transcribed from the
code at the commit noted in each section. If the doc and the code disagree, the code is
right — fix the doc in the same change, and add a `Changelog` entry for any rule change.

## v1

**Tagged:** 2026-09-06 · **Code snapshot:** `main` @ `22ad68d` ·
**Function:** `calculate_score()` in `src/app.py` (line 1283 on the snapshot above; the
issue text's `:823` predates later edits to the file).

### How a race is scored

For each race, every user's prediction (an ordered P1/P2/P3 of three distinct driver
IDs) is compared against the race result's podium (the actual P1/P2/P3 driver IDs)
inside `calculate_score(prediction, result)`. The function is a pure function: it reads
only `prediction['p1_driver_id' | 'p2_driver_id' | 'p3_driver_id']` and
`result['p1_driver_id' | 'p2_driver_id' | 'p3_driver_id']`, and returns a single
non-negative integer. Nothing else — no grid, laps, DNF, safety car, or fastest-lap
facts — influences the score.

### Point values

| Condition | Points | Code |
| --- | --- | --- |
| Predicted P1 driver actually finished P1 | **+10** | `app.py:1287-1288` |
| Predicted P2 driver actually finished P2 | **+6** | `app.py:1289-1290` |
| Predicted P3 driver actually finished P3 | **+4** | `app.py:1291-1292` |
| Predicted driver finished on the podium (P1–P3) but in a *different* slot than predicted | **+1 each** | `app.py:1297-1305` |
| Predicted driver finished outside the podium | **0** | (no branch awards it) |

- **Maximum per race: 20** (perfect podium: 10 + 6 + 4).
- **Minimum per race: 0** (no podium drivers predicted at all).
- Driver identity is compared as the integer driver `id` with plain `==`; there is no
  name/acronym fallback.

### Partial-credit details (podium, wrong position)

The function builds the set of the three predicted driver IDs and the set of the three
resulting podium driver IDs (`app.py:1294-1295`). For each predicted driver that is also
on the actual podium, it awards **+1** unless the driver is "exact" — i.e. the same
driver in the *same* slot in both prediction and result
(`app.py:1299-1304`). Consequences, as implemented:

- A predicted driver who finishes on the podium in the wrong slot earns **1 point total**
  for that driver — the +10/+6/+4 for their predicted slot is *not* also awarded.
  Example: predict P1=A, P2=B, P3=C; result P1=A, P2=C, P3=B → 10 (A) + 1 (C, podium but
  predicted P2) + 1 (B, podium but predicted P3) = **12**.
- A predicted driver who finishes P4 or lower earns nothing for that driver, even if
  other predicted drivers are on the podium.
- The loop iterates over the **set** of predicted driver IDs, so at most one +1 is
  awarded per unique predicted driver (the prediction form rejects duplicate drivers, so
  this only matters if the data is ever inconsistent).
- A driver who is exact in one slot cannot receive a partial-credit point in another —
  "exact" is defined per driver/slot, not per slot alone.

### How ties are handled

`calculate_score()` itself contains **no tie logic**: it returns a bare int per
(user, race) and never compares one user's score to another's. Tie behavior lives in the
ranking queries, and today they apply **no tiebreaker**:

- `get_rank_and_field()` (`app.py:1764`) orders all non-synthetic users by
  `SUM(points) DESC` only and assigns 1-indexed ranks by row position — users with equal
  totals get consecutive ranks in database order (arbitrary in SQLite).
- `get_standings()` (`app.py:1796`) sorts the same way (leaderboard `total` = podium
  `SUM(points)` + the safety-car pool balance, which is added at display time and is
  *not* part of `calculate_score()`), then assigns positions 1..N sequentially — tied
  totals are shown as separate consecutive positions, never a shared/tied position.

### Where scores are written

Scores are stored per race in the `scores` table (`user_id TEXT, race_id INTEGER,
points INTEGER`, `UNIQUE(user_id, race_id)` — schema at `app.py:345`). Both write paths
upsert (`INSERT … ON CONFLICT(user_id, race_id) DO UPDATE SET points = excluded.points`):

- `check_and_ingest_results()` in `src/app.py` (the OpenF1 results poll, `app.py:738`).
- The admin manual results entry route (`app.py:3486`).

The cron path does the same in `cron/race_manager.py` (`_save_results_and_score`).
Season totals are never stored — every leaderboard derives them with `SUM(points)` at
query time.

### Duplicated implementations (must be kept in sync)

The same rules are duplicated in two more places (not shared code — two near-identical
copies: `fetch_race_results.py` is line-for-line equivalent; `race_manager.py` is
logically identical but iterates `pred_set & res_set` directly). Any future rule change
must touch **all three** or the scoring paths will score the same race differently:

1. `calculate_score()` — `src/app.py:1283` (this doc's subject)
2. `calculate_score()` — `cron/fetch_race_results.py:173`
3. `_calculate_score()` — `cron/race_manager.py:246`

### Future recalculation (migration path for v2+)

If a future version (e.g. a safety-car bonus, F1-31/BUD-139) changes point values, every
existing `scores.points` value was computed under this v1 rule set and is now stale.
Because season totals are always derived (`SUM(points)` over `scores`), a recalculation
needs no schema change: re-run the v2 scoring function over each scored race's
`predictions` row and current `results` row and upsert the new points (the existing
`ON CONFLICT(user_id, race_id) DO UPDATE` pattern already supports overwrites). The
open concerns to resolve when v2 lands are: (a) it must use the *current* `results`
rows, which admins can still edit after scoring; (b) any downstream aggregate that has
been *materialized* elsewhere (stats/streak features, snapshots, cached totals) must be
regenerated too, since `scores` itself stores only per-race points; and (c) a decision
is needed on whether the recalc is applied retroactively to the whole season or only to
races from the v2 switch-over round, since mixing rule versions in one leaderboard is
what this doc's version tags are meant to prevent.

## Changelog

- v1 — 2026-09-06 — initial documentation of existing behavior
