## F1-10: Email magic-link authentication (no passwords)

**Tier:** T1 — **Dev:** Sonnet 5 → **QA:** Opus 5
**Epic:** E2 — Real User Accounts [MVP] — Phase 0
**Depends on:** none

### Description

Epic: E2 — Real User Accounts (Phase 0)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* `sqlite3 f1_predictions.db ".schema users"` shows no `password`, `password_hash`, or similar column anywhere in the schema.
* A user can request a sign-in link for an email address they've never used before; a token is generated, stored server-side (own table, e.g. `auth_tokens` or equivalent) with an expiry, and the link is delivered via the app's outbound email mechanism (see Notes — this must be built, nothing exists yet).
* Visiting the link before expiry authenticates the user and sets a persistent session (a `session` cookie that survives browser restart, i.e. has a `max-age`/`expires`, not the current default session-only cookie set in `set_username()` at `src/app.py:925`).
* The same email, opened in a second, unrelated browser/session (no shared cookies), reaches the same account and sees the same prediction history — verifiable with two separate `requests.Session()` (or two Playwright browser contexts) hitting the running app.
* An expired or already-used token is rejected (redirect + flash error, not a 500); re-requesting a link for the same email invalidates any prior unused token for that email.
* A user's `users` row is keyed by a stable identifier independent of `email` casing (`Foo@Bar.com` and `foo@bar.com` resolve to the same account).
* Existing `predictions`/`scores` FK relationships (`FOREIGN KEY (user_id) REFERENCES users(session_id)`, `src/app.py:174`, `198`) either continue to resolve correctly against the new user model, or are migrated consistently — no orphaned rows after the change (checked by a query joining `predictions`/`scores` back to `users`).

## Notes

* Today there are no real accounts: `set_username()` (`src/app.py:897-925`) takes a bare username from a form, creates (or reuses) a `users` row keyed on `session_id` (a `uuid4`, `src/app.py:126`), and stores it in a signed Flask session cookie via `app.secret_key` (`src/app.py:22`). There is no `email` column, no password, no login-link concept — this story is greenfield, not a modification of an existing auth path.
* `users` table today (`src/app.py:124-129`): `session_id TEXT PRIMARY KEY, username TEXT NOT NULL UNIQUE, created_at`. `user_id` throughout `predictions`/`scores`/etc. is a FK to `users.session_id` — decide whether `session_id` keeps meaning "user id" post-migration (recommended: keep the column name/semantics as the stable user id, stop treating it as a literal browser-session artifact) since every other query in `src/app.py` joins on it (e.g. `src/app.py:772-782`, `1071-1086`, `1279-1287`).
* **No email-sending capability exists anywhere in this repo.** `requirements.txt` has only `Flask`, `gunicorn`, `requests` — no `Flask-Mail`, no SMTP client, no transactional-email SDK (SendGrid/SES/Postmark/etc.), and `DEPLOYMENT.md` documents no mail relay in the cluster. This story includes standing up an outbound email path (library + provider account + secret in the deployment) — it is not just adding a route.
* `app.secret_key` currently defaults to a hardcoded dev value (`src/app.py:22`, `'dev-secret-key-change-in-production'`) if `SECRET_KEY` env isn't set — worth confirming prod actually sets it, since magic-link security depends on session-cookie integrity too.
* Session cookie today is Flask's default (browser-session cookie, no explicit `PERMANENT_SESSION_LIFETIME` / `session.permanent = True` anywhere in `src/app.py`) — "persists across devices" and "persists across restarts" both require deliberately setting cookie persistence, not just swapping the login mechanism.
* **Open question:** magic-link token delivery provider is not decided in the backlog or brief (SES/SendGrid/Postmark/other) — pick one during implementation and record it in `DEPLOYMENT.md`; don't block the AC on this, the AC only requires *a* working delivery path.

---

## F1-11: Migrate legacy session-cookie users

**Tier:** T1 — **Dev:** Sonnet 5 → **QA:** Opus 5
**Epic:** E2 — Real User Accounts [MVP] — Phase 0
**Depends on:** F1-10

### Description

Epic: E2 — Real User Accounts (Phase 0)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* An existing beta user (a `users` row created via the legacy `set_username()` flow, with no associated email) who still holds the original browser session cookie can "claim" that row by completing the F1-10 magic-link flow for an email address; after claiming, their existing `predictions` and `scores` rows (still keyed on the original `user_id`/`session_id`) are visible under the new email-authenticated login — verifiable by comparing `SELECT * FROM predictions WHERE user_id = ?` before and after claiming, same row count and content.
* A legacy row that is never claimed is left queryable (not silently claimed by someone else) and is excluded/expired after the end of the 2026 season per the backlog (`docs/BACKLOG.md` §5 E2, F1-11 AC: "unclaimed rows expire after the season") — write the expiry as a concrete, checkable rule (e.g. a script/query an agent can run: "no `users` row with `claimed_at IS NULL` remains after `<season-end-date>`, or all such rows are flagged/excluded from the leaderboard").
* Two different legacy `users` rows with the **same or confusingly similar username but different `session_id`s** (the exact duplicate-username scenario in `docs/BUGS_AND_FIXES.md` Bug 1 — multiple "brett" rows from different browsers) do not silently merge into one account just because they share a display name; claiming is keyed on possession of the original session cookie (or an explicit admin-assisted merge), never on username string match alone.
* Claiming a legacy account does not require re-entering any predictions — the claim operation must be provably non-destructive: run it against a seeded row with existing `predictions`/`scores`, assert row counts are unchanged after claim.
* The admin cleanup endpoint `POST /admin/delete-predictions` (`src/app.py:1562` area, Bug 1's fix) remains functional and is not broken by whatever schema change F1-11 introduces (add/keep a regression test that exercises it after migration).

## Notes

* `docs/BUGS_AND_FIXES.md` Bug 1 is the direct motivating incident: multiple "brett" `users` rows existed from different browsers because `set_username()` (`src/app.py:897-925`) silently reuses an existing username's `session_id` if the username string matches exactly (`src/app.py:908-915`), but a *new* browser with a *different* username spelling/case creates a brand-new row with no relation to the "real" one. The existing fix was a manual admin nuke (`username_pattern` + `keep_p1_name`, `src/app.py:1562-1612`) — F1-11 is the structural fix so that cleanup script never has to run again.
* Claim flow needs a concrete mechanism: the simplest that satisfies the ACs is "claim while the legacy session cookie is still present" (i.e. the browser doing the magic-link signup already has `session['session_id']` set to the legacy row) — check for that and offer "claim your existing predictions?" during the F1-10 sign-in flow, rather than trying to match by username after the fact.
* `users` schema will need a nullable `claimed_at` (or `email IS NULL` as the "unclaimed" signal, if F1-10 adds `email` as nullable) to distinguish legacy-unclaimed rows from real accounts — coordinate the exact column set with F1-10's implementation, don't duplicate migration logic.
* **Open question:** the backlog says "unclaimed rows expire after the season" but doesn't specify the exact date or the mechanics of "expire" (hard delete vs. archive vs. exclude from leaderboard only) — write the AC against the decided part (rows must be identifiable and excludable) and leave the literal expiry date/deletion-vs-archive choice as an open question for Brett.

---

## F1-12: Optional OAuth (Google) as a second sign-in method

**Tier:** T2 — **Dev:** Kimi K2.7-code → **QA:** Sonnet 5
**Epic:** E2 — Real User Accounts [MVP] — Phase 0
**Depends on:** F1-10

### Description

Epic: E2 — Real User Accounts (Phase 0)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* A "Sign in with Google" button on the sign-in page completes an OAuth flow and creates a session in one round trip (no separate password/link step) — verifiable end-to-end with a test Google account or a mocked OAuth provider in CI.
* If the Google account's email matches an existing email-authenticated (F1-10) account, the OAuth sign-in logs into that same account (same `user_id`, same predictions/scores) rather than creating a duplicate — verifiable by signing up via magic-link with `foo@bar.com`, then OAuth-signing-in with a Google account whose email is `foo@bar.com`, and asserting the resulting session's `user_id` is identical.
* If no account exists for that email, OAuth sign-in creates a new account equivalent to a fresh F1-10 signup (profile fields default sensibly, e.g. display name pre-filled from the Google profile per F1-13).
* No password is stored or requested at any point in this flow (same constraint as F1-10).
* OAuth client secret is read from an environment variable / deployment secret, not hardcoded — follow the existing pattern of `DRIVER_REFRESH_SECRET` (`src/app.py:31`, sourced from `os.environ.get(...)`).

## Notes

* Depends structurally on F1-10's account model existing first — specifically the "an account is keyed by email, multiple auth methods can point at one account" shape. Do not implement F1-12 against the legacy `session_id`+`username` model.
* No OAuth library is currently in `requirements.txt` (only `Flask`, `gunicorn`, `requests`) — this story adds a dependency (e.g. `Authlib` or manual `requests`-based OAuth2 code exchange against Google's endpoints). Pick the lighter-weight option given the app has no other framework-heavy dependencies.
* Requires a Google Cloud OAuth client (external setup, not just code) — flag this as a deployment/ops prerequisite, not purely a coding task.
* **Open question:** backlog doesn't specify whether Google is the *only* second method planned or a placeholder for others later — AC above only covers what's decided (Google, email-match linking).

---

## F1-13: Minimal profile (display name, avatar, favorite driver)

**Tier:** T3 — **Dev:** DeepSeek v4 Flash → **QA:** Kimi K2.7-code
**Epic:** E2 — Real User Accounts [MVP] — Phase 0
**Depends on:** F1-10

### Description

Epic: E2 — Real User Accounts (Phase 0)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* `users` table (or a new `profiles` table keyed on the same user id) has editable `display_name`, `avatar_color` or `avatar_emoji`, and `favorite_driver_id` (FK to `drivers.id`, `src/app.py:133-141`) fields.
* A logged-in user can edit all three fields via a form and see the change persist across a fresh login (not just the current session).
* `display_name` defaults to something reasonable at account creation (e.g. the legacy `username` for migrated accounts per F1-11, or the local part of the email for new F1-10 signups) — never blank.
* The leaderboard (`src/templates/leaderboard.html`, rendered from the query at `src/app.py:1071-1086`) and per-race prediction lists (`src/app.py:772-782`) show `display_name` and the avatar (color/emoji) instead of, or alongside, the raw `username` column — verifiable by inspecting rendered HTML for the new fields.
* Changing `display_name` does not break the FK relationships or historical `predictions`/`scores` rows (they're keyed on user id, not on the display name string) — no `username`-string joins remain in the codepaths this story touches.

## Notes

* Depends on F1-10 for there to be a stable authenticated user to attach a profile to; can be built in parallel with F1-11/F1-12 once F1-10 lands since it doesn't touch auth mechanics itself.
* Current leaderboard/race-detail queries join `users u ON p.user_id = u.session_id` and select `u.username` directly (`src/app.py:772`, `1073`, `1279`, `1415`) — every one of those `SELECT`/template pair needs the new field added, this is a "touches many small places" story, not a hard one.
* `favorite_driver_id` should reuse the existing `drivers` table (`src/app.py:133-141`, populated from OpenF1 per F1-01) rather than storing a free-text driver name.
* Called out in the backlog as feeding later design personalization work (E7) — keep the schema simple now (no need to anticipate E7's needs beyond having the three fields queryable).

---

## F1-14: Account deletion + data export

**Tier:** T2 — **Dev:** Kimi K2.7-code → **QA:** Sonnet 5
**Epic:** E2 — Real User Accounts [MVP] — Phase 0
**Depends on:** F1-10

### Description

Epic: E2 — Real User Accounts (Phase 0)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* A logged-in user can trigger self-serve account deletion from a settings/profile page (no admin/Brett involvement) that removes their `users` row and all rows in `predictions`, `scores`, and any profile/auth-token tables that reference their user id — verifiable by seeding a user with predictions/scores, deleting, then asserting zero rows remain across all four tables for that user id.
* Deletion does not corrupt other users' data: global leaderboard totals and other users' `scores`/`predictions` rows are unaffected (row counts for other user ids identical before/after).
* A logged-in user can trigger a data export that produces a machine-readable file (JSON or CSV) containing at minimum their profile fields, all their `predictions` rows, and all their `scores` rows — verifiable by downloading it and parsing it.
* Deletion is a hard, irreversible operation from the user's perspective (no "soft delete that admin can undo" is required by the AC — nothing in the backlog asks for that) but must not be triggerable accidentally (confirmation step required, e.g. re-typing username/email or a confirm dialog).
* Deletion works for accounts in any state produced by F1-10/F1-11/F1-12 (email-only, OAuth-linked, and migrated-legacy-then-claimed) without special-casing that leaves orphaned rows in any of those paths.

## Notes

* This is explicitly framed in the backlog as dual-purpose: "required for ads/privacy compliance later anyway" — it's a hard prerequisite for F1-80/F1-81 (can't run ads/consent-gated tracking on a site with no deletion path), even though it's filed under E2 not E9.
* Depends on F1-10 for there to be an authenticated account to delete; should be built after F1-11 lands too since legacy-row cleanup semantics (F1-11's "unclaimed rows expire") and user-initiated deletion (F1-14) will likely share a deletion helper — avoid writing two different "delete a user and their data" code paths.
* No existing deletion code to model this on — the closest precedent is the admin bulk-delete endpoint `POST /admin/delete-predictions` (`src/app.py:1562-1612`), which deletes `predictions`+`scores` rows for a pattern-matched set of users but never deletes the `users` row itself. F1-14 needs a proper `DELETE FROM users WHERE ...` plus its dependents, which the current codebase has never done.
* Watch cascading: `predictions`, `scores` reference `user_id`; F1-11's claim/auth-token tables and F1-13's profile table (if separate) also need cleanup in the same transaction to avoid partial deletes on failure.

---

## F1-80: Privacy policy, terms, cookie/consent management

**Tier:** T2 — **Dev:** Kimi K2.7-code → **QA:** Sonnet 5
**Epic:** E9 — Monetization & Compliance [MVP-lite] — Phase 3
**Depends on:** none

### Description

Epic: E9 — Monetization & Compliance (Phase 3)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* A `/privacy` route and a `/terms` route each render a static page with real, non-placeholder policy text (not "Lorem ipsum" or a TODO stub) — both linked from the site footer (`src/templates/base.html`) on every page.
* A cookie/consent banner appears for first-time visitors and offers at minimum "accept" and "reject/manage" — the choice is persisted (e.g. its own cookie or `localStorage` key) so it does not reappear on every page load once decided.
* No non-essential tracking (i.e. no analytics beyond what's strictly required to run the session — the auth session cookie itself is exempt as functionally necessary) fires before consent is granted — verifiable by inspecting network/script requests on first load pre-consent vs. post-consent.
* Rejecting consent still allows full core functionality (sign in, predict, view leaderboard) — consent gating never blocks the product, only optional tracking/ads.
* Privacy policy text accurately describes what F1-10/F1-11/F1-12/F1-13/F1-14 actually collect and store (email, display name, avatar, favorite driver, predictions) — an agent can check this by diffing the policy's data-collected list against the real schema.

## Notes

* Backlog explicitly frames this as "cheap to do now" and a hard prerequisite for F1-81 (AdSense) — §3.4 Stage 1 in `docs/BACKLOG.md` states AdSense "Requires privacy policy + consent (E9)". Sequence F1-80 before or alongside F1-81, never after.
* No consent-management code or library exists in the repo today (`requirements.txt` has no cookie-consent package) — this is greenfield. A lightweight vanilla-JS banner is sufficient; no need for a heavyweight CMP given the "pennies-revenue" framing in §3.1.
* This story doesn't depend on F1-90 (analytics) landing first, but the two should agree on the same consent gate — whichever analytics choice ships (§ E10, Plausible/Umami self-hosted per the backlog) should respect the same consent flag this story establishes, not a second one.
* Footer/global template is `src/templates/base.html` — confirm it currently has no footer links section to extend, since this adds the first one.

---

## F1-81: AdSense integration behind a feature flag

**Tier:** T2 — **Dev:** Kimi K2.7-code → **QA:** Sonnet 5
**Epic:** E9 — Monetization & Compliance [MVP-lite] — Phase 3
**Depends on:** F1-80

### Description

Epic: E9 — Monetization & Compliance (Phase 3)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* AdSense script/slot is controlled by a single feature flag (env var, e.g. `ADS_ENABLED`, following the existing `os.environ.get(...)`-boolean pattern already used for `USE_STUB_API` at `src/app.py:28`) that defaults to **off** — `grep` the deployed config/manifests and confirm the flag is unset or `false` at merge time, per §3.4 Stage 0/1 in `docs/BACKLOG.md` ("No ads at all" until "~1k monthly visitors" triggers Stage 1).
* With the flag off, no AdSense script tag, iframe, or ad-slot `<div>` is present in the rendered HTML of any page — verifiable by fetching a page and asserting absence of AdSense markup/script src.
* With the flag on (e.g. in a test/staging run), exactly one ad placement renders, below-fold, on race and sentiment pages only (per backlog: "single below-fold placement on race/sentiment pages") — not on the predict flow, leaderboard, or account pages.
* The ad slot's markup occupies a reserved layout space (fixed-height container) whether or not an ad actually fills it, so toggling the flag or an unfilled ad slot does not shift surrounding content — verifiable by comparing page layout/CSS with the flag on vs. off (the reserved space itself can render empty, but other content must not reflow).
* Enabling the flag with no privacy policy/consent gate in place (i.e. before F1-80 ships) is not possible — either a hard dependency check at startup or simply sequencing (F1-80 merged first) satisfies this; document which approach was taken.
* Turning the flag on/off requires no code change or redeploy of application logic — only a config/env value change, confirmed by toggling it via env var alone in a local run.

## Notes

* No feature-flag mechanism exists in the codebase today beyond ad hoc booleans read straight from `os.environ.get(...)` (`USE_STUB_API` at `src/app.py:28` is the closest precedent) — reuse that exact pattern rather than introducing a new flag framework; this app has no LaunchDarkly/Unleash-style system and doesn't need one for a single flag.
* The traffic trigger itself ("~1k monthly visitors", §3.4 Stage 1) is a business decision Brett makes manually by checking analytics (F1-90) — this story does **not** need to auto-detect traffic and flip the flag; the AC is about the flag mechanism defaulting off and being manually toggleable, not automatic activation.
* Depends on F1-80 for the consent gate; also implicitly benefits from F1-70's race-page slugs and F1-52's sentiment pages existing as the actual placement targets, but those are template/routing concerns, not blockers — the ad slot can be built and dark-launched against whatever race/sentiment templates currently exist and wired to real pages once F1-70/F1-52 land.
* **Open question:** backlog does not specify the AdSense publisher account status/approval (Google requires a live site review before serving ads) — that's an external/ops prerequisite outside this story's code scope, flagging so it isn't mistaken for something the AC should cover.

---
