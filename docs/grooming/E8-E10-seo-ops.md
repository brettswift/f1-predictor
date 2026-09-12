## F1-70: Race pages with clean slugs, meta/OG tags, sitemap.xml, robots.txt

**Tier:** T3 — **Dev:** DeepSeek v4 Flash → **QA:** Kimi K2.7-code
**Epic:** E8 — SEO & Content Infrastructure [MVP] — Phase 2
**Depends on:** none

### Description

Epic: E8 — SEO & Content Infrastructure (Phase 2)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* An anonymous request (no session cookie) to a `locked` or `completed` race's clean-slug URL (e.g. `GET /race/2026_chinese`) returns HTTP 200 with the race's name/date/results rendered — not a 302 redirect to `/`. Verified with `curl -s -o /dev/null -w '%{http_code}' --no-cookie-jar <url>`.
* Every race for the current `F1_SEASON` has a stable slug (existing `race_slug()` in `src/app.py:849`, or a redesigned scheme — see Notes) that does not collide across races and does not change once assigned.
* Each race page sets a per-race `<title>`, `<meta name="description">`, and Open Graph tags (`og:title`, `og:description`, `og:url`, `og:image` — a static fallback image is acceptable if a per-race image doesn't exist yet) — not the static `F1 Predictor` title currently hardcoded in `src/templates/base.html:6`.
* `/sitemap.xml` is served, lists every indexable race page plus the landing and calendar pages, and validates against the sitemap XML schema (e.g. via `xmllint --noout` or a sitemap validator library run in CI).
* `/robots.txt` is served, allows crawling of public race pages, and explicitly disallows `/admin/*` and any session-gated paths.
* Google Search Console property is verified for the production domain (`f1.brettswift.com`), the sitemap is submitted, and at least the locked/completed race pages show "Indexed" or "Discovered — currently not indexed" (not "Excluded" for a blocking reason) — a Search Console export/screenshot is captured in the PR as evidence, since indexing status can't be asserted from a unit test.

## Notes

* **This is the real work, not the meta tags:** `/race/<slug>` (`src/app.py:1316`) currently calls `get_current_user()` (`src/app.py:811`) and redirects anonymous visitors to `/`. It also only renders when `race['status']` is `locked` or `completed` (gated inside `_race_detail_impl`, `src/app.py:1258`) — any other status redirects to `/races` with a flash message. Both gates have to be relaxed (at minimum: no-login-required for locked/completed races) before any of the meta/sitemap work has an audience to reach. Google can't index a page that 302s an anonymous crawler.
* Existing slug format is `<season>_<name-without-suffix>`, e.g. `2026_chinese` (`race_slug()`, `src/app.py:849`) — not the dash-cased `/race/2027-bahrain-gp` shown in `docs/BACKLOG.md` §5/E8. **Open question:** keep the existing format (cheaper — already linked from `/races`, `src/templates/races.html`) or migrate to dash-case (needs 301s from old links; the app already has a precedent for id→slug 301 redirects at `race_detail_by_id`, `src/app.py:1298`). Recommend keeping the existing format unless Brett has a branding reason to change it — not deciding here.
* **Open question — scope of "race page" for pre-race SEO:** §3.3 of the backlog wants long-tail queries answered *before* race weekend, but the current `/race/<slug>` template renders a picks table that doesn't exist pre-lock, and pre-race SEO content is explicitly F1-52's job (sentiment page, a different epic/route). Recommend scoping F1-70 to locked/completed races only and letting F1-52 own the pre-race SEO surface — flagging because it changes what "all race pages indexed pre-launch" (per the original AC in `docs/BACKLOG.md`) actually covers.
* No CSS/JS static files exist in the repo — `src/templates/base.html` inlines a `<style>` block, there's no `static/` directory. The OG image fallback needs either a committed static asset or a rendered endpoint (F1-61's share-card generator may be reusable here later, but don't block on it).

---

## F1-71: Structured data (Event/SportsEvent schema) on race pages

**Tier:** T3 — **Dev:** DeepSeek v4 Flash → **QA:** Kimi K2.7-code
**Epic:** E8 — SEO & Content Infrastructure [MVP] — Phase 2
**Depends on:** F1-70 (race pages must be public and slugged before schema markup has anywhere to live)

### Description

Epic: E8 — SEO & Content Infrastructure (Phase 2)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* Every public race page (per F1-70) includes exactly one `<script type="application/ld+json">` block with `@type: "SportsEvent"` containing at minimum: `name`, `startDate` (ISO 8601, sourced from the `races.date` column), `eventStatus` (`EventScheduled`/`EventCompleted` mapped from the race's `status`), and `location`.
* The JSON-LD is well-formed and passes schema-level validation in CI: extract it from the rendered HTML (e.g. with `extruct` or an equivalent JSON-LD parser) and assert required `SportsEvent` properties are present and non-empty — this is the automatable gate.
* Google's Rich Results Test (`https://search.google.com/test/rich-results`) is run manually against at least one live race page as a final human-verified gate (not automatable — no stable public API), and the result is pasted/screenshotted into the PR.
* No JSON-LD is emitted for races the page itself won't render (i.e. don't add markup to a page that F1-70 still blocks for anonymous users).

## Notes

* **Real gap found:** the `races` table (`src/app.py:155`) only stores `name, round, date, status, session_key` — no circuit, venue, or country/location field. OpenF1's session/meeting data has `circuit_short_name` and `country_name` (used transiently in `fetch_races_from_api()`, `src/app.py:330-341`) but it is never persisted. `SportsEvent.location` needs *something* — either add a column and backfill it at race-fetch time (cheap, keeps the resilience/cache story intact — see F1-02's last-known-good cache, don't add a live OpenF1 call on every page render), or fall back to a generic/omitted location if a schema property being merely present-but-empty is acceptable. Don't invent the answer here; a one-line DB migration is likely correct but flagging since it wasn't scoped in the original backlog line for this story.
* Reference: schema.org `SportsEvent` type (subtype of `Event`) — https://schema.org/SportsEvent.

---

## F1-72: Performance pass — sub-2s mobile loads, Lighthouse ≥ 90

**Tier:** T2 — **Dev:** Kimi K2.7-code → **QA:** Sonnet 5
**Epic:** E8 — SEO & Content Infrastructure [MVP] — Phase 2
**Depends on:** F1-70 (needs a public race page URL to run Lighthouse against)

### Description

Epic: E8 — SEO & Content Infrastructure (Phase 2)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* A headless Lighthouse run (`lighthouse-ci`/`@lhci/cli`, or the `lighthouse` CLI directly against a running instance) executes in CI against: the landing page (`/`), one public race page (`/race/<slug>`), and the season calendar page (F1-73's `/races` or equivalent), each on mobile emulation.
* Every audited page scores **≥ 90** on both the Performance and SEO Lighthouse categories; the raw JSON report is captured as a CI artifact (not just a pass/fail line) so a regression is diffable later.
* The CI job fails the build (non-zero exit) on a score below 90, rather than only warning — otherwise this AC decays silently.
* Mobile load time to interactive is under 2s against the pages above, measured by the same Lighthouse run (`interactive` metric in the report), on a throttled mobile profile (Lighthouse's default "mobile" preset is acceptable — don't hand-roll a throttling profile).

## Notes

* The app is server-rendered Flask with **no static CSS/JS files at all** — `src/templates/base.html` inlines a `<style>` block and there's no `static/` directory found in the repo. "Keep JS minimal" is close to a non-issue today; the main performance risks are more likely to be response time (SQLite queries per request, e.g. the `races.html`/`race_detail.html` N+1-shaped prediction lookups at `src/app.py:1229-1238`) and image weight from whatever F1-70/F1-71 add for OG images.
* `playwright` and `pytest-playwright` are already test dependencies (`requirements.txt`) — Lighthouse-in-CI can reuse the same headless Chromium rather than adding a new browser dependency, if the CI runner supports it.
* Confirm this runs against a real deployed URL (dev overlay: `https://f1.home.brettswift.com`) or a locally-served build in CI — Lighthouse against `localhost` inside a CI container needs the app + a populated SQLite DB with at least one locked/completed race, or the audited race page will 404/redirect and skew the score.

---

## F1-73: Season calendar page

**Tier:** T2 — **Dev:** Kimi K2.7-code → **QA:** Sonnet 5
**Epic:** E8 — SEO & Content Infrastructure [MVP] — Phase 2
**Depends on:** F1-70 (calendar links out to individual race pages, which must be public first)

### Description

Epic: E8 — SEO & Content Infrastructure (Phase 2)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* A public, anonymous-accessible page (new route, e.g. `/calendar` — the existing `/races` route at `src/app.py:1216` is login-gated via `get_current_user()` and is a different concern: it shows the signed-in user's own prediction status per race) lists every race in the season with its lock/start time.
* Lock/start times are rendered in the visitor's local timezone client-side (the stored `races.date` is naive UTC — see `src/app.py:154` comment "date is race start (UTC)"); a no-JS fallback shows explicit UTC so the page still degrades sensibly.
* Each calendar row links to that race's public page (F1-70's `/race/<slug>`), including future races — link them even if F1-70 scoped the *page itself* to locked/completed only, so the calendar doesn't dead-end on a race that hasn't happened.
* The page is included in `/sitemap.xml` (F1-70) and returns HTTP 200 for an anonymous request with no session cookie.
* Indexable and useful standalone per the backlog's SEO framing ("F1 2027 calendar predictions" long-tail) — verified by the same anonymous-curl + title/meta check pattern as F1-70, not a subjective read.

## Notes

* Don't confuse this with the existing `/races` route (`src/app.py:1216`) — that page is the authenticated "your prediction status per race" view and should stay as-is; this is a new, public, no-account-required page.
* Race dates are stored as naive UTC strings (`YYYY-MM-DD HH:MM:SS`, `src/app.py:154`, `date_str = date_start.replace('T', ' ')[:19]` at `src/app.py:345`) with no stored timezone/offset — client-side conversion needs the `Z`/UTC assumption made explicit (e.g. render as `<time datetime="...Z">` and let `Intl.DateTimeFormat` or similar do the conversion), since the raw string alone is ambiguous.

---

## F1-90: Privacy-friendly analytics (self-hosted on the k8s cluster)

**Tier:** T2 — **Dev:** Kimi K2.7-code → **QA:** Sonnet 5
**Epic:** E10 — Analytics & Operations [MVP] — Phase 3
**Depends on:** none to deploy; F1-111 (synthetic-user isolation, epic E12) must land before the funnel numbers this feeds can be trusted for real decisions — see Notes.

### Description

Epic: E10 — Analytics & Operations (Phase 3)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* A self-hosted, privacy-friendly analytics tool (Plausible or Umami — see Open question) is deployed to the k8s cluster via GitOps: manifests live under a new app directory in `k8s_nas` (following the existing per-app layout — see e.g. `apps/travel-planner/` referenced in `k8s_nas/docs/AI_GUIDANCE.md`), deployed by pushing a feature branch to `live` per that repo's GitOps rules, never `kubectl apply`.
* **PVC (and any StatefulSet/database it depends on) is present in the app's `kustomization.yaml` `resources:` list from the very first commit that creates it** — not added in a follow-up commit. This is a direct, named rule from `k8s_nas/docs/AI_GUIDANCE.md` ("Stateful apps (PVCs) with ArgoCD auto-sync"), written after a real incident where `travel-planner`'s PVC was outside `kustomization.yaml` while auto-sync + selfHeal was on, and ArgoCD's reconciliation replaced the claim and wiped the volume. Verify with `kustomize build <overlay>` (or `kubectl kustomize`) that the PVC/database resources appear in the built manifest before merging, not just that the YAML file exists on disk.
* The f1-predictor app is instrumented to send page-view events to the new analytics instance (script tag or server-side event, depending on which tool is chosen).
* One dashboard (the tool's native UI is fine — no custom dashboard required) shows: page views, referrers, invite-link conversion, and the prediction funnel (visit → pick → sign-up) for a real time window — screenshotted or linked in the PR as evidence it's wired end-to-end, not just deployed and empty.
* The dashboard/instance is reachable only as needed for Brett's own use (auth in front of the dashboard UI — most self-hosted options ship this by default) — not a public data leak of traffic patterns.

## Notes

* **Open question — Plausible vs. Umami:** Plausible Community Edition needs Postgres *and* ClickHouse (heavier ops footprint, a second stateful service to back up). Umami needs only Postgres or MySQL (lighter, one DB to manage, and its own backup/PVC story to reconcile with F1-92's "don't pre-engineer Postgres" guidance for the *app* DB — this is a separate DB for analytics, so that guidance doesn't block it, but it's still one more thing on the same 1Gi-`local-path`-class cluster). Writing this AC tool-agnostically on purpose — pick based on whichever is less new infrastructure to babysit; not deciding here.
* This deploy lives in `k8s_nas` (infra repo), not `f1-predictor` (app repo) — same split as the existing apps in `k8s_nas/docs/AI_GUIDANCE.md`'s "Application image refresh" table. Add a row to that table for the new analytics app once it exists, per the doc's own instruction ("Add rows for other services as they gain the same pattern").
* **Coordinate with F1-111** (epic E12, not in this batch): the backlog's risk table explicitly calls out that synthetic/persona users must be excluded from analytics counts from the moment personas exist, "otherwise the beta dataset and F1-105's launch metrics are contaminated at the source." F1-90 doesn't need F1-111 built first to deploy the tool, but the event-sending code should support filtering/tagging synthetic traffic (e.g. an `is_synthetic` flag or excluded user-agent/cookie) so it isn't bolted on retroactively once contaminated data already exists.
* This also feeds §3.4's monetization sequencing: AdSense (F1-81, epic E9) triggers at "~1k+ monthly visitors" — this dashboard's page-view count is the actual signal that decision reads from, so its numbers need to be trustworthy (i.e., synthetic-free) before that trigger is used.

---

## F1-91: External uptime + cron monitoring (single notification owner)

**Tier:** T2 — **Dev:** Kimi K2.7-code → **QA:** Sonnet 5
**Epic:** E10 — Analytics & Operations [MVP] — Phase 3
**Depends on:** none (supersedes the alerting half of F1-03 / BUD-125 — see Notes)

### Description

Epic: E10 — Analytics & Operations (Phase 3)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* An uptime check hits the existing `/health` endpoint (`src/app.py:1688`, returns `{"status": "healthy"}`) on both the prod (`f1.brettswift.com`) and dev (`f1.home.brettswift.com`) URLs on a recurring schedule (external uptime tool, or a lightweight in-cluster CronJob — implementation-agnostic) and alerts on N consecutive failures (N should be small enough to catch a real outage within minutes, not hours).
* **The application never sends notifications.** No Telegram/webhook/email client ships in the app or its CronJob images; `grep -rn 'api.telegram.org' src/ cron/` returns nothing. App-side failure handling is limited to: record the failure in the DB, log to stderr, and exit non-zero. This is a hard constraint — a story that adds an outbound notifier to the app is rejected on sight.
* All alerting is owned by a **single external observer** running outside the app (OpenClaw-side), which polls the surfaces below and is the only component permitted to message Brett. Notification policy (thresholds, quiet hours, dedup across runs) lives there, not scattered per-app.
* Dedup survives process restarts: alert state is persisted by the observer, so N consecutive failing polls produce **one** alert, not one per poll. Verified by running the observer repeatedly against a surface that stays down and asserting exactly one message is sent.
* Kubernetes CronJob health is monitored, not just the HTTP endpoint: a failed Job (non-zero exit) for `f1-race-manager` (`base/race-manager-cronjob.yaml`) or `f1-driver-refresh` (`overlays/prod/driver-refresh-cronjob.yaml`) triggers the same Telegram alert path. `failedJobsHistoryLimit` on these CronJobs is 1-2, so a failed Job's evidence disappears quickly — the monitor needs to poll/watch (e.g. `kubectl get jobs --field-selector status.successful!=1` or a Kubernetes Events watch), not rely on someone noticing after the fact.
* **Verified by injected failure, not just by the alert code existing:** force one real failure per monitored surface (e.g. scale `f1-predictor` to 0 replicas briefly for the uptime check, and make one CronJob's container command exit 1 for the cron check) and confirm a Telegram message actually arrives within the expected window. The test run and message screenshot/timestamp are captured in the PR.
* Recovery is also observable — either an explicit "back to healthy" message, or clearly no false-positive repeat alerts once the underlying issue is fixed.

## Notes

* **Supersedes the alerting half of F1-03** (epic E1, BUD-125). F1-03 originally had the *app* send Telegram on fetch failure; that premise was rejected 2026-08-27 — apps under construction must not message Brett. F1-03 is re-scoped to persistence only (record the failed attempt, exit non-zero); the notification it used to send is this story's job.
* Two real CronJobs currently deployed (confirmed via `kubectl kustomize overlays/prod`): `f1-race-manager` and `f1-driver-refresh`. Two *other* CronJob manifests exist in the repo — `base/lock-races-cronjob.yaml` and `base/fetch-results-cronjob.yaml` — but are **not** listed in `base/kustomization.yaml`'s `resources:` and are therefore dead/undeployed (superseded by `race_manager.py`'s state machine, per its own docstring at `cron/race_manager.py:1`). Don't build monitoring around jobs that aren't actually running — verify what's live with `kubectl kustomize` before scoping the monitor's target list, and treat the two orphaned files as a small pre-existing cleanup opportunity worth a one-line mention to Brett, not something this story needs to fix.
* "Brett knows about failures before users do" is the actual bar from the backlog — the injected-failure test above is what proves that, since an alert path that's never fired in anger is not evidence of anything.

---

## F1-92: Nightly SQLite backup to off-cluster storage

**Tier:** T2 — **Dev:** Kimi K2.7-code → **QA:** Sonnet 5
**Epic:** E10 — Analytics & Operations [MVP] — Phase 3
**Depends on:** none

### Description

Epic: E10 — Analytics & Operations (Phase 3)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* A nightly Kubernetes CronJob (same pattern as `base/race-manager-cronjob.yaml` — `restartPolicy: OnFailure`, resource limits set, mounts the same `f1-predictor-data` PVC) runs `sqlite3 /data/f1_predictions.db ".backup '/tmp/backup.db'"` (the SQLite online-backup API, **not** a raw `cp`/`tar` of the live file, which risks copying a mid-write, corrupt file since the Flask pod writes to it concurrently) and ships the resulting file to off-cluster storage.
* The new CronJob manifest is added to `base/kustomization.yaml`'s `resources:` list in the same commit that creates it. **This repo already has a precedent for the failure mode of skipping this step**: `base/lock-races-cronjob.yaml` and `base/fetch-results-cronjob.yaml` both exist on disk but are absent from `base/kustomization.yaml`'s `resources:`, so neither is actually deployed (confirmed via `kubectl kustomize overlays/prod`, which shows only `f1-race-manager` and `f1-driver-refresh` as live CronJobs). Verify the new backup job appears in `kubectl kustomize overlays/prod` output before considering this AC met — a manifest that exists but isn't wired into kustomize is indistinguishable from "no backup" until someone checks three weeks later.
* Backups land somewhere genuinely off-cluster (not another PVC on the same `local-path` storage class/node) — object storage or a separate host are both fine; **which one is an open question, see Notes.**
* At least the last N nightly backups are retained (a simple rotation — delete anything older than N days — is sufficient; don't build a versioning system).
* **The restore path is actually exercised, not just assumed to work:** download the most recent off-cluster backup, run `sqlite3 <file> "PRAGMA integrity_check;"` and confirm it returns `ok`, then restore it into a scratch SQLite instance (a throwaway pod or a local file) and run a real query against it (e.g. `SELECT COUNT(*) FROM races;` / `SELECT COUNT(*) FROM predictions;`) confirming non-zero, sane row counts. This drill is run at least once as part of closing this story, with the commands and output captured in the PR — a backup that has never been restored is not a tested backup.
* No Postgres migration, connection pooling, or WAL-mode replication is introduced. Per the backlog's explicit framing: "SQLite is *fine* at this scale — revisit Postgres only if concurrent-write errors actually appear; don't pre-engineer." An AC that drifts toward a database migration is out of scope for this story.

## Notes

* Live DB path confirmed: `/data/f1_predictions.db` (`app.config['DATABASE_PATH']`, `src/app.py:23`), backed by the `f1-predictor-data` PVC (`base/pvc.yaml`, 1Gi, `storageClassName: local-path`) mounted into the Deployment (`base/deployment.yaml`) and every cron container that touches the DB.
* **Open question — where off-cluster:** the cluster already has AWS/Route53 credentials in play for DNS (per `k8s_nas/docs/AI_GUIDANCE.md`'s cert-manager/external-dns setup), which makes an S3 bucket a low-friction option; the alternative is `scp`/`rsync` to a separate host outright (e.g. off the `home-server` k3s node entirely, onto other infra Brett controls). Writing the AC storage-agnostically on purpose — the meaningful requirement is "not on the same node/storage class," not a specific vendor.
* `sqlite3`'s `.backup` command (or the equivalent `sqlite3.Connection.backup()` in Python, since the repo is already Python/Flask and `cron/` scripts import `sqlite3` directly) is the correct primitive here — it takes a live, consistent snapshot even while the source DB has an open connection, unlike a filesystem-level copy.

---

## F1-93: Rate limiting + basic abuse protection on auth and prediction endpoints

**Tier:** T2 — **Dev:** Kimi K2.7-code → **QA:** Sonnet 5
**Epic:** E10 — Analytics & Operations [MVP] — Phase 3
**Depends on:** F1-10 (magic-link auth) and F1-21 (league invite links) for the ACs that specifically target those flows — see Notes; the prediction-endpoint and username-creation ACs below apply to what exists today and don't need to wait.

### Description

Epic: E10 — Analytics & Operations (Phase 3)
Source: `docs/BACKLOG.md` on branch `docs/2026-relaunch-brief`

## Acceptance Criteria

* Rate limiting is applied to: the prediction-submission endpoint (`POST /predict/<race_id>`, `src/app.py:971`), the username/session-creation endpoint (`POST /set-username`, `src/app.py:897` — currently unauthenticated and creates a DB row with no limit at all), and, once built, the magic-link request endpoint (F1-10) and league invite-join endpoint (F1-21).
* Limits are enforced per-IP (and per-account where an account already exists at request time) using a standard mechanism — either an application-level library (e.g. `Flask-Limiter`, not currently in `requirements.txt`) or nginx-ingress rate-limit annotations on the Ingress resource (`base/service.yaml`'s Ingress counterparts in `overlays/*/ingress.yaml`) — either is acceptable, this AC is deliberately implementation-agnostic.
* Exceeding the limit returns HTTP 429 with a clear, non-leaky error (no stack traces, no internal state).
* Automated test: a script/test hits the rate-limited endpoint N+1 times in the configured window and asserts the (N+1)th response is 429, not 200/302 — this is the verifiable proof, not a description of intent.
* The magic-link request endpoint specifically cannot be used to spam an arbitrary email address (limit keyed by target email *and* by source IP, since either alone is bypassable) — this AC only becomes testable once F1-10 exists.
* League invite-join cannot be brute-forced (invite tokens/codes are rate-limited per source IP, and ideally the token space itself is large enough that brute force isn't practical within any reasonable rate limit) — this AC only becomes testable once F1-21 exists.

## Notes

* **Nothing that resembles rate limiting exists in the codebase today** — `requirements.txt` has no `Flask-Limiter` or similar, and a `grep` for `limiter`/`rate.limit` across `src/app.py` turns up nothing relevant. This is greenfield, not a hardening pass on existing infrastructure.
* Auth today is just a username typed into `/set-username` (no password, no email, no magic link) — F1-10 (magic-link) and F1-21 (invite links) are both still backlog items per `docs/f1-model-assignment.md` (not yet built). Two of this story's six ACs above are genuinely blocked on those landing first; the other four (predict endpoint, username creation) are exercisable against the app as it exists right now and should be done regardless of F1-10/F1-21's timing.
* Given E10 is scheduled for Phase 3 (same phase as E2/E3 finishing, per the release plan in `docs/BACKLOG.md` §4), the ordering should work out naturally — but call this out explicitly in Linear so this story isn't picked up and partially blocked by surprise.
