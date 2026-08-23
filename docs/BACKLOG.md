# F1 Predictor — Release Plan, Epics & Backlog

Companion to [2026-relaunch-brief.md](./2026-relaunch-brief.md). That doc captures the interview and decisions; this one is the strategy analysis and the actual backlog.

---

## 1. Product Thesis

**"The race sentiment + prediction site your group argues about all week."**

Not a data-entry form. Two intertwined products:

1. **Prediction game** — pick the podium before lock, score points, beat your friends. The *game* is the retention engine.
2. **Sentiment dashboard** — AI-summarized fan sentiment per driver/race, refreshed every few hours. The *content* is the acquisition engine (SEO long-tail) and the reason to visit between races.

Either alone is weak: a bare prediction form is boring (Brett's own words), and a sentiment page without a game has no reason to return. Together each covers the other's gap.

## 2. Current State (honest assessment)

**What exists and works:** Flask + SQLite on k8s (dev + prod envs, CI/CD, image pipeline), podium predictions, race locking, results auto-fetch cron, leaderboard, live leaderboard page, admin result-entry tools.

**Critical gaps for a public launch:**

| Gap | Why it blocks launch |
|-----|---------------------|
| **No real user accounts** — users are a session cookie + username | Clear cookies = lose your history. No cross-device. No way to build groups. Duplicate usernames from different browsers already caused data-cleanup pain (see BUGS_AND_FIXES.md Bug 1). This is the single biggest blocker. |
| **No groups/leagues** | Groups-of-friends is the primary audience *and* the only realistic growth loop (invites). Confirmed net-new in schema. |
| **Data source (Jolpica)** | Being replaced with OpenF1 (decided — see brief). No safety-car data on Jolpica at all. |
| **Reliability is assumed, not engineered** | Auto-lock and score-update bugs already documented. Public + ads = these become user-facing broken promises. |
| **Design** | Functional templates, not something anyone would screenshot or share. Brett's requirement: fantastic, streamlined, simple. |
| **Zero acquisition infrastructure** | No SEO pages, no share cards, no analytics, no sitemap. |

## 3. Strategic Analysis

### 3.1 The realism constraints (from Brett, taken seriously)

- No existing following; can't self-promote on Reddit.
- Expect near-zero traffic initially.
- Money expectation: pennies-to-dollars; ads first, maybe t-shirts/affiliates if a community forms.
- Solo builder (cloud architect — infra is cheap and easy for him; design/marketing are the scarce resources).

### 3.2 The single most important strategic call: launch timing

The 2026 season is more than half over. Launching publicly mid-season is launching into dead air — new users join a scoreboard they can't win, and F1 interest between seasons is at its annual low right after the finale.

**Therefore:**
- **Now → end of 2026 season**: private beta. Friends + friends-of-friends leagues only. Use real races to harden scoring/locking/results under real conditions with forgiving users.
- **Winter break (Dec 2026–Feb 2027)**: design overhaul, sentiment dashboard build-out, SEO page scaffolding — the stuff that needs calendar time, not race weekends.
- **March 2027, season opener**: public launch. Everyone starts at zero points — the only moment a new site has a fair scoreboard. All marketing effort concentrates here.

This turns "season's half over, no deadline" from a weakness into the schedule.

### 3.3 Growth model without a following

Ranked by expected yield, all cheap:

1. **League invite loop** (primary). Every engaged user recruits their group. Make creating a league + sharing an invite link the *second* thing a new user sees (right after making their first prediction). This is the only channel that compounds.
2. **SEO long-tail via sentiment pages** (secondary, slow-burn). One auto-generated, well-designed page per race: "Who will win the 2027 Bahrain GP? Fan sentiment + predictions." These queries have real volume before every race and low competition for the sentiment angle. 24 races/year × evergreen structure = a content site that builds itself from the cron jobs.
3. **Share cards** (amplifier). Auto-generated result images ("Nailed the Monaco podium 🏆 — 3-race streak") people post themselves. Turns bragging into distribution. No following needed — the users post, not Brett.
4. **Launch-window one-shots**: Product Hunt / Hacker News "Show HN" at season-opener launch (a sentiment dashboard with an AI angle is HN-compatible), F1 Discord communities (many allow project sharing where subreddits don't), r/formula1 *Daily Discussion* threads are more permissive than top-level posts — participate as a fan, not a promoter.
5. **Small paid test** (optional, later): $50–100 of Google Ads against "[race name] predictions" queries during a race week purely to test conversion, not as a strategy.

**What not to do:** paid ads at scale, influencer outreach, social account grinding. Wrong cost/benefit for a pennies-revenue project.

### 3.4 Monetization sequencing (matched to traffic reality)

| Stage | Trigger | Action |
|-------|---------|--------|
| 0. None | Launch → ~1k monthly visitors | No ads at all. Ads on an empty site scream "abandoned." Design credibility is worth more than $0.40/month. |
| 1. AdSense | Consistent ~1k+/mo visitors | One tasteful placement (below-fold on race pages). Requires privacy policy + consent (E9). |
| 2. Affiliates | An engaged community exists | F1 merch/streaming affiliate links inside content where genuinely relevant. |
| 3. Merch | People *ask* or leagues get competitive | Print-on-demand (no inventory) league-champion shirts — "2027 [League Name] Champion" is a better product than site-brand merch. |
| 4. Payouts/prizes | Far future | Legal complexity (gambling adjacency) — explicitly out of scope until it deserves real analysis. |

### 3.5 Design direction (the "fantastic, streamlined" requirement)

- **Aesthetic**: dark, broadcast-graphics-inspired — the F1 TV timing-tower look (dark background, team-color accents, monospace numerals, motion used sparingly). Instantly signals "F1" without infringing anything.
- **Principle**: the prediction flow is the hero. Three taps to a podium pick on mobile. Everything else (sentiment, stats, leagues) hangs off that spine.
- **Mobile-first**: group-league users predict from their phone in the group chat where the trash talk happens.
- **Fable design-review pass** happens on this epic (E7) specifically — that's where voice/feel/engagement review pays off, per the model plan in the brief.

## 4. Release Plan

```
Phase 0  now            Foundation: OpenF1 swap, resilience, accounts   (E1, E2)
Phase 1  rest of 2026   Private beta: leagues, scoring v2, real races   (E3, E5partial)
Phase 2  winter break   Design overhaul, sentiment dashboard, SEO       (E7, E6, E8)
Phase 3  Mar 2027       PUBLIC LAUNCH at season opener                  (E4, E9, E10, E11)
Phase 4  post-launch    Iterate on retention + monetization             (post-release backlog)
```

Initial release = everything tagged **[MVP]** below. Post-release = **[POST]**.

---

## 5. Epics & Stories

Story IDs are provisional (`F1-xx`); renumber if/when imported to Linear.

### E1 — Data Foundation & Resilience [MVP] — Phase 0

The "reliable data source" requirement, correctly framed: resilience is our engineering property, not the upstream's promise.

- **F1-01** Replace Jolpica with OpenF1 client (`session_result`, `starting_grid`, `race_control`, `sessions`, `drivers`). AC: all cron jobs and app reads go through one `openf1.py` module; Jolpica code deleted.
- **F1-02** Last-known-good cache layer. AC: every OpenF1 read stores its result; upstream failure serves cache with a data-age indicator, never a user-facing error.
- **F1-03** Fetch-failure alerting. AC: results fetch that fails or returns empty for a completed race alerts Brett (Telegram) within one cron cycle.
- **F1-04** Fix auto-lock reliability (BUGS Bug 3). AC: race locks within 5 min of start time with no manual action; verified across a full beta race weekend.
- **F1-05** Fix score-update pipeline (BUGS Bug 2). AC: scores + leaderboard correct within 30 min of official results on 3 consecutive beta races with zero manual admin steps.
- **F1-06** Admin manual-override hardening. AC: results can be fully entered/corrected by hand when OpenF1 is down; corrections trigger automatic re-score.
- **F1-07** Safety-car result ingestion from `race_control`. AC: post-race, "was there a safety car" (and count) stored per race — the data needed before safety-car predictions can score.

### E2 — Real User Accounts [MVP] — Phase 0

- **F1-10** Email magic-link authentication (no passwords). AC: sign up + sign in via emailed link; sessions persist across devices; no password storage anywhere.
- **F1-11** Migrate legacy session-cookie users. AC: existing beta users can claim their history by verifying an email; unclaimed rows expire after the season.
- **F1-12** Optional OAuth (Google) as a second method. AC: one-tap sign-in; same account if email matches.
- **F1-13** Minimal profile: display name, avatar color/emoji, favorite driver (feeds design personalization later). AC: editable, shown on leaderboards.
- **F1-14** Account deletion + data export. AC: self-serve delete; required for ads/privacy compliance later anyway.

### E3 — Leagues [MVP] — Phase 1

The growth loop. Highest product-priority epic.

- **F1-20** Create league (name, emoji/color). AC: any signed-in user; creator is admin.
- **F1-21** Invite link with join flow optimized for cold users. AC: opening an invite as a stranger shows league name + members + "make your first pick in 3 taps" — sign-up woven into first prediction, not a gate before it.
- **F1-22** League leaderboard (season + per-race). AC: global leaderboard remains; league board is the default view for league members.
- **F1-23** League admin basics: rename, remove member, transfer admin, delete. AC: all self-serve, no Brett involvement.
- **F1-24** Head-to-head view: any two members' season accuracy compared. AC: linkable (fuel for group-chat trash talk).
- **F1-25** [POST] League chat/comment thread per race. Deferred — group chats already exist elsewhere; don't rebuild WhatsApp.

### E4 — Prediction Experience v2 [MVP] — Phase 3 (design in Phase 2)

- **F1-30** Three-tap podium picker (mobile-first redesign of predict flow). AC: driver grid → tap P1/P2/P3 → done; under 15 seconds for a returning user; no dropdowns.
- **F1-31** Safety-car prediction (yes/no + optional "how many" tiebreaker). AC: scored from F1-07 data; shown as a distinct "bonus call" in the UI, not another form row.
- **F1-32** Prediction confirmation state that's worth screenshotting. AC: post-pick screen shows your podium with team colors/driver imagery-style treatment — designed for sharing (feeds F1-61).
- **F1-33** Pre-lock reminder emails (opt-in). AC: one email ~24h before lock with one-tap deep link into the picker; unsubscribe honored.
- **F1-34** [POST] Additional extras (DNFs, fastest lap, quali order) — *recommendation: ship safety car only at launch.* Each extra adds form friction for casual users; add extras one at a time post-launch as engagement features, gated behind "quick pick vs. full card" so the 3-tap flow stays sacred.
- **F1-35** [POST] Sprint-weekend support (predictions for sprint races). AC: sprint sessions appear as separate predictable events.

### E5 — Scoring, Stats & Streaks — Phase 1 core [MVP], rest [POST]

- **F1-40** Scoring v2, documented and versioned. AC: exact-position vs. podium-presence points defined on a public "how scoring works" page; scoring logic covered by unit tests against historical race fixtures.
- **F1-41** Season profile: your accuracy, best race, rank history. AC: every user has a permanent season-stats page.
- **F1-42** [POST] Streaks & badges (correct-podium streak, safety-car oracle, etc.). AC: badge on profile + leaderboard chip; designed to be screenshot-worthy.
- **F1-43** [POST] All-time/multi-season history once a second season exists.

### E6 — Sentiment Dashboard — Phase 2 [MVP]

The differentiator + SEO engine. Decisions from brief: Reddit official API, 2–3h poll, DeepSeek v4 Flash summarization.

- **F1-50** Reddit ingestion cron: pull hot/top threads from r/formula1 (+ r/F1Technical) on a 2–3h cycle, race-weekend aware (denser near sessions). AC: raw text stored with source refs; respects API terms/rate limits.
- **F1-51** Sentiment extraction with DeepSeek v4 Flash: per-driver sentiment score + 2–3 sentence narrative summary + predicted-winner/pole consensus. AC: structured JSON output stored per (race, driver, timestamp); cost measured and logged per run (budget guardrail: < $5/mo).
- **F1-52** Race sentiment page (the SEO asset): sentiment trend per driver, fan consensus vs. site users' predictions, updated timestamp. AC: beautiful on mobile, indexable, one per race, live before each race weekend.
- **F1-53** Sentiment vs. crowd module on the predict page: "Fans lean Verstappen for pole; 62% of predictors here agree." AC: descriptive only — never affects scoring (decision: sentiment is flavor, not mechanics).
- **F1-54** [POST] Post-race "sentiment vs. reality" recap page — how wrong was the internet? Strong shareable/SEO follow-up content, auto-generated.
- **F1-55** [POST] Additional sources (F1 Discords, news headlines) if Reddit-only proves thin.

### E7 — Design System & Visual Identity [MVP] — Phase 2 (winter break)

- **F1-60** Design system: dark broadcast-graphics aesthetic, team-color accent tokens, typography (monospace numerals for timing-data feel), spacing/component library. AC: documented tokens; every page consumes them; light mode optional, dark is default.
- **F1-61** Share-card generator: auto-rendered image for prediction confirmations and post-race results ("Nailed the Monaco podium — 3-race streak"). AC: OG-image endpoint per user-race; renders correctly when pasted into iMessage/WhatsApp/X.
- **F1-62** Landing page for logged-out users: what it is, this weekend's race sentiment teaser, one CTA (make your first pick / join your league). AC: passes the "would a stranger get it in 5 seconds" test.
- **F1-63** Fable review pass on the assembled design (voice, microcopy, fun). AC: review feedback triaged into follow-up stories.
- **F1-64** Empty-state design everywhere (new league, no predictions yet, pre-season). AC: no dead screens at launch when traffic is near zero — the site must feel alive with 10 users.

### E8 — SEO & Content Infrastructure [MVP] — Phase 2

- **F1-70** Race pages with clean slugs (`/race/2027-bahrain-gp`), proper titles/meta/OG tags, sitemap.xml, robots.txt. AC: Search Console verified, all race + sentiment pages indexed pre-launch.
- **F1-71** Structured data (Event/SportsEvent schema) on race pages. AC: validates in Google's rich-results test.
- **F1-72** Performance pass: sub-2s mobile loads (server-rendered Flask is already well-suited; keep JS minimal). AC: Lighthouse ≥ 90 performance/SEO on race + landing pages.
- **F1-73** Season calendar page (all races, lock times in visitor's timezone, links to each race page). AC: indexable, useful standalone — another long-tail magnet ("F1 2027 calendar predictions").

### E9 — Monetization & Compliance — Phase 3 [MVP-lite], rest staged

- **F1-80** Privacy policy, terms, cookie/consent management (needed before any ads; cheap to do now). AC: consent gate compliant for EU visitors; analytics respects it.
- **F1-81** AdSense integration behind a feature flag, single below-fold placement on race/sentiment pages. AC: flag stays OFF until ~1k monthly visitors (Stage 1 trigger); layout designed so the ad slot's presence/absence doesn't shift content.
- **F1-82** [POST] Affiliate content slots (merch/streaming links in sentiment pages where contextually honest).
- **F1-83** [POST] Print-on-demand league-champion merch (Printful or similar; zero inventory).

### E10 — Analytics & Operations [MVP] — Phase 3

- **F1-90** Privacy-friendly analytics (Plausible/Umami, self-hosted on the k8s cluster). AC: page views, referrers, invite-link conversion, prediction funnel (visit → pick → sign-up) visible on one dashboard.
- **F1-91** Uptime + cron monitoring with Telegram alerts (extend existing OpenClaw alerting patterns). AC: Brett knows about failures before users do.
- **F1-92** Nightly SQLite backup to off-cluster storage. AC: restore tested once. (SQLite is *fine* at this scale — revisit Postgres only if concurrent-write errors actually appear; don't pre-engineer.)
- **F1-93** Rate limiting + basic abuse protection on auth and prediction endpoints. AC: magic-link endpoint can't be used to spam email; league invites can't be brute-forced.

### E11 — Launch Execution — Phase 3 (March 2027)

- **F1-100** Beta retrospective: fix list from a full season-half of private-league use, triaged before launch.
- **F1-101** Launch-week content: season-opener sentiment page live 10+ days early (indexing lead time), landing page pointed at it.
- **F1-102** Product Hunt + Show HN posts prepared and timed to season-opener week (angle: "AI-summarized F1 fan sentiment + prediction leagues").
- **F1-103** Discord/community sharing list researched and posted per-community rules (genuine-participant framing).
- **F1-104** Friends' leagues seeded and active before launch day so the site is visibly alive (leaderboards populated, sentiment fresh) for first strangers.
- **F1-105** Launch metrics checkpoint 2 weeks post-launch: visitors, invite conversion, D7 return rate → decide next quarter's focus (growth vs. retention vs. monetization).

---

## 6. Post-Release Backlog (summary)

Everything tagged [POST] above, plus, unprioritized until post-launch data exists:

- Live race-day experience (would need OpenF1 paid tier — only if traffic justifies)
- Constructor/championship-long predictions (season-long bets between friends)
- Public league discovery ("join a public league" for solo users who want competition)
- Email digest: post-race results + your score + next race sentiment (retention loop)
- Native share-to-story formats (vertical share cards)
- i18n (F1's audience is global; ES/PT are the obvious first candidates)

## 7. Risks

| Risk | Mitigation |
|------|-----------|
| OpenF1 outage on race weekend | E1 resilience stories; manual override path is a first-class feature |
| OpenF1 free tier changes/dies | Adapter isolation (F1-01) means one module to swap; manual entry keeps the site functional meanwhile |
| Reddit API terms tighten | Sentiment degrades gracefully to predictions-only site; ingestion is a cron, not a runtime dependency |
| Zero organic traction at launch | League loop works at any scale — the site is fun with 10 friends even if SEO takes a year. The floor outcome is "great private game for Brett's friends," which is an acceptable floor. |
| Solo-builder burnout | Phases are sequenced so each one ships something usable; beta season provides real feedback/motivation early |
| F1 IP/trademark exposure | No official logos/photography; team *colors* + driver names/stats are facts; "not affiliated with Formula 1" disclaimer; avoid "F1" alone as the product brand name (naming story to add in E7) |

## 8. Open Items for Brett (running list, one at a time in chat)

- Product/domain name for launch (currently `f1.brettswift.com`; trademark-safe naming is an E7 concern).
- Which friends/groups to recruit as beta league #1 this season.
- Confirm the March-2027 public launch framing (everything above assumes it).
