# F1 Predictor — 2026 Relaunch Brief

Living document, compiled during a requirements interview with Brett (Aug 2026). Purpose: capture decisions and open questions before handing this to Opus for a full feature/architecture strategy pass, then Fable for a voice/UX/engagement review.

## Status

Not greenfield — this is a real, already-deployed app (Flask + SQLite, k8s, dev/prod envs, CI/CD). This brief is about the gap between what exists and a public, ad-monetized relaunch, not a from-scratch spec.

## Decisions Locked So Far

### Data source: swap Jolpica → OpenF1
- **Why**: Jolpica (Ergast mirror) has zero safety-car/race-control data — hard blocker for the safety-car prediction feature, independent of reliability.
- **OpenF1** (`api.openf1.org`) covers it: `session_result`, `starting_grid`, and `race_control` (flags/safety car/incidents) are all in the **free, no-auth historical tier**. Confirmed from their docs: `session_result` "becomes available a few minutes after the official results are published on the official Formula 1 website."
- Paid OpenF1 tier is only for **live in-session telemetry** (sub-second position/lap updates during a live session) — not required for the core predict → lock → score loop. Only relevant if a true live-ticking leaderboard during the race becomes a priority later.
- **Reliability is not solved by source choice** — no free F1 data source has an SLA (confirmed: even Jolpica's own maintainers say "no active monitoring, we only notice when someone reports it"; an OpenF1 user on their GitHub described real production outages with days of silence). This means resilience has to be an **engineering property of the app**, not an assumption about the upstream:
  - Cache last-known-good data; show "checking for results" instead of breaking
  - Manual admin override to enter results by hand when the API is down (app already partially has this — `enter_results.html`)
  - Buffer/grace period before locking predictions or declaring "final" if the feed is late
  - Alerting when a fetch fails, so Brett knows before users complain
  - This should be a top priority for Opus's strategy pass — it's the actual "reliability" ask, reframed correctly.

### Core prediction loop
- Podium (P1/P2/P3) + winner — already built.
- **New**: "extras" predictions — safety car (yes/no, or laps under SC) at minimum. Open question: how far to extend (DNFs, fastest lap, quali order) — see Open Questions.

### Audience & acquisition
- General public, found via search engines and ads — not a closed hobby project.
- Primary usage pattern: **groups of friends competing against each other**, plus **solo users** who just want their own ranking.
- Implication: needs low-friction onboarding, shareable group/league creation, and probably some SEO-friendly public content (see Sentiment Dashboard below — doubles as content marketing).

### Monetization
- Ads first. Possibly payouts/prizes later. Long-term maybe sponsorship.
- Explicitly modest expectations — "pennies to dollars," not "tens or hundreds of thousands." This should keep infra/complexity choices cheap and low-maintenance, not enterprise-scale.

### Timeline
- No hard deadline. Season is already halfway through 2026, so there's no "launch by opening race" pressure this year.

## New Feature Idea: Race Sentiment Dashboard

Brett's idea: use a cheap model (DeepSeek v4 Flash) to regularly pull sentiment from forums/Reddit/social sources — who people think will win, get pole, general positivity/negativity per driver — and surface it as a sentiment dashboard alongside predictions.

**Why this is a good idea, not just a nice-to-have:**
1. **Retention/return-visit hook independent of the predict-and-wait cycle.** The core loop (predict → wait for race → see score) only gives users a reason to visit twice per race weekend. Sentiment content gives a reason to check in more often — between qualifying, before lock, mid-week driver chatter.
2. **Content marketing / SEO value.** Pages like "who's favored to win the [race name] Grand Prix" are exactly the kind of long-tail query his stated acquisition channel (search) can rank for — this isn't just a feature, it's organic-traffic infrastructure.
3. **Cheap to run.** Matches his monetization reality — a scheduled cron pulling a few sources and summarizing with a cheap model is low-cost compared to trying to buy reliable official data.
4. **Feeds back into the core loop.** Sentiment can be shown as a soft signal ("62% of predictors + sentiment lean toward Verstappen for pole") without replacing user predictions — adds flavor without adding complexity to scoring.

**Decided**: use Reddit's official API (free tier, OAuth app registration) rather than raw scraping — Brett only needs a poll every couple of hours, well within Reddit's free-tier terms, and using the official API avoids the ToS/rate-limit risk of unauthorized scraping. Poll a handful of F1-relevant subreddits (e.g. r/formula1) every 2-3 hours, summarize with DeepSeek v4 Flash.

**Still open for Opus**: whether sentiment is shown pre-lock only or all the time, and whether it's purely descriptive or ever influences scoring/badges. Other forum sources beyond Reddit are optional/stretch, not required for v1.

## Other Retention/Engagement Ideas Worth Investigating

Brainstormed while thinking about "what keeps people coming back beyond the prediction itself" — not yet decided, just candidates for Opus/Fable to weigh:

- **Streaks & badges** — "correct podium 3 races running," "predicted every safety car this season," etc. Cheap to build, strong stickiness pattern in similar apps (fantasy sports, Wordle-style streaks).
- **Group/league social layer** — easy invite links, private group leaderboards, head-to-head rivalry stats between two specific friends across a season. This is the actual product for the "groups of friends" audience segment, not a side feature.
- **Shareable result/prediction cards** — auto-generated image/card ("I nailed the podium for Monaco 🏆") for social sharing — cheap viral loop, especially relevant since acquisition is search/ads and organic social sharing is free distribution.
- **Season-long stats/history** — "your all-time accuracy," "best predictor in your group ever" — gives long-time users something permanent to care about beyond one race.
- **Low-friction quick-predict** — one-tap podium picks for casual/ad-driven traffic who won't fill out a long form; deeper "extras" predictions optional for engaged users.
- **Pre-lock reminders** (opt-in email/push) — nudges people back right before qualifying/race lock, directly driving the return visits the core loop depends on.

## Open Questions (single-question interview format going forward)

Brett asked to be interviewed **one question at a time**, not in batches. Track answered/open here:

- [x] Core loop scope (podium + winner + safety car extras)
- [x] Audience (general public via search/ads, groups + solo)
- [x] Data source (OpenF1, free tier)
- [x] Monetization (ads → later sponsorship, modest expectations)
- [x] Timeline — none this year, confirmed
- [x] Groups/leagues — confirmed net-new, no existing schema for it
- [→] How far do "extras" predictions go beyond safety car? (DNFs, fastest lap, quali order?) — **deferred to Opus**. Brett doesn't have a firm answer; this is a launch-scope call, not a preference to extract now. Opus should propose a recommendation as part of the strategy pass rather than wait on this.
- [x] Sentiment dashboard source/cadence — Reddit official API, poll every 2-3 hours, DeepSeek v4 Flash for summarization.
- [→] Which of the "other retention ideas" above (if any) to build — **deferred to Opus**. It's a brainstorm list for the strategy pass to weigh/prioritize, not a pre-decided menu.

## Next Step

Interview is effectively complete — every question is either answered or explicitly deferred to Opus with a note on what to decide. Ready to hand this brief to Opus for the full feature/architecture strategy pass, then Fable for a voice/UX/engagement review pass.
