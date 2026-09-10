# Sentiment-first homepage rollout

Approved by Brett on 2026-09-10. Production target: https://f1.brettswift.com . No dev-account/environment changes and no domain migration in this release.

## Shipped

- One header brand; no repeated brand hero or footer. Race context remains the H1.
- Purple sentiment podium / Wire / source-story areas above optional amber personal card.
- No fabricated production sentiment: explicit awaiting-data states until verified feeds land.
- Click-to-open vote editor preserves existing podium picker, safety-car conviction, auth and server POST.
- Saved card renders on main page with disabled slots/slider; saved multiplier comes from sc_votes rather than repricing against current crowd.
- Crowd picks and standings remain available as separately labelled secondary disclosures. Mobile section navigation and accessible picker focus handling.

## Backend and data work

Existing BUD-136 (official Reddit ingestion) and BUD-137 (local-first extraction/ingest) updated for source timestamps, lineage, deduplication and real positive/neutral/negative counts. The scalar-score-only contract and duplicate-insert retry behavior are explicitly superseded.

- [BUD-185](https://linear.app/buddy-bot/issue/BUD-185): rolling 24h/48h read API, sentiment podium, thresholds, freshness and UI connection; blocked by BUD-137.
- [BUD-186](https://linear.app/buddy-bot/issue/BUD-186): sourced/versioned talking points and UI connection; blocked by BUD-137.
- [BUD-187](https://linear.app/buddy-bot/issue/BUD-187): personal-card/error read model, atomicity/abstention and immutable saved values.

All remain Backlog. External notifications belong to BUD-164, not application jobs.

## Verification

491 unit tests and 9 integration tests passed in separate invocations, matching CI. Three added regression cases cover visitor data boundaries, persisted/read-only submitted cards, and zero safety-car call.

Running unit and integration suites in a single process exposed an existing module/time-mocking interaction in test_multi_race_timezone; both pass independently as configured in CI. No production time logic changed.
