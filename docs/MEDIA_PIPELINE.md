# Media pipeline — design

**Written 2026-09-12.** Supersedes the Reddit-based E6 sentiment design and the
50/50 news-plus-votes blend. Read `FEATURE_STATE.md` first for what exists today.

---

## The decision that shapes everything

**We do not blend news into The Wire.** Two panels, two different claims:

| Panel | Source | Claim it makes |
|---|---|---|
| **Media** (left) | Open F1 news RSS | "Here's what the press is saying this weekend" — one summary, citation links only |
| **The Wire** (right) | This app's own predictions, plus the media digest for comparison | "71% of you backed Norris — the press doesn't agree" |

Blending them produces a number nobody can check and that answers no question a
user asked. Predictions are *a stake*; news is *a description*. Averaging them is
a category error.

The **tension between the panels is the feature**: "press leans Verstappen, but
71% of you backed Norris" is a better story than any average. The right panel
states that comparison outright rather than leaving the user to make it.

**Neither panel shows article text.** The left panel is one summary plus bare
citation links — no quote cards, no per-article blurbs. Surfacing 3–5 individual
articles was considered and dropped: it makes the page busy and buries the one
summary that matters. Evidence quotes are still captured in stage 2 (they keep
tints auditable and feed the digest) — they just aren't rendered as their own UI.

Per-article driver tints **are** in scope. A tint attached to a specific article
with a source link and an evidence quote is auditable. An aggregate site-wide
sentiment percentage is not. We build the first and never the second.

---

## Three stages

Each stage is a separate job. Stage 2 and 3 find their work by querying for rows
in a given state — **the tables are the queue**. No message broker. Polling now;
this design stays valid when we move to event-driven later, because the state
transitions are already explicit.

```
  [1] fetch          [2] analyse per article      [3] roll up
  RSS feeds   ──▶   media_articles  ──▶   media_insights ──┬──▶ media_digests
  (cron)            (status=pending)       (one/article)   │    (left panel)
                                                           │
                          predictions ────────────────────┴──▶ wire_comparisons
                          (pick distribution)                   (right panel)
```

### Stage 1 — fetch

A cron job polls the three verified open feeds and writes raw articles. No model
involved, no interpretation. Fast, and safe to run often.

- `https://www.formula1.com/en/latest/all.xml`
- `https://www.motorsport.com/rss/f1/news/`
- `https://www.autosport.com/rss/f1/news/`

All three are free, public, and need no auth — verified live against the
Madrid/Spanish GP weekend on 2026-09-12.

**Dedup keys off the feed's stable `<guid>`, never the date.** `pubDate` is for
freshness and ordering only. Re-running the job must never create a second row
for the same article.

Cadence is race-weekend aware: frequent Fri–Sun, sparse midweek. A feed being
down is logged and skipped — one bad feed never fails the run, and nothing here
touches prediction or scoring paths.

### Stage 2 — analyse each article

A local model picks up articles with `status = 'pending'`, one at a time, and
returns **structured JSON conforming to a schema** (below). Result lands in
`media_insights`; the article flips to `analysed` or `failed`.

This is where driver attribution and per-driver tints happen. Key rule: **every
tint carries an evidence quote** — the actual sentence from the article that
justifies it. That makes a wrong tint visible without re-reading the source, and
the quote doubles as display text on the site.

An article can mention several drivers with different tints. That's expected and
the schema handles it.

### Stage 3 — two rollups

Stage 3 produces **two separate outputs with different inputs and different
triggers.** Conflating them would mean regenerating the media digest every time
someone submits a pick, which is wasteful and makes the left panel churn for no
reason.

#### 3a — media digest (left panel)

A model pass summarises *the summaries* into one digest per race weekend. Input
is stage 2 insights only. Regenerates when new articles are analysed.

`strength` no longer decides what gets displayed — it weights which insights the
digest should emphasise, and which sources are worth citing. The output is one
paragraph plus a list of cited article URLs.

#### 3b — Wire comparison (right panel)

**Computed first, phrased second.** Code derives the actual relationship between
press coverage and player picks — which driver the media leans toward, which
driver players back and by what share, where the biggest divergence is. Those
computed facts are then handed to the model, which only turns them into a
sentence.

The model is never asked to *decide* whether the two agree. It receives
`{media_leader: "verstappen", crowd_leader: "norris", crowd_share: 0.71,
divergence: 0.34, ...}` and writes prose around it. This means the claim on
screen is always arithmetically true, and the model cannot invent a
disagreement that the numbers don't support.

Regenerates when either the media digest changes **or** the pick distribution
moves materially. Because picks shift right up to lock, this runs more often
than 3a.

Media leaning is derived from stage 2 tints: aggregate `sentiment` weighted by
`strength` per driver, across the weekend's articles. This is an internal
ranking input for the comparison — **not** a public sentiment percentage. We
still never display an aggregate media sentiment score.

Both rollups keep previous versions rather than overwriting, so a failed
regeneration leaves the last good one serving.

---

## JSON schema — stage 2 output

The model must return exactly this. Anything that fails validation marks the
article `failed` with the raw output retained for debugging — **never a partial
or coerced write.**

```json
{
  "summary": "One or two sentences, plain factual description of the article.",
  "drivers": [
    {
      "driver_id": "norris",
      "sentiment": "positive",
      "strength": 0.82,
      "evidence": "Norris looked untouchable in the final sector, taking pole by three tenths."
    }
  ],
  "topics": ["qualifying", "strategy"],
  "race_relevance": "madrid-2026",
  "confidence": 0.9
}
```

Field rules:

- `sentiment` — `positive` | `neutral` | `negative`. Nothing else.
- `strength` — 0.0–1.0. How strongly the article expresses that view. This is
  **not** a probability that the model is right; that's `confidence`. Used for
  ranking which articles surface.
- `evidence` — must be a verbatim span from the article. If the model can't
  quote it, it doesn't get to claim it. Worth a post-check that the string
  actually appears in the source text.
- `driver_id` — must match an existing row in `drivers.driver_id`. Unknown
  drivers are dropped with a warning, not invented.
- `race_relevance` — canonical race identifier, or `null` for general F1 news
  not tied to a weekend. **Keep unattributed news separate; never fold it into a
  race weekend's digest.**
- `confidence` — model's own certainty. Low-confidence insights can be stored but
  held back from display.

---

## Tables

Following existing conventions in `init_db()` — SQLite, no ORM, `CREATE TABLE IF
NOT EXISTS`, timestamps in UTC.

```sql
-- Stage 1 output: raw articles, one row per feed item, ever.
CREATE TABLE IF NOT EXISTS media_articles (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    guid          TEXT NOT NULL UNIQUE,      -- dedup key, from the feed
    source        TEXT NOT NULL,             -- 'formula1' | 'motorsport' | 'autosport'
    url           TEXT NOT NULL,
    title         TEXT NOT NULL,
    body          TEXT,                      -- summary/description from the feed
    published_at  TIMESTAMP,                 -- from pubDate; display + ordering only
    fetched_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status        TEXT DEFAULT 'pending'
                  CHECK (status IN ('pending', 'analysed', 'failed', 'skipped'))
);

-- Stage 2 output: one row per article, holding validated model JSON.
CREATE TABLE IF NOT EXISTS media_insights (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id     INTEGER NOT NULL UNIQUE REFERENCES media_articles(id),
    summary        TEXT NOT NULL,
    payload        TEXT NOT NULL,            -- full validated JSON
    race_id        INTEGER REFERENCES races(id),
    confidence     REAL,
    model_id       TEXT NOT NULL,            -- which model + version produced this
    prompt_version TEXT NOT NULL,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Flattened per-driver tints. Denormalised from payload so the site can query
-- "articles about Hamilton this weekend" without parsing JSON in SQL.
CREATE TABLE IF NOT EXISTS media_driver_sentiment (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    insight_id INTEGER NOT NULL REFERENCES media_insights(id),
    driver_id  TEXT NOT NULL REFERENCES drivers(driver_id),
    sentiment  TEXT NOT NULL CHECK (sentiment IN ('positive', 'neutral', 'negative')),
    strength   REAL NOT NULL,
    evidence   TEXT NOT NULL
);

-- Stage 3a output: the left panel. One summary per race weekend, versioned.
CREATE TABLE IF NOT EXISTS media_digests (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id        INTEGER NOT NULL REFERENCES races(id),
    version        INTEGER NOT NULL,
    summary        TEXT NOT NULL,
    citations      TEXT NOT NULL,            -- JSON: [{title, url, source}] for the link chips
    article_count  INTEGER NOT NULL,         -- how many insights fed this digest
    model_id       TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (race_id, version)
);

-- Stage 3b output: the right panel. Computed comparison + model phrasing.
CREATE TABLE IF NOT EXISTS wire_comparisons (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id        INTEGER NOT NULL REFERENCES races(id),
    version        INTEGER NOT NULL,
    digest_id      INTEGER NOT NULL REFERENCES media_digests(id),
    facts          TEXT NOT NULL,            -- JSON: the computed numbers handed to the model
    summary        TEXT NOT NULL,            -- model prose, phrased strictly from facts
    alignment      TEXT NOT NULL
                   CHECK (alignment IN ('aligned', 'mixed', 'contrasting')),
    prediction_count INTEGER NOT NULL,       -- sample size behind the crowd side
    model_id       TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (race_id, version)
);
```

`facts` is stored deliberately: it's the audit trail proving the prose matched
the arithmetic at generation time. `alignment` is computed from the divergence,
not chosen by the model — it drives any UI treatment without trusting prose.

Storing `model_id` and `prompt_version` on every generated row is deliberate:
when output quality shifts, we need to know what produced what.

---

## Rules that keep this honest

- **Never display a claim without its source link.** The media digest always
  ships with its citation chips; every cited URL is a real fetched article.
- **The right panel never asserts more than the arithmetic.** Model prose is
  phrased from computed facts, never from its own reading of the relationship.
  If `facts` and `summary` disagree, that's a bug.
- **Never fabricate.** No sentiment where there are no articles. Empty state says
  "no coverage yet" — the existing `test_sentiment_desk.py` already guards this
  posture and must keep passing.
- **Media never touches scoring.** Predictions are scored exactly as they are
  today. Sentiment is colour commentary, never mechanics. (This was the original
  F1-53 decision and it still holds.)
- **Keep the panels separately named in code and UI.** User predictions,
  aggregate player picks, and media sentiment are three distinct data objects
  with their own provenance. Do not let them merge behind one label.
- **Model work stays off the request path.** Stages 2 and 3 are batch jobs. A
  page render never waits on inference.
- **Local inference only.** No per-article cloud spend, so the old $5/mo
  guardrail no longer applies.

---

## Failure behaviour

| Failure | Behaviour |
|---|---|
| One feed down | Log, skip, carry on with the others |
| All feeds down | Job exits nonzero; site shows last known articles, marked stale |
| Model returns invalid JSON | Article → `failed`, raw output kept; never a partial write |
| Model times out | Article stays `pending`, retried next run |
| Evidence quote not found in source | Reject that driver tint, keep the rest |
| Digest generation fails | Previous version keeps serving |
| No articles for a weekend | Left panel empty state; right panel shows picks only, no comparison |
| Too few predictions to compare | Right panel shows the media digest side only, never a comparison on a tiny sample |
| Comparison prose contradicts `facts` | Treat as a bug, not a display choice — fail the generation |

Re-running any stage must be safe. Stage 1 dedups on `guid`; stage 2 is unique
per `article_id`; both stage 3 rollups write a new version rather than mutating.

**The right panel degrades to the left panel's absence, not the reverse.** With
no media there is still a Wire — it just doesn't claim a comparison. The Wire
never depends on the media pipeline being healthy.

---

## What this replaces

- **Reddit ingestion** — out of scope entirely. `src/source_access.py` and
  `tests/unit/test_source_access.py` are Reddit-only and should be retired.
- **`docs/SOURCE_ACCESS_GATE.md`** (unmerged branch) — its rule that "editorial
  coverage is never included in the fan-sentiment denominator" is satisfied here
  by not having a fan-sentiment denominator at all. Media sentiment is labelled
  as press coverage, never as fan opinion.
- **The 50/50 blend** — dropped, per above.

Open question for whoever picks this up: the site copy should stop saying "fan
sentiment" anywhere it now means press coverage. "What the press is saying" is
both accurate and more interesting.
