# Sentiment source-access gate

Status at 2026-09-11: **implementation readiness: ready; Reddit fan-source
access readiness: blocked; editorial RSS readiness: verified.** Editorial
coverage is never included in the fan-sentiment denominator.

## Reddit fan sentiment (blocked pending human action)

The future collector may use only Reddit's documented OAuth API
(<https://www.reddit.com/dev/api/>) for `r/formula1` and `r/F1Technical`, with
an application owned by the authorized operator. It must identify itself with a
descriptive, versioned User-Agent. OAuth token issuance/renewal follows
<https://github.com/reddit-archive/reddit/wiki/OAuth2>; this must be rechecked
against Reddit's current developer documentation before enablement.
No HTML scraping, credential sharing, account creation, or paid subscription is
authorized by this repository.

Required human action:

1. Confirm that the intended Reddit account may create/use an API application
   and that its use complies with Reddit's current Developer Terms and the
   subreddits' rules.
2. Create or approve an OAuth application and enter these values through the
   protected masked-secret flow only: `REDDIT_CLIENT_ID`,
   `REDDIT_CLIENT_SECRET`, and `REDDIT_REFRESH_TOKEN`. Do not place them in a
   `.env` file, Kubernetes manifest, GitHub secret dump, command line, or chat.
3. Authorize a read-only token for OAuth endpoint
   `https://oauth.reddit.com`, then run a token-free probe of both
   `GET /r/formula1/comments/<article>` and
   `GET /r/F1Technical/comments/<article>` (or the documented listing/comment
   endpoints). Record only status, source-created UTC, fullname/stable ID and
   permalink in private evidence.

The minimum requested capability is `read`; identity is used only if Reddit's
current OAuth flow requires it to establish the application context. The
collector must obey current API headers and documented limits, including
`Retry-After` and reset/rate-limit metadata. Refresh-token expiration or a
401/403 is an `auth_failure`, stops the source run nonzero, and must not be
reported as an empty poll. The readiness helper intentionally keeps configured
secret references blocked until the authenticated probe supplies all three
metadata fields.

Before enabling Reddit, the operator must verify the current Reddit Developer
Terms for permitted retention and display. The collector will attribute every
displayed item with its canonical source URL; it retains only the minimum raw
text/metadata needed by the approved terms and configured retention period.

## Verified editorial source

`https://www.motorsport.com/rss/f1/news/` returned HTTP 200 and parseable XML
on 2026-09-11. Sanitized metadata is in
[`evidence/BUD-189-source-access-2026-09-11.json`](evidence/BUD-189-source-access-2026-09-11.json).
It is an editorial feed: it may later produce attributed coverage or summaries,
but is excluded from fan-opinion collection, fan counts, shares, rankings, and
the fan denominator. No launch may imply fan sentiment while Reddit remains
blocked.

## Storage, ownership, and diagnostics

The initial collector and local Unsloth analyzer run on Buddy. The collector
owns `RAW_SENTIMENT_DIR`, laid out as
`RAW_SENTIMENT_DIR/reddit/{subreddit}/{UTC-run}.jsonl` and
`RAW_SENTIMENT_DIR/motorsport_f1_rss/{UTC-run}.jsonl`; the analyzer receives
read-only access. The website host receives only validated structured ingest,
not the raw directory or a shared SQLite database. Raw files are private,
operator-owned, minimally retained per verified provider terms, and never
included in public diagnostics, exports, source-access evidence, or logs.
Records omit author profiles unless a documented provider requirement makes a
minimal identifier necessary.

## QA procedure

1. Run `pytest tests/unit/test_source_access.py` to reproduce configured-name,
   sanitized-evidence, complete-read, and invalid/expired-token handling.
2. With protected credentials supplied, make one real read from each enabled
   subreddit and save only private, token-free metadata. Confirm 401/403 after
   a deliberately invalid/expired credential is `auth_failure`.
3. Search the repository, test output, and exported evidence for each secret
   value/reference policy violation before enabling collection.

Access readiness is a source-specific external gate. Implementation readiness
only means the code/docs can correctly report that gate; it is not evidence of
Reddit access.
