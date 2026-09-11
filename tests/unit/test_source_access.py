from source_access import AccessState, authenticated_fetch_evidence, reddit_readiness


def test_reddit_missing_protected_setup_is_blocked_without_values():
    result = reddit_readiness({"REDDIT_CLIENT_ID": "present"})
    assert result.state is AccessState.BLOCKED
    assert result.authenticated_read_proven is False
    assert result.missing_secret_refs == ("REDDIT_CLIENT_SECRET", "REDDIT_REFRESH_TOKEN")
    assert "present" not in str(result.sanitized())


def test_configured_names_do_not_claim_access():
    result = reddit_readiness({name: "configured" for name in (
        "REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET", "REDDIT_REFRESH_TOKEN"
    )})
    assert result.state is AccessState.BLOCKED
    assert result.authenticated_read_proven is False


def test_authenticated_probe_requires_stable_metadata():
    result = authenticated_fetch_evidence(
        status_code=200, stable_id="t1_example",
        permalink="https://www.reddit.com/r/formula1/comments/example/comment/example/",
        source_created_at="2026-09-11T11:00:00Z",
    )
    assert result.state is AccessState.READY
    assert result.authenticated_read_proven is True


def test_expired_or_invalid_token_is_not_ready():
    result = authenticated_fetch_evidence(
        status_code=401, stable_id=None, permalink=None, source_created_at=None
    )
    assert result.state is AccessState.AUTH_FAILURE
    assert result.authenticated_read_proven is False
    assert "token" in result.reason
