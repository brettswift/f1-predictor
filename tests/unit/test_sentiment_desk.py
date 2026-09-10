"""Production data boundaries and persistence for the approved sentiment desk."""
from datetime import datetime, timedelta, timezone
import re


def seed(client):
    from app import get_db
    db = get_db()
    when = (datetime.now(timezone.utc) + timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S')
    db.execute("INSERT INTO races (name, round, date, status) VALUES ('Review GP', 901, ?, 'open')", (when,))
    race = db.execute('SELECT id FROM races WHERE round=901').fetchone()['id']
    for i, name in enumerate(['One Driver', 'Two Driver', 'Three Driver'], 901):
        db.execute('INSERT INTO drivers (id, driver_id, name, number, code) VALUES (?, ?, ?, ?, ?)', (i, str(i), name, i, 'DRV'))
    db.commit()
    client.post('/set-username', data={'username': 'review-user'})
    return race


def test_visitor_sentiment_is_not_fabricated(client):
    page = client.get('/').get_data(as_text=True)
    assert 'Awaiting data' in page
    assert '3,240' not in page
    assert page.index('id="the-wire"') < page.index('id="your-race-card"')
    assert page.count('THE UNDER<span>·</span>CUT') == 1


def test_vote_persists_and_returns_readonly_card(app, client):
    from app import get_db
    race = seed(client)
    page = client.get('/home').get_data(as_text=True)
    assert '<details class="vote-disclosure"' in page
    response = client.post(f'/predict/{race}', data={'p1':901,'p2':902,'p3':903,'sc_conviction':65}, follow_redirects=True)
    assert response.status_code == 200
    page = response.get_data(as_text=True)
    assert 'Your saved vote' in page
    assert '<details class="vote-disclosure"' not in page
    assert re.search(r'<input[^>]*id="sr"[^>]*disabled', page)
    assert 'Read-only' in page
    saved = get_db().execute('SELECT * FROM sc_votes WHERE race_id=?', (race,)).fetchone()
    assert saved['conviction'] == 65
    assert f'data-saved-multiplier="{saved["multiplier"]}"' in page
    assert 'Your saved vote' in client.get('/home').get_data(as_text=True)
    assert 'Awaiting data' in page  # votes must never fabricate sentiment


def test_zero_safety_call_still_displays_readonly(app, client):
    race = seed(client)
    page = client.post(f'/predict/{race}', data={'p1':901,'p2':902,'p3':903,'sc_conviction':0}, follow_redirects=True).get_data(as_text=True)
    assert 'Your saved vote' in page
    assert re.search(r'<input[^>]*id="sr"[^>]*value="0"[^>]*disabled', page)
