"""Seed a local database with crowd picks so The Wire renders for visual review.

Local preview only; not imported by the app or the test suite.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import app as app_module  # noqa: E402

DRIVERS = [
    ('VER', 'Max Verstappen', 'Red Bull'),
    ('NOR', 'Lando Norris', 'McLaren'),
    ('PIA', 'Oscar Piastri', 'McLaren'),
    ('LEC', 'Charles Leclerc', 'Ferrari'),
    ('HAM', 'Lewis Hamilton', 'Ferrari'),
    ('RUS', 'George Russell', 'Mercedes'),
    ('ALO', 'Fernando Alonso', 'Aston Martin'),
    ('TSU', 'Yuki Tsunoda', 'RB'),
]

# How many of the 40 cards put each driver on the podium / pick them to win.
SHAPE = [(38, 16), (31, 9), (28, 7), (24, 5), (19, 2), (13, 1), (8, 0), (4, 0)]

RACE_ID = 9100


def main():
    with app_module.app.app_context():
        app_module.init_db()
        db = app_module.get_db()

        db.execute('DELETE FROM predictions WHERE race_id = ?', (RACE_ID,))
        db.execute('DELETE FROM races WHERE id = ?', (RACE_ID,))
        db.execute(
            'INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
            (RACE_ID, 'Italian Grand Prix', 16, '2026-09-13 13:00:00', 'open'),
        )

        ids = {}
        for i, (code, name, team) in enumerate(DRIVERS):
            did = RACE_ID + 1 + i
            ids[code] = did
            db.execute('DELETE FROM drivers WHERE id = ?', (did,))
            db.execute(
                'INSERT INTO drivers (id, driver_id, name, team, number, code, nationality)'
                ' VALUES (?, ?, ?, ?, ?, ?, ?)',
                (did, f'{code.lower()}-preview', name, team, did, code, 'NA'),
            )

        # Build 40 cards whose aggregate matches SHAPE closely enough to review.
        total = 40
        podium_slots = []
        for (code, _, _), (podium, win) in zip(DRIVERS, SHAPE):
            podium_slots.append((ids[code], podium, win))

        for card in range(total):
            sid = f'wire-preview-{card}'
            db.execute('DELETE FROM users WHERE session_id = ?', (sid,))
            db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                       (sid, f'previewer{card}'))
            picks = [did for did, podium, _ in podium_slots if card < podium]
            while len(picks) < 3:
                picks.append(ids['TSU'])
            winner = next(
                (did for did, _, win in podium_slots if card < win), picks[0]
            )
            ordered = [winner] + [p for p in picks if p != winner][:2]
            while len(ordered) < 3:
                ordered.append(ids['ALO'])
            db.execute(
                'INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id)'
                ' VALUES (?, ?, ?, ?, ?)',
                (sid, RACE_ID, ordered[0], ordered[1], ordered[2]),
            )

        db.commit()
        dist = app_module.get_pick_distribution(db, RACE_ID)
        print(f"status={dist['status']} total={dist['total']}")
        for r in dist['rows']:
            print(f"  {r['code']:>4}  podium {r['backed_pct']:>3}%  "
                  f"off {r['passed_pct']:>3}%  win {r['win_pct']:>3}%")


if __name__ == '__main__':
    main()
