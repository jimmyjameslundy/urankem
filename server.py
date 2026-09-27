#!/usr/bin/env python3
"""#U-RankEm multi-pool web server. Postgres when DATABASE_URL is set."""
from __future__ import annotations

import json
import os
import random
import sqlite3
import threading
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None

ESPN_RANKINGS = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/rankings"

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)
DB = DATA / "urankem.db"
STATIC = ROOT / "static"

WEEK = 4
TARGET_WEEK = 5
DEFAULT_POLL = {
    "week": WEEK,
    "headline": "AP Top 25 — Week 4 (Sep 20, 2026)",
    "ranks": [
        [1, "Texas", "3-0", "at #14 Tennessee"],
        [2, "Georgia", "3-0", "vs Oklahoma"],
        [3, "Notre Dame", "3-0", "at Purdue"],
        [4, "Ole Miss", "3-0", "at #21 Florida"],
        [5, "Indiana", "3-0", "vs Northwestern"],
        [6, "Miami", "3-0", "vs Central Michigan"],
        [7, "Ohio State", "2-1", "vs Illinois"],
        [8, "Alabama", "3-0", "vs South Carolina"],
        [9, "BYU", "3-0", "BYE"],
        [10, "LSU", "2-1", "vs #23 Texas A&M"],
        [11, "Texas Tech", "3-0", "vs Sam Houston"],
        [12, "USC", "4-0", "vs #20 Oregon"],
        [13, "Penn State", "3-0", "vs Wisconsin"],
        [14, "Tennessee", "3-0", "vs #1 Texas"],
        [15, "Utah", "3-0", "at Iowa State"],
        [16, "Louisville", "2-1", "vs Wake Forest"],
        [17, "Iowa", "3-0", "at #18 Michigan"],
        [18, "Michigan", "3-0", "vs #17 Iowa"],
        [19, "Missouri", "3-0", "at #24 Mississippi State"],
        [20, "Oregon", "2-1", "at #12 USC"],
        [21, "Florida", "3-0", "vs #4 Ole Miss"],
        [22, "SMU", "2-1", "vs Missouri State"],
        [23, "Texas A&M", "2-1", "at #10 LSU"],
        [24, "Mississippi State", "3-0", "vs #19 Missouri"],
        [25, "Houston", "2-1", "at Georgia Southern"],
        [26, "Washington", "3-0", "vs Minnesota"],
        [27, "Kentucky", "2-1", "vs South Alabama"],
        [28, "West Virginia", "3-0", "vs Oklahoma State"],
        [29, "Oklahoma", "2-1", "at #2 Georgia"],
        [30, "Oklahoma State", "2-1", "at West Virginia"],
    ],
}

LOCK = threading.Lock()
DATABASE_URL = (os.environ.get("DATABASE_URL") or "").strip()
USE_PG = bool(DATABASE_URL)


class Conn:
    """Tiny wrapper: ? placeholders, dict rows, commit/close."""

    def __init__(self, raw, pg):
        self.raw = raw
        self.pg = pg

    def _sql(self, sql):
        return sql.replace("?", "%s") if self.pg else sql

    def execute(self, sql, args=()):
        cur = self.raw.cursor()
        cur.execute(self._sql(sql), args)
        return cur

    def fetchone(self):
        raise RuntimeError("call on cursor")

    def commit(self):
        self.raw.commit()

    def close(self):
        self.raw.close()


class CursorView:
    def __init__(self, cur, pg):
        self.cur = cur
        self.pg = pg

    def fetchone(self):
        row = self.cur.fetchone()
        return _row(row, self.pg)

    def fetchall(self):
        return [_row(r, self.pg) for r in self.cur.fetchall()]


def _row(row, pg):
    if row is None:
        return None
    if pg:
        return row
    return row


def db():
    if USE_PG:
        if not psycopg2:
            raise RuntimeError("psycopg2 is required when DATABASE_URL is set")
        url = DATABASE_URL
        if url.startswith("postgres://"):
            url = "postgresql://" + url[len("postgres://") :]
        raw = psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)
        return PgConn(raw)
    conn = sqlite3.connect(DB, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return SqliteConn(conn)


class SqliteConn:
    def __init__(self, raw):
        self.raw = raw

    def execute(self, sql, args=()):
        return self.raw.execute(sql, args)

    def commit(self):
        self.raw.commit()

    def close(self):
        self.raw.close()


class PgConn:
    def __init__(self, raw):
        self.raw = raw

    def execute(self, sql, args=()):
        cur = self.raw.cursor()
        cur.execute(sql.replace("?", "%s"), args)
        return cur

    def commit(self):
        self.raw.commit()

    def close(self):
        self.raw.close()


SCHEMA_PG = """
CREATE TABLE IF NOT EXISTS pools (
    id SERIAL PRIMARY KEY,
    code TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    host_name TEXT NOT NULL,
    host_pin TEXT NOT NULL,
    target_week INTEGER NOT NULL DEFAULT 5,
    created TEXT NOT NULL,
    host_email TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS players (
    id SERIAL PRIMARY KEY,
    pool_id INTEGER NOT NULL REFERENCES pools(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    pin TEXT NOT NULL,
    is_host INTEGER NOT NULL DEFAULT 0,
    UNIQUE(pool_id, name)
);
CREATE TABLE IF NOT EXISTS ballots (
    id SERIAL PRIMARY KEY,
    pool_id INTEGER NOT NULL REFERENCES pools(id) ON DELETE CASCADE,
    player_id INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    week INTEGER NOT NULL,
    teams TEXT NOT NULL,
    submitted TEXT NOT NULL,
    UNIQUE(pool_id, player_id, week)
);
CREATE TABLE IF NOT EXISTS official (
    pool_id INTEGER NOT NULL REFERENCES pools(id) ON DELETE CASCADE,
    week INTEGER NOT NULL,
    ranks TEXT NOT NULL,
    PRIMARY KEY(pool_id, week)
);
CREATE TABLE IF NOT EXISTS scores (
    pool_id INTEGER NOT NULL REFERENCES pools(id) ON DELETE CASCADE,
    week INTEGER NOT NULL,
    player_id INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    total INTEGER NOT NULL,
    exact INTEGER NOT NULL,
    lines TEXT NOT NULL,
    PRIMARY KEY(pool_id, week, player_id)
);
CREATE TABLE IF NOT EXISTS reports (
    pool_id INTEGER NOT NULL REFERENCES pools(id) ON DELETE CASCADE,
    week INTEGER NOT NULL,
    text TEXT NOT NULL,
    created TEXT NOT NULL,
    PRIMARY KEY(pool_id, week)
);
CREATE TABLE IF NOT EXISTS meta (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);
"""

SCHEMA_SQLITE = SCHEMA_PG.replace("id SERIAL PRIMARY KEY", "id INTEGER PRIMARY KEY")


def init_db():
    conn = db()
    if USE_PG:
        cur = conn.raw.cursor()
        cur.execute(SCHEMA_PG)
        conn.commit()
        cur = conn.raw.cursor()
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name='pools' AND column_name='host_email'
            """
        )
        if not cur.fetchone():
            conn.raw.cursor().execute("ALTER TABLE pools ADD COLUMN host_email TEXT DEFAULT ''")
            conn.commit()
    else:
        conn.raw.executescript(SCHEMA_SQLITE)
        cols = [r["name"] for r in conn.execute("PRAGMA table_info(pools)")]
        if "host_email" not in cols:
            conn.execute("ALTER TABLE pools ADD COLUMN host_email TEXT DEFAULT ''")
        conn.commit()
    conn.close()


def code_gen():
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(random.choice(alphabet) for _ in range(6))


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def score_slot(pick, actual):
    if actual is None or actual > 20:
        return {"pts": -(20 - pick), "type": "dropped", "note": f"Out of Top 20: -{20 - pick}"}
    if actual == pick:
        return {"pts": 10 + pick, "type": "exact", "note": f"Exact #{pick}: +{10 + pick}"}
    d = abs(actual - pick)
    return {"pts": -d, "type": "miss", "note": f"Off by {d}"}


def official_map(ranks):
    out = {}
    for r in ranks:
        if 1 <= r.get("rank", 0) <= 20:
            out[norm(r["name"])] = r["rank"]
            out[r["name"]] = r["rank"]
    return out


def norm(name):
    return " ".join((name or "").lower().replace(".", "").split())


def fetch_ap():
    req = urllib.request.Request(ESPN_RANKINGS, headers={"User-Agent": "U-RankEm/1.0"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        data = json.loads(resp.read().decode())
    poll = None
    for block in data.get("rankings") or []:
        if str(block.get("id")) == "1" or "AP" in (block.get("name") or ""):
            poll = block
            break
    if not poll:
        raise ValueError("AP poll not in ESPN feed.")
    week = int((poll.get("occurrence") or {}).get("number") or 0)
    ranks = []
    for row in poll.get("ranks") or []:
        team = row.get("team") or {}
        name = team.get("nickname") or team.get("location") or team.get("displayName") or ""
        rec = ""
        recs = team.get("recordSummary") or team.get("record")
        if isinstance(recs, str):
            rec = recs
        ranks.append(
            {
                "rank": int(row.get("current") or 0),
                "name": name,
                "record": rec,
                "next": "",
            }
        )
    live = {
        "week": week,
        "headline": poll.get("headline") or f"AP Top 25 — Week {week}",
        "updated": poll.get("lastUpdated") or now(),
        "ranks": [[r["rank"], r["name"], r["record"], r["next"]] for r in ranks],
        "official": [{"rank": r["rank"], "name": r["name"]} for r in ranks],
    }
    return live


def score_pool_week(conn, pool, week, official_ranks):
    mapping = official_map(official_ranks)
    ballots = conn.execute(
        "SELECT player_id, teams FROM ballots WHERE pool_id=? AND week=?",
        (pool["id"], week),
    ).fetchall()
    if not ballots:
        return None
    conn.execute("DELETE FROM scores WHERE pool_id=? AND week=?", (pool["id"], week))
    names = {
        r["id"]: r["name"]
        for r in conn.execute("SELECT id, name FROM players WHERE pool_id=?", (pool["id"],))
    }
    results = []
    for b in ballots:
        teams = json.loads(b["teams"])
        lines = []
        for i, team in enumerate(teams):
            pick = i + 1
            actual = mapping.get(team) or mapping.get(norm(team))
            slot = score_slot(pick, actual)
            lines.append({"pick": pick, "team": team, "actual": actual, **slot})
        total = sum(x["pts"] for x in lines)
        exact = sum(1 for x in lines if x["type"] == "exact")
        conn.execute(
            "INSERT INTO scores(pool_id,week,player_id,total,exact,lines) VALUES(?,?,?,?,?,?)",
            (pool["id"], week, b["player_id"], total, exact, json.dumps(lines)),
        )
        results.append(
            {"name": names.get(b["player_id"], "?"), "total": total, "exact": exact}
        )
    results.sort(key=lambda x: (-x["total"], x["name"]))
    lines = [f"#U-RankEm — {pool['name']}", f"Week {week} scored from the published AP poll", ""]
    for i, r in enumerate(results, 1):
        lines.append(f"{i}. {r['name']} — {r['total']} pts ({r['exact']} exact)")
    if results:
        lines.append("")
        lines.append(f"Weekly winner: {results[0]['name']}")
    text = "\n".join(lines)
    conn.execute(
        """INSERT INTO reports(pool_id,week,text,created) VALUES(?,?,?,?)
           ON CONFLICT(pool_id,week) DO UPDATE SET text=excluded.text, created=excluded.created""",
        (pool["id"], week, text, now()),
    )
    conn.execute(
        "UPDATE pools SET target_week=? WHERE id=? AND target_week<=?",
        (week + 1, pool["id"], week),
    )
    return {"week": week, "players": len(results), "report": text}


def apply_published_poll(conn, live):
    week = live["week"]
    official = live["official"]
    if week < 1 or len(official) < 20:
        return {"poll": live, "scored": []}
    scored = []
    for pool in conn.execute("SELECT * FROM pools").fetchall():
        conn.execute(
            """INSERT INTO official(pool_id,week,ranks) VALUES(?,?,?)
               ON CONFLICT(pool_id,week) DO UPDATE SET ranks=excluded.ranks""",
            (pool["id"], week, json.dumps(official)),
        )
        already = conn.execute(
            "SELECT 1 FROM scores WHERE pool_id=? AND week=? LIMIT 1",
            (pool["id"], week),
        ).fetchone()
        has_ballots = conn.execute(
            "SELECT 1 FROM ballots WHERE pool_id=? AND week=? LIMIT 1",
            (pool["id"], week),
        ).fetchone()
        if has_ballots and not already:
            result = score_pool_week(conn, pool, week, official)
            if result:
                scored.append({"code": pool["code"], "name": pool["name"], **result})
    conn.execute(
        "INSERT INTO meta(k,v) VALUES('last_poll',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
        (json.dumps(live),),
    )
    conn.commit()
    return {"poll": live, "week": week, "scored": scored}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"[{self.log_date_time_string()}] {fmt % args}", flush=True)

    def _json(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _read(self):
        n = int(self.headers.get("Content-Length") or 0)
        if not n:
            return {}
        raw = self.rfile.read(n)
        try:
            return json.loads(raw.decode() or "{}")
        except json.JSONDecodeError:
            return {}

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        if path in ("/", "/index.html"):
            return self._file(STATIC / "index.html", "text/html; charset=utf-8")
        if path.startswith("/api/"):
            return self._api_get(path, parse_qs(parsed.query))
        rel = path.lstrip("/")
        dest = (STATIC / rel).resolve()
        if str(dest).startswith(str(STATIC.resolve())) and dest.is_file():
            ctype = "text/plain"
            if dest.suffix == ".css":
                ctype = "text/css"
            elif dest.suffix == ".js":
                ctype = "application/javascript"
            elif dest.suffix == ".svg":
                ctype = "image/svg+xml"
            return self._file(dest, ctype)
        self._json(404, {"error": "Not found"})

    def _file(self, path: Path, ctype: str):
        if not path.is_file():
            self._json(404, {"error": "Missing file"})
            return
        data = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        path = urlparse(self.path).path
        body = self._read()
        try:
            with LOCK:
                self._api_post(path, body)
        except Exception as e:
            self._json(400, {"error": str(e)})

    def _pool(self, conn, code):
        row = conn.execute("SELECT * FROM pools WHERE code=?", (code.upper(),)).fetchone()
        if not row:
            raise ValueError("No pool with that code.")
        return row

    def _player(self, conn, pool_id, name, pin):
        row = conn.execute(
            "SELECT * FROM players WHERE pool_id=? AND name=?", (pool_id, name.strip())
        ).fetchone()
        if not row:
            raise ValueError("Name not in this pool.")
        if row["pin"] != pin:
            raise ValueError("Wrong PIN for that name.")
        return row

    def _snapshot(self, conn, pool):
        players = [
            dict(r)
            for r in conn.execute(
                "SELECT id, name, is_host FROM players WHERE pool_id=? ORDER BY name",
                (pool["id"],),
            )
        ]
        week = pool["target_week"]
        ballots = {}
        for r in conn.execute(
            "SELECT player_id, teams, submitted FROM ballots WHERE pool_id=? AND week=?",
            (pool["id"], week),
        ):
            ballots[r["player_id"]] = {
                "teams": json.loads(r["teams"]),
                "submitted": r["submitted"],
            }
        official = conn.execute(
            "SELECT week, ranks FROM official WHERE pool_id=? ORDER BY week",
            (pool["id"],),
        ).fetchall()
        official_out = {str(r["week"]): json.loads(r["ranks"]) for r in official}
        scores = {}
        for r in conn.execute(
            "SELECT week, player_id, total, exact, lines FROM scores WHERE pool_id=?",
            (pool["id"],),
        ):
            scores.setdefault(str(r["week"]), []).append(
                {
                    "player_id": r["player_id"],
                    "total": r["total"],
                    "exact": r["exact"],
                    "lines": json.loads(r["lines"]),
                }
            )
        season = {}
        for week_key, rows in scores.items():
            for row in rows:
                pid = row["player_id"]
                season.setdefault(pid, {"pts": 0, "weeks": 0, "wins": 0})
                season[pid]["pts"] += row["total"]
                season[pid]["weeks"] += 1
        for week_key, rows in scores.items():
            if not rows:
                continue
            best = max(r["total"] for r in rows)
            winners = [r["player_id"] for r in rows if r["total"] == best]
            for pid in winners:
                season[pid]["wins"] += 1
        names = {p["id"]: p["name"] for p in players}
        season_list = [
            {
                "player_id": pid,
                "name": names.get(pid, "?"),
                "pts": v["pts"],
                "weeks": v["weeks"],
                "wins": v["wins"],
            }
            for pid, v in season.items()
        ]
        season_list.sort(key=lambda x: (-x["pts"], -x["wins"], x["name"]))
        report = conn.execute(
            "SELECT week, text, created FROM reports WHERE pool_id=? ORDER BY week DESC LIMIT 1",
            (pool["id"],),
        ).fetchone()
        live_row = conn.execute("SELECT v FROM meta WHERE k='last_poll'").fetchone()
        poll = json.loads(live_row["v"]) if live_row else DEFAULT_POLL
        return {
            "pool": {
                "code": pool["code"],
                "name": pool["name"],
                "host_name": pool["host_name"],
                "host_email": pool["host_email"] if "host_email" in pool.keys() else "",
                "target_week": pool["target_week"],
                "created": pool["created"],
            },
            "report": dict(report) if report else None,
            "players": players,
            "ballots": ballots,
            "official": official_out,
            "scores": scores,
            "season": season_list,
            "poll": poll,
        }

    def _api_get(self, path, qs):
        if path == "/api/health":
            return self._json(200, {"ok": True, "service": "U-RankEm", "db": "postgres" if USE_PG else "sqlite"})
        if path == "/api/poll":
            conn = db()
            try:
                row = conn.execute("SELECT v FROM meta WHERE k='last_poll'").fetchone()
                return self._json(200, json.loads(row["v"]) if row else DEFAULT_POLL)
            finally:
                conn.close()
        if path == "/api/tick":
            conn = db()
            try:
                live = fetch_ap()
                with LOCK:
                    out = apply_published_poll(conn, live)
                return self._json(200, out)
            except Exception as e:
                return self._json(502, {"error": f"Could not read AP poll: {e}"})
            finally:
                conn.close()
        parts = path.strip("/").split("/")
        if len(parts) == 3 and parts[0] == "api" and parts[1] == "pool":
            code = parts[2].upper()
            conn = db()
            try:
                pool = self._pool(conn, code)
                return self._json(200, self._snapshot(conn, pool))
            except ValueError as e:
                return self._json(404, {"error": str(e)})
            finally:
                conn.close()
        self._json(404, {"error": "Unknown API"})

    def _api_post(self, path, body):
        conn = db()
        try:
            if path == "/api/pools":
                name = (body.get("name") or "").strip()
                host = (body.get("host_name") or "").strip()
                pin = (body.get("host_pin") or "").strip()
                email = (body.get("host_email") or "").strip()
                if not name or not host or len(pin) < 4:
                    raise ValueError("Pool name, your name, and a 4+ digit PIN are required.")
                for _ in range(20):
                    code = code_gen()
                    if not conn.execute("SELECT 1 FROM pools WHERE code=?", (code,)).fetchone():
                        break
                conn.execute(
                    "INSERT INTO pools(code,name,host_name,host_pin,target_week,created,host_email) VALUES(?,?,?,?,?,?,?)",
                    (code, name, host, pin, TARGET_WEEK, now(), email),
                )
                pid = conn.execute("SELECT id FROM pools WHERE code=?", (code,)).fetchone()["id"]
                conn.execute(
                    "INSERT INTO players(pool_id,name,pin,is_host) VALUES(?,?,?,1)",
                    (pid, host, pin),
                )
                conn.commit()
                pool = self._pool(conn, code)
                return self._json(
                    200,
                    {
                        "code": code,
                        "player": host,
                        "is_host": True,
                        "snapshot": self._snapshot(conn, pool),
                    },
                )

            if path.endswith("/join") and path.startswith("/api/pools/"):
                code = path.split("/")[3].upper()
                pool = self._pool(conn, code)
                pname = (body.get("name") or "").strip()
                pin = (body.get("pin") or "").strip()
                if not pname or len(pin) < 4:
                    raise ValueError("Name and a 4+ digit PIN are required.")
                existing = conn.execute(
                    "SELECT * FROM players WHERE pool_id=? AND lower(name)=lower(?)",
                    (pool["id"], pname),
                ).fetchone()
                if existing:
                    if existing["pin"] != pin:
                        raise ValueError("That name is already in this pool. Enter the PIN you used before, or pick a different name.")
                    player = existing
                else:
                    conn.execute(
                        "INSERT INTO players(pool_id,name,pin,is_host) VALUES(?,?,?,0)",
                        (pool["id"], pname, pin),
                    )
                    conn.commit()
                    player = conn.execute(
                        "SELECT * FROM players WHERE pool_id=? AND name=?",
                        (pool["id"], pname),
                    ).fetchone()
                return self._json(
                    200,
                    {
                        "code": pool["code"],
                        "player": player["name"],
                        "is_host": bool(player["is_host"]),
                        "snapshot": self._snapshot(conn, pool),
                    },
                )

            if path == "/api/signin":
                code = (body.get("code") or "").strip().upper()
                pool = self._pool(conn, code)
                player = self._player(conn, pool["id"], body.get("name") or "", body.get("pin") or "")
                return self._json(
                    200,
                    {
                        "code": pool["code"],
                        "player": player["name"],
                        "is_host": bool(player["is_host"]),
                        "snapshot": self._snapshot(conn, pool),
                    },
                )

            if path == "/api/ballot":
                pool = self._pool(conn, body.get("code") or "")
                player = self._player(conn, pool["id"], body.get("name") or "", body.get("pin") or "")
                teams = body.get("teams") or []
                if len(teams) != 20 or any(not t for t in teams):
                    raise ValueError("Fill all 20 slots.")
                if len(set(teams)) != 20:
                    raise ValueError("Each team only once.")
                week = int(body.get("week") or pool["target_week"])
                conn.execute(
                    """INSERT INTO ballots(pool_id,player_id,week,teams,submitted)
                       VALUES(?,?,?,?,?)
                       ON CONFLICT(pool_id,player_id,week)
                       DO UPDATE SET teams=excluded.teams, submitted=excluded.submitted""",
                    (pool["id"], player["id"], week, json.dumps(teams), now()),
                )
                conn.commit()
                return self._json(200, {"ok": True, "snapshot": self._snapshot(conn, pool)})

            if path == "/api/official":
                pool = self._pool(conn, body.get("code") or "")
                player = self._player(conn, pool["id"], body.get("name") or "", body.get("pin") or "")
                if not player["is_host"]:
                    raise ValueError("Only the host can post the official poll.")
                week = int(body.get("week") or pool["target_week"])
                ranks = body.get("ranks") or []
                cleaned = []
                for i, item in enumerate(ranks):
                    if isinstance(item, str):
                        name = item.strip()
                        rank = i + 1
                    else:
                        name = (item.get("name") or "").strip()
                        rank = int(item.get("rank") or i + 1)
                    if name:
                        cleaned.append({"rank": rank, "name": name})
                if len(cleaned) < 20:
                    raise ValueError("Need at least 20 official teams.")
                conn.execute(
                    """INSERT INTO official(pool_id,week,ranks) VALUES(?,?,?)
                       ON CONFLICT(pool_id,week) DO UPDATE SET ranks=excluded.ranks""",
                    (pool["id"], week, json.dumps(cleaned)),
                )
                conn.execute("UPDATE pools SET target_week=? WHERE id=?", (week + 1, pool["id"]))
                conn.commit()
                pool = conn.execute("SELECT * FROM pools WHERE id=?", (pool["id"],)).fetchone()
                return self._json(200, {"ok": True, "snapshot": self._snapshot(conn, pool)})

            if path == "/api/score":
                pool = self._pool(conn, body.get("code") or "")
                player = self._player(conn, pool["id"], body.get("name") or "", body.get("pin") or "")
                if not player["is_host"]:
                    raise ValueError("Only the host can score.")
                week = int(body.get("week") or (pool["target_week"] - 1))
                off = conn.execute(
                    "SELECT ranks FROM official WHERE pool_id=? AND week=?",
                    (pool["id"], week),
                ).fetchone()
                if not off:
                    raise ValueError(f"No official Top 20 saved for week {week}.")
                ranks = json.loads(off["ranks"])
                mapping = official_map(ranks)
                ballots = conn.execute(
                    "SELECT player_id, teams FROM ballots WHERE pool_id=? AND week=?",
                    (pool["id"], week),
                ).fetchall()
                if not ballots:
                    raise ValueError("No submitted ballots for that week.")
                conn.execute("DELETE FROM scores WHERE pool_id=? AND week=?", (pool["id"], week))
                results = []
                for b in ballots:
                    teams = json.loads(b["teams"])
                    lines = []
                    for i, team in enumerate(teams):
                        pick = i + 1
                        actual = mapping.get(team)
                        slot = score_slot(pick, actual)
                        lines.append({"pick": pick, "team": team, "actual": actual, **slot})
                    total = sum(x["pts"] for x in lines)
                    exact = sum(1 for x in lines if x["type"] == "exact")
                    conn.execute(
                        "INSERT INTO scores(pool_id,week,player_id,total,exact,lines) VALUES(?,?,?,?,?,?)",
                        (pool["id"], week, b["player_id"], total, exact, json.dumps(lines)),
                    )
                    results.append({"player_id": b["player_id"], "total": total, "exact": exact})
                conn.commit()
                pool = conn.execute("SELECT * FROM pools WHERE id=?", (pool["id"],)).fetchone()
                return self._json(
                    200, {"ok": True, "week": week, "snapshot": self._snapshot(conn, pool)}
                )

            raise ValueError("Unknown action.")
        finally:
            conn.close()


def main():
    init_db()
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))
    httpd = ThreadingHTTPServer((host, port), Handler)
    print(f"#U-RankEm running at http://{host}:{port}", flush=True)
    httpd.serve_forever()


if __name__ == "__main__":
    main()
