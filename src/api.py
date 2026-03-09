from __future__ import annotations

import logging
import os
import sqlite3
import sys
import threading
import time
import unicodedata
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

from .config import get_sqlite_path, load_settings
from .db import IssuesRepository
from .jira_client import JiraClient
from .sync import run_sync

if getattr(sys, "frozen", False):
    BASE_DIR = sys._MEIPASS
    RUNTIME_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    RUNTIME_DIR = BASE_DIR

LOG_DIR = os.path.join(RUNTIME_DIR, "logs")
LOG_PATH = os.path.join(LOG_DIR, "sync.log")

os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("jira_sync_api")
logger.setLevel(logging.INFO)

if not any(isinstance(h, logging.FileHandler) and h.baseFilename == os.path.abspath(LOG_PATH) for h in logger.handlers):
    file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

_status_lock = threading.Lock()
_is_running = False

_last_status: Dict[str, Any] = {
    "last_run_at": None,
    "success": None,
    "upserted": 0,
    "duration_ms": None,
    "last_error": None,
    "is_running": False,
}

TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

DEFAULT_CLOSED_STATUSES = [
    "fermé",
    "fermée",
    "terminé",
    "terminée",
    "résolu",
    "résolue",
    "closed",
    "resolved",
    "done",
]


def _set_status(**updates: Any) -> None:
    global _last_status
    with _status_lock:
        _last_status.update(updates)


def _get_status() -> Dict[str, Any]:
    with _status_lock:
        return dict(_last_status)


def _run_sync_job() -> None:
    global _is_running
    started = time.perf_counter()
    started_at_iso = datetime.now(timezone.utc).isoformat()

    _set_status(is_running=True, last_error=None)
    logger.info("Sync started")

    try:
        settings = load_settings()

        logger.info("JQL used for sync: %s", settings.jql)
        logger.info("SQLite path: %s", settings.sqlite_path)

        jira = JiraClient(
            base_url=settings.jira_base_url,
            email=settings.jira_email,
            api_token=settings.jira_api_token,
        )
        repo = IssuesRepository(settings.sqlite_path)

        stats = run_sync(
            jira=jira,
            repo=repo,
            jql=settings.jql,
            page_size=settings.page_size,
        )

        duration_ms = int((time.perf_counter() - started) * 1000)

        _set_status(
            last_run_at=started_at_iso,
            success=True,
            upserted=int(stats.get("upserted", 0)),
            duration_ms=duration_ms,
            last_error=None,
            is_running=False,
        )
        logger.info(
            "Sync finished: success=true upserted=%s duration_ms=%s",
            stats.get("upserted", 0),
            duration_ms,
        )
    except Exception as e:
        duration_ms = int((time.perf_counter() - started) * 1000)

        _set_status(
            last_run_at=started_at_iso,
            success=False,
            upserted=0,
            duration_ms=duration_ms,
            last_error=str(e),
            is_running=False,
        )
        logger.exception("Sync failed: %s", e)
    finally:
        with _status_lock:
            _is_running = False


def _try_start_sync() -> bool:
    global _is_running
    with _status_lock:
        if _is_running:
            return False
        _is_running = True
        _last_status["is_running"] = True
        _last_status["last_error"] = None

    t = threading.Thread(target=_run_sync_job, name="jira-sync", daemon=True)
    t.start()
    return True


def _normalize_for_match(value: Optional[str]) -> str:
    if not value:
        return ""
    s = value.strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower().strip()


def _get_closed_statuses_normalized() -> set[str]:
    raw = os.getenv("CLOSED_STATUS_LIST", "").strip()
    if not raw:
        items = DEFAULT_CLOSED_STATUSES
    else:
        items = [x for x in raw.split(",")]

    out: set[str] = set()
    for x in items:
        nx = _normalize_for_match(x)
        if nx:
            out.add(nx)
    return out


def _parse_jira_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    s = value.strip()
    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            if len(s) >= 5 and (s[-5] in ["+", "-"]) and s[-2] != ":":
                s = s[:-2] + ":" + s[-2:]
            dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _age_hours(now_utc: datetime, then_utc: datetime) -> int:
    seconds = (now_utc - then_utc).total_seconds()
    if seconds < 0:
        seconds = 0
    return int(seconds // 3600)


def _connect_sqlite(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _assignee_label(value: Optional[str]) -> str:
    return value if value and value.strip() else "Unassigned"


def _is_closed_status(status_value: Optional[str], closed_norm: set[str]) -> bool:
    s = _normalize_for_match(status_value)
    return bool(s) and (s in closed_norm)


def _hours_from_seconds(seconds: Optional[int]) -> float:
    if seconds is None:
        return 0.0
    try:
        return float(seconds) / 3600.0
    except Exception:
        return 0.0


def _issues_table_exists(path: str) -> bool:
    if not os.path.exists(path):
        return False
    try:
        with sqlite3.connect(path) as conn:
            row = conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table' AND name = 'issues'
                """
            ).fetchone()
            return row is not None
    except Exception:
        return False


@asynccontextmanager
async def lifespan(_: FastAPI):
    db_path = get_sqlite_path()
    db_dir = os.path.dirname(db_path)

    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    if not _issues_table_exists(db_path):
        logger.info("SQLite DB or issues table missing. Starting initial sync.")
        _try_start_sync()

    yield


app = FastAPI(title="Jira Support Sync", version="1.3.0", lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
def home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/sync")
def trigger_sync() -> Dict[str, Any]:
    started = _try_start_sync()
    if not started:
        raise HTTPException(status_code=409, detail="A sync is already running. Please wait for it to finish.")
    return {"message": "Sync started", "status": _get_status()}


@app.get("/sync/status")
def sync_status() -> Dict[str, Any]:
    return _get_status()


@app.get("/config")
def config_info() -> Dict[str, Any]:
    db_path = get_sqlite_path()
    closed_statuses = sorted(_get_closed_statuses_normalized())

    try:
        settings = load_settings()
        jql = settings.jql
        page_size = settings.page_size
    except Exception as e:
        jql = None
        page_size = None

    return {
        "sqlite_path": db_path,
        "jira_jql": jql,
        "jira_page_size": page_size,
        "closed_statuses": closed_statuses,
    }


@app.get("/stats/overview")
def stats_overview() -> Dict[str, Any]:
    db_path = get_sqlite_path()
    now = datetime.now(timezone.utc)
    closed_norm = _get_closed_statuses_normalized()

    if not _issues_table_exists(db_path):
        return {
            "total_tickets": 0,
            "open_tickets": 0,
            "closed_tickets": 0,
            "tickets_by_status": {},
            "tickets_by_priority": {},
            "oldest_open_ticket": None,
            "oldest_open_ticket_by_updated": None,
            "newest_ticket": None,
        }

    with _connect_sqlite(db_path) as conn:
        total_tickets = int(conn.execute("SELECT COUNT(*) AS c FROM issues").fetchone()["c"])

        tickets_by_status: Dict[str, int] = {}
        for row in conn.execute(
            """
            SELECT COALESCE(status, 'UNKNOWN') AS k, COUNT(*) AS c
            FROM issues
            GROUP BY COALESCE(status, 'UNKNOWN')
            ORDER BY c DESC
            """
        ):
            tickets_by_status[str(row["k"])] = int(row["c"])

        tickets_by_priority: Dict[str, int] = {}
        for row in conn.execute(
            """
            SELECT COALESCE(priority, 'UNKNOWN') AS k, COUNT(*) AS c
            FROM issues
            GROUP BY COALESCE(priority, 'UNKNOWN')
            ORDER BY c DESC
            """
        ):
            tickets_by_priority[str(row["k"])] = int(row["c"])

        rows = list(conn.execute("SELECT issue_key, status, created, updated FROM issues"))

        open_tickets = 0
        closed_tickets = 0

        oldest_open_key: Optional[str] = None
        oldest_open_created: Optional[datetime] = None

        oldest_open_by_updated_key: Optional[str] = None
        oldest_open_updated: Optional[datetime] = None

        for r in rows:
            if _is_closed_status(r["status"], closed_norm):
                closed_tickets += 1
                continue

            open_tickets += 1

            dt_created = _parse_jira_dt(r["created"])
            if dt_created:
                if oldest_open_created is None or dt_created < oldest_open_created:
                    oldest_open_created = dt_created
                    oldest_open_key = str(r["issue_key"])

            dt_updated = _parse_jira_dt(r["updated"])
            if dt_updated:
                if oldest_open_updated is None or dt_updated < oldest_open_updated:
                    oldest_open_updated = dt_updated
                    oldest_open_by_updated_key = str(r["issue_key"])

        oldest_open_ticket: Optional[Dict[str, Any]] = None
        if oldest_open_key and oldest_open_created:
            oldest_open_ticket = {"key": oldest_open_key, "age_hours": _age_hours(now, oldest_open_created)}

        oldest_open_ticket_by_updated: Optional[Dict[str, Any]] = None
        if oldest_open_by_updated_key and oldest_open_updated:
            oldest_open_ticket_by_updated = {
                "key": oldest_open_by_updated_key,
                "age_hours": _age_hours(now, oldest_open_updated),
            }

        newest_key: Optional[str] = None
        newest_created: Optional[datetime] = None
        for row in conn.execute(
            """
            SELECT issue_key, created
            FROM issues
            WHERE created IS NOT NULL
            """
        ):
            dt = _parse_jira_dt(row["created"])
            if not dt:
                continue
            if newest_created is None or dt > newest_created:
                newest_created = dt
                newest_key = str(row["issue_key"])

        newest_ticket: Optional[Dict[str, Any]] = None
        if newest_key and newest_created:
            newest_ticket = {"key": newest_key, "age_hours": _age_hours(now, newest_created)}

    return {
        "total_tickets": total_tickets,
        "open_tickets": open_tickets,
        "closed_tickets": closed_tickets,
        "tickets_by_status": tickets_by_status,
        "tickets_by_priority": tickets_by_priority,
        "oldest_open_ticket": oldest_open_ticket,
        "oldest_open_ticket_by_updated": oldest_open_ticket_by_updated,
        "newest_ticket": newest_ticket,
    }


@app.get("/stats/by_assignee")
def stats_by_assignee(
    only_open: bool = Query(True, description="If true, only considers open tickets for counts/ages and filters out assignees with 0 open tickets."),
) -> List[Dict[str, Any]]:
    db_path = get_sqlite_path()
    now = datetime.now(timezone.utc)
    closed_norm = _get_closed_statuses_normalized()

    if not _issues_table_exists(db_path):
        return []

    agg: Dict[str, Dict[str, Any]] = {}

    with _connect_sqlite(db_path) as conn:
        rows = conn.execute(
            """
            SELECT assignee, status, created, updated
            FROM issues
            """
        ).fetchall()

    for r in rows:
        assignee = _assignee_label(r["assignee"])
        entry = agg.get(assignee)
        if entry is None:
            entry = {"assignee": assignee, "open_count": 0, "_min_created": None, "_min_updated": None}
            agg[assignee] = entry

        if _is_closed_status(r["status"], closed_norm):
            continue

        entry["open_count"] += 1

        dt_created = _parse_jira_dt(r["created"])
        if dt_created:
            cur = entry["_min_created"]
            if cur is None or dt_created < cur:
                entry["_min_created"] = dt_created

        dt_updated = _parse_jira_dt(r["updated"])
        if dt_updated:
            cur = entry["_min_updated"]
            if cur is None or dt_updated < cur:
                entry["_min_updated"] = dt_updated

    out: List[Dict[str, Any]] = []
    for _, entry in agg.items():
        open_count = int(entry["open_count"])
        if only_open and open_count == 0:
            continue

        oldest_created_hours = _age_hours(now, entry["_min_created"]) if entry["_min_created"] is not None else None
        oldest_updated_hours = _age_hours(now, entry["_min_updated"]) if entry["_min_updated"] is not None else None

        out.append(
            {
                "assignee": entry["assignee"],
                "open_count": open_count,
                "oldest_open_created_hours": oldest_created_hours,
                "oldest_open_updated_hours": oldest_updated_hours,
            }
        )

    out.sort(key=lambda x: (-x["open_count"], x["assignee"]))
    return out


@app.get("/stats/top_oldest_open")
def stats_top_oldest_open(
    limit: int = Query(200, ge=1, le=10000),
    sort: str = Query("created", pattern="^(created|updated)$"),
) -> List[Dict[str, Any]]:
    db_path = get_sqlite_path()
    now = datetime.now(timezone.utc)
    closed_norm = _get_closed_statuses_normalized()

    if not _issues_table_exists(db_path):
        return []

    with _connect_sqlite(db_path) as conn:
        rows = conn.execute(
            """
            SELECT issue_key, status, priority, assignee, created, updated
            FROM issues
            """
        ).fetchall()

    items: List[Dict[str, Any]] = []
    for r in rows:
        if _is_closed_status(r["status"], closed_norm):
            continue

        dt_created = _parse_jira_dt(r["created"])
        dt_updated = _parse_jira_dt(r["updated"])
        dt_sort = dt_created if sort == "created" else dt_updated
        if dt_sort is None:
            continue

        items.append(
            {
                "key": str(r["issue_key"]),
                "status": r["status"],
                "priority": r["priority"],
                "assignee": _assignee_label(r["assignee"]),
                "created": r["created"],
                "updated": r["updated"],
                "age_hours": _age_hours(now, dt_sort),
            }
        )

    items.sort(key=lambda x: x["age_hours"], reverse=True)
    return items[:limit]


@app.get("/stats/time_by_project")
def stats_time_by_project() -> List[Dict[str, Any]]:
    db_path = get_sqlite_path()
    closed_norm = _get_closed_statuses_normalized()

    if not _issues_table_exists(db_path):
        return []

    agg: Dict[str, Dict[str, Any]] = {}

    with _connect_sqlite(db_path) as conn:
        rows = conn.execute(
            """
            SELECT project_key, status, time_spent_seconds
            FROM issues
            """
        ).fetchall()

    for r in rows:
        project_key = (r["project_key"] or "UNKNOWN").strip() if r["project_key"] else "UNKNOWN"
        entry = agg.get(project_key)
        if entry is None:
            entry = {
                "project_key": project_key,
                "total_issues": 0,
                "open_issues": 0,
                "closed_issues": 0,
                "time_spent_seconds": 0,
            }
            agg[project_key] = entry

        entry["total_issues"] += 1

        if _is_closed_status(r["status"], closed_norm):
            entry["closed_issues"] += 1
        else:
            entry["open_issues"] += 1

        ts = r["time_spent_seconds"]
        if ts is None:
            ts = 0
        try:
            entry["time_spent_seconds"] += int(ts)
        except Exception:
            pass

    out: List[Dict[str, Any]] = []
    for _, entry in agg.items():
        hours = _hours_from_seconds(entry["time_spent_seconds"])
        out.append(
            {
                "project_key": entry["project_key"],
                "total_issues": entry["total_issues"],
                "open_issues": entry["open_issues"],
                "closed_issues": entry["closed_issues"],
                "time_spent_seconds": entry["time_spent_seconds"],
                "time_spent_hours": round(hours, 1),
            }
        )

    out.sort(key=lambda x: x["time_spent_seconds"], reverse=True)
    return out