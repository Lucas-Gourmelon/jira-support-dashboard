from __future__ import annotations

import asyncio
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
from collections import deque

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
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

class InMemoryLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
        except Exception:
            msg = record.getMessage()
        _push_live_log(msg)


if not any(isinstance(h, InMemoryLogHandler) for h in logger.handlers):
    memory_handler = InMemoryLogHandler()
    memory_handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    logger.addHandler(memory_handler)

SYNC_INTERVAL_SECONDS = int(os.getenv("AUTO_SYNC_INTERVAL_SECONDS", "120"))
REFRESH_INTERVAL_SECONDS = int(os.getenv("AUTO_REFRESH_SECONDS", "10"))

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

_live_log_lock = threading.Lock()
_live_log_lines = deque(maxlen=200)

TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

STATIC_DIR = os.path.join(BASE_DIR, "static")

STATUS_FAMILY_LABELS = [
    "Open",
    "Analyse Client",
    "Analyse Luxtrust",
    "Closed",
]

STATUS_FAMILY_MAP: Dict[str, str] = {
    "declared": "Open",
    "reopened": "Open",
    "reopen": "Open",
    "re opened": "Open",
    "rouvert": "Open",
    "ouvert": "Open",
    "started": "Open",
    "planned": "Open",
    "planne": "Open",
    "prevu": "Open",
    "no customer answer": "Analyse Client",
    "waiting for customer": "Analyse Client",
    "en attente du client": "Analyse Client",
    "analyse client": "Analyse Client",
    "escalated": "Analyse Luxtrust",
    "in progress": "Analyse Luxtrust",
    "estimated lt": "Analyse Luxtrust",
    "new release": "Analyse Luxtrust",
    "work in progress": "Analyse Luxtrust",
    "to plan": "Analyse Luxtrust",
    "to analyse lt": "Analyse Luxtrust",
    "to analyse it": "Analyse Luxtrust",
    "pending": "Analyse Luxtrust",
    "test": "Analyse Luxtrust",
    "quote": "Analyse Luxtrust",
    "analyse luxtrust": "Analyse Luxtrust",
    "en cours": "Analyse Luxtrust",
    "done": "Closed",
    "pre completed": "Closed",
    "cancelled": "Closed",
    "canceled": "Closed",
    "annule": "Closed",
    "rejected": "Closed",
    "rejete": "Closed",
    "suspended": "Closed",
    "published": "Closed",
    "ferme": "Closed",
    "closed": "Closed",
    "resolu": "Closed",
    "termine": "Closed",
    "termine e": "Closed",
}

STATUS_FAMILY_ORDER = {
    "Open": 0,
    "Analyse Client": 1,
    "Analyse Luxtrust": 2,
    "Closed": 3,
}


def _set_status(**updates: Any) -> None:
    global _last_status
    with _status_lock:
        _last_status.update(updates)


def _get_status() -> Dict[str, Any]:
    with _status_lock:
        return dict(_last_status)


def _push_live_log(message: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    line = f"{timestamp} • {message}"
    with _live_log_lock:
        _live_log_lines.append(line)


def _get_live_logs(limit: int = 4) -> List[str]:
    with _live_log_lock:
        if limit <= 0:
            return []
        return list(_live_log_lines)[-limit:]
    

def _run_sync_job() -> None:
    global _is_running
    started = time.perf_counter()
    started_at_iso = datetime.now(timezone.utc).isoformat()

    _set_status(is_running=True, last_error=None)
    _push_live_log("Sync started")
    logger.info("Sync started")

    try:
        settings = load_settings()

        logger.info("Loading settings")
        logger.info("JQL used for sync: %s", settings.jql)
        logger.info("SQLite path: %s", settings.sqlite_path)

        jira = JiraClient(
            base_url=settings.jira_base_url,
            email=settings.jira_email,
            api_token=settings.jira_api_token,
        )
        repo = IssuesRepository(settings.sqlite_path)

        logger.info("Jira client initialized")
        logger.info("Repository initialized")
        logger.info("Fetching Jira issues...")

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
        _push_live_log(
            f"Sync finished • {int(stats.get('upserted', 0))} issues • {duration_ms} ms"
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
        _push_live_log(f"Sync failed • {e}")
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


async def _auto_sync_loop() -> None:
    logger.info("Auto-sync loop started with interval=%s seconds", SYNC_INTERVAL_SECONDS)

    while True:
        await asyncio.sleep(SYNC_INTERVAL_SECONDS)

        try:
            started = _try_start_sync()
            if started:
                logger.info("Auto-sync triggered")
            else:
                logger.info("Auto-sync skipped because a sync is already running")
        except Exception as e:
            logger.exception("Auto-sync trigger failed: %s", e)


def _normalize_for_match(value: Optional[str]) -> str:
    if not value:
        return ""
    s = value.strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower().strip()


def _normalize_status_key(value: Optional[str]) -> str:
    s = _normalize_for_match(value)
    for ch in ["_", "-", "/", "(", ")", "[", "]", "{", "}", ".", ",", ";", ":"]:
        s = s.replace(ch, " ")
    s = " ".join(s.split())
    return s


def _display_status_label(value: Optional[str]) -> str:
    raw = (value or "").strip()
    return raw if raw else "UNKNOWN"


def _map_status_to_family(value: Optional[str]) -> str:
    raw_label = _display_status_label(value)
    key = _normalize_status_key(value)

    if key in STATUS_FAMILY_MAP:
        return STATUS_FAMILY_MAP[key]

    if "customer" in key or "client" in key:
        return "Analyse Client"

    if "luxtrust" in key or "support" in key:
        return "Analyse Luxtrust"

    if "progress" in key or "analyse" in key or "analysis" in key:
        return "Analyse Luxtrust"

    if "pending" in key or "quote" in key or "release" in key or "test" in key:
        return "Analyse Luxtrust"

    if "plan" in key:
        return "Analyse Luxtrust"

    if "open" in key or "ouvert" in key or "reopen" in key or "rouvert" in key or "declared" in key or "start" in key or "prevu" in key:
        return "Open"

    if "closed" in key or "done" in key or "reject" in key or "rejete" in key or "cancel" in key or "annule" in key or "publish" in key or "suspend" in key:
        return "Closed"

    if "ferme" in key or "resolu" in key or "termine" in key:
        return "Closed"

    return f"Other: {raw_label}"


def _status_family_rank(family: str) -> int:
    return STATUS_FAMILY_ORDER.get(family, 999)


def _status_sort_key(status_value: Optional[str]) -> tuple[int, str, str]:
    raw_label = _display_status_label(status_value)
    family = _map_status_to_family(status_value)
    return (
        _status_family_rank(family),
        family.lower(),
        raw_label.lower(),
    )
    

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


def _duration_hours(start_utc: datetime, end_utc: datetime) -> float:
    seconds = (end_utc - start_utc).total_seconds()
    if seconds < 0:
        return 0.0
    return float(seconds) / 3600.0


def _connect_sqlite(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _assignee_label(value: Optional[str]) -> str:
    return value if value and value.strip() else "Unassigned"


def _is_closed_status(status_value: Optional[str]) -> bool:
    return _map_status_to_family(status_value) == "Closed"


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
async def lifespan(app: FastAPI):
    db_path = get_sqlite_path()
    db_dir = os.path.dirname(db_path)

    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    logger.info("Application startup")
    logger.info("Auto-sync interval configured to %s seconds", SYNC_INTERVAL_SECONDS)

    started = _try_start_sync()
    if started:
        logger.info("Initial sync triggered at startup")
    else:
        logger.info("Initial sync skipped because a sync is already running")

    auto_sync_task = asyncio.create_task(_auto_sync_loop())
    app.state.auto_sync_task = auto_sync_task

    try:
        yield
    finally:
        logger.info("Application shutdown")
        auto_sync_task.cancel()
        try:
            await auto_sync_task
        except asyncio.CancelledError:
            logger.info("Auto-sync loop stopped")


app = FastAPI(title="Jira Support Sync", version="1.6.0", lifespan=lifespan)

if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


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


@app.get("/sync/live")
def sync_live(limit: int = Query(4, ge=1, le=20)) -> Dict[str, Any]:
    status = _get_status()
    return {
        "is_running": bool(status.get("is_running")),
        "success": status.get("success"),
        "last_error": status.get("last_error"),
        "lines": _get_live_logs(limit),
    }


@app.get("/config")
def config_info() -> Dict[str, Any]:
    db_path = get_sqlite_path()
    closed_statuses = sorted(
        key for key, family in STATUS_FAMILY_MAP.items() if family == "Closed"
    )

    try:
        settings = load_settings()
        jql = settings.jql
        page_size = settings.page_size
    except Exception:
        jql = None
        page_size = None

    return {
        "sqlite_path": db_path,
        "jira_jql": jql,
        "jira_page_size": page_size,
        "closed_status_keys": closed_statuses,
        "status_family_map": STATUS_FAMILY_MAP,
        "status_family_labels": STATUS_FAMILY_LABELS,
        "auto_sync_interval_seconds": SYNC_INTERVAL_SECONDS,
        "auto_refresh_seconds": REFRESH_INTERVAL_SECONDS,
    }


@app.get("/stats/overview")
def stats_overview() -> Dict[str, Any]:
    db_path = get_sqlite_path()
    now = datetime.now(timezone.utc)

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
            if _is_closed_status(r["status"]):
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

        if _is_closed_status(r["status"]):
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
        if _is_closed_status(r["status"]):
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

    if not _issues_table_exists(db_path):
        return []

    agg: Dict[str, Dict[str, Any]] = {}

    with _connect_sqlite(db_path) as conn:
        rows = conn.execute(
            """
            SELECT project_key, status, created, resolved, time_spent_seconds
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
                "resolved_issues_with_dates": 0,
                "resolution_total_hours": 0.0,
            }
            agg[project_key] = entry

        entry["total_issues"] += 1

        if _is_closed_status(r["status"]):
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

        dt_created = _parse_jira_dt(r["created"])
        dt_resolved = _parse_jira_dt(r["resolved"])
        if dt_created and dt_resolved:
            resolution_hours = _duration_hours(dt_created, dt_resolved)
            entry["resolution_total_hours"] += resolution_hours
            entry["resolved_issues_with_dates"] += 1

    out: List[Dict[str, Any]] = []
    for _, entry in agg.items():
        hours = _hours_from_seconds(entry["time_spent_seconds"])
        resolution_count = int(entry["resolved_issues_with_dates"])
        avg_resolution_hours = round(entry["resolution_total_hours"] / resolution_count, 1) if resolution_count > 0 else None

        out.append(
            {
                "project_key": entry["project_key"],
                "total_issues": entry["total_issues"],
                "open_issues": entry["open_issues"],
                "closed_issues": entry["closed_issues"],
                "time_spent_seconds": entry["time_spent_seconds"],
                "time_spent_hours": round(hours, 1),
                "resolved_issues_with_dates": resolution_count,
                "avg_resolution_hours": avg_resolution_hours,
            }
        )

    out.sort(key=lambda x: x["time_spent_seconds"], reverse=True)
    return out


@app.get("/stats/status_family_distribution")
def stats_status_family_distribution() -> Dict[str, Any]:
    db_path = get_sqlite_path()

    if not _issues_table_exists(db_path):
        return {
            "total_tickets": 0,
            "families": [],
            "known_families": STATUS_FAMILY_LABELS,
            "raw_status_counts": {},
            "raw_status_items": [],
        }

    family_counts: Dict[str, int] = {}
    raw_status_counts: Dict[str, int] = {}

    with _connect_sqlite(db_path) as conn:
        rows = conn.execute(
            """
            SELECT status
            FROM issues
            """
        ).fetchall()

    total = 0
    for row in rows:
        status = row["status"]
        raw_label = _display_status_label(status)
        family = _map_status_to_family(status)

        total += 1
        raw_status_counts[raw_label] = raw_status_counts.get(raw_label, 0) + 1
        family_counts[family] = family_counts.get(family, 0) + 1

    ordered_labels = STATUS_FAMILY_LABELS[:]
    other_labels = sorted(
        [label for label in family_counts.keys() if label not in STATUS_FAMILY_LABELS],
        key=str.lower,
    )
    final_labels = ordered_labels + other_labels

    families: List[Dict[str, Any]] = []
    for label in final_labels:
        count = int(family_counts.get(label, 0))
        if count <= 0:
            continue

        pct = round((count / total) * 100.0, 1) if total > 0 else 0.0
        families.append(
            {
                "label": label,
                "count": count,
                "percentage": pct,
            }
        )

    raw_status_items: List[Dict[str, Any]] = []
    for raw_label, count in raw_status_counts.items():
        family = _map_status_to_family(raw_label)
        raw_status_items.append(
            {
                "label": raw_label,
                "family": family,
                "count": int(count),
            }
        )

    raw_status_items.sort(key=lambda x: _status_sort_key(x["label"]))

    return {
        "total_tickets": total,
        "families": families,
        "known_families": STATUS_FAMILY_LABELS,
        "raw_status_counts": raw_status_counts,
        "raw_status_items": raw_status_items,
    }