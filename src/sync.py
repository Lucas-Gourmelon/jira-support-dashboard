from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .jira_client import JiraClient
from .db import IssuesRepository

logger = logging.getLogger("jira_sync_api")


def _display_name(user_obj: Optional[Dict[str, Any]]) -> Optional[str]:
    if not user_obj:
        return None
    return user_obj.get("displayName") or user_obj.get("emailAddress") or user_obj.get("accountId")


def map_issue(issue: Dict[str, Any], last_sync_iso: str) -> Dict[str, Any]:
    fields = issue.get("fields", {}) or {}

    project_key = (fields.get("project") or {}).get("key")
    issue_type = (fields.get("issuetype") or {}).get("name")
    status = (fields.get("status") or {}).get("name")
    priority = (fields.get("priority") or {}).get("name")

    assignee = _display_name(fields.get("assignee"))
    reporter = _display_name(fields.get("reporter"))

    created = fields.get("created")
    updated = fields.get("updated")
    resolved = fields.get("resolutiondate")

    time_spent_seconds = fields.get("timespent")

    if time_spent_seconds is None:
        time_spent_seconds = fields.get("aggregatetimespent")

    return {
        "issue_key": issue.get("key"),
        "project_key": project_key,
        "issue_type": issue_type,
        "status": status,
        "priority": priority,
        "assignee": assignee,
        "reporter": reporter,
        "created": created,
        "updated": updated,
        "resolved": resolved,
        "time_spent_seconds": time_spent_seconds,
        "last_sync": last_sync_iso,
    }


def run_sync(
    jira: JiraClient,
    repo: IssuesRepository,
    jql: str,
    page_size: int,
) -> Dict[str, int]:
    logger.info("Initializing database")
    repo.init_db()

    last_jql = repo.get_meta("last_jql")
    if last_jql != jql:
        logger.info("JQL changed since last run, clearing cached issues")
        repo.clear_issues()
        repo.set_meta("last_jql", jql)

    last_sync_iso = datetime.now(timezone.utc).isoformat()

    wanted_fields = [
        "project",
        "issuetype",
        "status",
        "priority",
        "assignee",
        "reporter",
        "created",
        "updated",
        "resolutiondate",
        "timespent",
        "aggregatetimespent",
    ]

    logger.info("Starting Jira fetch with page_size=%s", page_size)

    count = 0
    for issue in jira.search_issues(jql=jql, fields=wanted_fields, page_size=page_size):
        row = map_issue(issue, last_sync_iso)
        repo.upsert_issue(row)
        count += 1

        if count == 1:
            logger.info("First issue received: %s", row.get("issue_key"))
        elif count % 25 == 0:
            logger.info("%s issues processed", count)

    logger.info("Upsert finished: total=%s", count)

    return {"upserted": count}