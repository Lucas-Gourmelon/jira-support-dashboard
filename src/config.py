import os
import sys
from dataclasses import dataclass
from dotenv import load_dotenv


def _get_runtime_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


dotenv_path = os.path.join(_get_runtime_dir(), ".env")
load_dotenv(dotenv_path=dotenv_path, override=True)


@dataclass(frozen=True)
class Settings:
    jira_base_url: str
    jira_email: str
    jira_api_token: str
    jql: str
    page_size: int
    sqlite_path: str
    auto_sync_interval_seconds: int


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_sqlite_path() -> str:
    value = os.getenv("SQLITE_PATH", "").strip()
    if value:
        return value
    return os.path.join(_get_runtime_dir(), "jira_issues.db")


def load_settings() -> Settings:
    jira_base_url = _require_env("JIRA_BASE_URL").rstrip("/")
    jira_email = _require_env("JIRA_EMAIL")
    jira_api_token = _require_env("JIRA_API_TOKEN")
    jql = _require_env("JIRA_JQL")

    print("JQL USED:", jql)

    page_size_raw = os.getenv("JIRA_PAGE_SIZE", "100").strip()
    auto_sync_interval_raw = os.getenv("AUTO_SYNC_INTERVAL_SECONDS", "120").strip()
    sqlite_path = get_sqlite_path()

    try:
        page_size = int(page_size_raw)
        if page_size <= 0 or page_size > 1000:
            raise ValueError()
    except ValueError:
        raise RuntimeError("JIRA_PAGE_SIZE must be an integer between 1 and 1000")

    try:
        auto_sync_interval_seconds = int(auto_sync_interval_raw)
        if auto_sync_interval_seconds <= 0:
            raise ValueError()
    except ValueError:
        raise RuntimeError("AUTO_SYNC_INTERVAL_SECONDS must be a positive integer")

    return Settings(
        jira_base_url=jira_base_url,
        jira_email=jira_email,
        jira_api_token=jira_api_token,
        jql=jql,
        page_size=page_size,
        sqlite_path=sqlite_path,
        auto_sync_interval_seconds=auto_sync_interval_seconds,
    )