"""Full Steam store app catalog + global CCU leaderboard."""
from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import requests

from steam_etl import get_current_players, utc_now_iso

STORE_APP_LIST_URL = "https://api.steampowered.com/IStoreService/GetAppList/v1/"

DEFAULT_CACHE_PATH = Path(__file__).resolve().parent / "data" / "steam_catalog_apps.json"
DEFAULT_LEADERBOARD_PATH = Path(__file__).resolve().parent / "data" / "steam_ccu_leaderboard.json"


def _ccu_workers() -> int:
    try:
        return max(4, min(64, int(os.environ.get("STEAM_FETCH_WORKERS", "32"))))
    except ValueError:
        return 32


def _ccu_chunk_size() -> int:
    try:
        return max(100, min(10000, int(os.environ.get("STEAM_CCU_CHUNK", "2500"))))
    except ValueError:
        return 2500


def _catalog_max_apps() -> int | None:
    raw = os.environ.get("STEAM_CATALOG_MAX_APPS", "").strip()
    if not raw:
        return None
    try:
        n = int(raw)
        return n if n > 0 else None
    except ValueError:
        return None


def fetch_all_store_apps(
    api_key: str,
    *,
    on_progress: Callable[[int], None] | None = None,
    max_apps: int | None = None,
) -> list[dict[str, Any]]:
    """
    Paginated IStoreService/GetAppList — public Steam catalog (games).
    Requires a Steam Web API key. Reuses HTTP connections between pages.
    """
    cap = max_apps if max_apps is not None else _catalog_max_apps()
    out: list[dict[str, Any]] = []
    last_appid = 0
    max_results = 50000

    session = requests.Session()

    while True:
        r = session.get(
            STORE_APP_LIST_URL,
            params={
                "key": api_key,
                "max_results": max_results,
                "last_appid": last_appid,
            },
            timeout=180,
        )
        r.raise_for_status()
        data = r.json().get("response", {})
        batch = data.get("apps") or []
        for a in batch:
            aid = int(a["appid"])
            out.append(
                {
                    "appid": aid,
                    "name": (a.get("name") or "").strip() or f"appid_{aid}",
                    "last_modified": int(a.get("last_modified") or 0),
                }
            )
            if cap is not None and len(out) >= cap:
                if on_progress:
                    on_progress(len(out))
                session.close()
                return out

        if on_progress:
            on_progress(len(out))

        if not data.get("have_more_results"):
            break
        last_appid = int(data.get("last_appid") or 0)
        if not batch:
            break

    session.close()
    return out


def _atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        suffix=".json",
        dir=str(path.parent),
        text=True,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def save_catalog_cache(path: Path, apps: list[dict[str, Any]]) -> None:
    payload = {
        "saved_at": utc_now_iso(),
        "count": len(apps),
        "apps": apps,
    }
    _atomic_write_json(path, payload)


def load_catalog_cache(path: Path) -> list[dict[str, Any]] | None:
    if not path.is_file():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        apps = data.get("apps")
        if not isinstance(apps, list) or not apps:
            return None
        return [
            {
                "appid": int(a["appid"]),
                "name": str(a.get("name") or ""),
                "last_modified": int(a.get("last_modified") or 0),
            }
            for a in apps
        ]
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError):
        return None


def leaderboard_cache_path() -> Path:
    p = os.environ.get("STEAM_LEADERBOARD_CACHE_PATH", "").strip()
    return Path(p) if p else DEFAULT_LEADERBOARD_PATH


def save_leaderboard_cache(
    path: Path,
    rows: list[dict[str, Any]],
    *,
    source_catalog_count: int,
) -> None:
    payload = {
        "saved_at": utc_now_iso(),
        "source_catalog_count": source_catalog_count,
        "count": len(rows),
        "rows": rows,
    }
    _atomic_write_json(path, payload)


def load_leaderboard_cache(
    path: Path,
    *,
    expected_catalog_count: int,
) -> list[dict[str, Any]] | None:
    if not path.is_file():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if int(data.get("source_catalog_count") or -1) != expected_catalog_count:
            return None
        rows = data.get("rows")
        if not isinstance(rows, list) or not rows:
            return None
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "appid": int(r["appid"]),
                    "name": str(r.get("name") or ""),
                    "current_players": int(r.get("current_players", -1)),
                }
            )
        return out
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError):
        return None


def attach_ccu_chunk(rows: list[dict[str, Any]], *, max_workers: int | None = None) -> list[dict[str, Any]]:
    """Add current_players; preserve order."""
    if not rows:
        return []
    workers = max_workers if max_workers is not None else _ccu_workers()

    def one(r: dict[str, Any]) -> dict[str, Any]:
        appid = int(r["appid"])
        name = r.get("name") or f"appid_{appid}"
        try:
            n = get_current_players(appid)
        except Exception:
            n = -1
        return {"appid": appid, "name": name, "current_players": n}

    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(one, rows))


def build_ccu_leaderboard(
    apps: list[dict[str, Any]],
    *,
    on_progress: Callable[[int, int], None] | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch concurrent players for every app, then sort most → least.
    Processes in chunks to bound memory and report progress.
    """
    if not apps:
        return []

    chunk = _ccu_chunk_size()
    merged: list[dict[str, Any]] = []
    total = len(apps)
    for i in range(0, total, chunk):
        batch = apps[i : i + chunk]
        merged.extend(attach_ccu_chunk(batch))
        if on_progress:
            on_progress(min(i + len(batch), total), total)

    merged.sort(
        key=lambda x: (x["current_players"] if x["current_players"] >= 0 else -1),
        reverse=True,
    )
    return merged


def slice_page(
    ordered: list[dict[str, Any]],
    page: int,
    per_page: int,
) -> tuple[list[dict[str, Any]], int]:
    """Return rows for page (1-based) and total count."""
    total = len(ordered)
    if total == 0:
        return [], 0
    page = max(1, page)
    start = (page - 1) * per_page
    if start >= total:
        return [], total
    end = min(start + per_page, total)
    return ordered[start:end], total


def catalog_cache_path() -> Path:
    p = os.environ.get("STEAM_CATALOG_CACHE_PATH", "").strip()
    return Path(p) if p else DEFAULT_CACHE_PATH
