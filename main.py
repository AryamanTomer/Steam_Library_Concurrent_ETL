"""Steam catalog: global leaderboard by concurrent players (most → least)."""
from __future__ import annotations

import os
import threading
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from steam_catalog import (
    build_ccu_leaderboard,
    catalog_cache_path,
    fetch_all_store_apps,
    leaderboard_cache_path,
    load_catalog_cache,
    load_leaderboard_cache,
    save_catalog_cache,
    save_leaderboard_cache,
    slice_page,
)
from steam_etl import utc_now_iso

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

app = FastAPI(title="Steam catalog · concurrent players")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")

_catalog_lock = threading.Lock()
_apps_appid: list[dict] = []
_catalog_loading = False
_catalog_fetch_progress = 0
_catalog_error: str | None = None
_catalog_loaded_at: str | None = None
_catalog_ready = False

_leaderboard_rows: list[dict] = []
_leaderboard_loading = False
_leaderboard_progress_done = 0
_leaderboard_progress_total = 0
_leaderboard_error: str | None = None
_leaderboard_ready = False

_reload_lock = threading.Lock()
_leaderboard_job_lock = threading.Lock()


def _set_catalog(apps: list[dict]) -> None:
    global _apps_appid, _catalog_ready, _catalog_loaded_at, _catalog_error
    _apps_appid = apps
    _catalog_ready = True
    _catalog_loaded_at = utc_now_iso()
    _catalog_error = None


def _invalidate_leaderboard() -> None:
    global _leaderboard_rows, _leaderboard_ready, _leaderboard_error, _leaderboard_progress_done, _leaderboard_loading
    _leaderboard_rows = []
    _leaderboard_ready = False
    _leaderboard_error = None
    _leaderboard_progress_done = 0
    _leaderboard_progress_total = 0
    _leaderboard_loading = False
    try:
        p = leaderboard_cache_path()
        if p.is_file():
            p.unlink()
    except OSError:
        pass


def _apply_leaderboard_cache(rows: list[dict]) -> None:
    global _leaderboard_rows, _leaderboard_ready, _leaderboard_error
    _leaderboard_rows = rows
    _leaderboard_ready = True
    _leaderboard_error = None


def _try_start_leaderboard_build() -> None:
    global _leaderboard_loading, _leaderboard_error

    with _catalog_lock:
        n = len(_apps_appid)
        if n == 0:
            _leaderboard_error = "Catalog has no games; download the catalog first."
            return
        if _leaderboard_ready:
            return

    with _leaderboard_job_lock:
        if _leaderboard_loading:
            return
        _leaderboard_loading = True
        _leaderboard_progress_done = 0
        _leaderboard_progress_total = n

    threading.Thread(target=_leaderboard_job, daemon=True).start()


def _leaderboard_job() -> None:
    global _leaderboard_loading, _leaderboard_error, _leaderboard_progress_done, _leaderboard_progress_total

    with _catalog_lock:
        apps = list(_apps_appid)
    total = len(apps)
    if total == 0:
        with _catalog_lock:
            _leaderboard_error = "Catalog has no games to rank."
            _leaderboard_ready = False
        _leaderboard_loading = False
        return

    path = leaderboard_cache_path()

    def on_progress(done: int, tot: int) -> None:
        global _leaderboard_progress_done, _leaderboard_progress_total
        with _catalog_lock:
            _leaderboard_progress_done = done
            _leaderboard_progress_total = tot

    try:
        rows = build_ccu_leaderboard(apps, on_progress=on_progress)
        save_leaderboard_cache(path, rows, source_catalog_count=total)
        with _catalog_lock:
            _leaderboard_rows = rows
            _leaderboard_ready = True
            _leaderboard_error = None
    except Exception as e:
        with _catalog_lock:
            _leaderboard_error = str(e)
            _leaderboard_ready = False
    finally:
        _leaderboard_loading = False


def _load_catalog_job(*, force_fetch: bool) -> None:
    global _catalog_loading, _catalog_error, _catalog_ready, _catalog_fetch_progress

    api_key = os.environ.get("STEAM_API_KEY", "").strip()
    path = catalog_cache_path()

    if not force_fetch:
        cached = load_catalog_cache(path)
        if cached:
            with _catalog_lock:
                _catalog_fetch_progress = 0
                _set_catalog(cached)
            cnt = len(cached)
            lb = load_leaderboard_cache(
                leaderboard_cache_path(),
                expected_catalog_count=cnt,
            )
            with _catalog_lock:
                if lb:
                    _apply_leaderboard_cache(lb)
                else:
                    _invalidate_leaderboard()
            if not lb:
                _try_start_leaderboard_build()
            _catalog_loading = False
            return

    if not api_key:
        with _catalog_lock:
            _catalog_error = (
                "STEAM_API_KEY is required to download the Steam catalog "
                "(IStoreService/GetAppList)."
            )
            _catalog_ready = False
            _catalog_fetch_progress = 0
        _catalog_loading = False
        return

    _invalidate_leaderboard()

    def on_progress(n: int) -> None:
        global _catalog_fetch_progress
        with _catalog_lock:
            _catalog_fetch_progress = n

    try:
        with _catalog_lock:
            _catalog_fetch_progress = 0
        apps = fetch_all_store_apps(api_key, on_progress=on_progress)
        save_catalog_cache(path, apps)
        with _catalog_lock:
            _catalog_fetch_progress = 0
            _set_catalog(apps)
        _try_start_leaderboard_build()
    except Exception as e:
        with _catalog_lock:
            _catalog_error = str(e)
            _catalog_ready = False
            _catalog_fetch_progress = 0
    finally:
        _catalog_loading = False


def try_start_catalog_load(*, force_fetch: bool) -> None:
    global _catalog_loading

    with _reload_lock:
        if _catalog_loading:
            return
        _catalog_loading = True

    threading.Thread(
        target=_load_catalog_job,
        kwargs={"force_fetch": force_fetch},
        daemon=True,
    ).start()


def _load_cached_catalog_into_memory() -> None:
    """Hydrate catalog and/or leaderboard from disk caches.

    - If this worker has no apps in RAM, load the catalog file (and matching leaderboard if present).
    - If the catalog is already in RAM but leaderboard is not ready and no build is running,
      try loading ``steam_ccu_leaderboard.json`` so another worker/process can populate us.

    Without the second step, multi-worker setups never see a leaderboard finished on a peer worker.
    """
    with _catalog_lock:
        if _catalog_loading:
            return
        have_apps = _catalog_ready and len(_apps_appid) > 0

    if not have_apps:
        path = catalog_cache_path()
        cached = load_catalog_cache(path)
        if not cached:
            return
        with _catalog_lock:
            if _catalog_ready and len(_apps_appid) > 0:
                return
            if _catalog_loading:
                return
            _set_catalog(cached)
        cnt = len(cached)
        lb = load_leaderboard_cache(
            leaderboard_cache_path(),
            expected_catalog_count=cnt,
        )
        with _catalog_lock:
            if lb:
                _apply_leaderboard_cache(lb)
            else:
                _invalidate_leaderboard()
        if not lb:
            _try_start_leaderboard_build()
        return

    with _catalog_lock:
        if _leaderboard_ready or _leaderboard_loading:
            return
        cnt = len(_apps_appid)

    lb = load_leaderboard_cache(
        leaderboard_cache_path(),
        expected_catalog_count=cnt,
    )
    if not lb:
        return
    with _catalog_lock:
        if _leaderboard_ready or _leaderboard_loading:
            return
        _apply_leaderboard_cache(lb)


def _auto_download_on_start() -> bool:
    return os.environ.get("STEAM_AUTO_DOWNLOAD_CATALOG", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


@app.on_event("startup")
def startup_catalog() -> None:
    if os.environ.get("STEAM_SKIP_CATALOG_LOAD", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        return

    def boot() -> None:
        if _auto_download_on_start():
            try_start_catalog_load(force_fetch=False)
            return
        _load_cached_catalog_into_memory()
        with _catalog_lock:
            if not _catalog_ready:
                _catalog_error = None

    threading.Thread(target=boot, daemon=True).start()


def _catalog_status() -> dict:
    _load_cached_catalog_into_memory()
    with _catalog_lock:
        cap = os.environ.get("STEAM_CATALOG_MAX_APPS", "").strip()
        return {
            "ready": _catalog_ready,
            "loading": _catalog_loading,
            "total_apps": len(_apps_appid),
            "download_progress": _catalog_fetch_progress,
            "catalog_loaded_at": _catalog_loaded_at,
            "last_error": _catalog_error,
            "cache_path": str(catalog_cache_path()),
            "auto_download_on_start": _auto_download_on_start(),
            "catalog_max_apps": cap or None,
            "leaderboard_ready": _leaderboard_ready,
            "leaderboard_loading": _leaderboard_loading,
            "leaderboard_progress_done": _leaderboard_progress_done,
            "leaderboard_progress_total": _leaderboard_progress_total,
            "leaderboard_error": _leaderboard_error,
        }


@app.get("/api/catalog/status")
def api_catalog_status() -> JSONResponse:
    return JSONResponse(_catalog_status())


@app.post("/api/catalog/download")
def api_catalog_download() -> JSONResponse:
    """Always fetch the app list from Steam (ignores local catalog cache)."""
    if not os.environ.get("STEAM_API_KEY", "").strip():
        raise HTTPException(
            status_code=503,
            detail="STEAM_API_KEY is required to download the catalog.",
        )
    with _reload_lock:
        if _catalog_loading or _leaderboard_loading:
            return JSONResponse({"status": "already_running", **_catalog_status()})
    try_start_catalog_load(force_fetch=True)
    return JSONResponse({"status": "started", **_catalog_status()})


@app.post("/api/catalog/reload")
def api_catalog_reload() -> JSONResponse:
    if not os.environ.get("STEAM_API_KEY", "").strip():
        raise HTTPException(
            status_code=503,
            detail="STEAM_API_KEY is required to refresh the catalog from Steam.",
        )
    global _catalog_loading

    with _reload_lock:
        if _catalog_loading or _leaderboard_loading:
            return JSONResponse({"status": "already_running", **_catalog_status()})
        p = catalog_cache_path()
        try:
            if p.is_file():
                p.unlink()
        except OSError:
            pass
        _invalidate_leaderboard()
        _catalog_loading = True

    threading.Thread(
        target=_load_catalog_job,
        kwargs={"force_fetch": True},
        daemon=True,
    ).start()
    return JSONResponse({"status": "started", **_catalog_status()})


def _do_leaderboard_rebuild() -> JSONResponse:
    """Re-fetch CCU for every app and rebuild global sort (slow)."""
    _load_cached_catalog_into_memory()
    with _catalog_lock:
        if not _catalog_ready or not _apps_appid:
            raise HTTPException(
                status_code=503,
                detail="Catalog not loaded. Download the catalog first, or ensure "
                + str(catalog_cache_path())
                + " exists and is valid.",
            )
    with _reload_lock:
        if _catalog_loading or _leaderboard_loading:
            return JSONResponse({"status": "already_running", **_catalog_status()})
    _invalidate_leaderboard()
    _try_start_leaderboard_build()
    return JSONResponse({"status": "started", **_catalog_status()})


@app.get("/api/leaderboard/rebuild")
def api_leaderboard_rebuild_get() -> None:
    """So a browser GET shows 405 Not Found vs 404 — confirms this process has the route."""
    raise HTTPException(
        status_code=405,
        detail="Use POST on this URL to rebuild the leaderboard.",
        headers={"Allow": "POST"},
    )


@app.post("/api/leaderboard/rebuild")
@app.post("/api/leaderboard/rebuild/")
@app.post("/api/rebuild-leaderboard")
@app.post("/api/games/rebuild")
def api_leaderboard_rebuild() -> JSONResponse:
    """POST aliases: trailing slash, short path, and /api/games/rebuild (easy proxy rules)."""
    return _do_leaderboard_rebuild()


def _filter_leaderboard_by_query(rows: list[dict], q: str | None) -> list[dict]:
    """Filter rows: case-insensitive name substring, exact App ID, or digit substring in App ID (≥3 digits)."""
    if not q or not (t := q.strip()):
        return rows
    t_lower = t.lower()
    digits_only = t.isdigit()
    want_exact = int(t) if digits_only else None

    out: list[dict] = []
    for r in rows:
        aid = int(r["appid"])
        name = str(r.get("name") or "").lower()
        if t_lower in name:
            out.append(r)
            continue
        if digits_only and want_exact is not None:
            if aid == want_exact:
                out.append(r)
                continue
            # e.g. "573" matches …573, 730 matches …730 (avoid 1–2 digit noise in appids)
            if len(t) >= 3 and t in str(aid):
                out.append(r)
    return out


@app.get("/api/games")
def api_games(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=250),
    q: str | None = Query(
        None,
        max_length=200,
        description="Filter: substring of game title, or numeric Steam App ID.",
    ),
    catalog_sort: str | None = Query(
        None,
        description="Deprecated; leaderboard is always global CCU order. Ignored.",
    ),
    page_order: str | None = Query(
        None,
        description="Deprecated; ignored.",
    ),
) -> JSONResponse:
    _load_cached_catalog_into_memory()
    with _catalog_lock:
        loading = _catalog_loading
        cat_err = _catalog_error
        cat_ready = _catalog_ready
        dlprog = _catalog_fetch_progress
        lb_ready = _leaderboard_ready
        lb_loading = _leaderboard_loading
        lb_done = _leaderboard_progress_done
        lb_tot = _leaderboard_progress_total
        lb_err = _leaderboard_error
        rows_snapshot = list(_leaderboard_rows)

    # Stuck: catalog loaded but leaderboard job never started (e.g. boot race) or finished without error
    if (
        cat_ready
        and not lb_ready
        and not lb_loading
        and lb_err is None
    ):
        _try_start_leaderboard_build()
        with _catalog_lock:
            lb_loading = _leaderboard_loading
            lb_done = _leaderboard_progress_done
            lb_tot = _leaderboard_progress_total
            lb_err = _leaderboard_error

    search_echo = q.strip() if q and q.strip() else None

    if not cat_ready:
        return JSONResponse(
            {
                "games": [],
                "page": page,
                "per_page": per_page,
                "total": 0,
                "total_pages": 0,
                "search": search_echo,
                "catalog_ready": False,
                "leaderboard_ready": False,
                "loading": loading,
                "leaderboard_loading": lb_loading,
                "last_error": cat_err,
                "leaderboard_error": lb_err,
                "download_progress": dlprog,
                "leaderboard_progress_done": lb_done,
                "leaderboard_progress_total": lb_tot,
                "chart": {"labels": [], "values": []},
            }
        )

    if not lb_ready:
        return JSONResponse(
            {
                "games": [],
                "page": page,
                "per_page": per_page,
                "total": 0,
                "total_pages": 0,
                "search": search_echo,
                "catalog_ready": True,
                "leaderboard_ready": False,
                "loading": loading,
                "leaderboard_loading": lb_loading,
                "last_error": cat_err,
                "leaderboard_error": lb_err,
                "download_progress": dlprog,
                "leaderboard_progress_done": lb_done,
                "leaderboard_progress_total": lb_tot,
                "chart": {"labels": [], "values": []},
            }
        )

    filtered = _filter_leaderboard_by_query(rows_snapshot, q)
    page_rows, total = slice_page(filtered, page, per_page)
    total_pages = (total + per_page - 1) // per_page if total else 0

    chart_labels = []
    chart_values = []
    for r in page_rows:
        chart_labels.append(str(r.get("name") or f"appid_{r['appid']}")[:48])
        cp = int(r.get("current_players") or -1)
        chart_values.append(cp if cp >= 0 else 0)

    return JSONResponse(
        {
            "games": page_rows,
            "page": page,
            "per_page": per_page,
            "total": total,
            "total_pages": total_pages,
            "search": search_echo,
            "catalog_ready": True,
            "leaderboard_ready": True,
            "loading": loading,
            "leaderboard_loading": lb_loading,
            "last_error": None,
            "leaderboard_error": None,
            "download_progress": dlprog,
            "leaderboard_progress_done": lb_done,
            "leaderboard_progress_total": lb_tot,
            "source": "steam_ccu_leaderboard",
            "chart": {"labels": chart_labels, "values": chart_values},
        }
    )


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
