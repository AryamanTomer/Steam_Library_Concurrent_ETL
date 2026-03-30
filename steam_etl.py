"""Steam library: owned games + current player counts from the Steam Web API."""
from __future__ import annotations

import csv
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import requests

OWNED_GAMES_URL = "https://api.steampowered.com/IPlayerService/GetOwnedGames/v1/"
CURRENT_PLAYERS_URL = (
    "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
)
RESOLVE_VANITY_URL = "https://api.steampowered.com/ISteamUser/ResolveVanityURL/v0001/"

# 64-bit Steam ID base (individual accounts, universe 1)
_STEAM64_BASE = 76561197960265728


def default_csv_path() -> Path:
    return Path(__file__).resolve().parent / "my_steam_library_by_current_players.csv"


def fetch_workers() -> int:
    try:
        return max(1, min(48, int(os.environ.get("STEAM_FETCH_WORKERS", "20"))))
    except ValueError:
        return 20


def _steam2_to_64(text: str) -> str | None:
    """STEAM_0:Y:Z or STEAM_1:Y:Z → SteamID64."""
    m = re.match(r"^STEAM_[01]:([01]):(\d+)$", text.strip(), re.IGNORECASE)
    if not m:
        return None
    y, z = int(m.group(1)), int(m.group(2))
    return str(_STEAM64_BASE + (z * 2) + y)


def _steam3_to_64(text: str) -> str | None:
    """[U:1:N] or U:1:N (SteamID3) → SteamID64."""
    cleaned = re.sub(r"\s+", "", text.strip())
    m = re.match(r"^\[?U:1:(\d+)\]?$", cleaned, re.IGNORECASE)
    if not m:
        return None
    return str(_STEAM64_BASE + int(m.group(1)))


def _extract_from_community_input(raw: str) -> str | None:
    """
    If the value looks like a Steam Community URL or path, return either a
    numeric SteamID64 string or a vanity name to resolve. Otherwise None.
    """
    t = raw.strip()
    if not t:
        return None

    m = re.match(r"^profiles/(\d+)", t, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.match(r"^id/([^/?#]+)", t, re.IGNORECASE)
    if m:
        return m.group(1)

    low = t.lower()
    if "steamcommunity.com" not in low:
        return None

    if not t.startswith(("http://", "https://")):
        t = "https://" + t.lstrip("/")

    p = urlparse(t)
    if "steamcommunity.com" not in p.netloc.lower():
        return None

    parts = [unquote(x) for x in p.path.strip("/").split("/") if x]
    if len(parts) >= 2 and parts[0].lower() == "profiles" and parts[1].isdigit():
        return parts[1]
    if len(parts) >= 2 and parts[0].lower() == "id":
        return parts[1]
    return None


def _resolve_vanity(api_key: str, vanity: str) -> str:
    v = vanity.strip()
    if not v:
        raise ValueError("Vanity / profile name is empty.")
    r = requests.get(
        RESOLVE_VANITY_URL,
        params={"key": api_key, "vanityurl": v},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json().get("response", {})
    if data.get("success") != 1:
        raise ValueError(
            "Could not resolve that Steam profile identifier to a SteamID64. "
            "Try: numeric Steam ID, STEAM_0:Y:Z, [U:1:N], or a valid custom URL / vanity name."
        )
    steamid = data.get("steamid")
    if not steamid:
        raise ValueError("ResolveVanityURL returned no steamid.")
    return str(steamid)


def normalize_steamid64(api_key: str, steamid_or_vanity: str) -> str:
    """
    GetOwnedGames requires the numeric SteamID64. Accept common forms:

    - 17-digit SteamID64 (76561198…)
    - STEAM_0:Y:Z / STEAM_1:Y:Z
    - [U:1:N] or U:1:N (SteamID3)
    - Full or partial steamcommunity.com URLs (…/profiles/7656… or …/id/vanity)
    - Custom URL name (vanity), resolved via ISteamUser/ResolveVanityURL
    """
    s = steamid_or_vanity.strip()
    if not s:
        raise ValueError("STEAM_ID64 is empty.")

    if s.isdigit() and len(s) >= 15:
        return s

    sid = _steam2_to_64(s)
    if sid:
        return sid

    sid = _steam3_to_64(s)
    if sid:
        return sid

    extracted = _extract_from_community_input(s)
    if extracted:
        if extracted.isdigit() and len(extracted) >= 15:
            return extracted
        s = extracted

    if s.isdigit() and len(s) >= 15:
        return s

    return _resolve_vanity(api_key, s)


def get_owned_games(api_key: str, steamid64: str) -> list[dict[str, Any]]:
    params = {
        "key": api_key,
        "steamid": steamid64,
        "include_appinfo": True,
        "include_played_free_games": True,
    }
    r = requests.get(OWNED_GAMES_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()["response"].get("games", [])


def get_current_players(appid: int) -> int:
    r = requests.get(CURRENT_PLAYERS_URL, params={"appid": appid}, timeout=30)
    r.raise_for_status()
    return int(r.json()["response"]["player_count"])


def _row_for_game(g: dict[str, Any]) -> dict[str, Any]:
    appid = int(g["appid"])
    name = g.get("name", f"appid_{appid}")
    try:
        current = get_current_players(appid)
    except Exception:
        current = -1
    return {"appid": appid, "name": name, "current_players": current}


def fetch_library_from_steam(
    api_key: str,
    steamid64: str,
    *,
    max_workers: int | None = None,
) -> list[dict[str, Any]]:
    """
    Load owned games and concurrent player counts from Steam (parallel HTTP).
    Returns rows sorted by current_players descending.
    """
    steamid64 = normalize_steamid64(api_key, steamid64)
    games = get_owned_games(api_key, steamid64)
    if not games:
        return []

    workers = max_workers if max_workers is not None else fetch_workers()
    rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_row_for_game, g): g for g in games}
        for fut in as_completed(futures):
            rows.append(fut.result())

    rows.sort(key=lambda x: x["current_players"], reverse=True)
    return rows


def run_etl(
    api_key: str,
    steamid64: str,
    output_path: Path | None = None,
) -> Path:
    """CLI/export: fetch from Steam and write CSV snapshot."""
    rows = fetch_library_from_steam(api_key, steamid64)
    out = output_path or default_csv_path()
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["appid", "name", "current_players"])
        writer.writeheader()
        writer.writerows(rows)
    return out


def require_env(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        raise SystemExit(f"Missing required environment variable: {name}")
    return v


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
