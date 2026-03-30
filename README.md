# Steam games · global concurrent leaderboard

FastAPI app that loads the **full Steam store game list** (`IStoreService/GetAppList`), then calls **`GetNumberOfCurrentPlayers` for every app**, sorts **most players → least**, caches that in `data/steam_ccu_leaderboard.json`, and **paginates** that sorted list. **Pages are not ordered by Steam App ID**—only by live concurrent player count.

## Flow

1. **Download catalog** — Fetches the app list from Steam (always hits the API from the UI button; local `steam_catalog_apps.json` is updated with an atomic write).
2. **Build leaderboard** — Fetches CCU for each app in chunks (`STEAM_CCU_CHUNK`, default 2500), sorts globally, saves `steam_ccu_leaderboard.json`.
3. **Browse** — Table/chart use the cached ranking. Use **Rebuild leaderboard** to refresh CCU for everyone (slow).

A **full** catalog can take a **very long** time to rank (one CCU request per game). Use `STEAM_CATALOG_MAX_APPS=20000` (or similar) in `.env` while testing.

## Setup

1. [Steam Web API key](https://steamcommunity.com/dev/apikey) in `.env` as `STEAM_API_KEY`.
2. `pip install -r requirements.txt`
3. `python main.py` → `http://127.0.0.1:8000`
4. Click **Download catalog**, wait for the list, then wait for **Fetching player counts** (global sort).

Optional: `STEAM_AUTO_DOWNLOAD_CATALOG=1` loads the catalog from disk on start if present; first-time still needs **Download catalog** unless a cache file exists.

## API

| Endpoint | Purpose |
|----------|---------|
| `GET /api/games?page=&per_page=` | Paginated rows from the **CCU-sorted** leaderboard (`per_page` ≤ 250) |
| `GET /api/catalog/status` | Catalog + leaderboard loading progress |
| `POST /api/catalog/download` | Fetch app list from Steam (ignores stale local catalog for this action) |
| `POST /api/catalog/reload` | Delete caches and re-fetch catalog |
| `POST /api/leaderboard/rebuild` | Re-fetch CCU for all apps and re-sort |
| `GET /healthz` | Health check |

## CLI (your library → CSV)

`python SteamConcurrent.py` exports your owned games using `STEAM_API_KEY` + `STEAM_ID64`.

## Docker

```bash
docker build -t steam-catalog .
docker run --rm -p 8000:8000 -e STEAM_API_KEY=your_key -v steamdata:/app/data steam-catalog
```

## Security

Do not commit your API key.
