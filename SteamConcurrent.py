import csv
import time
import requests

STEAM_API_KEY = "STEAM_API_KEY"
STEAM_ID64 = "STEAM_ID_64"

OWNED_GAMES_URL = "https://api.steampowered.com/IPlayerService/GetOwnedGames/v1/"
CURRENT_PLAYERS_URL = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"

def get_owned_games(api_key: str, steamid64: str):
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

def main():
    games = get_owned_games(STEAM_API_KEY, STEAM_ID64)
    rows = []

    for i, g in enumerate(games, start=1):
        appid = g["appid"]
        name = g.get("name", f"appid_{appid}")
        try:
            current = get_current_players(appid)
        except Exception:
            current = -1  # couldn't fetch
        rows.append({"appid": appid, "name": name, "current_players": current})

        # light throttling so you don't spam requests
        if i % 25 == 0:
            time.sleep(1)

    rows.sort(key=lambda x: x["current_players"], reverse=True)

    with open("my_steam_library_by_current_players.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["appid", "name", "current_players"])
        writer.writeheader()
        writer.writerows(rows)

    print("Wrote: my_steam_library_by_current_players.csv")

if __name__ == "__main__":
    main()
