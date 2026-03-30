"""CLI: refresh Steam library CSV (same ETL as the web app)."""

from pathlib import Path

from dotenv import load_dotenv

from steam_etl import default_csv_path, require_env, run_etl

load_dotenv(Path(__file__).resolve().parent / ".env")


def main():
    api_key = require_env("STEAM_API_KEY")
    steamid = require_env("STEAM_ID64")
    out = run_etl(api_key, steamid, default_csv_path())
    print(f"Wrote: {out}")


if __name__ == "__main__":
    main()
