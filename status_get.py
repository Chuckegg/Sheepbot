import os
import argparse
import json
from pathlib import Path
from typing import Dict, Optional
import requests


SCRIPT_DIR = Path(__file__).parent.absolute()


def read_api_key_file() -> Optional[str]:
    """Read API key from API_KEY.txt next to the script, if present."""
    key_path = SCRIPT_DIR / "API_KEY.txt"
    if key_path.exists():
        try:
            content = key_path.read_text(encoding="utf-8").strip()
            if content:
                return content
        except Exception:
            pass
    return None


def get_uuid(username: str) -> tuple[str, str]:
    """Get UUID and properly-cased username from Mojang API.
    
    Args:
        username: Minecraft username (IGN)
    
    Returns:
        tuple[str, str]: (uuid, properly_cased_username)
    """
    r = requests.get(
        f"https://api.mojang.com/users/profiles/minecraft/{username}",
        timeout=15
    )
    r.raise_for_status()
    data = r.json()
    return data["id"], data.get("name", username)


def get_hypixel_status(uuid: str, api_key: str) -> Dict:
    """Fetch status information for a player from Hypixel API.
    
    Args:
        uuid: Player UUID
        api_key: Hypixel API key
    
    Returns:
        dict: API response
    """
    r = requests.get(
        "https://api.hypixel.net/v2/status",
        headers={"API-Key": api_key},
        params={"uuid": uuid},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()


def main():
    parser = argparse.ArgumentParser(description="Get player status from Hypixel API")
    parser.add_argument("-ign", "--username", required=True, help="Minecraft IGN")
    args = parser.parse_args()
    
    api_key = read_api_key_file()
    if not api_key:
        print("[ERROR] API key not found in API_KEY.txt")
        return
    
    try:
        print(f"[INFO] Converting username '{args.username}' to UUID...")
        uuid, proper_name = get_uuid(args.username)
        print(f"[INFO] Got UUID: {uuid} (proper name: {proper_name})")
        
        print(f"[INFO] Fetching status from Hypixel API...")
        status_data = get_hypixel_status(uuid, api_key)
        
        print("[INFO] Success!")
        print(json.dumps(status_data, indent=2))
        
    except requests.exceptions.HTTPError as e:
        print(f"[ERROR] HTTP error: {e}")
        if e.response.status_code == 404:
            print(f"[ERROR] Player '{args.username}' not found")
        elif e.response.status_code == 403:
            print(f"[ERROR] Invalid API key")
        else:
            print(f"[ERROR] Status code: {e.response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Request failed: {e}")
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")


if __name__ == "__main__":
    main()
