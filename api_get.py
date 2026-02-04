import os
import argparse
import json
from pathlib import Path
from typing import Dict, Optional, List
import requests

# Import database helper
from db_helper import (
    init_database,
    update_user_stats,
    update_user_meta,
    get_user_stats_with_deltas,
    user_exists,
    store_uuid,
    resolve_username_to_uuid,
    update_username_for_uuid,
    get_db_connection,
    store_hotbar_layouts
)

SCRIPT_DIR = Path(__file__).parent.absolute()


# Sheep name mappings for Sheep Wars
SHEEP_NAME_MAPPING = {
    "RED_SHEEP": "EXPLOSIVE_SHEEP",
    "ORANGE_SHEEP": "HIGH_EXPLOSIVE_SHEEP",
    "GREEN_SHEEP": "HOMING_SHEEP",
    "PINK_SHEEP": "HEALING_SHEEP",
    "BROWN_SHEEP": "EARTHQUAKE_SHEEP",
    "BLACK_SHEEP": "BLACK_HOLE_SHEEP",
    "WHITE_SHEEP": "ONBOARDING_SHEEP"
}

# Capture the Wool item position mappings
CTW_ITEM_MAPPING = {
    0: "SWORD",
    1: "BOW",
    2: "BLOCKS",
    3: "BLOCKS",
    4: "BLOCKS",
    5: "AXE",
    6: "GAPPLE",
    7: "PICKAXE",
    8: "ARROW"
}


def read_api_key_file(use_temp: bool = False) -> Optional[str]:
    """Read API key from API_KEY.txt or API_KEY_TEMP.txt next to the script, if present.
    
    Args:
        use_temp: If True, read from API_KEY_TEMP.txt instead of API_KEY.txt
    """
    filename = "API_KEY_TEMP.txt" if use_temp else "API_KEY.txt"
    key_path = SCRIPT_DIR / filename
    if key_path.exists():
        try:
            content = key_path.read_text(encoding="utf-8").strip()
            if content:
                return content
        except Exception:
            # ignore read errors and fall back to other sources
            pass
    return None


# -------- Wool Games level calculation --------

def experience_to_level(exp: float) -> float:
    """Calculate Wool Games level from experience with prestige scaling.
    
    At each prestige (0, 100, 200, 300, etc), the XP requirements reset:
    - Level X+0 to X+1: 1000 XP
    - Level X+1 to X+2: 2000 XP
    - Level X+2 to X+3: 3000 XP
    - Level X+3 to X+4: 4000 XP
    - Level X+4 to X+5: 5000 XP
    - Level X+5 to X+100: 5000 XP each (95 levels)
    
    Total XP per prestige (100 levels): 1000+2000+3000+4000+5000 + 95*5000 = 490000
    
    Returns float with decimal precision for partial level progress.
    """
    if exp <= 0:
        return 0.0
    
    XP_PER_PRESTIGE = 490000
    prestige_count = int(exp / XP_PER_PRESTIGE)
    remaining_xp = exp - (prestige_count * XP_PER_PRESTIGE)
    
    # Calculate level within current prestige with decimal precision
    if remaining_xp < 1000:
        level_in_prestige = remaining_xp / 1000.0
    elif remaining_xp < 3000:  # 1000 + 2000
        level_in_prestige = 1 + (remaining_xp - 1000) / 2000.0
    elif remaining_xp < 6000:  # 1000 + 2000 + 3000
        level_in_prestige = 2 + (remaining_xp - 3000) / 3000.0
    elif remaining_xp < 10000:  # 1000 + 2000 + 3000 + 4000
        level_in_prestige = 3 + (remaining_xp - 6000) / 4000.0
    elif remaining_xp < 15000:  # 1000 + 2000 + 3000 + 4000 + 5000
        level_in_prestige = 4 + (remaining_xp - 10000) / 5000.0
    else:
        # Level 5+ in prestige: 5000 XP each
        remaining_after_first_5 = remaining_xp - 15000
        level_in_prestige = 5 + (remaining_after_first_5 / 5000.0)
    
    # Convert to 1-based display level (Hypixel shows levels starting at 1)
    return prestige_count * 100 + level_in_prestige + 1


def experience_delta_to_level_delta(exp_delta: float) -> float:
    """Convert an experience DELTA to a level DELTA (without prestige resets).
    
    This is used for calculating level gains over a period (daily, weekly, etc).
    It assumes the XP gain is within a reasonable range and averages the XP requirements.
    
    For simplicity, we use an average of ~4800 XP per level across the prestige cycle:
    - First 5 levels: (1000+2000+3000+4000+5000)/5 = 3000 avg
    - Remaining 95 levels: 5000 each
    - Overall average: 490000/100 = 4900 XP/level
    
    For better accuracy at lower levels, we use a weighted approximation.
    """
    if exp_delta <= 0:
        return 0.0
    
    # For small XP gains (likely low levels), use a more accurate calculation
    if exp_delta < 15000:  # First 5 levels worth
        remaining = exp_delta
        level_gain = 0.0
        
        if remaining >= 1000:
            level_gain += 1.0
            remaining -= 1000
        elif remaining > 0:
            return remaining / 1000.0
        
        if remaining >= 2000:
            level_gain += 1.0
            remaining -= 2000
        elif remaining > 0:
            return level_gain + (remaining / 2000.0)
        
        if remaining >= 3000:
            level_gain += 1.0
            remaining -= 3000
        elif remaining > 0:
            return level_gain + (remaining / 3000.0)
        
        if remaining >= 4000:
            level_gain += 1.0
            remaining -= 4000
        elif remaining > 0:
            return level_gain + (remaining / 4000.0)
        
        if remaining >= 5000:
            level_gain += 1.0
            remaining -= 5000
        elif remaining > 0:
            return level_gain + (remaining / 5000.0)
        
        # Any remaining is at 5000 XP/level
        return level_gain + (remaining / 5000.0)
    else:
        # For larger XP gains, use 5000 XP/level average (most common rate)
        return exp_delta / 5000.0


# -------- API helpers --------

def get_uuid(username: str) -> tuple[str, str]:
    """Get UUID and properly-cased username from Mojang API.
    
    Returns:
        tuple[str, str]: (uuid, properly_cased_username)
    """
    # Try Mojang first (authoritative)
    try:
        r = requests.get(f"https://api.mojang.com/users/profiles/minecraft/{username}", timeout=5)
        if r.status_code == 200:
            data = r.json()
            return data["id"], data.get("name", username)
    except Exception:
        pass
        
    # Try PlayerDB fallback (may have cached old data)
    try:
        r = requests.get(f"https://playerdb.co/api/player/minecraft/{username}", timeout=5)
        if r.status_code == 200:
            data = r.json()
            if data.get('success'):
                meta = data.get('data', {}).get('player', {})
                uuid = meta.get('raw_id')
                cached_username = meta.get('username', username)
                
                if uuid:
                    # Verify with Mojang sessionserver to get current username
                    try:
                        verify_r = requests.get(f"https://sessionserver.mojang.com/session/minecraft/profile/{uuid}", timeout=5)
                        if verify_r.status_code == 200:
                            verify_data = verify_r.json()
                            current_username = verify_data.get('name', cached_username)
                            print(f"[UUID] PlayerDB returned cached '{cached_username}', verified current is '{current_username}'")
                            return uuid, current_username
                    except:
                        pass
                    
                    # If verification failed, use PlayerDB data
                    return uuid, cached_username
    except Exception:
        pass

    raise requests.exceptions.RequestException(f"Could not resolve UUID for {username}")


def get_hypixel_player(uuid: str, api_key: str) -> Dict:
    r = requests.get(
        "https://api.hypixel.net/v2/player",
        headers={"API-Key": api_key},
        params={"uuid": uuid},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()


def get_hypixel_guild(uuid: str, api_key: str) -> Dict:
    """Fetch guild information for a player from Hypixel API."""
    r = requests.get(
        "https://api.hypixel.net/v2/guild",
        headers={"API-Key": api_key},
        params={"player": uuid},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()


def extract_guild_info(guild_json: Dict) -> tuple[Optional[str], Optional[str]]:
    """Extract guild tag and tag color from Hypixel guild API response.
    
    Returns (tag, tagColor). Returns ("", "") if not in a guild.
    """
    if not isinstance(guild_json, dict):
        return None, None
    
    guild = guild_json.get("guild")
    if not guild or not isinstance(guild, dict):
        return "", ""
    
    tag = guild.get("tag") or ""
    tag_color = guild.get("tagColor") or ""
    
    return tag, tag_color


def extract_player_rank(player_json: Dict) -> Optional[str]:
    """Extract the player's rank from Hypixel API response.
    
    Returns rank in order of priority, skipping "NONE" values:
    rank, monthlyPackageRank, newPackageRank, packageRank
    """
    player = player_json.get("player", {}) if isinstance(player_json, dict) else {}
    if not isinstance(player, dict):
        return None
    
    # Check in order of priority, skip "NONE" values
    rank = player.get("rank")
    if rank and rank.upper() != "NONE":
        return rank
    
    monthly = player.get("monthlyPackageRank")
    if monthly and monthly.upper() != "NONE":
        return monthly
    
    new_package = player.get("newPackageRank")
    if new_package and new_package.upper() != "NONE":
        return new_package
    
    package = player.get("packageRank")
    if package and package.upper() != "NONE":
        return package
    
    return None


def convert_sheep_names(layout):
    """Convert sheep color names to descriptive names."""
    if not layout:
        return layout
    
    converted = {}
    for slot, item in layout.items():
        if item in SHEEP_NAME_MAPPING:
            converted[slot] = SHEEP_NAME_MAPPING[item]
        else:
            converted[slot] = item
    return converted


def convert_item_names(layout):
    """Convert item IDs to readable names."""
    if not layout:
        return layout
    
    converted = {}
    for slot, item in layout.items():
        if item == "POTION_16389":
            converted[slot] = "SPLASH_POTION_HEALING"
        else:
            converted[slot] = item
    return converted


def convert_ctw_layout(layout):
    """Convert CTW position indices to item names."""
    if not layout:
        return layout
    
    converted = {}
    for slot, position in layout.items():
        # Position is a number that maps to the actual item
        if isinstance(position, int) and position in CTW_ITEM_MAPPING:
            converted[slot] = CTW_ITEM_MAPPING[position]
        else:
            converted[slot] = position
    return converted


def extract_hotbar_layouts(player_json: Dict) -> List[Dict[str, any]]:
    """Extract hotbar layouts from player data.
    
    Returns list of layout dicts with keys: game, kit, slot_0..slot_8
    """
    player = player_json.get("player", {}) if isinstance(player_json, dict) else {}
    stats_root = player.get("stats", {}) if isinstance(player, dict) else {}
    
    # Try common wool keys
    wool_keys = ["WoolGames", "WOOL_GAMES", "Wool_Games", "WoolWars"]
    wool_games = None
    for k in wool_keys:
        if k in stats_root:
            wool_games = stats_root[k]
            break
    
    if not isinstance(wool_games, dict):
        return []
    
    db_rows = []
    
    # Process Sheep Wars
    sheep_wars_raw_layout = wool_games.get("sheep_wars", {}).get("layout", {}).get("slot")
    if sheep_wars_raw_layout:
        sheep_wars_layout = convert_sheep_names(sheep_wars_raw_layout)
        kit = wool_games.get("sheep_wars", {}).get("default_kit") or "NULL"
        row = {
            "game": "sheep_wars",
            "kit": kit,
            "slot_0": sheep_wars_layout.get("0"),
            "slot_1": sheep_wars_layout.get("1"),
            "slot_2": sheep_wars_layout.get("2"),
            "slot_3": sheep_wars_layout.get("3"),
            "slot_4": sheep_wars_layout.get("4"),
            "slot_5": sheep_wars_layout.get("5"),
            "slot_6": sheep_wars_layout.get("6"),
            "slot_7": sheep_wars_layout.get("7"),
            "slot_8": sheep_wars_layout.get("8")
        }
        db_rows.append(row)
    
    # Process Capture The Wool (only slots 0-8, not inventory)
    ctw_raw_layout = wool_games.get("capture_the_wool", {}).get("layout")
    if ctw_raw_layout:
        ctw_layout = convert_ctw_layout(ctw_raw_layout)
        # Filter to only include hotbar slots (0-8)
        hotbar_layout = {k: v for k, v in ctw_layout.items() if k in [str(i) for i in range(9)]}
        if hotbar_layout:
            row = {
                "game": "capture_the_wool",
                "kit": "NULL",
                "slot_0": hotbar_layout.get("0"),
                "slot_1": hotbar_layout.get("1"),
                "slot_2": hotbar_layout.get("2"),
                "slot_3": hotbar_layout.get("3"),
                "slot_4": hotbar_layout.get("4"),
                "slot_5": hotbar_layout.get("5"),
                "slot_6": hotbar_layout.get("6"),
                "slot_7": hotbar_layout.get("7"),
                "slot_8": hotbar_layout.get("8")
            }
            db_rows.append(row)
    
    # Process Wool Wars (multiple kits, one row per kit)
    wool_wars_layouts = wool_games.get("wool_wars", {}).get("layouts")
    if wool_wars_layouts:
        for kit_name, layout in wool_wars_layouts.items():
            # Convert item names for wool wars layouts
            converted_layout = convert_item_names(layout)
            row = {
                "game": "wool_wars",
                "kit": kit_name,
                "slot_0": converted_layout.get("0"),
                "slot_1": converted_layout.get("1"),
                "slot_2": converted_layout.get("2"),
                "slot_3": converted_layout.get("3"),
                "slot_4": converted_layout.get("4"),
                "slot_5": converted_layout.get("5"),
                "slot_6": converted_layout.get("6"),
                "slot_7": converted_layout.get("7"),
                "slot_8": converted_layout.get("8")
            }
            db_rows.append(row)
    else:
        # Add NULL row for wool wars if no data
        row = {
            "game": "wool_wars",
            "kit": "NULL",
            "slot_0": None,
            "slot_1": None,
            "slot_2": None,
            "slot_3": None,
            "slot_4": None,
            "slot_5": None,
            "slot_6": None,
            "slot_7": None,
            "slot_8": None
        }
        db_rows.append(row)
    
    return db_rows


def extract_wool_games_all(player_json: Dict) -> Dict:
    """Extract ALL Wool Games data including general, sheep wars, CTW, and WW stats.
    
    Returns dict with stats using these prefixes:
    - No prefix: general stats (level, experience, coins, playtime, available_layers)
    - No prefix: sheep wars stats (kills, deaths, wins, etc.)
    - ctw_: Capture the Wool stats
    - ww_: Wool Wars stats (including class-specific with ww_classname_stat format)
    """
    player = player_json.get("player", {}) if isinstance(player_json, dict) else {}
    stats_root = player.get("stats", {}) if isinstance(player, dict) else {}
    
    # Try common wool keys
    wool_keys = ["WoolGames", "WOOL_GAMES", "Wool_Games", "WoolWars"]
    wool = None
    for k in wool_keys:
        if k in stats_root:
            wool = stats_root[k]
            break
    
    if not isinstance(wool, dict):
        return {}

    flat: Dict[str, float] = {}

    # ===== GENERAL STATS =====
    progression = wool.get("progression")
    if isinstance(progression, dict):
        if "available_layers" in progression:
            flat["available_layers"] = progression.get("available_layers", 0)
        if "experience" in progression:
            exp_val = progression.get("experience") or 0
            flat["experience"] = exp_val
            try:
                flat["level"] = experience_to_level(exp_val)
            except Exception:
                flat["level"] = 0

    if "coins" in wool:
        flat["coins"] = wool.get("coins", 0)
    if "playtime" in wool:
        flat["playtime"] = wool.get("playtime", 0)

    # ===== SHEEP WARS STATS =====
    sheep_stats = (wool.get("sheep_wars", {}) or {}).get("stats")
    if isinstance(sheep_stats, dict):
        for k, v in sheep_stats.items():
            if isinstance(v, (int, float)):
                flat[k] = v

    # ===== CAPTURE THE WOOL STATS =====
    ctw_stats = (wool.get("capture_the_wool", {}) or {}).get("stats")
    if isinstance(ctw_stats, dict):
        for k, v in ctw_stats.items():
            if isinstance(v, (int, float)):
                flat[f"ctw_{k}"] = v

    # ===== WOOL WARS STATS =====
    ww_data = wool.get("wool_wars", {}) or {}
    ww_stats = ww_data.get("stats") if isinstance(ww_data, dict) else {}
    
    if isinstance(ww_stats, dict):
        # Top-level WW stats
        for k, v in ww_stats.items():
            if k == "classes":
                continue
            if isinstance(v, (int, float)):
                flat[f"ww_{k}"] = v
        
        # Class-specific stats
        classes = ww_stats.get("classes", {})
        if isinstance(classes, dict):
            for class_name, class_stats in classes.items():
                if isinstance(class_stats, dict):
                    for stat_name, stat_value in class_stats.items():
                        if isinstance(stat_value, (int, float)):
                            flat[f"ww_{class_name}_{stat_name}"] = stat_value

    return flat


def get_rank_color(rank: Optional[str]) -> str:
    """Get the default color for a rank.
    
    Returns hex color code based on rank priority.
    """
    if not rank:
        return "#FFFFFF"  # White for no rank
    
    rank_upper = rank.upper()
    
    # Rank color mapping
    rank_colors = {
        "ADMIN": "#FF5555",           # Red (c)
        "SUPERSTAR": "#FFAA00",       # Gold (6)
        "MVP_PLUS": "#55FFFF",        # Aqua (b)
        "MVP_PLUS_PLUS": "#FFAA00",   # Gold (6) - MVP++ permanent is gold too
        "MVP": "#55FFFF",             # Aqua (b)
        "VIP_PLUS": "#00AA00",        # Dark Green (2)
        "VIP": "#55FF55",             # Green (a)
    }
    
    return rank_colors.get(rank_upper, "#FFFFFF")  # Default to white


def save_user_color_and_rank(username: str, rank: Optional[str], guild_tag: Optional[str] = None, guild_color: Optional[str] = None):
    """Save or update user's rank and guild info in database.
    
    Only assigns color automatically for NEW users based on their rank.
    Existing users keep their custom color.
    """
    from db_helper import get_user_meta, update_user_meta
    
    username_key = username
    
    # Check if user already exists in database
    existing_meta = get_user_meta(username_key)
    
    if existing_meta:
        # User exists - only update rank and guild info, preserve their color
        print(f"[DEBUG] User {username} already exists with data: {existing_meta}")
        
        update_user_meta(username_key,
                        rank=rank,
                        guild_tag=guild_tag,
                        guild_hex=guild_color)
    else:
        # NEW USER - do not set ign_color, let it default to NULL so rank color is used dynamically
        print(f"[DEBUG] NEW USER {username} - saving rank {rank}, guild: {guild_tag}")
        update_user_meta(username_key,
                        rank=rank,
                        guild_tag=guild_tag,
                        guild_hex=guild_color)


def api_update_database(username: str, api_key: str, snapshot_sections: set[str] | None = None):
    """Update user stats in database from Hypixel API.
    
    Args:
        username: Minecraft username (can be current or old username)
        api_key: Hypixel API key
        snapshot_sections: Set of periods to snapshot ("session", "daily", "yesterday", "monthly")
        
    Returns:
        Dict with update results
    """
    try:
        # Ensure database exists
        init_database()
        
        # First, try direct username lookup
        try:
            uuid, proper_username = get_uuid(username)
            print(f"[API] Username '{username}' is valid, resolved to {proper_username} (UUID: {uuid})")
        except Exception as e:
            # Username lookup failed - this might be an old/invalid username
            # Check if we have it in our database with a UUID
            print(f"[UUID] Username lookup failed for '{username}': {e}")
            print(f"[UUID] Checking if '{username}' is in our database...")
            
            resolved = resolve_username_to_uuid(username)
            if resolved:
                uuid, current_username = resolved
                print(f"[UUID] Found '{username}' in database with UUID {uuid}")
                print(f"[UUID] Stored current username: {current_username}")
                
                # Query Mojang with UUID to get actual current username
                print(f"[UUID] Querying Mojang API with UUID {uuid} to verify current username...")
                try:
                    r = requests.get(f"https://sessionserver.mojang.com/session/minecraft/profile/{uuid}", timeout=5)
                    if r.status_code == 200:
                        data = r.json()
                        proper_username = data.get("name")
                        print(f"[UUID] Current username from Mojang: {proper_username}")
                        
                        # If username changed, we'll handle migration after storing UUID
                    else:
                        # Mojang API failed, use stored username
                        print(f"[UUID] Mojang API returned {r.status_code}, using stored username: {current_username}")
                        proper_username = current_username
                except Exception as e2:
                    print(f"[UUID] Mojang API query failed: {e2}, using stored username: {current_username}")
                    proper_username = current_username
            else:
                # Not in our database either - this is truly an invalid username
                print(f"[ERROR] Username '{username}' not found in Mojang API or database")
                raise requests.exceptions.RequestException(f"Username '{username}' does not exist and is not in database")
        
        # Store/update UUID in database
        store_uuid(proper_username, uuid)
        
        # If the username we were given differs from proper_username, migrate data
        if username.lower() != proper_username.lower():
            print(f"[UUID] Username changed: {username} → {proper_username}")
            print(f"[UUID] Migrating all data from '{username}' to '{proper_username}'...")
            update_username_for_uuid(uuid, proper_username)
            
            # Also ensure the new username is in tracked_users if the old one was
            from db_helper import is_tracked_user
            with get_db_connection() as conn:
                cursor = conn.cursor()
                # Check if old username was tracked
                cursor.execute('SELECT is_tracked FROM tracked_users WHERE LOWER(username) = LOWER(?)', (username,))
                old_tracked = cursor.fetchone()
                if old_tracked and old_tracked[0] == 1:
                    # Old username was tracked, ensure new username is tracked
                    cursor.execute('SELECT username FROM tracked_users WHERE LOWER(username) = LOWER(?)', (proper_username,))
                    new_exists = cursor.fetchone()
                    if not new_exists:
                        # Add new username to tracked_users
                        print(f"[UUID] Adding '{proper_username}' to tracked_users")
                        cursor.execute('INSERT INTO tracked_users (username, uuid, is_tracked) VALUES (?, ?, 1)',
                                     (proper_username, uuid))
                        conn.commit()
        
        # Get player data from Hypixel
        data = get_hypixel_player(uuid, api_key)
        
    except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
        should_fallback = False
        if isinstance(e, requests.exceptions.HTTPError) and e.response is not None:
            if e.response.status_code == 429:
                print(f"[WARNING] Rate limited (429) for {username}. Attempting snapshot fallback.")
                should_fallback = True
            elif e.response.status_code >= 500:
                print(f"[WARNING] API Server Error ({e.response.status_code}) for {username}. Attempting snapshot fallback.")
                should_fallback = True
        elif isinstance(e, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)):
            print(f"[WARNING] Connection error for {username}: {e}. Attempting snapshot fallback.")
            should_fallback = True

        if should_fallback:
            # For rate limiting: just take snapshots without updating lifetime values
            print("[INFO] Taking snapshots from existing database values")
            
            # Try to use proper_username if we resolved it, otherwise use original
            fallback_username = proper_username if 'proper_username' in locals() else username
            
            try:
                # Get existing stats
                existing_stats = get_user_stats_with_deltas(fallback_username)
                if existing_stats:
                    # Extract just lifetime values
                    lifetime_stats = {stat: data['lifetime'] for stat, data in existing_stats.items()}
                    # Update with snapshots
                    update_user_stats(fallback_username, lifetime_stats, snapshot_sections)
                    print(f"[FALLBACK] Snapshots taken for {fallback_username}")
                    return {
                        "skipped": True,
                        "reason": "rate_limited",
                        "username": fallback_username,
                        "snapshots_written": True,
                    }
                else:
                    print(f"[ERROR] No existing data found for {fallback_username}")
                    return {
                        "skipped": True,
                        "reason": "rate_limited",
                        "username": fallback_username,
                        "snapshots_written": False,
                    }
            except Exception as fe:
                print(f"[ERROR] Snapshot fallback failed: {fe}")
                return {
                    "skipped": True,
                    "reason": "rate_limited",
                    "username": username,
                    "snapshots_written": False,
                }
        else:
            # Non-recoverable error (e.g. 404)
            print(f"[ERROR] API request failed for {username}: {e}")
            return {
                "skipped": True,
                "reason": "api_error",
                "error": str(e),
                "username": username,
                "snapshots_written": False,
            }
    
    # Extract Wool Games stats
    current = extract_wool_games_all(data)
    if not current:
        raise RuntimeError(f"No Wool Games stats for {proper_username}")

    print(f"[API] Extracted {len(current)} stats for {proper_username}")

    # Fetch guild information
    print(f"[DEBUG] Fetching guild information for {proper_username} (UUID: {uuid})")
    try:
        guild_data = get_hypixel_guild(uuid, api_key)
        # Save guild data to file for inspection
        guild_file = SCRIPT_DIR / "guild_info.json"
        with open(guild_file, 'w') as f:
            json.dump(guild_data, f, indent=2)
        print(f"[DEBUG] Guild data saved to guild_info.json")
        guild_tag, guild_color = extract_guild_info(guild_data)
        print(f"[DEBUG] Extracted guild tag: {guild_tag}, color: {guild_color}")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            print(f"[DEBUG] Rate limited (429) fetching guild data for {proper_username}. Using cached data.")
            guild_tag, guild_color = None, None
        else:
            print(f"[DEBUG] Failed to fetch guild data: {e}")
            guild_tag, guild_color = None, None
    except Exception as e:
        print(f"[DEBUG] Failed to fetch guild data: {e}")
        guild_tag, guild_color = None, None

    # Extract and save player rank and guild info to database
    rank = extract_player_rank(data)
    print(f"[DEBUG] Extracted rank for {proper_username}: {rank}")
    save_user_color_and_rank(proper_username, rank, guild_tag, guild_color)

    # Determine which stat categories are NEW for this user
    new_stat_categories = set()
    if user_exists(proper_username):
        # Get existing stats to see what categories we already have
        existing_stats = get_user_stats_with_deltas(proper_username)
        
        # Check if user has any existing CTW or WW stats
        has_ctw = any(stat.startswith('ctw_') for stat in existing_stats.keys())
        has_ww = any(stat.startswith('ww_') for stat in existing_stats.keys())
        
        # If we're adding CTW/WW stats for the first time, mark as new
        has_ctw_now = any(k.startswith('ctw_') for k in current.keys())
        has_ww_now = any(k.startswith('ww_') for k in current.keys())
        
        if not has_ctw and has_ctw_now:
            new_stat_categories.add('ctw')
            print(f"[DB] First time adding CTW stats for {proper_username}")
        
        if not has_ww and has_ww_now:
            new_stat_categories.add('ww')
            print(f"[DB] First time adding WW stats for {proper_username}")

    # Update database with stats
    print(f"[DB] Updating stats for {proper_username}")
    update_user_stats(proper_username, current, snapshot_sections, new_stat_categories)
    
    # Update metadata
    level = int(current.get('level', 0))
    # Calculate prestige icon (placeholder - you can add icon logic here)
    icon = None
    update_user_meta(proper_username, level, icon, None, guild_tag, guild_color)
    
    # Extract and store hotbar layouts
    print(f"[DB] Extracting hotbar layouts for {proper_username}")
    layouts = extract_hotbar_layouts(data)
    if layouts:
        store_hotbar_layouts(proper_username, layouts)
    else:
        print(f"[DB] No hotbar layouts found for {proper_username}")
    
    # Get processed stats with deltas for return value
    processed_stats = get_user_stats_with_deltas(proper_username)
    
    print(f"[DB] Successfully updated {proper_username}")
    
    return {
        "uuid": uuid,
        "stats": current,
        "processed_stats": processed_stats,
        "database": "stats.db",
        "username": proper_username
    }


def main():
    parser = argparse.ArgumentParser(description="API-based Wool Games stats to SQLite database")
    parser.add_argument("-ign", "--username", required=True, help="Minecraft IGN")
    parser.add_argument("-session", action="store_true", help="Take session snapshot")
    parser.add_argument("-daily", action="store_true", help="Take daily snapshot")
    parser.add_argument("-yesterday", action="store_true", help="Take yesterday snapshot")
    parser.add_argument("-monthly", action="store_true", help="Take monthly snapshot")
    parser.add_argument("--use-temp-key", action="store_true", help="Use API_KEY_TEMP.txt instead of API_KEY.txt")
    args = parser.parse_args()

    # Use the appropriate API key file based on flag
    api_key = read_api_key_file(use_temp=args.use_temp_key)
    if not api_key:
        key_file = "API_KEY_TEMP.txt" if args.use_temp_key else "API_KEY.txt"
        raise RuntimeError(
            f"Missing API key: create {key_file} next to api_get.py containing your Hypixel API key"
        )
    
    sections = set()
    if args.session:
        sections.add("session")
    if args.daily:
        sections.add("daily")
    if args.yesterday:
        sections.add("yesterday")
    if args.monthly:
        sections.add("monthly")

    res = api_update_database(args.username, api_key, snapshot_sections=sections)
    print(json.dumps(res, default=str))


if __name__ == "__main__":
    main()
