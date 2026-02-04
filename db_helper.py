#!/usr/bin/env python3
"""
Database helper module for stats.db SQLite database operations.
Updated to support categorized stat tables (general_stats, sheep_stats, ctw_stats, ww_stats).
"""

import sqlite3
import time
from pathlib import Path
from typing import Dict, List, Optional, Set
from contextlib import contextmanager


DB_FILE = Path(__file__).parent / "stats.db"

# Define which stats belong to which category
GENERAL_STATS = {'available_layers', 'experience', 'coins', 'playtime', 'level'}
SHEEP_STATS = {
    'sheep_thrown', 'wins', 'games_played', 'deaths', 'damage_dealt',
    'kills', 'losses', 'deaths_explosive', 'magic_wool_hit', 'kills_void',
    'deaths_void', 'deaths_bow', 'kills_explosive', 'kills_bow',
    'kills_melee', 'deaths_melee'
}
CTW_STATS = {
    'ctw_deaths', 'ctw_kills', 'ctw_assists', 'ctw_gold_spent', 'ctw_kills_on_woolholder',
    'ctw_experienced_wins', 'ctw_experienced_losses', 'ctw_fastest_win', 'ctw_wools_stolen',
    'ctw_longest_game', 'ctw_participated_wins', 'ctw_most_kills_and_assists', 'ctw_gold_earned',
    'ctw_participated_losses', 'ctw_most_gold_earned', 'ctw_deaths_to_woolholder',
    'ctw_kills_with_wool', 'ctw_deaths_with_wool', 'ctw_fastest_wool_capture', 'ctw_wools_captured'
}
WW_STATS = {
    'ww_assists', 'ww_blocks_broken', 'ww_deaths', 'ww_games_played',
    'ww_kills', 'ww_powerups_gotten', 'ww_wool_placed', 'ww_wins',
    # Class-specific stats
    'ww_engineer_blocks_broken', 'ww_engineer_deaths', 'ww_engineer_wool_placed',
    'ww_engineer_powerups_gotten', 'ww_engineer_kills', 'ww_engineer_assists',
    'ww_tank_assists', 'ww_tank_blocks_broken', 'ww_tank_deaths',
    'ww_tank_kills', 'ww_tank_powerups_gotten', 'ww_tank_wool_placed',
    'ww_assault_blocks_broken', 'ww_assault_deaths', 'ww_assault_powerups_gotten',
    'ww_assault_wool_placed', 'ww_assault_assists', 'ww_assault_kills',
    'ww_golem_assists', 'ww_golem_blocks_broken', 'ww_golem_deaths',
    'ww_golem_kills', 'ww_golem_powerups_gotten', 'ww_golem_wool_placed',
    'ww_archer_deaths', 'ww_archer_powerups_gotten', 'ww_archer_assists',
    'ww_archer_kills', 'ww_archer_wool_placed', 'ww_archer_blocks_broken',
    'ww_swordsman_deaths', 'ww_swordsman_powerups_gotten', 'ww_swordsman_kills',
    'ww_swordsman_assists', 'ww_swordsman_blocks_broken', 'ww_swordsman_wool_placed'
}


def get_stat_table(stat_name: str) -> str:
    """Determine which table a stat belongs to."""
    if stat_name in GENERAL_STATS:
        return 'general_stats'
    elif stat_name in SHEEP_STATS:
        return 'sheep_stats'
    elif stat_name.startswith('ctw_') or stat_name in CTW_STATS:
        return 'ctw_stats'
    elif stat_name.startswith('ww_') or stat_name in WW_STATS:
        return 'ww_stats'
    else:
        # Default to sheep_stats for backward compatibility
        return 'sheep_stats'


@contextmanager
def get_db_connection(db_path: Optional[Path] = None):
    """Context manager for database connections with automatic cleanup.
    
    Args:
        db_path: Optional custom path to database file
        
    Yields:
        sqlite3.Connection: Database connection object
    """
    path = db_path or DB_FILE
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row  # Enable dict-like access
    try:
        yield conn
    finally:
        conn.close()


def init_database(db_path: Optional[Path] = None):
    """Initialize database schema if it doesn't exist.
    
    Args:
        db_path: Optional custom path to database file
    """
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()
        
        # Create categorized stat tables
        tables = ['general_stats', 'sheep_stats', 'ctw_stats', 'ww_stats']
        
        for table in tables:
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {table} (
                    username TEXT NOT NULL,
                    stat_name TEXT NOT NULL,
                    lifetime REAL DEFAULT 0,
                    session REAL DEFAULT 0,
                    daily REAL DEFAULT 0,
                    yesterday REAL DEFAULT 0,
                    weekly REAL DEFAULT 0,
                    monthly REAL DEFAULT 0,
                    PRIMARY KEY (username, stat_name)
                )
            ''')
            
            # Create indexes for faster lookups
            cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{table}_username ON {table}(username)')
            cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{table}_stat_name ON {table}(stat_name)')
        
        # User metadata table - stores level, icon, colors, etc.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_meta (
                username TEXT PRIMARY KEY,
                level INTEGER DEFAULT 0,
                icon TEXT DEFAULT '',
                ign_color TEXT DEFAULT NULL,
                guild_tag TEXT DEFAULT NULL,
                guild_hex TEXT DEFAULT NULL,
                rank TEXT DEFAULT NULL,
                uuid TEXT DEFAULT NULL
            )
        ''')
        
        # Check if uuid column exists in user_meta, add if not
        cursor.execute("PRAGMA table_info(user_meta)")
        columns = [col[1] for col in cursor.fetchall()]
        if 'uuid' not in columns:
            cursor.execute('ALTER TABLE user_meta ADD COLUMN uuid TEXT DEFAULT NULL')
        
        # Create index on UUID for fast lookups
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_meta_uuid ON user_meta(uuid)')
        
        # User links table - maps usernames to Discord IDs
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_links (
                username TEXT PRIMARY KEY,
                discord_id TEXT NOT NULL
            )
        ''')
        
        # Default users table - maps Discord IDs to default usernames
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS default_users (
                discord_id TEXT PRIMARY KEY,
                username TEXT NOT NULL
            )
        ''')
        
        # Tracked streaks table - stores winstreaks and killstreaks
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tracked_streaks (
                username TEXT PRIMARY KEY,
                winstreak INTEGER DEFAULT 0,
                killstreak INTEGER DEFAULT 0,
                last_wins INTEGER DEFAULT 0,
                last_losses INTEGER DEFAULT 0,
                last_kills INTEGER DEFAULT 0,
                last_deaths INTEGER DEFAULT 0
            )
        ''')
        
        # Tracked users table - list of users being actively tracked
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tracked_users (
                username TEXT PRIMARY KEY,
                added_at INTEGER DEFAULT (strftime('%s', 'now')),
                is_tracked INTEGER DEFAULT 1,
                uuid TEXT DEFAULT NULL
            )
        ''')
        
        # Migrate existing rows to have is_tracked=1 if column doesn't exist yet
        cursor.execute("PRAGMA table_info(tracked_users)")
        columns = [col[1] for col in cursor.fetchall()]
        if 'is_tracked' not in columns:
            cursor.execute('ALTER TABLE tracked_users ADD COLUMN is_tracked INTEGER DEFAULT 1')
            cursor.execute('UPDATE tracked_users SET is_tracked = 1')
        if 'uuid' not in columns:
            cursor.execute('ALTER TABLE tracked_users ADD COLUMN uuid TEXT DEFAULT NULL')
        
        # Create index on UUID for tracked users
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tracked_users_uuid ON tracked_users(uuid)')
        
        # UUID history table - maps old usernames to UUIDs for handling username changes
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS uuid_history (
                uuid TEXT NOT NULL,
                username TEXT NOT NULL,
                last_seen INTEGER DEFAULT (strftime('%s', 'now')),
                PRIMARY KEY (uuid, username)
            )
        ''')
        
        # Create indexes for UUID history lookups
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_uuid_history_uuid ON uuid_history(uuid)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_uuid_history_username ON uuid_history(LOWER(username))')
        
        # Create indexes for other tables
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_discord_id ON user_links(discord_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_default_discord ON default_users(discord_id)')
        
        # Hotbar layouts table - stores player hotbar configurations
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS hotbar_layouts (
                username TEXT NOT NULL,
                game TEXT NOT NULL,
                kit TEXT,
                slot_0 TEXT,
                slot_1 TEXT,
                slot_2 TEXT,
                slot_3 TEXT,
                slot_4 TEXT,
                slot_5 TEXT,
                slot_6 TEXT,
                slot_7 TEXT,
                slot_8 TEXT,
                last_updated INTEGER DEFAULT (strftime('%s', 'now')),
                PRIMARY KEY (username, game, kit)
            )
        ''')
        
        # Create indexes for layouts
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_hotbar_layouts_username ON hotbar_layouts(username)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_hotbar_layouts_game ON hotbar_layouts(game)')
        
        conn.commit()


def get_all_usernames() -> List[str]:
    """Get list of all usernames in the database.
    
    Returns:
        List of usernames
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        # Get usernames from any of the stat tables
        cursor.execute('''
            SELECT DISTINCT username FROM (
                SELECT username FROM general_stats
                UNION
                SELECT username FROM sheep_stats
                UNION
                SELECT username FROM ctw_stats
                UNION
                SELECT username FROM ww_stats
            ) ORDER BY username
        ''')
        return [row[0] for row in cursor.fetchall()]


def get_user_stats(username: str) -> Dict[str, Dict[str, float]]:
    """Get all stats for a specific user across all tables.
    
    Args:
        username: Username to query
        
    Returns:
        Dict mapping stat_name to dict of period values
        Example: {"kills": {"lifetime": 100, "session": 5, "daily": 10, ...}, ...}
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        stats = {}
        tables = ['general_stats', 'sheep_stats', 'ctw_stats', 'ww_stats']
        
        for table in tables:
            cursor.execute(f'''
                SELECT stat_name, lifetime, session, daily, yesterday, weekly, monthly
                FROM {table}
                WHERE username = ?
            ''', (username,))
            
            for row in cursor.fetchall():
                stats[row[0]] = {
                    'lifetime': row[1] or 0,
                    'session': row[2] or 0,
                    'daily': row[3] or 0,
                    'yesterday': row[4] or 0,
                    'weekly': row[5] or 0,
                    'monthly': row[6] or 0
                }
        
        return stats


def get_user_meta(username: str) -> Optional[Dict]:
    """Get user metadata (level, icon, colors, etc).
    
    Args:
        username: Username to query
        
    Returns:
        Dict with metadata or None if not found
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT level, icon, ign_color, guild_tag, guild_hex, rank
            FROM user_meta
            WHERE LOWER(username) = LOWER(?)
        ''', (username,))
        row = cursor.fetchone()
        
        if row:
            return {
                'level': row[0],
                'icon': row[1],
                'ign_color': row[2],
                'guild_tag': row[3],
                'guild_hex': row[4],
                'rank': row[5]
            }
        return None


def get_all_user_meta() -> Dict[str, Dict]:
    """Get metadata for all users.
    
    Returns:
        Dict mapping username to metadata
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT username, level, icon, ign_color, guild_tag, guild_hex, rank FROM user_meta')
        return {
            row['username']: {
                'level': row['level'],
                'icon': row['icon'],
                'ign_color': row['ign_color'],
                'guild_tag': row['guild_tag'],
                'guild_hex': row['guild_hex'],
                'rank': row['rank']
            }
            for row in cursor.fetchall()
        }


def update_user_stats(username: str, stats: Dict[str, float], 
                     snapshot_sections: Optional[Set[str]] = None,
                     new_stat_categories: Optional[Set[str]] = None):
    """Update user stats with new API data.
    
    This function:
    1. Updates lifetime values with current API data
    2. Optionally takes snapshots for specified periods
    3. Calculates deltas (current - snapshot) for all periods
    4. Normalizes username casing to prevent duplicates
    5. For NEW stat categories (CTW/WW), sets ALL snapshots to lifetime value on first update
    
    Args:
        username: Username to update (will be normalized to existing casing if found)
        stats: Dict mapping stat_name to current lifetime value
        snapshot_sections: Set of periods to snapshot ("session", "daily", "monthly")
        new_stat_categories: Set of stat categories that are new ("ctw", "ww") - these will
                           have snapshots set to lifetime value to make initial deltas = lifetime
    """
    snapshot_sections = snapshot_sections or set()
    new_stat_categories = new_stat_categories or set()
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Check if user already exists (case-insensitive) and get proper casing
        cursor.execute('''
            SELECT DISTINCT username FROM (
                SELECT username FROM general_stats
                UNION
                SELECT username FROM sheep_stats
                UNION
                SELECT username FROM ctw_stats
                UNION
                SELECT username FROM ww_stats
            ) WHERE LOWER(username) = LOWER(?) LIMIT 1
        ''', (username,))
        existing_user = cursor.fetchone()
        if existing_user:
            # Use existing casing to prevent duplicates
            username = existing_user[0]
        
        for stat_name, lifetime_value in stats.items():
            # Determine which table this stat belongs to
            table = get_stat_table(stat_name)
            
            # Determine if this is a "new" stat category
            is_new_category = False
            if table == 'ctw_stats' and 'ctw' in new_stat_categories:
                is_new_category = True
            elif table == 'ww_stats' and 'ww' in new_stat_categories:
                is_new_category = True
            
            # Get existing record or create new one
            cursor.execute(f'''
                SELECT session, daily, yesterday, weekly, monthly
                FROM {table}
                WHERE username = ? AND stat_name = ?
            ''', (username, stat_name))
            
            existing = cursor.fetchone()
            if existing:
                # Existing stat - keep current snapshots unless we're updating them
                session_snap = existing[0] if existing[0] is not None else lifetime_value
                daily_snap = existing[1] if existing[1] is not None else lifetime_value
                yesterday_snap = existing[2] if existing[2] is not None else lifetime_value
                weekly_snap = existing[3] if existing[3] is not None else lifetime_value
                monthly_snap = existing[4] if existing[4] is not None else lifetime_value
            else:
                # New stat - decide how to initialize snapshots
                if is_new_category:
                    # For NEW categories (CTW/WW on first API call), set all snapshots to lifetime
                    # This makes deltas = lifetime (showing all progress since start)
                    session_snap = lifetime_value
                    daily_snap = lifetime_value
                    yesterday_snap = lifetime_value
                    weekly_snap = lifetime_value
                    monthly_snap = lifetime_value
                else:
                    # For existing categories (sheep wars), initialize to lifetime
                    # This makes initial deltas = 0, which is correct for a new stat
                    session_snap = lifetime_value
                    daily_snap = lifetime_value
                    yesterday_snap = lifetime_value
                    weekly_snap = lifetime_value
                    monthly_snap = lifetime_value
            
            # Update snapshots if explicitly requested
            if "session" in snapshot_sections:
                session_snap = lifetime_value
            if "daily" in snapshot_sections:
                daily_snap = lifetime_value
            if "yesterday" in snapshot_sections:
                yesterday_snap = lifetime_value
            if "weekly" in snapshot_sections:
                weekly_snap = lifetime_value
            if "monthly" in snapshot_sections:
                monthly_snap = lifetime_value
            
            # Insert or update - store SNAPSHOTS, not deltas
            # Deltas are calculated on read
            cursor.execute(f'''
                INSERT OR REPLACE INTO {table}
                (username, stat_name, lifetime, session, daily, yesterday, weekly, monthly)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (username, stat_name, lifetime_value, 
                  session_snap, daily_snap, yesterday_snap, weekly_snap, monthly_snap))
        
        conn.commit()


def get_user_stats_with_deltas(username: str) -> Dict[str, Dict[str, float]]:
    """Get user stats with calculated deltas across all tables.
    
    This returns the format expected by the bot:
    - lifetime: current value from API
    - session/daily/yesterday/monthly: calculated deltas (lifetime - snapshot)
    
    Args:
        username: Username to query
        
    Returns:
        Dict mapping stat_name to dict with lifetime and delta values
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        stats = {}
        tables = ['general_stats', 'sheep_stats', 'ctw_stats', 'ww_stats']
        
        for table in tables:
            cursor.execute(f'''
                SELECT stat_name, lifetime, session, daily, yesterday, weekly, monthly
                FROM {table}
                WHERE username = ?
            ''', (username,))
            
            for row in cursor.fetchall():
                stat_name = row[0]
                lifetime = row[1] or 0
                session_snap = row[2] or 0
                daily_snap = row[3] or 0
                yesterday_snap = row[4] or 0
                weekly_snap = row[5] or 0
                monthly_snap = row[6] or 0
                
                stats[stat_name] = {
                    'lifetime': lifetime,
                    'session': lifetime - session_snap,
                    'daily': lifetime - daily_snap,
                    'yesterday': lifetime - yesterday_snap,
                    'weekly': lifetime - weekly_snap,
                    'monthly': lifetime - monthly_snap
                }
        
        return stats


def update_user_meta(username: str, level: Optional[int] = None, icon: Optional[str] = None,
                    ign_color: Optional[str] = None,
                    guild_tag: Optional[str] = None,
                    guild_hex: Optional[str] = None,
                    rank: Optional[str] = None):
    """Update user metadata.
    
    None values are ignored (existing values preserved).
    To clear a text value (color/guild), pass an empty string "".
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Check if user exists
        cursor.execute('SELECT username FROM user_meta WHERE LOWER(username) = LOWER(?)', (username,))
        row = cursor.fetchone()
        
        if row:
            # Update existing record - only update fields that are not None
            target_username = row['username']
            updates = []
            params = []
            
            if level is not None:
                updates.append("level = ?")
                params.append(level)
            if icon is not None:
                updates.append("icon = ?")
                params.append(icon)
            if ign_color is not None:
                updates.append("ign_color = ?")
                params.append(ign_color if ign_color != "" else None)
            if guild_tag is not None:
                updates.append("guild_tag = ?")
                val = guild_tag if guild_tag != "" else None
                params.append(str(val) if val is not None else None)
            if guild_hex is not None:
                updates.append("guild_hex = ?")
                params.append(guild_hex if guild_hex != "" else None)
            if rank is not None:
                updates.append("rank = ?")
                params.append(rank)
            
            if updates:
                params.append(target_username)
                sql = f"UPDATE user_meta SET {', '.join(updates)} WHERE username = ?"
                cursor.execute(sql, params)
        else:
            # Insert new record
            cursor.execute('''
                INSERT INTO user_meta 
                (username, level, icon, ign_color, guild_tag, guild_hex, rank)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                username, 
                level if level is not None else 0, 
                icon if icon is not None else '', 
                ign_color if ign_color != "" else None, 
                str(guild_tag) if guild_tag and guild_tag != "" else None, 
                guild_hex if guild_hex != "" else None,
                rank
            ))
        
        conn.commit()


def rotate_daily_to_yesterday(usernames: List[str]) -> Dict[str, bool]:
    """Copy daily snapshot to yesterday snapshot for specified users.
    
    This is called before the daily refresh to preserve yesterday's stats.
    
    Args:
        usernames: List of usernames to rotate
        
    Returns:
        Dict mapping username to success status
    """
    results = {}
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        tables = ['general_stats', 'sheep_stats', 'ctw_stats', 'ww_stats']
        
        for username in usernames:
            try:
                # Copy daily column to yesterday column for all stats in all tables
                for table in tables:
                    cursor.execute(f'''
                        UPDATE {table}
                        SET yesterday = daily
                        WHERE username = ?
                    ''', (username,))
                
                results[username] = True
            except Exception as e:
                print(f"[ERROR] Failed to rotate {username}: {e}")
                results[username] = False
        
        conn.commit()
    
    return results


def reset_weekly_snapshots(usernames: List[str]) -> Dict[str, bool]:
    """Reset weekly snapshot to current lifetime values for specified users.
    
    This is called every Monday at 9:30 AM EST to reset the weekly tracking period.
    
    Args:
        usernames: List of usernames to reset
        
    Returns:
        Dict mapping username to success status
    """
    results = {}
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        tables = ['general_stats', 'sheep_stats', 'ctw_stats', 'ww_stats']
        
        for username in usernames:
            try:
                # Set weekly snapshot to current lifetime value for all stats in all tables
                for table in tables:
                    cursor.execute(f'''
                        UPDATE {table}
                        SET weekly = lifetime
                        WHERE username = ?
                    ''', (username,))
                
                results[username] = True
            except Exception as e:
                print(f"[ERROR] Failed to reset weekly for {username}: {e}")
                results[username] = False
        
        conn.commit()
    
    return results


def user_exists(username: str) -> bool:
    """Check if a user exists in the database.
    
    Args:
        username: Username to check
        
    Returns:
        True if user has any stats, False otherwise
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        # Check all stat tables
        for table in ['general_stats', 'sheep_stats', 'ctw_stats', 'ww_stats']:
            cursor.execute(f'''
                SELECT COUNT(*) FROM {table} WHERE LOWER(username) = LOWER(?)
            ''', (username,))
            count = cursor.fetchone()[0]
            if count > 0:
                return True
        return False


def delete_user(username: str):
    """Delete all data for a user.
    
    Args:
        username: Username to delete
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        tables = ['general_stats', 'sheep_stats', 'ctw_stats', 'ww_stats']
        for table in tables:
            cursor.execute(f'DELETE FROM {table} WHERE LOWER(username) = LOWER(?)', (username,))
        cursor.execute('DELETE FROM user_meta WHERE LOWER(username) = LOWER(?)', (username,))
        conn.commit()


def get_database_stats() -> Dict:
    """Get database statistics.
    
    Returns:
        Dict with database statistics
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Get unique usernames across all stat tables
        cursor.execute('''
            SELECT COUNT(DISTINCT username) FROM (
                SELECT username FROM general_stats
                UNION
                SELECT username FROM sheep_stats
                UNION
                SELECT username FROM ctw_stats
                UNION
                SELECT username FROM ww_stats
            )
        ''')
        user_count = cursor.fetchone()[0]
        
        # Get total stat count
        total_stats = 0
        for table in ['general_stats', 'sheep_stats', 'ctw_stats', 'ww_stats']:
            cursor.execute(f'SELECT COUNT(*) FROM {table}')
            total_stats += cursor.fetchone()[0]
        
        return {
            'users': user_count,
            'total_stats': total_stats,
            'db_file': str(DB_FILE),
            'exists': DB_FILE.exists()
        }


def backup_database(backup_path: Path) -> bool:
    """Create a backup copy of the database.
    
    Args:
        backup_path: Destination path for backup
        
    Returns:
        True if successful, False otherwise
    """
    try:
        import shutil
        shutil.copy2(DB_FILE, backup_path)
        return True
    except Exception as e:
        print(f"[ERROR] Database backup failed: {e}")
        return False


# ============================================================================
# User Links Functions (username <-> Discord ID mappings)
# ============================================================================

def get_discord_id(username: str) -> Optional[str]:
    """Get Discord ID for a username.
    
    Args:
        username: Minecraft username
        
    Returns:
        Discord ID or None if not found
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT discord_id FROM user_links WHERE LOWER(username) = LOWER(?)', (username,))
        row = cursor.fetchone()
        return row['discord_id'] if row else None


def set_discord_link(username: str, discord_id: str):
    """Link a username to a Discord ID.
    
    Args:
        username: Minecraft username
        discord_id: Discord user ID
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO user_links (username, discord_id)
            VALUES (?, ?)
            ON CONFLICT(username) DO UPDATE SET discord_id = excluded.discord_id
        ''', (username.lower(), discord_id))
        conn.commit()


def get_all_user_links() -> Dict[str, str]:
    """Get all username -> Discord ID mappings.
    
    Returns:
        Dictionary mapping usernames to Discord IDs
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT username, discord_id FROM user_links')
        return {row['username']: row['discord_id'] for row in cursor.fetchall()}


# ============================================================================
# Default Users Functions (Discord ID -> default username mappings)
# ============================================================================

def get_default_username(discord_id: str) -> Optional[str]:
    """Get default username for a Discord ID.
    
    Args:
        discord_id: Discord user ID
        
    Returns:
        Username or None if not found
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT username FROM default_users WHERE discord_id = ?', (discord_id,))
        row = cursor.fetchone()
        return row['username'] if row else None


def set_default_username(discord_id: str, username: str):
    """Set default username for a Discord ID.
    
    Args:
        discord_id: Discord user ID
        username: Minecraft username
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO default_users (discord_id, username)
            VALUES (?, ?)
            ON CONFLICT(discord_id) DO UPDATE SET username = excluded.username
        ''', (discord_id, username))
        conn.commit()


def get_all_default_users() -> Dict[str, str]:
    """Get all Discord ID -> username mappings.
    
    Returns:
        Dictionary mapping Discord IDs to usernames
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT discord_id, username FROM default_users')
        return {row['discord_id']: row['username'] for row in cursor.fetchall()}


# ============================================================================
# Tracked Streaks Functions
# ============================================================================

def get_tracked_streaks(username: str) -> Optional[Dict]:
    """Get streak tracking data for a username.
    
    Args:
        username: Minecraft username
        
    Returns:
        Dictionary with streak data or None if not found
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT winstreak, killstreak, last_wins, last_losses, last_kills, last_deaths
            FROM tracked_streaks WHERE username = ?
        ''', (username,))
        row = cursor.fetchone()
        if row:
            return {
                'winstreak': row['winstreak'],
                'killstreak': row['killstreak'],
                'last_wins': row['last_wins'],
                'last_losses': row['last_losses'],
                'last_kills': row['last_kills'],
                'last_deaths': row['last_deaths']
            }
        return None


def update_tracked_streaks(username: str, streak_data: Dict):
    """Update streak tracking data for a username.
    
    Args:
        username: Minecraft username
        streak_data: Dictionary with streak data
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO tracked_streaks 
            (username, winstreak, killstreak, last_wins, last_losses, last_kills, last_deaths)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(username) DO UPDATE SET
                winstreak = excluded.winstreak,
                killstreak = excluded.killstreak,
                last_wins = excluded.last_wins,
                last_losses = excluded.last_losses,
                last_kills = excluded.last_kills,
                last_deaths = excluded.last_deaths
        ''', (
            username,
            streak_data.get('winstreak', 0),
            streak_data.get('killstreak', 0),
            streak_data.get('last_wins', 0),
            streak_data.get('last_losses', 0),
            streak_data.get('last_kills', 0),
            streak_data.get('last_deaths', 0)
        ))
        conn.commit()


def get_all_tracked_streaks() -> Dict[str, Dict]:
    """Get all tracked streaks.
    
    Returns:
        Dictionary mapping usernames to streak data
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT username, winstreak, killstreak, last_wins, last_losses, last_kills, last_deaths
            FROM tracked_streaks
        ''')
        result = {}
        for row in cursor.fetchall():
            result[row['username']] = {
                'winstreak': row['winstreak'],
                'killstreak': row['killstreak'],
                'last_wins': row['last_wins'],
                'last_losses': row['last_losses'],
                'last_kills': row['last_kills'],
                'last_deaths': row['last_deaths']
            }
        return result


# ============================================================================
# Tracked Users Functions
# ============================================================================

def add_tracked_user(username: str) -> bool:
    """Add a username to tracked users (actively tracked with periodic updates).
    
    Args:
        username: Minecraft username to track
        
    Returns:
        bool: True if user was added, False if already tracked
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        # Check if user already exists (case-insensitive)
        cursor.execute('SELECT username, is_tracked FROM tracked_users WHERE LOWER(username) = LOWER(?)', (username,))
        existing = cursor.fetchone()
        
        if existing:
            # If user exists but is not actively tracked, update them to tracked
            if existing['is_tracked'] == 0:
                cursor.execute('UPDATE tracked_users SET is_tracked = 1 WHERE LOWER(username) = LOWER(?)', (username,))
                conn.commit()
                return True
            # Already actively tracked
            return False
        
        # Add the user with the provided casing and is_tracked=1
        cursor.execute('INSERT INTO tracked_users (username, is_tracked) VALUES (?, 1)', (username,))
        conn.commit()
        return cursor.rowcount > 0


def remove_tracked_user(username: str):
    """Remove a username from tracked users (stops periodic updates but keeps them registered).
    
    Args:
        username: Minecraft username to stop tracking
        
    Returns:
        bool: True if user was updated, False if not found
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        # Instead of deleting, set is_tracked to 0 to keep them registered
        cursor.execute('UPDATE tracked_users SET is_tracked = 0 WHERE LOWER(username) = LOWER(?)', (username,))
        conn.commit()
        return cursor.rowcount > 0


def is_tracked_user(username: str) -> bool:
    """Check if a username is being actively tracked (gets periodic updates).
    
    This function is UUID-aware and will resolve old usernames.
    
    Args:
        username: Minecraft username (current or old)
        
    Returns:
        True if user is actively tracked, False otherwise
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # First, resolve username to UUID to get current username
        resolved = resolve_username_to_uuid(username)
        if resolved:
            uuid, current_username = resolved
            # Check if current username is tracked
            cursor.execute('SELECT is_tracked FROM tracked_users WHERE LOWER(username) = LOWER(?)', (current_username,))
            row = cursor.fetchone()
            return row is not None and row['is_tracked'] == 1
        
        # Not in UUID system, check username directly
        cursor.execute('SELECT is_tracked FROM tracked_users WHERE LOWER(username) = LOWER(?)', (username,))
        row = cursor.fetchone()
        return row is not None and row['is_tracked'] == 1


def get_tracked_users() -> List[str]:
    """Get list of all actively tracked usernames (only those receiving periodic updates).
    
    Returns:
        List of actively tracked usernames
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT username FROM tracked_users WHERE is_tracked = 1 ORDER BY username')
        return [row['username'] for row in cursor.fetchall()]


def set_tracked_users(usernames: List[str]):
    """Replace all actively tracked users with new list (keeps registered users).
    
    Args:
        usernames: List of usernames to track
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        # Set all users to is_tracked=0 first
        cursor.execute('UPDATE tracked_users SET is_tracked = 0')
        # Then set the specified users to is_tracked=1
        for username in usernames:
            cursor.execute('''
                INSERT INTO tracked_users (username, is_tracked) VALUES (?, 1)
                ON CONFLICT(username) DO UPDATE SET is_tracked = 1
            ''', (username,))
        conn.commit()


def register_user(username: str):
    """Register a user in the database without actively tracking them.
    
    This allows the user to appear in leaderboards with accurate positioning,
    but they won't receive periodic stat updates.
    
    Args:
        username: Minecraft username to register
        
    Returns:
        bool: True if user was registered, False if already exists
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        # Check if user already exists (case-insensitive)
        cursor.execute('SELECT username FROM tracked_users WHERE LOWER(username) = LOWER(?)', (username,))
        if cursor.fetchone():
            return False
        
        # Add the user with is_tracked=0 (registered but not actively tracked)
        cursor.execute('INSERT INTO tracked_users (username, is_tracked) VALUES (?, 0)', (username,))
        conn.commit()
        return cursor.rowcount > 0


def is_registered_user(username: str) -> bool:
    """Check if a username is registered (tracked or not).
    
    Args:
        username: Minecraft username
        
    Returns:
        True if user is registered (either tracked or just registered), False otherwise
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM tracked_users WHERE LOWER(username) = LOWER(?)', (username,))
        return cursor.fetchone() is not None


# ============================================================================
# UUID Management Functions
# ============================================================================

def store_uuid(username: str, uuid: str):
    """Store or update UUID for a username in user_meta and uuid_history.
    
    This function:
    1. Updates the UUID in user_meta for the current username
    2. Records this username-UUID pair in uuid_history with current timestamp
    3. Handles case-insensitive username lookups
    
    Args:
        username: Current Minecraft username (will be stored as-is)
        uuid: Player's Mojang UUID (without dashes)
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Update UUID in user_meta (case-insensitive check)
        cursor.execute('SELECT username FROM user_meta WHERE LOWER(username) = LOWER(?)', (username,))
        row = cursor.fetchone()
        
        if row:
            # Update existing user's UUID
            actual_username = row['username']
            cursor.execute('UPDATE user_meta SET uuid = ? WHERE username = ?', (uuid, actual_username))
        else:
            # Create new user_meta entry with UUID
            cursor.execute('INSERT INTO user_meta (username, uuid) VALUES (?, ?)', (username, uuid))
        
        # Record in UUID history (updates last_seen if pair exists)
        cursor.execute('''
            INSERT INTO uuid_history (uuid, username, last_seen)
            VALUES (?, ?, strftime('%s', 'now'))
            ON CONFLICT(uuid, username) DO UPDATE SET last_seen = strftime('%s', 'now')
        ''', (uuid, username))
        
        # Update UUID in tracked_users if they're tracked
        cursor.execute('SELECT username FROM tracked_users WHERE LOWER(username) = LOWER(?)', (username,))
        tracked_row = cursor.fetchone()
        if tracked_row:
            actual_tracked_username = tracked_row['username']
            cursor.execute('UPDATE tracked_users SET uuid = ? WHERE username = ?', (uuid, actual_tracked_username))
        
        conn.commit()


def get_uuid_for_username(username: str) -> Optional[str]:
    """Get stored UUID for a username (case-insensitive).
    
    Args:
        username: Minecraft username
        
    Returns:
        UUID string or None if not found
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT uuid FROM user_meta WHERE LOWER(username) = LOWER(?)', (username,))
        row = cursor.fetchone()
        return row['uuid'] if row and row['uuid'] else None


def get_current_username_for_uuid(uuid: str) -> Optional[str]:
    """Get the most recent username associated with a UUID.
    
    Args:
        uuid: Player's Mojang UUID
        
    Returns:
        Most recent username or None if UUID not found
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT username FROM uuid_history 
            WHERE uuid = ? 
            ORDER BY last_seen DESC 
            LIMIT 1
        ''', (uuid,))
        row = cursor.fetchone()
        return row['username'] if row else None


def resolve_username_to_uuid(username: str) -> Optional[tuple[str, str]]:
    """Resolve a username to UUID, checking if it's an old username.
    
    This function:
    1. First checks if we have a UUID for this exact username
    2. If not found, checks uuid_history to see if this was an old username
    3. Returns the UUID and the current username for that UUID
    
    Args:
        username: Username to resolve (might be current or old)
        
    Returns:
        tuple of (uuid, current_username) or None if username not found in system
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # First try to get UUID from user_meta (current username)
        cursor.execute('SELECT username, uuid FROM user_meta WHERE LOWER(username) = LOWER(?)', (username,))
        row = cursor.fetchone()
        if row and row['uuid']:
            # Found current username with UUID - return actual username from DB (preserves casing)
            return (row['uuid'], row['username'])
        
        # Not found as current username, check uuid_history for old username
        cursor.execute('SELECT uuid FROM uuid_history WHERE LOWER(username) = LOWER(?)', (username,))
        history_row = cursor.fetchone()
        if history_row and history_row['uuid']:
            uuid = history_row['uuid']
            # Get the current username for this UUID from user_meta
            cursor.execute('SELECT username FROM user_meta WHERE uuid = ?', (uuid,))
            meta_row = cursor.fetchone()
            if meta_row:
                return (uuid, meta_row['username'])
            else:
                # UUID exists in history but not in user_meta, get from history
                current = get_current_username_for_uuid(uuid)
                if current:
                    return (uuid, current)
                else:
                    # UUID exists but no current username mapping, use the one we found
                    return (uuid, username)
        
        # Not found anywhere
        return None


def update_username_for_uuid(uuid: str, new_username: str):
    """Update all database references when a player changes their username.
    
    This function:
    1. Updates username in all stat tables
    2. Updates username in user_meta, user_links, tracked_users, tracked_streaks
    3. Preserves all stats and history
    4. Records the new username in uuid_history
    
    Args:
        uuid: Player's UUID
        new_username: New username to update to
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Find old username
        cursor.execute('SELECT username FROM user_meta WHERE uuid = ?', (uuid,))
        row = cursor.fetchone()
        if not row:
            # UUID not in system yet, just store it
            store_uuid(new_username, uuid)
            return
        
        old_username = row['username']
        
        # If username hasn't changed, just update history timestamp
        if old_username.lower() == new_username.lower():
            store_uuid(new_username, uuid)
            return
        
        print(f"[UUID] Updating username {old_username} -> {new_username} (UUID: {uuid})")
        
        # Check for duplicates in each table and handle them
        # For stat tables: if both usernames exist, we need to merge or delete duplicates
        for table in ['general_stats', 'sheep_stats', 'ctw_stats', 'ww_stats']:
            # Check if new_username already has stats
            cursor.execute(f'SELECT COUNT(*) FROM {table} WHERE LOWER(username) = LOWER(?)', (new_username,))
            new_count = cursor.fetchone()[0]
            
            if new_count > 0:
                # New username exists, delete old username entries (duplicates)
                print(f"[UUID] Removing duplicate entries in {table} for old username '{old_username}'")
                cursor.execute(f'DELETE FROM {table} WHERE LOWER(username) = LOWER(?)', (old_username,))
            else:
                # New username doesn't exist, safe to rename
                cursor.execute(f'UPDATE {table} SET username = ? WHERE LOWER(username) = LOWER(?)', 
                              (new_username, old_username))
        
        # Handle user_meta: check for duplicate
        cursor.execute('SELECT username FROM user_meta WHERE LOWER(username) = LOWER(?)', (new_username,))
        new_meta = cursor.fetchone()
        if new_meta:
            # New username exists, delete old entry
            print(f"[UUID] Removing duplicate user_meta entry for old username '{old_username}'")
            cursor.execute('DELETE FROM user_meta WHERE LOWER(username) = LOWER(?)', (old_username,))
        else:
            # Safe to rename
            cursor.execute('UPDATE user_meta SET username = ? WHERE LOWER(username) = LOWER(?)', 
                          (new_username, old_username))
        
        # Update user_links
        cursor.execute('UPDATE user_links SET username = ? WHERE username = ?', 
                      (new_username, old_username))
        
        # Update default_users
        cursor.execute('UPDATE default_users SET username = ? WHERE username = ?', 
                      (new_username, old_username))
        
        # Update tracked_streaks
        cursor.execute('UPDATE tracked_streaks SET username = ? WHERE username = ?', 
                      (new_username, old_username))
        
        # Handle tracked_users: check if new username already exists
        cursor.execute('SELECT username, is_tracked FROM tracked_users WHERE LOWER(username) = LOWER(?)', (new_username,))
        new_entry = cursor.fetchone()
        cursor.execute('SELECT username, is_tracked FROM tracked_users WHERE LOWER(username) = LOWER(?)', (old_username,))
        old_entry = cursor.fetchone()
        
        if new_entry and old_entry:
            # Both exist - delete the old one, keep the new one
            print(f"[UUID] Removing duplicate tracked_users entry for old username '{old_username}'")
            cursor.execute('DELETE FROM tracked_users WHERE LOWER(username) = LOWER(?)', (old_username,))
        elif old_entry:
            # Only old exists - rename it
            cursor.execute('UPDATE tracked_users SET username = ? WHERE username = ?', 
                          (new_username, old_username))
        # If only new exists or neither exists, nothing to do
        
        # Record new username in history
        cursor.execute('''
            INSERT INTO uuid_history (uuid, username, last_seen)
            VALUES (?, ?, strftime('%s', 'now'))
            ON CONFLICT(uuid, username) DO UPDATE SET last_seen = strftime('%s', 'now')
        ''', (uuid, new_username))
        
        conn.commit()


def get_all_usernames_for_uuid(uuid: str) -> List[tuple[str, int]]:
    """Get all known usernames for a UUID with last seen timestamps.
    
    Args:
        uuid: Player's UUID
        
    Returns:
        List of (username, last_seen_timestamp) tuples, newest first
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT username, last_seen FROM uuid_history 
            WHERE uuid = ? 
            ORDER BY last_seen DESC
        ''', (uuid,))
        return [(row['username'], row['last_seen']) for row in cursor.fetchall()]


def store_hotbar_layouts(username: str, layouts: List[Dict[str, any]], db_path: Optional[Path] = None):
    """Store hotbar layouts for a user. Overwrites existing data for the same username.
    
    Args:
        username: Player's username
        layouts: List of layout dictionaries with keys:
                 game, kit, slot_0, slot_1, ..., slot_8
        db_path: Optional custom path to database file
    """
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()
        
        # Delete existing layouts for this user
        cursor.execute('DELETE FROM hotbar_layouts WHERE username = ?', (username,))
        
        # Insert new layouts
        for layout in layouts:
            cursor.execute('''
                INSERT INTO hotbar_layouts 
                (username, game, kit, slot_0, slot_1, slot_2, slot_3, slot_4, slot_5, slot_6, slot_7, slot_8, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%s', 'now'))
            ''', (
                username,
                layout.get('game'),
                layout.get('kit'),
                layout.get('slot_0'),
                layout.get('slot_1'),
                layout.get('slot_2'),
                layout.get('slot_3'),
                layout.get('slot_4'),
                layout.get('slot_5'),
                layout.get('slot_6'),
                layout.get('slot_7'),
                layout.get('slot_8')
            ))
        
        conn.commit()
        print(f"[DB] Stored {len(layouts)} hotbar layout(s) for {username}")


def get_hotbar_layouts(username: str, game: Optional[str] = None, db_path: Optional[Path] = None) -> List[Dict[str, any]]:
    """Get hotbar layouts for a user.
    
    Args:
        username: Player's username
        game: Optional game filter (e.g., 'sheep_wars', 'capture_the_wool', 'wool_wars')
        db_path: Optional custom path to database file
        
    Returns:
        List of layout dictionaries
    """
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()
        
        if game:
            cursor.execute('''
                SELECT game, kit, slot_0, slot_1, slot_2, slot_3, slot_4, slot_5, slot_6, slot_7, slot_8, last_updated
                FROM hotbar_layouts
                WHERE username = ? AND game = ?
                ORDER BY game, kit
            ''', (username, game))
        else:
            cursor.execute('''
                SELECT game, kit, slot_0, slot_1, slot_2, slot_3, slot_4, slot_5, slot_6, slot_7, slot_8, last_updated
                FROM hotbar_layouts
                WHERE username = ?
                ORDER BY game, kit
            ''', (username,))
        
        return [dict(row) for row in cursor.fetchall()]

