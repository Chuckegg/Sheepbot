# UUID Tracking System Documentation

## Overview

The UUID tracking system has been implemented to handle Minecraft username changes gracefully. Since Mojang UUIDs never change (even when players change their username), we can use them to track players reliably.

## Problem Solved

**Before:** If a tracked player changed their username (e.g., DaJJay → Jyyroh), the system would:
- Fail to fetch their stats (username not found)
- Break leaderboards and tracking
- Lose historical data
- Require manual intervention

**After:** When a username change is detected:
- System automatically detects the change via UUID lookup
- All database references are updated to the new username
- Historical data is preserved
- Old username is recorded in history
- Everything continues working seamlessly

## How It Works

### 1. UUID Storage

When a player's stats are fetched from the Hypixel API:

```python
# api_get.py automatically:
1. Resolves username → UUID (checks both Mojang and PlayerDB APIs)
2. Stores UUID in user_meta table
3. Records username-UUID pair in uuid_history table with timestamp
4. Updates UUID in tracked_users if applicable
```

This happens automatically during normal API fetches - no manual intervention needed.

### 2. Username Change Detection

When an API fetch is called with any username:

```python
# The system:
1. Checks if we have a UUID for this username in our database
2. If found, uses that UUID to verify current username with Mojang
3. If username differs, triggers automatic migration
4. If not found, does fresh UUID lookup and stores it
```

### 3. Automatic Migration

When a username change is detected:

```python
update_username_for_uuid(uuid, new_username) performs:
1. Updates all stat tables (general_stats, sheep_stats, ctw_stats, ww_stats)
2. Updates user_meta (preserves rank, guild, colors, etc.)
3. Updates user_links (Discord ID mappings)
4. Updates default_users (default username for Discord users)
5. Updates tracked_streaks (win/kill streaks)
6. Updates tracked_users (tracking list)
7. Records new username in uuid_history
```

All stats, colors, settings, and history are preserved!

## Database Schema Changes

### New Tables

**uuid_history** - Tracks all usernames associated with each UUID
```sql
CREATE TABLE uuid_history (
    uuid TEXT NOT NULL,              -- Player's Mojang UUID
    username TEXT NOT NULL,          -- Username they had
    last_seen INTEGER,               -- Timestamp when this username was last seen
    PRIMARY KEY (uuid, username)
)
```

### Modified Tables

**user_meta** - Added UUID column
```sql
ALTER TABLE user_meta ADD COLUMN uuid TEXT DEFAULT NULL
```

**tracked_users** - Added UUID column
```sql
ALTER TABLE tracked_users ADD COLUMN uuid TEXT DEFAULT NULL
```

## Key Functions

### In db_helper.py

```python
store_uuid(username: str, uuid: str)
# Stores/updates UUID for a username in user_meta and uuid_history

get_uuid_for_username(username: str) -> Optional[str]
# Gets stored UUID for a username (case-insensitive)

resolve_username_to_uuid(username: str) -> Optional[tuple[str, str]]
# Resolves username to (uuid, current_username)
# Works with both current and old usernames

update_username_for_uuid(uuid: str, new_username: str)
# Migrates all database references to new username

get_current_username_for_uuid(uuid: str) -> Optional[str]
# Gets the most recent username for a UUID

get_all_usernames_for_uuid(uuid: str) -> List[tuple[str, int]]
# Gets all known usernames for a UUID with timestamps
```

### In api_get.py

The `api_update_database()` function now:
1. Attempts to resolve input username to UUID
2. Verifies current username with Mojang API
3. Stores/updates UUID automatically
4. Detects and handles username changes
5. Updates all database references if username changed

## Gradual Rollout

The system implements a **gradual rollout** approach:

1. **No immediate changes** - Existing players continue working as before
2. **UUIDs added on fetch** - When a player's stats are fetched, their UUID is stored
3. **History builds over time** - Username history accumulates with each fetch
4. **Automatic migration** - Username changes are detected and handled automatically

Players **without** UUIDs yet will get them next time they're fetched. No manual migration needed!

## Usage Examples

### Basic API Fetch (Automatic UUID Storage)
```bash
# This now automatically stores UUID
python api_get.py -ign DaJJay

# Output will show:
# [API] Fetching data for DaJJay (UUID: abc123...)
# [UUID] Stored UUID for DaJJay
```

### Handling Username Change
```bash
# Player changed from DaJJay to Jyyroh
# Using old username still works:
python api_get.py -ign DaJJay

# Output:
# [UUID] Resolved 'DaJJay' to UUID abc123... (current username: Jyyroh)
# [UUID] Detected username change: DaJJay -> Jyyroh
# [UUID] Updating username DaJJay -> Jyyroh (UUID: abc123...)
# [API] Fetching data for Jyyroh (UUID: abc123...)
# ... stats updated successfully ...
```

### Testing UUID System
```bash
# Test UUID tracking for a player
python test_uuid_tracking.py DaJJay

# This will:
# 1. Fetch UUID from Mojang
# 2. Store in database
# 3. Test resolution
# 4. Show username history
```

## Discord Bot Integration

The Discord bot automatically benefits from UUID tracking:

1. **Commands work with old usernames** - `/stats DaJJay` works even if they changed to Jyyroh
2. **Tracked users update seamlessly** - Periodic updates handle username changes
3. **Leaderboards stay accurate** - Stats follow the player, not the old username
4. **Links preserved** - Discord ID mappings update to new username

## Migration Safety

The system is designed to be **safe and non-destructive**:

- ✅ All existing functionality continues working
- ✅ No data is deleted or lost
- ✅ Username changes are logged in history
- ✅ Case-insensitive username matching preserved
- ✅ Backwards compatible with non-UUID data
- ✅ Automatic fallback if UUID lookup fails

## Troubleshooting

### "Username not found" error after username change

**If old username doesn't work:**
```bash
# Use new username for first fetch after change:
python api_get.py -ign NewUsername

# After this, both old and new work (UUID system active)
```

### Check UUID status for a player

```bash
python test_uuid_tracking.py --skip-fetch Username
```

### View username history in database

```sql
sqlite3 stats.db
SELECT * FROM uuid_history WHERE uuid = 'player_uuid';
```

### Check if player has UUID stored

```sql
sqlite3 stats.db
SELECT username, uuid FROM user_meta WHERE username = 'PlayerName';
```

## Technical Details

### UUID Format
- UUIDs are stored **without dashes** (32 character hex string)
- Example: `abc123def456...` not `abc123de-f456-...`
- This matches Mojang and Hypixel API format

### API Fallback Chain
When resolving username to UUID:
1. Check database for stored UUID
2. Try Mojang API (https://api.mojang.com/users/profiles/minecraft/{username})
3. Try PlayerDB fallback (https://playerdb.co/api/player/minecraft/{username})

### Case Sensitivity
- All username comparisons use `LOWER()` for case-insensitive matching
- Stored usernames preserve original casing from Mojang API
- UUID lookups are case-insensitive

### Performance
- UUID lookups use indexed columns (fast)
- Migration happens only when username change detected
- No performance impact on existing queries
- Minimal overhead during API fetches

## Future Enhancements

Possible future improvements:
1. Bulk UUID backfill script for existing tracked users
2. UUID-based duplicate detection
3. Username change notifications in Discord
4. Historical username viewer command
5. API endpoint to query by UUID directly

## Testing

Run the test script to verify UUID system:

```bash
# Test with any Minecraft username
python test_uuid_tracking.py Technoblade
python test_uuid_tracking.py DreamWasTaken
python test_uuid_tracking.py YourUsername

# Test without API fetch (database only)
python test_uuid_tracking.py ExistingPlayer --skip-fetch
```

## Summary

The UUID tracking system provides:
- ✅ Automatic username change handling
- ✅ Preserved stats and history
- ✅ Seamless user experience
- ✅ No manual intervention required
- ✅ Gradual rollout (non-disruptive)
- ✅ Backwards compatible

Players can change their Minecraft username freely without breaking your tracking system!
