#!/usr/bin/env python3
"""
Batch add guilds to the database without tracking them.

This script takes a list of guild names and:
1. Fetches their stats using api_update_guild_database
2. Registers them in the database without active tracking (is_tracked=0)

This allows the guilds to appear in leaderboards with accurate positions
without receiving periodic stat updates.

Usage:
    python add_guilds_to_db.py <filename>
    python add_guilds_to_db.py guilds_to_add.txt
"""

import sys
import argparse
from pathlib import Path
from typing import List

from db_helper import init_database, register_guild, is_registered_guild, is_tracked_guild
from api_get import api_update_guild_database, read_api_key_file


def read_guild_list(filename: str) -> List[str]:
    """Read guild names from a file (one per line).
    
    Args:
        filename: Path to text file with guild names
        
    Returns:
        List of guild names
    """
    file_path = Path(filename)
    
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {filename}")
    
    with open(file_path, 'r') as f:
        lines = f.readlines()
    
    # Strip whitespace and filter empty lines
    guild_names = [line.strip() for line in lines if line.strip()]
    
    return guild_names


def add_guilds_to_database(guild_names: List[str], api_key: str) -> dict:
    """Add guilds to database by fetching their stats.
    
    Args:
        guild_names: List of guild names to add
        api_key: Hypixel API key
        
    Returns:
        Dict with results: {guild_name: (success, already_existed, message)}
    """
    results = {}
    
    print(f"\n[INFO] Processing {len(guild_names)} guilds...")
    print("="*60)
    
    for idx, guild_name in enumerate(guild_names, 1):
        print(f"\n[{idx}/{len(guild_names)}] Processing: {guild_name}")
        
        # Check if already tracked
        if is_tracked_guild(guild_name):
            print(f"  ⏭️  Already tracked - skipping API call")
            results[guild_name] = (True, True, "Already tracked")
            continue
        
        # Check if registered but not tracked
        if is_registered_guild(guild_name):
            print(f"  🔄 Already registered - updating stats")
        
        # Fetch and update guild data
        try:
            result = api_update_guild_database(guild_name, api_key)
            
            if 'error' in result:
                print(f"  ❌ Failed: {result['error']}")
                results[guild_name] = (False, False, result['error'])
            else:
                tag = result.get('guild_tag', '')
                color = result.get('guild_tag_color', '')
                print(f"  ✅ Success - [{tag}] ({color})")
                results[guild_name] = (True, False, f"Added with tag [{tag}]")
        
        except Exception as e:
            error_msg = str(e)
            print(f"  ❌ Error: {error_msg[:50]}")
            results[guild_name] = (False, False, error_msg[:50])
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Batch add guilds to database (auto-registered with is_tracked=0)"
    )
    parser.add_argument(
        "file",
        help="Text file with guild names (one per line)"
    )
    parser.add_argument(
        "--use-temp-key",
        action="store_true",
        help="Use API_KEY_TEMP.txt instead of API_KEY.txt"
    )
    
    args = parser.parse_args()
    
    # Initialize database
    init_database()
    
    # Get API key
    api_key = read_api_key_file(use_temp=args.use_temp_key)
    if not api_key:
        key_file = "API_KEY_TEMP.txt" if args.use_temp_key else "API_KEY.txt"
        print(f"❌ [ERROR] Missing API key: {key_file}")
        sys.exit(1)
    
    # Read guild list
    try:
        guild_names = read_guild_list(args.file)
    except FileNotFoundError as e:
        print(f"❌ [ERROR] {e}")
        sys.exit(1)
    
    if not guild_names:
        print("❌ [ERROR] No guild names found in file")
        sys.exit(1)
    
    # Process guilds
    results = add_guilds_to_database(guild_names, api_key)
    
    # Print summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    successful = sum(1 for success, _, _ in results.values() if success)
    failed = sum(1 for success, _, _ in results.values() if not success)
    already_existed = sum(1 for _, existed, _ in results.values() if existed)
    
    print(f"✅ Successful: {successful}/{len(guild_names)}")
    print(f"⏭️  Already tracked: {already_existed}")
    print(f"❌ Failed: {failed}")
    
    if failed > 0:
        print("\nFailed guilds:")
        for guild_name, (success, _, msg) in results.items():
            if not success:
                print(f"  • {guild_name}: {msg}")
    
    print("="*60)


if __name__ == "__main__":
    main()
