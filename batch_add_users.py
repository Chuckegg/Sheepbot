#!/usr/bin/env python3
"""
Batch process users from a text file with rate limiting.

This script:
1. Reads the first N usernames from a text file
2. Processes them directly (fetches stats and adds to database)
3. Removes processed usernames from the file
4. Shows when to run the script again (after 5 minutes)

Usage:
    python batch_add_users.py
    python batch_add_users.py --txt my_users.txt --limit 100
    python batch_add_users.py -t users.txt -l 50
"""

import sys
import subprocess
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import List

from db_helper import init_database, register_user, is_registered_user, is_tracked_user


def fetch_user_stats(username: str) -> bool:
    """Fetch user stats using api_get.py with the temporary API key.
    
    Args:
        username: Minecraft username to fetch
        
    Returns:
        True if successful, False otherwise
    """
    try:
        result = subprocess.run(
            ['python3', 'api_get.py', '-ign', username, '--use-temp-key'],
            capture_output=True,
            text=True,
            timeout=30
        )
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print(f"  ⚠️  Timeout fetching stats for {username}")
        return False
    except Exception as e:
        print(f"  ⚠️  Error fetching stats for {username}: {e}")
        return False


def process_users(usernames: List[str], skip_existing: bool = True) -> tuple[dict, List[str]]:
    """Process a list of usernames and add them to the database.
    
    Args:
        usernames: List of usernames to process
        skip_existing: If True, skip users already registered
        
    Returns:
        Tuple of (statistics dict, list of processed usernames)
    """
    stats = {
        'total': len(usernames),
        'added': 0,
        'skipped': 0,
        'failed': 0,
        'already_tracked': 0
    }
    
    processed = []  # Track which users were actually processed
    
    print(f"\n📋 Processing {stats['total']} user(s)...\n")
    
    for i, username in enumerate(usernames, 1):
        print(f"[{i}/{stats['total']}] Processing '{username}'...")
        
        # Check if user is already registered
        if skip_existing and is_registered_user(username):
            if is_tracked_user(username):
                print(f"  ⏭️  Already tracked (skipping)")
                stats['already_tracked'] += 1
            else:
                print(f"  ⏭️  Already registered (skipping)")
                stats['skipped'] += 1
            continue
        
        # Fetch stats from API
        print(f"  🔄 Fetching stats from API...")
        if not fetch_user_stats(username):
            print(f"  ❌ Failed to fetch stats")
            stats['failed'] += 1
            continue
        
        processed.append(username)  # Mark as processed even if already in DB
        
        # Register user as not tracked
        print(f"  📝 Registering in database...")
        if register_user(username):
            print(f"  ✅ Successfully registered (not tracked)")
            stats['added'] += 1
        else:
            # User might have been added during the fetch
            if not is_tracked_user(username):
                print(f"  ✅ Successfully added to database (not tracked)")
                stats['added'] += 1
            else:
                print(f"  ⚠️  User was already tracked")
                stats['already_tracked'] += 1
    
    return stats, processed


def print_summary(stats: dict):
    """Print a summary of the operation."""
    print("\n" + "="*60)
    print("📊 SUMMARY")
    print("="*60)
    print(f"Total users processed:    {stats['total']}")
    print(f"✅ Successfully added:     {stats['added']}")
    print(f"⏭️  Already registered:     {stats['skipped']}")
    print(f"🔒 Already tracked:        {stats['already_tracked']}")
    print(f"❌ Failed:                 {stats['failed']}")
    print("="*60)
    
    if stats['added'] > 0:
        print("\n💡 Tip: These users are now in the database for leaderboards")
        print("   but won't receive periodic stat updates.")
        print("   Use /track to start tracking them if needed.")


def main():
    parser = argparse.ArgumentParser(
        description='Batch process users from text file with rate limiting',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python batch_add_users.py                          # Process 200 users from more_users.txt
  python batch_add_users.py --limit 100              # Process 100 users
  python batch_add_users.py -t users.txt -l 50       # Process 50 users from users.txt
        """
    )
    parser.add_argument(
        '-t', '--txt',
        type=str,
        default='more_users.txt',
        help='Text file containing usernames (default: more_users.txt)'
    )
    parser.add_argument(
        '-l', '--limit',
        type=int,
        default=200,
        help='Number of users to process in this batch (default: 200)'
    )
    
    args = parser.parse_args()
    
    input_file = Path(args.txt)
    
    # Check if input file exists
    if not input_file.exists():
        print(f"❌ Error: '{input_file}' not found")
        sys.exit(1)
    
    # Initialize database
    print(f"🔧 Initializing database...")
    init_database()
    
    # Read all usernames
    print(f"📖 Reading usernames from '{input_file}'...")
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            all_usernames = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        sys.exit(1)
    
    if not all_usernames:
        print(f"✅ No usernames to process in '{input_file}'")
        sys.exit(0)
    
    # Get batch to process
    batch_size = min(args.limit, len(all_usernames))
    batch_usernames = all_usernames[:batch_size]
    remaining_usernames = all_usernames[batch_size:]
    
    print(f"📋 Found {len(all_usernames)} total usernames")
    print(f"🔄 Processing {batch_size} usernames in this batch")
    print(f"📝 {len(remaining_usernames)} usernames will remain for next run")
    
    # Process the batch
    try:
        stats, processed = process_users(batch_usernames, skip_existing=True)
        print_summary(stats)
        
        # Update input file with remaining usernames
        print(f"\n🔄 Updating '{input_file}'...")
        try:
            with open(input_file, 'w', encoding='utf-8') as f:
                for username in remaining_usernames:
                    f.write(f"{username}\n")
            print(f"✅ Removed {batch_size} processed usernames from '{input_file}'")
        except Exception as e:
            print(f"❌ Error updating file: {e}")
            sys.exit(1)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
    
    # Show summary and next run time
    print("\n" + "="*60)
    print(f"📝 Remaining in '{input_file}': {len(remaining_usernames)}")
    
    if remaining_usernames:
        # Calculate next run time (5 minutes from now)
        next_run = datetime.now() + timedelta(minutes=5)
        print("\n" + "="*60)
        print("⏰ RATE LIMIT REMINDER")
        print("="*60)
        print(f"⏳ Wait 5 minutes before running again to avoid API rate limits")
        print(f"\n🕐 Current time:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏰ Run again at:  {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
        print("\n💡 Next command:")
        cmd_parts = ["   python batch_add_users.py"]
        if args.txt != 'more_users.txt':
            cmd_parts.append(f"--txt {args.txt}")
        if args.limit != 200:
            cmd_parts.append(f"--limit {args.limit}")
        print(" ".join(cmd_parts))
        print("="*60)
    else:
        print("\n🎉 All usernames processed! No more users in '{input_file}'")
        print("="*60)
    
    # Exit with appropriate code
    if stats['failed'] == stats['total']:
        sys.exit(1)  # All failed
    elif stats['failed'] > 0:
        sys.exit(2)  # Some failed
    else:
        sys.exit(0)  # All succeeded


if __name__ == "__main__":
    main()
