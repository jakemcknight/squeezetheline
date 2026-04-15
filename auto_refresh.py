"""
Daily auto-refresh script — runs at 10am ET via GitHub Actions.

Pulls today's data, generates Strong Over / Strong Under picks, flags
the top 5 of each, and saves everything to Supabase.

Usage:
    python auto_refresh.py
"""

import sys
import datetime
import traceback

from auto_picks import generate_and_save_picks


def main():
    try:
        n = generate_and_save_picks(datetime.date.today())
        print(f"OK — saved {n} picks.")
        sys.exit(0)
    except Exception as e:
        print(f"FAILED: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
