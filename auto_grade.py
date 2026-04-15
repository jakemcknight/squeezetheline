"""
Daily grading script — runs at 2am ET via GitHub Actions.

Backfills yesterday's box scores, then grades every pending auto pick
against the actual stats and updates Supabase.

Usage:
    python auto_grade.py
"""

import sys
import datetime
import traceback

from auto_picks import grade_pending_picks


def main():
    try:
        # Make sure we have yesterday's box scores
        from backfill import backfill
        # Backfill is fast (one call per season, only updates new dates)
        backfill()
        n = grade_pending_picks(datetime.date.today())
        print(f"OK — graded {n} picks.")
        sys.exit(0)
    except Exception as e:
        print(f"FAILED: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
