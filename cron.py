"""Automated cron / digest / auto-picks pipeline, triggered via webhook.

A free cron service like cron-job.org pings the app on a schedule:
  https://squeezetheline.streamlit.app/?refresh_token=YOUR_SECRET
If the token matches, we run the daily jobs and stop — never rendering the full
app or requiring auth. Runs from Streamlit Cloud's IP so NBA.com doesn't block
it.

``handle_cron_webhook`` is called at the very top of ``app.py`` (before the auth
gate). It returns ``True`` if it handled the request (in which case the caller
has already had ``st.stop`` invoked), so in practice control never returns when
a refresh token is present.
"""

import os

import streamlit as st


def _run_pipeline(report: dict):
    """Runs the full daily pipeline. Writes per-step status into `report`."""
    import datetime as _dt
    import traceback as _tb
    try:
        from auto_picks import generate_and_save_picks, grade_pending_picks
        from scrapers.odds_api import get_all_props
        from data import prepare_props, load_historical_data
        from prop_history import snapshot_props, grade_props, snapshot_line_movement
        from backfill import backfill

        today = _dt.date.today()
        report["date"] = str(today)
        print(f"[webhook] Starting daily pipeline for {today}...")

        # Config sanity
        srv_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or st.secrets.get("SUPABASE_SERVICE_ROLE_KEY", "")
        report["service_role_key_present"] = bool(srv_key)
        if not srv_key:
            report["error"] = "SUPABASE_SERVICE_ROLE_KEY missing — nothing can be written to Supabase."
            return

        # 1. Generate auto picks
        try:
            n_picks = generate_and_save_picks(today)
            report["picks_saved"] = n_picks
            print(f"[webhook] Generated {n_picks} auto picks.")
        except Exception as e:
            report["picks_error"] = f"{type(e).__name__}: {e}"
            print(f"[webhook] Picks failed: {e}"); _tb.print_exc()

        # 2. Snapshot + line movement
        try:
            raw_props = get_all_props(today)
            tidy = prepare_props(raw_props)
            report["props_fetched"] = len(tidy)
            n_snap = snapshot_props(today, tidy)
            n_move = snapshot_line_movement(today, tidy)
            report["snapshots"] = n_snap
            report["line_movement_rows"] = n_move
            print(f"[webhook] Snapshotted {n_snap} props / {n_move} movement rows.")
        except Exception as e:
            report["snapshot_error"] = f"{type(e).__name__}: {e}"
            print(f"[webhook] Snapshot failed: {e}"); _tb.print_exc()

        # 3. Backfill + grade (with diagnostics)
        try:
            # Capture backfill stdout so silent failures show up in the report
            from io import StringIO
            from contextlib import redirect_stdout
            buf = StringIO()
            try:
                with redirect_stdout(buf):
                    backfill()
            finally:
                out = buf.getvalue()
                # Just keep the last ~2KB so the report stays readable
                report["backfill_log_tail"] = out[-2000:] if out else ""
            hist = load_historical_data()
            if not hist.empty:
                report["historical_rows"] = len(hist)
                # What's the most recent date in our box-score data?
                if "date_string" in hist.columns:
                    try:
                        latest = hist["date_string"].dropna().max()
                        report["historical_latest_date"] = str(latest)
                    except Exception:
                        pass
            else:
                report["historical_rows"] = 0

            # Look at the pending picks: how many, what dates, do those
            # dates exist in historical data?
            from auto_runner import _supabase_admin
            sb_diag = _supabase_admin()
            if sb_diag:
                pend = sb_diag.table("auto_picks").select("date,player").eq("result", "pending").lte("date", str(today)).limit(500).execute()
                pending_rows = pend.data or []
                report["pending_picks_count"] = len(pending_rows)
                if pending_rows:
                    pending_dates = sorted(set(r["date"] for r in pending_rows))
                    report["pending_pick_dates"] = pending_dates[:10]
                    # Of those dates, how many are present in historical data?
                    if not hist.empty and "date_string" in hist.columns:
                        hist_dates = set(hist["date_string"].dropna().astype(str).unique())
                        missing = [d for d in pending_dates if d not in hist_dates]
                        report["pending_dates_missing_from_history"] = missing[:10]
                    # Sample one pending pick + does its (player, date) exist in history?
                    sample = pending_rows[0]
                    report["sample_pending_pick"] = sample
                    if not hist.empty:
                        hist_renamed = hist.rename(columns={"player": "name"}) if "player" in hist.columns else hist
                        match = hist_renamed[
                            (hist_renamed.get("name", hist_renamed.get("player")) == sample["player"])
                            & (hist_renamed["date_string"].astype(str) == sample["date"])
                        ]
                        report["sample_pending_pick_found_in_history"] = len(match) > 0

            n_graded = grade_pending_picks(today)
            n_props = grade_props(hist, today)
            report["picks_graded"] = n_graded
            report["props_graded"] = n_props
            print(f"[webhook] Graded {n_graded} picks / {n_props} props.")
        except Exception as e:
            report["grade_error"] = f"{type(e).__name__}: {e}"
            print(f"[webhook] Grade failed: {e}"); _tb.print_exc()

        # 4. Digest
        try:
            from digest import send_daily_digest
            res = send_daily_digest(today)
            report["digest_picks"] = res.get("picks", 0)
            report["digest_email_sent"] = res.get("email_sent", False)
            if res.get("skipped_reason"):
                report["digest_skipped_reason"] = res["skipped_reason"]
        except Exception as e:
            report["digest_error"] = f"{type(e).__name__}: {e}"

        # 5. Priority alerts (line movement + high-EV)
        try:
            from alerts import send_priority_alerts
            alert_res = send_priority_alerts(today)
            report["alerts_sent"] = alert_res.get("sent", False)
            report["alerts_moves"] = alert_res.get("moves", 0)
            report["alerts_high_ev"] = alert_res.get("high_ev", 0)
        except Exception as e:
            report["alerts_error"] = f"{type(e).__name__}: {e}"

        report["status"] = "ok"
        print("[webhook] Pipeline complete.")
    except Exception as e:
        report["fatal_error"] = f"{type(e).__name__}: {e}"
        _tb.print_exc()


def handle_cron_webhook() -> bool:
    """Inspect the request for a ``refresh_token`` query param and, if it matches
    the configured secret, run the daily pipeline and ``st.stop``.

    Returns ``True`` if the request was a webhook (handled), else ``False`` so
    the caller continues rendering the normal app. When ``True`` is returned the
    function has already called ``st.stop``, so control does not actually return.
    """
    _webhook_token = st.query_params.get("refresh_token")
    if not _webhook_token:
        return False

    _expected_token = ""
    try:
        _expected_token = st.secrets.get("REFRESH_TOKEN", "")
    except Exception:
        pass
    if not _expected_token:
        _expected_token = os.environ.get("REFRESH_TOKEN", "")

    if _expected_token and _webhook_token == _expected_token:
        import threading

        debug_mode = st.query_params.get("debug") in ("1", "true", "yes")

        if debug_mode:
            # Run synchronously and show the full report on-screen
            report = {}
            with st.spinner("Running pipeline synchronously (debug mode)..."):
                _run_pipeline(report)
            st.markdown("**Squeeze the Line — debug webhook**")
            st.json(report)
            st.stop()
        else:
            # Fire-and-forget background thread for scheduled cron pings
            report = {}
            thread = threading.Thread(target=_run_pipeline, args=(report,), daemon=True)
            thread.start()
            st.markdown("**Squeeze the Line — webhook triggered**")
            st.success(
                "Jobs started in the background. "
                "Add `&debug=1` to the URL to run synchronously and see the full report here."
            )
            st.stop()
    else:
        st.error("Invalid refresh token.")
        st.stop()
    return True
