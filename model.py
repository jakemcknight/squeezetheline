"""
XGBoost model for NBA player stat prediction.

Trained on our 320k historical box scores (2014-2026). Features are
simple but sturdy: who's the player, who's the opponent, home/away,
rest context, and the player's recent rolling averages up to the
game's date. Target is the stat value that player posted in that game.

Usage:
    # Train once (writes data/model_<stat>.json + data/feature_map.pkl)
    python -m model train points rebounds assists

    # At prediction time
    from model import predict_player_stat
    predict_player_stat("LeBron James", "points", opponent="BOS", home=True, rest_days=2)
"""

import os
import sys
import json
import pickle
import datetime
from typing import Optional

import numpy as np
import pandas as pd

from data import DATA_DIR, load_historical_data

MODEL_DIR = os.path.join(DATA_DIR, "models")
os.makedirs(MODEL_DIR, exist_ok=True)

STAT_COLUMNS = ("points", "rebounds", "assists", "pra", "threes", "steals", "blocks")


def _prep_dataframe() -> pd.DataFrame:
    """Load historical data and build the feature-ready dataframe."""
    df = load_historical_data()
    if df.empty:
        raise RuntimeError("No historical data — run backfill first.")

    df = df.rename(columns={
        "player": "name", "team_code": "team", "opponent_code": "opponent",
        "pts": "points", "reb": "rebounds", "ast": "assists", "min": "minutes",
        "threefm": "threes", "stl": "steals", "blk": "blocks",
    })
    for c in ("points", "rebounds", "assists", "minutes", "threes", "steals", "blocks"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["pra"] = df["points"] + df["rebounds"] + df["assists"]
    df["gameday"] = pd.to_datetime(df.get("game_gameday", df.get("date_string")), errors="coerce")

    # Home/away from 'game_loc' if available
    if "game_loc" in df.columns:
        loc = df["game_loc"].astype(str).str.lower().str.strip()
        df["is_home"] = loc.isin({"h", "home"}).astype(int)
    else:
        df["is_home"] = 0

    df = df.dropna(subset=["gameday", "name", "team", "opponent"])
    df = df[df["minutes"] > 0]
    df = df.sort_values(["name", "gameday"])
    return df


def _add_rolling_features(df: pd.DataFrame, stat: str) -> pd.DataFrame:
    """For each player, compute trailing 5/10/25-game averages as features.
    Uses shift(1) so we never leak the current game's stat into its own features."""
    grp = df.groupby("name")[stat]
    df[f"{stat}_avg_5"] = grp.transform(lambda s: s.shift(1).rolling(5, min_periods=1).mean())
    df[f"{stat}_avg_10"] = grp.transform(lambda s: s.shift(1).rolling(10, min_periods=1).mean())
    df[f"{stat}_avg_25"] = grp.transform(lambda s: s.shift(1).rolling(25, min_periods=1).mean())
    # Trailing minutes as a proxy for "how much is he playing"
    mins = df.groupby("name")["minutes"]
    df["min_avg_10"] = mins.transform(lambda s: s.shift(1).rolling(10, min_periods=1).mean())
    df["rest_days"] = df.groupby("name")["gameday"].transform(
        lambda s: (s - s.shift(1)).dt.days.fillna(3).clip(0, 10)
    )
    return df


def train_stat_model(stat: str) -> dict:
    """Train an XGBoost regressor for `stat` and save it. Returns metrics."""
    from xgboost import XGBRegressor
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import mean_absolute_error, r2_score

    print(f"Training model for {stat}...")
    df = _prep_dataframe()
    df = _add_rolling_features(df, stat)
    df = df.dropna(subset=[f"{stat}_avg_10"])  # need at least one prior game

    # Encode categoricals (player, team, opponent) to integer codes
    cat_maps = {}
    for col in ("name", "team", "opponent"):
        cats = pd.Categorical(df[col])
        df[f"{col}_idx"] = cats.codes
        cat_maps[col] = dict(zip(cats.categories, range(len(cats.categories))))

    feature_cols = [
        "name_idx", "team_idx", "opponent_idx",
        "is_home", "rest_days", "min_avg_10",
        f"{stat}_avg_5", f"{stat}_avg_10", f"{stat}_avg_25",
    ]
    X = df[feature_cols].fillna(0).values
    y = df[stat].values

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = XGBRegressor(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        n_jobs=-1,
        tree_method="hist",
    )
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    mae = float(mean_absolute_error(y_test, preds))
    r2 = float(r2_score(y_test, preds))
    print(f"  MAE: {mae:.2f}  R²: {r2:.3f}")

    # Save model + categorical maps
    model.save_model(os.path.join(MODEL_DIR, f"{stat}.json"))
    with open(os.path.join(MODEL_DIR, f"{stat}_maps.pkl"), "wb") as f:
        pickle.dump({"cat_maps": cat_maps, "feature_cols": feature_cols}, f)
    with open(os.path.join(MODEL_DIR, f"{stat}_metrics.json"), "w") as f:
        json.dump({"mae": mae, "r2": r2, "n_rows": len(df), "trained_at": datetime.datetime.now().isoformat()}, f)
    return {"mae": mae, "r2": r2, "n_rows": len(df)}


def load_model(stat: str):
    """Load a previously-trained model (or return None if missing)."""
    model_path = os.path.join(MODEL_DIR, f"{stat}.json")
    maps_path = os.path.join(MODEL_DIR, f"{stat}_maps.pkl")
    if not os.path.exists(model_path) or not os.path.exists(maps_path):
        return None
    try:
        from xgboost import XGBRegressor
        model = XGBRegressor()
        model.load_model(model_path)
        with open(maps_path, "rb") as f:
            maps = pickle.load(f)
        return {"model": model, "maps": maps}
    except Exception as e:
        print(f"Failed to load model for {stat}: {e}")
        return None


def predict_player_stat(
    player: str, stat: str, opponent: str, team: str,
    home: bool = True, rest_days: int = 2,
    recent_averages: Optional[dict] = None,
) -> Optional[float]:
    """Use the trained model to predict a player's stat for tonight.

    recent_averages: {"avg_5": X, "avg_10": Y, "avg_25": Z, "min_avg_10": W}
    """
    loaded = load_model(stat)
    if loaded is None:
        return None
    model = loaded["model"]
    maps = loaded["maps"]
    cat_maps = maps["cat_maps"]
    feature_cols = maps["feature_cols"]

    recent_averages = recent_averages or {}
    row = {
        "name_idx": cat_maps["name"].get(player, -1),
        "team_idx": cat_maps["team"].get(team, -1),
        "opponent_idx": cat_maps["opponent"].get(opponent, -1),
        "is_home": int(home),
        "rest_days": rest_days,
        "min_avg_10": recent_averages.get("min_avg_10", 28.0),
        f"{stat}_avg_5": recent_averages.get("avg_5", 0.0),
        f"{stat}_avg_10": recent_averages.get("avg_10", 0.0),
        f"{stat}_avg_25": recent_averages.get("avg_25", 0.0),
    }
    if row["name_idx"] < 0:
        return None  # unseen player
    X = np.array([[row[c] for c in feature_cols]], dtype=float)
    pred = float(model.predict(X)[0])
    return pred


def get_model_metrics(stat: str) -> Optional[dict]:
    """Return the metrics saved at training time, if available."""
    path = os.path.join(MODEL_DIR, f"{stat}_metrics.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


if __name__ == "__main__":
    # Usage: python -m model train points rebounds assists
    if len(sys.argv) > 1 and sys.argv[1] == "train":
        stats = sys.argv[2:] or list(STAT_COLUMNS)
        for s in stats:
            try:
                m = train_stat_model(s)
                print(f"{s}: {m}")
            except Exception as e:
                print(f"{s} failed: {e}")
