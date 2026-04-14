import os

import pandas as pd
from unidecode import unidecode

from config import DATA_DIR

# --- File paths ---

HISTORICAL_DATA_PATH = os.path.join(DATA_DIR, "historical_data.csv")
HISTORICAL_DATA_GZ_PATH = os.path.join(DATA_DIR, "historical_data.csv.gz")
HISTORICAL_PROPS_PATH = os.path.join(DATA_DIR, "historical_props.csv")
STATS_PATH = os.path.join(DATA_DIR, "stats.csv")
SKIP_DATES_PATH = os.path.join(DATA_DIR, "skip_dates.csv")

# --- Name normalization ---

# Basketball Reference uses different name formats than NatStat.
# This map translates Basketball Reference names -> NatStat names.
NAME_REPLACEMENTS = {
    " Jr.": "",
    " Sr.": "",
    "Herbert Jones": "Herb Jones",
    "Vince Williams": "Vincent Williams",
    "Nicolas Claxton": "Nic Claxton",
    "Cui Yongxi": "Yongxi Cui",
    "E.J. Liddell": "EJ Liddell",
    "Dereck Lively II": "Dereck Lively",
    "P.J. Washington": "PJ Washington",
    "Tim Hardaway": "Tim Hardaway Jr",
    "Ron Holland": "Ronald Holland",
    "Lindy Waters III": "Lindy Waters",
    "Gary Payton II": "Gary Payton",
    "T.J. McConnell": "TJ McConnell",
    "Jeenathan Williams": "Nate Williams",
    "A.J. Green": "AJ Green",
    "Terrence Shannon ": "Terrence Shannon",
    "CJ McCollum": "C.J. McCollum",
    "Jae'Sean Tate": "Jae'sean Tate",
    "Ricky Council IV": "Ricky Council",
    "D.J. Steward": "DJ Steward",
    "A.J. Lawson": "AJ Lawson",
    "Marvin Bagley III": "Marvin Bagley",
    "Trey Murphy III": "Trey Murphy",
    "Tristan Da Silva": "Tristan da Silva",
}


def normalize_names(df: pd.DataFrame, column: str = "name") -> pd.DataFrame:
    """Apply unidecode and known name replacements to a name column."""
    df[column] = df[column].apply(lambda x: unidecode(str(x)))
    for old, new in NAME_REPLACEMENTS.items():
        df[column] = df[column].str.replace(old, new, regex=False)
    return df


# --- CSV loading / saving ---

def load_historical_data() -> pd.DataFrame:
    """Load the historical player performance data.

    Prefers the uncompressed .csv (written by backfill), falls back to the
    .csv.gz snapshot that's committed to the repo for fresh deploys.
    """
    if os.path.exists(HISTORICAL_DATA_PATH):
        return pd.read_csv(HISTORICAL_DATA_PATH, low_memory=False)
    if os.path.exists(HISTORICAL_DATA_GZ_PATH):
        return pd.read_csv(HISTORICAL_DATA_GZ_PATH, low_memory=False, compression="gzip")
    return pd.DataFrame()


def save_historical_data(df: pd.DataFrame):
    """Save historical data locally (uncompressed). The .csv.gz in the repo
    is the deploy snapshot — refresh it manually when needed."""
    df.to_csv(HISTORICAL_DATA_PATH, index=False)


def load_historical_props() -> pd.DataFrame:
    if os.path.exists(HISTORICAL_PROPS_PATH):
        return pd.read_csv(HISTORICAL_PROPS_PATH)
    return pd.DataFrame()


def save_historical_props(df: pd.DataFrame):
    df.to_csv(HISTORICAL_PROPS_PATH, index=False)


def load_skip_dates() -> list[str]:
    if os.path.exists(SKIP_DATES_PATH):
        sd = pd.read_csv(SKIP_DATES_PATH)
        return list(sd["dates"].unique())
    return []


def save_skip_dates(dates: list[str]):
    df = pd.DataFrame({"dates": dates})
    df.to_csv(SKIP_DATES_PATH, index=False)


def save_stats(df: pd.DataFrame):
    df.to_csv(STATS_PATH, index=False)


# --- Data transformations ---

def prepare_player_data(players: pd.DataFrame, positions: pd.DataFrame) -> pd.DataFrame:
    """Merge players with positions, normalize names, drop missing positions."""
    players["name"] = players["name"].apply(lambda x: unidecode(x))
    positions = normalize_names(positions)
    players = players.merge(positions, how="left")
    players = players[~players["position"].isna()]
    return players


def prepare_stats(stats: pd.DataFrame, positions: pd.DataFrame) -> pd.DataFrame:
    """Merge stats with positions, sort, rank, and filter zero-minute games.

    Expects stats to already have: name, team-code, gameday, minutes, points,
    rebounds, assists (as produced by scrapers.nba.get_current_season_stats).
    """
    positions = normalize_names(positions.copy())
    df = stats.merge(positions, on="name", how="left")
    df = df[~df["position"].isna()]
    df = df.sort_values("gameday", ascending=False)
    df = df[df["minutes"] != 0]
    df["rank"] = df.groupby("name")["gameday"].rank(method="dense", ascending=False)
    return df


def prepare_props(props: pd.DataFrame) -> pd.DataFrame:
    """Rename columns and deduplicate to one line per player per prop type."""
    props = props.rename(columns={"player": "name"})
    props["spread"] = props["spread"].astype(float)
    group_cols = ["type", "name"]
    if "team" in props.columns:
        group_cols.append("team")
    props = props.groupby(group_cols)["spread"].max().reset_index()
    return props


def pivot_props(props: pd.DataFrame) -> pd.DataFrame:
    """Pivot props so each prop type becomes its own column."""
    return props.pivot(index=["name"], columns="type", values="spread").reset_index()
