"""One-time data processing pipeline for TFT match data.

Reads raw CSV inputs, performs structural normalization and light enrichment,
and writes canonical analysis-ready CSV tables.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Sequence, Union

import pandas as pd

DEFAULT_DELIMITER = ";"
DEFAULT_EXPECTED_PARTICIPANTS = 8
DEFAULT_PROCESSED_BASE_DIR = Path("data/processed")


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #
def _ensure_columns(df: pd.DataFrame, required: Iterable[str], label: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in {label}: {missing}")


def validate_participant_counts(
    participants: pd.DataFrame,
    expected_per_match: int = DEFAULT_EXPECTED_PARTICIPANTS,
    strict: bool = False,
) -> dict[str, int]:
    """
    Check each match has the expected participant count.

    Returns {match_id: observed_count} for mismatches. Raises when strict=True.
    """
    counts = participants.groupby("match_id")["puuid"].nunique()
    bad = counts[counts != expected_per_match]
    if bad.empty:
        return {}
    details = bad.to_dict()
    msg = (
        f"Participant count mismatch for matches: {details} "
        f"(expected {expected_per_match})"
    )
    if strict:
        raise ValueError(msg)
    return details


# --------------------------------------------------------------------------- #
# Parsing helpers
# --------------------------------------------------------------------------- #
def parse_delimited_column(
    series: pd.Series,
    delimiter: str = DEFAULT_DELIMITER,
    drop_empty: bool = True,
) -> pd.Series:
    """Split delimiter-separated strings into lists."""

    def _split(value: object) -> List[str]:
        if pd.isna(value) or value == "":
            return [] if drop_empty else [value]
        parts = [part.strip() for part in str(value).split(delimiter)]
        return [p for p in parts if p] if drop_empty else parts

    return series.apply(_split)


def normalize_delimited_column(
    df: pd.DataFrame, column: str, delimiter: str = DEFAULT_DELIMITER
) -> pd.DataFrame:
    """Ensure a column is consistently delimiter-joined (no dup delimiters/whitespace)."""
    if column not in df.columns:
        return df
    out = df.copy()
    out[column] = (
        parse_delimited_column(out[column], delimiter=delimiter)
        .apply(lambda parts: delimiter.join(parts))
    )
    return out


def split_delimited_to_columns(
    df: pd.DataFrame,
    column: str,
    prefix: str,
    delimiter: str = DEFAULT_DELIMITER,
    max_parts: int | None = None,
) -> pd.DataFrame:
    """Add one column per delimiter-split value: prefix_0, prefix_1, ..."""
    if column not in df.columns:
        return df

    list_series = parse_delimited_column(df[column], delimiter=delimiter)
    inferred_max = list_series.map(len).max() if not list_series.empty else 0
    max_len = max_parts if max_parts is not None else inferred_max

    out = df.copy()
    for i in range(max_len):
        out[f"{prefix}_{i}"] = list_series.apply(
            lambda lst, idx=i: lst[idx] if idx < len(lst) else pd.NA
        )
    return out


def explode_delimited_column(
    df: pd.DataFrame,
    column: str,
    delimiter: str = DEFAULT_DELIMITER,
    value_name: str | None = None,
) -> pd.DataFrame:
    """Explode a delimiter-separated column into multiple rows."""
    value_name = value_name or column
    exploded = df.copy()
    exploded[column] = parse_delimited_column(exploded[column], delimiter=delimiter)
    exploded = exploded.explode(column)
    exploded = exploded.dropna(subset=[column])
    return exploded.rename(columns={column: value_name})


# --------------------------------------------------------------------------- #
# Core transformations
# --------------------------------------------------------------------------- #
def add_participant_flags(
    participants: pd.DataFrame, win_threshold: int = 4
) -> pd.DataFrame:
    """Add derived flags like is_win (placement <= win_threshold)."""
    _ensure_columns(participants, ["placement"], "participants")
    out = participants.copy()
    out["is_win"] = out["placement"] <= win_threshold
    return out


def resolve_processed_output_dir(
    base_dir: Union[str, Path],
    label: str = "canonical_original",
) -> Path:
    """
    Resolve a stable, non-overwriting output directory for processed data.

    Example:
    data/processed/canonical_original/
    """
    base_dir = Path(base_dir)
    out_dir = base_dir / label
    if out_dir.exists():
        raise FileExistsError(
            f"Processed directory already exists: {out_dir}. "
            "Delete it or choose a different label."
        )
    out_dir.mkdir(parents=True, exist_ok=False)
    return out_dir


def build_canonical_tables(
    participants_path: Union[str, Path],
    units_path: Union[str, Path],
    traits_path: Union[str, Path],
    processed_base_dir: Union[str, Path] = DEFAULT_PROCESSED_BASE_DIR,
    delimiter: str = DEFAULT_DELIMITER,
    label: str = "canonical_original",
) -> Path:
    """
    Build canonical CSV tables from raw match exports.

    Outputs are written under data/processed/ by default.
    The directory is created once and must not already exist.

    Outputs:
    - participants.csv
    - units.csv (fixed item slots: item_0, item_1, item_2; nullable)
    - traits.csv
    """

    processed_base_dir = Path(processed_base_dir)
    processed_base_dir.mkdir(parents=True, exist_ok=True)

    output_dir = resolve_processed_output_dir(processed_base_dir, label=label)

    participants = pd.read_csv(participants_path)
    units = pd.read_csv(units_path)
    traits = pd.read_csv(traits_path)

    _ensure_columns(
        participants, ["match_id", "puuid", "placement", "level"], "participants"
    )
    _ensure_columns(
        units, ["match_id", "puuid", "unit_id", "star_level", "items"], "units"
    )
    _ensure_columns(
        traits, ["match_id", "puuid", "trait_id", "tier_current"], "traits"
    )

    # Participant-level enrichment
    participants = add_participant_flags(participants)

    # Normalize and expand unit items into fixed slots (nullable, max 3)
    units = normalize_delimited_column(units, column="items", delimiter=delimiter)
    units = split_delimited_to_columns(
        units,
        column="items",
        prefix="item",
        delimiter=delimiter,
        max_parts=3,
    )

    units = units.drop(columns=["items"])

    units = units.rename(
        columns={
            "unit_id": "unit_name",
            "star_level": "unit_tier",
        }
    )

    # Normalize traits (active traits only)
    traits = traits.loc[traits["tier_current"] > 0].copy()

    # Write outputs
    participants.to_csv(output_dir / "participants.csv", index=False)
    units.to_csv(output_dir / "units.csv", index=False)
    traits.to_csv(output_dir / "traits.csv", index=False)

    return output_dir


__all__ = [
    "parse_delimited_column",
    "normalize_delimited_column",
    "split_delimited_to_columns",
    "explode_delimited_column",
    "validate_participant_counts",
    "add_participant_flags",
    "resolve_processed_output_dir",
    "build_canonical_tables",
]
if __name__ == "__main__":
    build_canonical_tables(
        participants_path="data/raw/participants_match.csv",
        units_path="data/raw/units_match.csv",
        traits_path="data/raw/traits_match.csv",
    )