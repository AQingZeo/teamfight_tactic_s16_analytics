"""Utility script to split delimited columns in trait/item reference tables.

- traits_s16.csv: splits `num_for_tier` (up to 4 parts) into tier_0..tier_n
- items_s16.csv: splits `comp` (2 parts) into comp_0, comp_1

Outputs are written to data/processed:
 - traits_s16_split.csv
 - items_s16_split.csv
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd

DEFAULT_DELIMITER = ";"


def split_column(
    df: pd.DataFrame,
    column: str,
    prefix: str,
    delimiter: str = DEFAULT_DELIMITER,
    max_parts: Optional[int] = None,
    drop_original: bool = True,
) -> pd.DataFrame:
    """Split a delimiter-separated column into multiple columns with a prefix, inserted after the original column."""
    if column not in df.columns:
        return df

    series = df[column].fillna("")
    parts_list: List[List[str]] = [
        [p.strip() for p in str(val).split(delimiter) if p.strip()] if val != "" else []
        for val in series
    ]

    inferred_max = max((len(parts) for parts in parts_list), default=0)
    width = max_parts if max_parts is not None else inferred_max

    new_cols = {f"{prefix}_{i}": [parts[i] if i < len(parts) else pd.NA for parts in parts_list] for i in range(width)}

    out = df.copy()
    for name, values in new_cols.items():
        out[name] = values

    # Build column order placing new columns after the original position
    ordered_cols = []
    for col in df.columns:
        if col == column:
            if not drop_original:
                ordered_cols.append(col)
            ordered_cols.extend(new_cols.keys())
        else:
            ordered_cols.append(col)

    if drop_original:
        out = out.drop(columns=[column])

    out = out[ordered_cols]
    return out


def process_traits(input_path: Path, output_path: Path) -> Path:
    df = pd.read_csv(input_path)
    df_split = split_column(df, column="num_for_tier", prefix="tier", max_parts=4, drop_original=True)
    df_split.to_csv(output_path, index=False)
    return output_path


def process_items(input_path: Path, output_path: Path) -> Path:
    df = pd.read_csv(input_path)
    df_split = split_column(df, column="comp", prefix="comp", max_parts=2, drop_original=True)
    df_split.to_csv(output_path, index=False)
    return output_path


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    raw_dir = project_root / "data" / "raw"
    processed_dir = project_root / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    traits_out = processed_dir / "traits_s16_split.csv"
    items_out = processed_dir / "items_s16_split.csv"

    traits_in = raw_dir / "traits_s16.csv"
    items_in = raw_dir / "items_s16.csv"

    print(f"Splitting traits from {traits_in} -> {traits_out}")
    process_traits(traits_in, traits_out)

    print(f"Splitting items from {items_in} -> {items_out}")
    process_items(items_in, items_out)

    print("Done.")


if __name__ == "__main__":
    main()
