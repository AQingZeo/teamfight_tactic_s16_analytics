# TFT Info Collector — Data Schema (Set 16)

This document describes the **analysis-ready CSV schemas** produced or used by this project.
All files are designed to be **flat, CSV-friendly**, and easy to join in pandas / SQL.

Naming convention:
- One row = one atomic entity
- IDs are explicit to allow joins
- Text values are normalized (suffixes stripped)

---

## Champions (`champions_s16.csv`)

One row per champion.

| Column | Description |
|------|-------------|
| champion_id | Canonical champion key |
| name | Champion name |
| cost | Shop cost |
| role | Champion role / archetype |
| traits | `;`-separated trait names |

---

## Items (`items_s16.csv`)

Forged (full) items only.

| Column | Description |
|------|-------------|
| name | Item name (canonical key) |
| comp | `;`-separated component item names (2 parts) |
| type | Item category (Damage, Tank, Utility, etc.) |
| description | Text description |

Derived split output: `items_s16_split.csv` adds `comp_0`, `comp_1` and drops `comp`.

---

## Traits (`traits_s16.csv`)

One row per trait.

| Column | Description |
|------|-------------|
| name | Trait name |
| name_corrected | Normalized trait name for matching |
| rank | Letter rank for power band (D..S) |
| num_for_tier | `;`-separated breakpoints for activation counts |
| is_unique | Whether the trait is unique |

Derived split output: `traits_s16_split.csv` adds `tier_0..tier_3` from `num_for_tier` and drops `num_for_tier`.

---

## Champion Pool Odds (`pool_odd_s16.csv`)

Shop odds by level and cost.

| Column | Description |
|------|-------------|
| level | Player level |
| cost_1 | % chance for 1-cost |
| cost_2 | % chance for 2-cost |
| cost_3 | % chance for 3-cost |
| cost_4 | % chance for 4-cost |
| cost_5 | % chance for 5-cost |

---

## Augments — Stage 2-1 (`augments_2-1_s16.csv`)
## Augments — Stage 3-2 (`augments_3-2_s16.csv`)
## Augments — Stage 4-2 (`augments_4-2_s16.csv`)

All augment tables share the same schema.

| Column | Description |
|------|-------------|
| augment_id | Augment ID |
| name | Augment name |
| tier | S / A / B / C |
| rarity | Silver / Gold / Prismatic |
| types | `;`-separated categories (Combat, Econ, Items, etc.) |

---

## Matches (Cleaned) (`matches_<REGION>.csv`)

One row per **unit on a final board**.
This format enables composition, item, and trait analysis.

| Column | Description |
|------|-------------|
| match_id | Match identifier |
| puuid | Player identifier |
| placement | Final placement |
| level | Final player level |
| unit_name | Champion name |
| unit_tier | Star level |
| items | `;`-separated item names |
| traits | `;`-separated active traits |

Notes:
- Region is encoded in the filename (not duplicated per row)
- Cosmetic and identity data are excluded by default
- Designed for **composition + item pattern analysis**

---


They are **not** meant to replicate live meta sites.
