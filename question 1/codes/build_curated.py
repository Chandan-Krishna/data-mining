from pathlib import Path
from datetime import datetime
import re
import shutil

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds


# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

SALES_DIR = Path("..") / "sales"
OUT_DIR = Path("curated")

# Set True for a clean, repeatable full rebuild.
# This removes only the local curated output folder.
CLEAN_OUTPUT = True


# ------------------------------------------------------------
# Column normalization
# ------------------------------------------------------------

def normalize_columns(df):
    df.columns = [
        str(c).strip().replace("\ufeff", "").lower()
        for c in df.columns
    ]

    aliases = {
        "item_code": "product_code",
        "quantity": "qty",
        "rate": "unit_price",
        "type": "line_type",
        "txn_time": "ts",
    }

    df = df.rename(columns=aliases)

    required = [
        "bill_no",
        "line_no",
        "product_code",
        "qty",
        "unit_price",
        "line_type",
        "ts",
    ]

    missing = [c for c in required if c not in df.columns]

    if missing:
        raise ValueError(
            f"Missing columns {missing}; found {list(df.columns)}"
        )

    return df[required].copy()


# ------------------------------------------------------------
# Read and standardize one source CSV
# ------------------------------------------------------------

def read_source(path):
    # Detect comma or semicolon-delimited CSV.
    try:
        df = pd.read_csv(
            path,
            sep=None,
            engine="python",
            encoding="utf-8-sig",
        )
    except Exception:
        df = pd.read_csv(
            path,
            sep=";",
            encoding="utf-8-sig",
        )

    df = normalize_columns(df)

    # Normalize text fields.
    for col in ["bill_no", "line_no", "product_code"]:
        df[col] = df[col].astype("string").str.strip()

    df["line_type"] = (
        df["line_type"]
        .astype("string")
        .str.strip()
        .str.upper()
    )

    # Numeric fields.
    df["qty"] = pd.to_numeric(df["qty"], errors="raise")
    df["unit_price"] = pd.to_numeric(
        df["unit_price"],
        errors="raise",
    )

    # Parse timestamps:
    # S10-S12 use epoch seconds; other stores use timestamp strings.
    ts_numeric = pd.to_numeric(df["ts"], errors="coerce")
    non_null_ts = df["ts"].notna()

    if non_null_ts.any() and ts_numeric[non_null_ts].notna().all():
        df["ts"] = pd.to_datetime(
            ts_numeric,
            unit="s",
            utc=True,
            errors="raise",
        )
    else:
        df["ts"] = pd.to_datetime(
            df["ts"],
            format="mixed",
            dayfirst=True,
            errors="raise",
        )

    return df


# ------------------------------------------------------------
# Main ingestion
# ------------------------------------------------------------

if not SALES_DIR.exists():
    raise FileNotFoundError(
        f"Sales source directory not found: {SALES_DIR.resolve()}"
    )

if CLEAN_OUTPUT and OUT_DIR.exists():
    shutil.rmtree(OUT_DIR)

OUT_DIR.mkdir(parents=True, exist_ok=True)

frames = {}

count_files = 0
count_rows = 0
skipped_files = 0

filename_pattern = re.compile(
    r"SALES_(S\d+)_(\d{8})(?:__R\d+)?\.csv$",
    re.IGNORECASE,
)

for path in sorted(SALES_DIR.rglob("*.csv")):
    match = filename_pattern.search(path.name)

    if not match:
        print(f"Skipping unexpected filename: {path}")
        skipped_files += 1
        continue

    store_id = match.group(1).upper()
    date_text = match.group(2)

    # The filename date is the business date.
    business_date = datetime.strptime(
        date_text,
        "%Y%m%d",
    ).date()

    try:
        df = read_source(path)
    except Exception as e:
        raise RuntimeError(
            f"Failed processing {path}: {e}"
        ) from e

    df["store_id"] = store_id
    df["business_date"] = pd.Timestamp(business_date)
    df["source_file"] = path.name

    # Group in memory by store/year/month before writing.
    key = (
        store_id,
        business_date.year,
        business_date.month,
    )

    frames.setdefault(key, []).append(df)

    count_files += 1
    count_rows += len(df)

print(f"Read {count_files:,} CSV files.")
print(f"Read {count_rows:,} raw rows.")
print(f"Skipped {skipped_files:,} unexpected files.")


# ------------------------------------------------------------
# Combine and deduplicate resend copies
# ------------------------------------------------------------

if not frames:
    raise RuntimeError("No valid sales CSV files were found.")

tables = []

for key, pieces in frames.items():
    tables.append(
        pd.concat(pieces, ignore_index=True)
    )

all_data = pd.concat(tables, ignore_index=True)

# Vendor rule: deduplicate by bill number + line number.
# Keep the first occurrence across original/resend copies.
before_dedup = len(all_data)

all_data = all_data.drop_duplicates(
    subset=["bill_no", "line_no"],
    keep="first",
).copy()

duplicates_removed = before_dedup - len(all_data)

print(f"Removed {duplicates_removed:,} duplicate line rows.")
print(f"Rows after deduplication: {len(all_data):,}")


# ------------------------------------------------------------
# Add Hive partition columns
# ------------------------------------------------------------

all_data["year"] = (
    all_data["business_date"]
    .dt.year
    .astype(str)
)

all_data["month"] = (
    all_data["business_date"]
    .dt.month
    .astype(str)
    .str.zfill(2)
)


# ------------------------------------------------------------
# Select stable output column order
# ------------------------------------------------------------

all_data = all_data[
    [
        "store_id",
        "year",
        "month",
        "business_date",
        "bill_no",
        "line_no",
        "product_code",
        "qty",
        "unit_price",
        "line_type",
        "ts",
        "source_file",
    ]
]


# ------------------------------------------------------------
# Write partitioned Parquet dataset
# ------------------------------------------------------------

table = pa.Table.from_pandas(
    all_data,
    preserve_index=False,
)

ds.write_dataset(
    table,
    base_dir=str(OUT_DIR),
    format="parquet",
    partitioning=["store_id", "year", "month"],
    partitioning_flavor="hive",
    existing_data_behavior="delete_matching",
)

print()
print("Curated Parquet build complete.")
print(f"Output directory: {OUT_DIR.resolve()}")
print(f"Curated row count: {len(all_data):,}")
print(f"Duplicate rows removed: {duplicates_removed:,}")