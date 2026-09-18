from pathlib import Path
import re
from minio import Minio

# Current folder is ...\data_2\data\answers
SALES_DIR = Path("..") / "sales"

client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin123",
    secure=False,
)

BUCKET = "annapurna"
PREFIX = "raw"

# Filename date is the business date.
# Resend suffixes such as __R1 and __R2 are retained in the object name.
pattern = re.compile(
    r"^SALES_(S\d{2})_(\d{8})(?:__R\d+)?\.(csv|parquet)$",
    re.IGNORECASE,
)

if not client.bucket_exists(BUCKET):
    client.make_bucket(BUCKET)

files = sorted(SALES_DIR.iterdir())
uploaded = 0
skipped = 0
total_bytes = 0

for file_path in files:
    if not file_path.is_file():
        continue

    match = pattern.match(file_path.name)
    if not match:
        print(f"SKIP (filename not recognized): {file_path.name}")
        skipped += 1
        continue

    store_id, date_text, extension = match.groups()
    year = date_text[:4]
    month = date_text[4:6]

    object_name = (
        f"{PREFIX}/store_id={store_id.upper()}/"
        f"year={year}/month={month}/{file_path.name}"
    )

    client.fput_object(
        BUCKET,
        object_name,
        str(file_path),
    )

    size = file_path.stat().st_size
    uploaded += 1
    total_bytes += size

    if uploaded % 250 == 0:
        print(f"Uploaded {uploaded} files...")

print("\nUpload complete.")
print(f"Uploaded files: {uploaded}")
print(f"Skipped files:  {skipped}")
print(f"Uploaded bytes: {total_bytes:,}")