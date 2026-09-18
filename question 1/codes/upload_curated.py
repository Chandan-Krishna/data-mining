from pathlib import Path
from minio import Minio

# Local curated Parquet folder
LOCAL_DIR = Path("curated")

# MinIO connection
client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin123",
    secure=False,
)

BUCKET = "annapurna"

if not LOCAL_DIR.exists():
    raise FileNotFoundError(
        f"Curated folder not found: {LOCAL_DIR.resolve()}"
    )

uploaded = 0
total_bytes = 0

for file_path in sorted(LOCAL_DIR.rglob("*.parquet")):
    # Preserve the local Hive partition directory structure.
    relative_path = file_path.relative_to(LOCAL_DIR).as_posix()
    object_name = f"curated/{relative_path}"

    size = file_path.stat().st_size

    client.fput_object(
        BUCKET,
        object_name,
        str(file_path),
        content_type="application/vnd.apache.parquet",
    )

    uploaded += 1
    total_bytes += size
    print(f"Uploaded: {object_name}")

print()
print(f"Parquet files uploaded: {uploaded}")
print(f"Total bytes uploaded: {total_bytes:,}")