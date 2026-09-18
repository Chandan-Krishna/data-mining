from pathlib import Path

sales_dir = Path("..") / "sales"

samples = [
    "SALES_S01_20240101.csv",
    "SALES_S06_20240101.csv",
    "SALES_S10_20240101.csv",
]

for name in samples:
    path = sales_dir / name
    print(f"\n--- {name} ---")
    if not path.exists():
        print("Not found")
        continue

    with path.open("rb") as f:
        print(f.read(1200).decode("utf-8-sig", errors="replace"))