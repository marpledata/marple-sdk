# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pyarrow>=23.0.1",
#     "tabulate>=0.9.0",
# ]
# ///
# python analyze.py <path>
# Ouick script to validate the parquet files
from pathlib import Path
import sys
from pyarrow.parquet import ParquetFile
from tabulate import tabulate


def fmt_size(size: int) -> str:
    if size < 1024:
        return f"{size} B"
    if size < 1024 * 1024:
        return f"{size / 1024:.2f} KB"
    if size < 1024 * 1024 * 1024:
        return f"{size / 1024 / 1024:.2f} MB"
    return f"{size / 1024 / 1024 / 1024:.2f} GB"


def print_file_info(path: Path):
    pf = ParquetFile(path)
    print("-" * 100)
    print("Name:", path.name)
    print("Size:", fmt_size(path.stat().st_size))
    print("Created by:", pf.metadata.created_by)
    rg = pf.metadata.row_group(0)
    header = ["name", "compression", "uncompressed", "compressed"]
    data = []
    for j, name in enumerate(pf.schema.names):
        col = rg.column(j)
        data.append([name, col.compression, fmt_size(col.total_uncompressed_size), fmt_size(col.total_compressed_size)])
    print(tabulate(data, headers=header, tablefmt="grid"))


def analyze(path: Path):
    for entry in path.glob("**/*.parquet"):
        print_file_info(entry)


if __name__ == "__main__":
    analyze(Path(sys.argv[1]))
