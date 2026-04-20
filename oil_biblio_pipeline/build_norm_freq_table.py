from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path


INPUT_PATH = Path(r"D:\毕业论文\institution_name_table_doi_required_trans_norm_2011_2025.csv")
OUTPUT_PATH = Path(r"D:\毕业论文\institution_name_table_doi_required_norm_freq_2011_2025.csv")

INPUT_ENCODING = "gb18030"
OUTPUT_ENCODING = "gb18030"

OUT_FIELDS = ["institution_norm", "count", "cnki_count", "wos_count", "cscd_count"]


def to_int(value: object) -> int:
    text = str(value or "").strip()
    return int(text) if text else 0


def build_table(input_path: Path, output_path: Path) -> None:
    agg: dict[str, dict[str, int]] = defaultdict(
        lambda: {"count": 0, "cnki_count": 0, "wos_count": 0, "cscd_count": 0}
    )
    source_rows = 0
    skipped_blank_norm = 0

    with input_path.open("r", encoding=INPUT_ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            source_rows += 1
            institution_norm = (row.get("institution_norm") or "").strip()
            if not institution_norm:
                skipped_blank_norm += 1
                continue

            bucket = agg[institution_norm]
            bucket["count"] += to_int(row.get("count"))
            bucket["cnki_count"] += to_int(row.get("cnki_count"))
            bucket["wos_count"] += to_int(row.get("wos_count"))
            bucket["cscd_count"] += to_int(row.get("cscd_count"))

    rows = sorted(
        (
            {
                "institution_norm": institution_norm,
                "count": values["count"],
                "cnki_count": values["cnki_count"],
                "wos_count": values["wos_count"],
                "cscd_count": values["cscd_count"],
            }
            for institution_norm, values in agg.items()
        ),
        key=lambda row: (-row["count"], row["institution_norm"]),
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding=OUTPUT_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"source_rows={source_rows}")
    print(f"aggregated_rows={len(rows)}")
    print(f"skipped_blank_norm={skipped_blank_norm}")
    print(f"output={output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(INPUT_PATH))
    parser.add_argument("--output", default=str(OUTPUT_PATH))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    build_table(Path(args.input), Path(args.output))
