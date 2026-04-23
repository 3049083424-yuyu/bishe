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

GENERIC_NORM_BLACKLIST = {
    "大学",
    "学院",
    "研究院",
    "研究所",
    "技术大学",
    "石油大学",
    "联邦大学",
    "石油研究所",
    "化学工程系",
    "国家研究中心",
}

LOW_CONF_METHODS = {"低置信自动补译", "原文回退保留", "自动分词翻译"}


def to_int(value: object) -> int:
    text = str(value or "").strip()
    return int(text) if text else 0


def should_skip_norm(institution_norm: str, candidate_method: str, candidate_review_flag: str) -> bool:
    clean_norm = str(institution_norm or "").strip()
    clean_method = str(candidate_method or "").strip()
    clean_review_flag = str(candidate_review_flag or "").strip()
    if not clean_norm:
        return True
    if "待区分" in clean_norm:
        return True
    if clean_method == "人工复核回灌" and clean_review_flag == "是":
        return True
    if clean_norm in GENERIC_NORM_BLACKLIST:
        return True
    if clean_method in LOW_CONF_METHODS:
        if len(clean_norm) <= 4 and any(token in clean_norm for token in ("大学", "学院", "研究院", "研究所", "中心", "实验室")):
            return True
        if len(clean_norm) <= 5 and clean_norm.endswith(("大学", "学院")):
            return True
    return False


def build_table(input_path: Path, output_path: Path) -> None:
    agg: dict[str, dict[str, int]] = defaultdict(
        lambda: {"count": 0, "cnki_count": 0, "wos_count": 0, "cscd_count": 0}
    )
    source_rows = 0
    skipped_blank_norm = 0
    skipped_generic_norm = 0

    with input_path.open("r", encoding=INPUT_ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            source_rows += 1
            institution_norm = (row.get("institution_norm") or "").strip()
            candidate_method = (row.get("candidate_method") or "").strip()
            candidate_review_flag = (row.get("candidate_review_flag") or "").strip()
            if not institution_norm:
                skipped_blank_norm += 1
                continue
            if should_skip_norm(institution_norm, candidate_method, candidate_review_flag):
                skipped_generic_norm += 1
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
    print(f"skipped_generic_norm={skipped_generic_norm}")
    print(f"output={output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(INPUT_PATH))
    parser.add_argument("--output", default=str(OUTPUT_PATH))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    build_table(Path(args.input), Path(args.output))
