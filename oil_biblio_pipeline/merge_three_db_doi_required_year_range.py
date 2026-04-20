"""
Merge CNKI, WOS, and CSCD records into one dataset with DOI and year filters.

Rules:
- A record must have a non-empty DOI to enter the merged dataset.
- A record must fall inside the optional year range before DOI deduplication.
- Deduplication is performed only on normalized DOI.
- Source priority remains CNKI > WOS > CSCD.

This implementation uses only the Python standard library so it can run in
environments without pandas.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import OrderedDict
from pathlib import Path


CNKI_PATH = Path(r"D:\毕业论文\cnki_clean_dedup.csv")
WOS_PATH = Path(r"D:\毕业论文\DBdata数据_2025_11_14.csv")
CSCD_PATH = Path(r"D:\毕业论文\CSCD数据_2025_11_14.csv")
DEFAULT_OUT_PATH = Path(r"D:\毕业论文\merged_clean_doi_required.csv")

OUT_COLS = [
    "title_en",
    "title_cn",
    "title_cn_en",
    "author",
    "author_cn",
    "institution",
    "institution_extracted",
    "institution_norm",
    "journal_en",
    "journal_cn",
    "doi",
    "year",
    "publish_date",
    "abstract_en",
    "abstract_cn",
    "keywords_en",
    "keywords_cn",
    "cited_count",
    "source_db",
]

SOURCE_ORDER = ("CNKI", "WOS", "CSCD")

MONTH_MAP = {
    "jan": "01",
    "feb": "02",
    "mar": "03",
    "apr": "04",
    "may": "05",
    "jun": "06",
    "jul": "07",
    "aug": "08",
    "sep": "09",
    "oct": "10",
    "nov": "11",
    "dec": "12",
}

YEAR_RE = re.compile(r"(19|20)\d{2}")


def maximize_csv_field_limit() -> None:
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit //= 10


def text_or_empty(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def pick_first_nonempty(row: dict[str, str], *columns: str) -> str:
    for column in columns:
        text = text_or_empty(row.get(column, ""))
        if text:
            return text
    return ""


def clean_doi(value: object) -> str:
    text = text_or_empty(value).lower()
    return "" if text in {"", "none", "null"} else text


def parse_year_int(value: object) -> int | None:
    text = text_or_empty(value)
    if not text:
        return None
    match = YEAR_RE.search(text)
    if not match:
        return None
    return int(match.group(0))


def clean_year(value: object) -> str:
    year = parse_year_int(value)
    return str(year) if year is not None else ""


def normalize_publish_date(value: object) -> str:
    text = text_or_empty(value)
    if not text:
        return ""

    match = re.match(r"^(\d{4})-(\d{2})", text)
    if match:
        return f"{match.group(1)}-{match.group(2)}"

    match = re.match(r"^(\d{4})(?:\.0+)?$", text)
    if match:
        return f"{match.group(1)}-00"

    parts = text.replace(".", "").split()
    year = ""
    month = "00"
    for part in parts:
        if re.fullmatch(r"\d{4}", part):
            year = part
        elif part.lower() in MONTH_MAP:
            month = MONTH_MAP[part.lower()]
    if year:
        return f"{year}-{month}"

    return text


def map_cnki_row(row: dict[str, str]) -> dict[str, str]:
    return {
        "title_en": text_or_empty(row.get("title_en")),
        "title_cn": text_or_empty(row.get("title")),
        "title_cn_en": text_or_empty(row.get("title_cn_en")),
        "author": "",
        "author_cn": text_or_empty(row.get("author")),
        "institution": text_or_empty(row.get("institution")),
        "institution_extracted": text_or_empty(row.get("institution_extracted")),
        "institution_norm": text_or_empty(row.get("institution_norm")),
        "journal_en": text_or_empty(row.get("journal_en")),
        "journal_cn": text_or_empty(row.get("journal_cn")),
        "doi": text_or_empty(row.get("doi")),
        "year": clean_year(row.get("year")),
        "publish_date": normalize_publish_date(row.get("publish_date")),
        "abstract_en": text_or_empty(row.get("abstract_en")),
        "abstract_cn": text_or_empty(row.get("abstract_cn")),
        "keywords_en": text_or_empty(row.get("keywords_en")),
        "keywords_cn": text_or_empty(row.get("keywords_cn")),
        "cited_count": text_or_empty(row.get("cited_count")),
        "source_db": "CNKI",
    }


def map_wos_row(row: dict[str, str]) -> dict[str, str]:
    pd_value = text_or_empty(row.get("pd"))
    py_value = text_or_empty(row.get("py"))
    raw_publish_date = f"{pd_value} {py_value}".strip() if pd_value else py_value

    return {
        "title_en": text_or_empty(row.get("ti")),
        "title_cn": "",
        "title_cn_en": text_or_empty(row.get("ti")),
        "author": pick_first_nonempty(row, "af", "au"),
        "author_cn": "",
        "institution": text_or_empty(row.get("c1")),
        "institution_extracted": "",
        "institution_norm": "",
        "journal_en": text_or_empty(row.get("so")),
        "journal_cn": "",
        "doi": text_or_empty(row.get("di")),
        "year": clean_year(row.get("py")),
        "publish_date": normalize_publish_date(raw_publish_date),
        "abstract_en": text_or_empty(row.get("ab")),
        "abstract_cn": "",
        "keywords_en": text_or_empty(row.get("de")),
        "keywords_cn": "",
        "cited_count": text_or_empty(row.get("tc")),
        "source_db": "WOS",
    }


def map_cscd_row(row: dict[str, str]) -> dict[str, str]:
    title_parts: list[str] = []
    for column in ("ti", "z1"):
        text = text_or_empty(row.get(column))
        if text:
            title_parts.append(text)

    return {
        "title_en": text_or_empty(row.get("ti")),
        "title_cn": text_or_empty(row.get("z1")),
        "title_cn_en": " | ".join(title_parts),
        "author": text_or_empty(row.get("au")),
        "author_cn": text_or_empty(row.get("z2")),
        "institution": pick_first_nonempty(row, "c1", "z6"),
        "institution_extracted": "",
        "institution_norm": "",
        "journal_en": text_or_empty(row.get("so")),
        "journal_cn": text_or_empty(row.get("z3")),
        "doi": text_or_empty(row.get("di")),
        "year": clean_year(row.get("py")),
        "publish_date": normalize_publish_date(row.get("py")),
        "abstract_en": text_or_empty(row.get("ab")),
        "abstract_cn": text_or_empty(row.get("z4")),
        "keywords_en": text_or_empty(row.get("de")),
        "keywords_cn": text_or_empty(row.get("z5")),
        "cited_count": text_or_empty(row.get("z9")),
        "source_db": "CSCD",
    }


def iter_mapped_rows(source: str):
    if source == "CNKI":
        path = CNKI_PATH
        mapper = map_cnki_row
    elif source == "WOS":
        path = WOS_PATH
        mapper = map_wos_row
    elif source == "CSCD":
        path = CSCD_PATH
        mapper = map_cscd_row
    else:
        raise ValueError(f"Unknown source: {source}")

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield mapper(row)


def year_in_range(year_value: str, year_start: int | None, year_end: int | None) -> tuple[bool, str]:
    year_int = parse_year_int(year_value)
    if year_int is None:
        return False, "missing_year"
    if year_start is not None and year_int < year_start:
        return False, "out_of_range"
    if year_end is not None and year_int > year_end:
        return False, "out_of_range"
    return True, "kept"


def format_stats_table(rows: list[dict[str, int]]) -> str:
    columns = [
        "source_db",
        "original_rows",
        "year_kept_rows",
        "dropped_missing_year",
        "dropped_out_of_range",
        "doi_kept_rows",
        "dropped_no_doi",
        "dedup_kept_rows",
    ]
    widths: dict[str, int] = {}
    for col in columns:
        widths[col] = max(len(col), *(len(str(row.get(col, ""))) for row in rows))

    lines = []
    header = "  ".join(col.ljust(widths[col]) for col in columns)
    lines.append(header)
    lines.append("  ".join("-" * widths[col] for col in columns))
    for row in rows:
        lines.append("  ".join(str(row.get(col, "")).ljust(widths[col]) for col in columns))
    return "\n".join(lines)


def merge_and_dedup(out_path: Path, year_start: int | None = None, year_end: int | None = None) -> None:
    maximize_csv_field_limit()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    stats = OrderedDict(
        (
            source,
            {
                "source_db": source,
                "original_rows": 0,
                "year_kept_rows": 0,
                "dropped_missing_year": 0,
                "dropped_out_of_range": 0,
                "doi_kept_rows": 0,
                "dropped_no_doi": 0,
                "dedup_kept_rows": 0,
            },
        )
        for source in SOURCE_ORDER
    )

    seen_doi: set[str] = set()
    merged_rows = 0

    with out_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUT_COLS)
        writer.writeheader()

        for source in SOURCE_ORDER:
            print(f"处理 {source} ...")
            for mapped_row in iter_mapped_rows(source):
                source_stats = stats[source]
                source_stats["original_rows"] += 1

                in_range, reason = year_in_range(mapped_row["year"], year_start, year_end)
                if not in_range:
                    source_stats[f"dropped_{reason}"] += 1
                    continue

                source_stats["year_kept_rows"] += 1
                doi_key = clean_doi(mapped_row["doi"])
                if not doi_key:
                    source_stats["dropped_no_doi"] += 1
                    continue

                source_stats["doi_kept_rows"] += 1
                merged_rows += 1

                if doi_key in seen_doi:
                    continue

                seen_doi.add(doi_key)
                writer.writerow({col: mapped_row[col] for col in OUT_COLS})
                source_stats["dedup_kept_rows"] += 1

    result_rows = sum(row["dedup_kept_rows"] for row in stats.values())

    print()
    if year_start is not None or year_end is not None:
        print(
            "年份筛选区间："
            f"{year_start if year_start is not None else '-inf'}"
            f" - "
            f"{year_end if year_end is not None else 'inf'}"
        )
    print("各库处理统计：")
    print(format_stats_table(list(stats.values())))
    print()
    print(f"合并后总行数（要求年份在区间内且 DOI 非空，去重前）: {merged_rows}")
    print(f"去重后总行数: {result_rows}")
    print()
    print("去重后来源分布：")
    for source in SOURCE_ORDER:
        print(f"{source}: {stats[source]['dedup_kept_rows']}")
    print()
    print(f"已输出: {out_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=str(DEFAULT_OUT_PATH), help="Output CSV path.")
    parser.add_argument("--year-start", type=int, default=None, help="Inclusive start year.")
    parser.add_argument("--year-end", type=int, default=None, help="Inclusive end year.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.year_start is not None and args.year_end is not None and args.year_start > args.year_end:
        raise ValueError("--year-start cannot be greater than --year-end")
    merge_and_dedup(Path(args.out), year_start=args.year_start, year_end=args.year_end)
