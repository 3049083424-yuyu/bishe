"""
Merge CNKI, WOS, and CSCD records into one dataset.

Updated rule:
- Non-empty DOI is a hard requirement for a record to enter the merged dataset.
- Deduplication is performed only on normalized DOI.
- Source priority remains CNKI > WOS > CSCD.
- An optional year range filter is applied before DOI deduplication.
"""

from __future__ import annotations

import argparse
import re

import pandas as pd


CNKI_PATH = r"D:\毕业论文\cnki_clean_dedup.csv"
WOS_PATH = r"D:\毕业论文\DBdata数据_2025_11_14.csv"
CSCD_PATH = r"D:\毕业论文\CSCD数据_2025_11_14.csv"
DEFAULT_OUT_PATH = r"D:\毕业论文\merged_clean_doi_required.csv"

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

SOURCE_ORDER = {"CNKI": 0, "WOS": 1, "CSCD": 2}

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


def series_or_default(df: pd.DataFrame, column: str, default: str = "") -> pd.Series:
    if column in df.columns:
        return df[column]
    return pd.Series([default] * len(df), index=df.index)


def pick_first_nonempty(row: pd.Series, *columns: str) -> str:
    for column in columns:
        value = row.get(column, "")
        if pd.notna(value):
            text = str(value).strip()
            if text and text.lower() != "nan":
                return text
    return ""


def clean_doi(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip().lower()
    if text in {"", "nan", "none", "null"}:
        return ""
    return text


def clean_year(value: object) -> str:
    year = parse_year_int(value)
    return str(year) if year is not None else ""


def parse_year_int(value: object) -> int | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    match = YEAR_RE.search(text)
    if not match:
        return None
    return int(match.group(0))


def normalize_publish_date(value: object) -> str:
    if pd.isna(value):
        return ""

    text = str(value).strip()
    if not text or text.lower() == "nan":
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
        if re.match(r"^\d{4}$", part):
            year = part
        elif part.lower() in MONTH_MAP:
            month = MONTH_MAP[part.lower()]
    if year:
        return f"{year}-{month}"

    return text


def build_cnki_output() -> pd.DataFrame:
    df = pd.read_csv(CNKI_PATH, encoding="utf-8-sig", low_memory=False)
    out = pd.DataFrame(index=df.index)
    out["title_en"] = series_or_default(df, "title_en")
    out["title_cn"] = series_or_default(df, "title")
    out["title_cn_en"] = series_or_default(df, "title_cn_en")
    out["author"] = pd.Series([""] * len(df), index=df.index)
    out["author_cn"] = series_or_default(df, "author")
    out["institution"] = series_or_default(df, "institution")
    out["institution_extracted"] = series_or_default(df, "institution_extracted")
    out["institution_norm"] = series_or_default(df, "institution_norm")
    out["journal_en"] = series_or_default(df, "journal_en")
    out["journal_cn"] = series_or_default(df, "journal_cn")
    out["doi"] = series_or_default(df, "doi")
    out["year"] = series_or_default(df, "year").apply(clean_year)
    out["publish_date"] = series_or_default(df, "publish_date").apply(normalize_publish_date)
    out["abstract_en"] = series_or_default(df, "abstract_en")
    out["abstract_cn"] = series_or_default(df, "abstract_cn")
    out["keywords_en"] = series_or_default(df, "keywords_en")
    out["keywords_cn"] = series_or_default(df, "keywords_cn")
    out["cited_count"] = series_or_default(df, "cited_count")
    out["source_db"] = "CNKI"
    return out


def build_wos_output() -> pd.DataFrame:
    df = pd.read_csv(WOS_PATH, encoding="utf-8-sig", low_memory=False)

    def wos_publish_date(row: pd.Series) -> str:
        pd_value = str(row.get("pd", "")).strip()
        py_value = str(row.get("py", "")).strip()
        raw = f"{pd_value} {py_value}".strip() if pd_value and pd_value != "nan" else py_value
        return normalize_publish_date(raw)

    out = pd.DataFrame(index=df.index)
    out["title_en"] = series_or_default(df, "ti")
    out["title_cn"] = pd.Series([""] * len(df), index=df.index)
    out["title_cn_en"] = series_or_default(df, "ti")
    out["author"] = df.apply(lambda row: pick_first_nonempty(row, "af", "au"), axis=1)
    out["author_cn"] = pd.Series([""] * len(df), index=df.index)
    out["institution"] = series_or_default(df, "c1")
    out["institution_extracted"] = pd.Series([""] * len(df), index=df.index)
    out["institution_norm"] = pd.Series([""] * len(df), index=df.index)
    out["journal_en"] = series_or_default(df, "so")
    out["journal_cn"] = pd.Series([""] * len(df), index=df.index)
    out["doi"] = series_or_default(df, "di")
    out["year"] = series_or_default(df, "py").apply(clean_year)
    out["publish_date"] = df.apply(wos_publish_date, axis=1)
    out["abstract_en"] = series_or_default(df, "ab")
    out["abstract_cn"] = pd.Series([""] * len(df), index=df.index)
    out["keywords_en"] = series_or_default(df, "de")
    out["keywords_cn"] = pd.Series([""] * len(df), index=df.index)
    out["cited_count"] = series_or_default(df, "tc")
    out["source_db"] = "WOS"
    return out


def build_cscd_output() -> pd.DataFrame:
    df = pd.read_csv(CSCD_PATH, encoding="utf-8-sig", low_memory=False)

    def cscd_title_cn_en(row: pd.Series) -> str:
        parts = []
        for column in ("ti", "z1"):
            value = row.get(column, "")
            if pd.notna(value):
                text = str(value).strip()
                if text and text.lower() != "nan":
                    parts.append(text)
        return " | ".join(parts)

    out = pd.DataFrame(index=df.index)
    out["title_en"] = series_or_default(df, "ti")
    out["title_cn"] = series_or_default(df, "z1")
    out["title_cn_en"] = df.apply(cscd_title_cn_en, axis=1)
    out["author"] = series_or_default(df, "au")
    out["author_cn"] = series_or_default(df, "z2")
    out["institution"] = df.apply(lambda row: pick_first_nonempty(row, "c1", "z6"), axis=1)
    out["institution_extracted"] = pd.Series([""] * len(df), index=df.index)
    out["institution_norm"] = pd.Series([""] * len(df), index=df.index)
    out["journal_en"] = series_or_default(df, "so")
    out["journal_cn"] = series_or_default(df, "z3")
    out["doi"] = series_or_default(df, "di")
    out["year"] = series_or_default(df, "py").apply(clean_year)
    out["publish_date"] = series_or_default(df, "py").apply(normalize_publish_date)
    out["abstract_en"] = series_or_default(df, "ab")
    out["abstract_cn"] = series_or_default(df, "z4")
    out["keywords_en"] = series_or_default(df, "de")
    out["keywords_cn"] = series_or_default(df, "z5")
    out["cited_count"] = series_or_default(df, "z9")
    out["source_db"] = "CSCD"
    return out


def require_doi(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    out = df.copy()
    out["_doi_key"] = out["doi"].apply(clean_doi)
    kept = out[out["_doi_key"] != ""].copy()
    dropped = len(out) - len(kept)
    return kept, dropped


def filter_year_range(
    df: pd.DataFrame,
    year_start: int | None,
    year_end: int | None,
) -> tuple[pd.DataFrame, int, int]:
    if year_start is None and year_end is None:
        return df.copy(), 0, 0

    out = df.copy()
    out["_year_int"] = out["year"].apply(parse_year_int)
    valid_mask = out["_year_int"].notna()
    keep_mask = valid_mask.copy()

    if year_start is not None:
        keep_mask &= out["_year_int"] >= year_start
    if year_end is not None:
        keep_mask &= out["_year_int"] <= year_end

    kept = out[keep_mask].copy()
    dropped_missing_year = int((~valid_mask).sum())
    dropped_out_of_range = int((valid_mask & ~keep_mask).sum())
    return kept, dropped_missing_year, dropped_out_of_range


def merge_and_dedup(
    out_path: str,
    year_start: int | None = None,
    year_end: int | None = None,
) -> None:
    print("读取并映射三库数据...")
    cnki = build_cnki_output()
    wos = build_wos_output()
    cscd = build_cscd_output()

    source_frames = {"CNKI": cnki, "WOS": wos, "CSCD": cscd}
    kept_frames: list[pd.DataFrame] = []
    stats: list[dict[str, int]] = []

    for source, frame in source_frames.items():
        year_filtered, dropped_missing_year, dropped_out_of_range = filter_year_range(
            frame,
            year_start,
            year_end,
        )
        kept, dropped_no_doi = require_doi(year_filtered)
        kept_frames.append(kept)
        stats.append(
            {
                "source_db": source,
                "original_rows": len(frame),
                "year_kept_rows": len(year_filtered),
                "dropped_missing_year": dropped_missing_year,
                "dropped_out_of_range": dropped_out_of_range,
                "doi_kept_rows": len(kept),
                "dropped_no_doi": dropped_no_doi,
            }
        )

    merged = pd.concat(kept_frames, ignore_index=True)
    merged["_src_order"] = merged["source_db"].map(SOURCE_ORDER)
    merged = merged.sort_values("_src_order").reset_index(drop=True)

    result = merged.drop_duplicates(subset=["_doi_key"], keep="first").copy()
    result = result[OUT_COLS]

    stats_df = pd.DataFrame(stats)
    dedup_counts = result["source_db"].value_counts().to_dict()
    stats_df["dedup_kept_rows"] = stats_df["source_db"].map(dedup_counts).fillna(0).astype(int)

    if year_start is not None or year_end is not None:
        print(
            "\n年份筛选区间："
            f"{year_start if year_start is not None else '-inf'}"
            f" - "
            f"{year_end if year_end is not None else 'inf'}"
        )
    print("\n各库处理统计：")
    print(stats_df.to_string(index=False))
    print(f"\n合并后总行数（要求 DOI 非空，去重前）: {len(merged)}")
    print(f"去重后总行数: {len(result)}")

    print("\n去重后来源分布：")
    print(result["source_db"].value_counts().to_string())

    result.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n已输出: {out_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=DEFAULT_OUT_PATH, help="Output CSV path.")
    parser.add_argument("--year-start", type=int, default=None, help="Inclusive start year.")
    parser.add_argument("--year-end", type=int, default=None, help="Inclusive end year.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.year_start is not None and args.year_end is not None and args.year_start > args.year_end:
        raise ValueError("--year-start cannot be greater than --year-end")
    merge_and_dedup(args.out, year_start=args.year_start, year_end=args.year_end)
