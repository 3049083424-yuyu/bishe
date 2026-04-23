from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import defaultdict
from pathlib import Path


REBUILD_ROOT = Path(r"D:\graduate\thesis_rebuild")

DEFAULT_INPUT_PATH = Path(
    "D:/\u6bd5\u4e1a\u8bba\u6587/thesis_rebuild/corpus/cnki_norm/data_\u77e5\u7f51\u6570\u636e_2025_11_14.csv"
)
DEFAULT_OUTPUT_PATH = REBUILD_ROOT / "corpus" / "cnki_clean_rebuild.csv"
DEFAULT_STATS_PATH = REBUILD_ROOT / "corpus" / "cnki_clean_rebuild_stats.csv"
DEFAULT_NOTE_PATH = REBUILD_ROOT / "corpus" / "cnki_clean_rebuild_method_note.txt"
DEFAULT_REVIEW_PATH = REBUILD_ROOT / "qa" / "review_cnki_unstable_doi_rebuild.csv"

YEAR_RE = re.compile(r"(19|20)\d{2}")
STD_DOI_RE = re.compile(r"(10\.\d{4,9}/\S+)", re.I)
RE_STRIP_PUNCT = re.compile(r"[\W_]+", re.UNICODE)


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


def clean_row(row: dict[str, str]) -> dict[str, str]:
    return {key: text_or_empty(value) for key, value in row.items()}


def pick_first_nonempty(row: dict[str, str], *columns: str) -> str:
    for column in columns:
        text = text_or_empty(row.get(column, ""))
        if text:
            return text
    return ""


def parse_year_int(value: object) -> int | None:
    match = YEAR_RE.search(text_or_empty(value))
    if not match:
        return None
    return int(match.group(0))


def clean_year(value: object) -> str:
    year_int = parse_year_int(value)
    return str(year_int) if year_int is not None else ""


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

    year = ""
    month = "00"
    month_map = {
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
    for token in text.replace(".", " ").split():
        if re.fullmatch(r"\d{4}", token):
            year = token
        elif token.lower() in month_map:
            month = month_map[token.lower()]

    if year:
        return f"{year}-{month}"
    return text


def normalize_standard_doi(value: object) -> str:
    text = text_or_empty(value)
    if not text:
        return ""

    text = re.sub(r"^(?:doi:\s*|https?://(?:dx\.)?doi\.org/)", "", text, flags=re.I).strip()
    match = STD_DOI_RE.search(text)
    if not match:
        return ""

    doi = match.group(1).strip().strip("()[]{}<>.,;")
    return doi.lower()


def normalize_key_text(value: object) -> str:
    return RE_STRIP_PUNCT.sub("", text_or_empty(value).lower())


def derive_title(row: dict[str, str]) -> str:
    return pick_first_nonempty(row, "title_cn_en", "title", "title_en")


def derive_year(row: dict[str, str]) -> str:
    return clean_year(pick_first_nonempty(row, "publish_date", "year"))


def derive_journal(row: dict[str, str]) -> str:
    return pick_first_nonempty(row, "journal_cn", "journal_en")


def derive_institution(row: dict[str, str]) -> str:
    return pick_first_nonempty(row, "institution_extracted", "institution", "institution_norm")


def build_title_cn_en(row: dict[str, str]) -> str:
    parts: list[str] = []
    for field in ("title", "title_en"):
        value = text_or_empty(row.get(field, ""))
        if value and value not in parts:
            parts.append(value)
    return " | ".join(parts)


def build_journal_cn_en(row: dict[str, str]) -> str:
    parts: list[str] = []
    for field in ("journal_cn", "journal_en"):
        value = text_or_empty(row.get(field, ""))
        if value and value not in parts:
            parts.append(value)
    return " | ".join(parts)


def admission_reasons(row: dict[str, str]) -> list[str]:
    reasons: list[str] = []
    if not derive_title(row):
        reasons.append("missing_title")
    if not derive_year(row):
        reasons.append("missing_year")
    if not derive_journal(row):
        reasons.append("missing_journal")
    if not derive_institution(row):
        reasons.append("missing_institution")
    return reasons


def scan_cnki_input(input_path: Path) -> tuple[dict[str, int], dict[str, int]]:
    stats = {
        "input_rows": 0,
        "kept_rows": 0,
        "dropped_missing_any_required": 0,
        "dropped_missing_title": 0,
        "dropped_missing_year": 0,
        "dropped_missing_journal": 0,
        "dropped_missing_institution": 0,
        "rows_with_candidate_standard_doi": 0,
        "rows_with_registered_only_standard_doi": 0,
    }
    doi_titles: dict[str, set[str]] = defaultdict(set)

    with input_path.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        for raw_row in reader:
            stats["input_rows"] += 1
            row = clean_row(raw_row)
            reasons = admission_reasons(row)
            if reasons:
                stats["dropped_missing_any_required"] += 1
                for reason in reasons:
                    stats[f"dropped_{reason}"] += 1
                continue

            stats["kept_rows"] += 1

            standard_from_doi = normalize_standard_doi(row.get("doi", ""))
            standard_from_registered = normalize_standard_doi(row.get("registered_doi", ""))
            standard_doi_key = standard_from_doi or standard_from_registered
            if not standard_doi_key:
                continue

            stats["rows_with_candidate_standard_doi"] += 1
            if not standard_from_doi and standard_from_registered:
                stats["rows_with_registered_only_standard_doi"] += 1

            title_signature = normalize_key_text(derive_title(row))
            if title_signature:
                doi_titles[standard_doi_key].add(title_signature)

    unstable_doi_title_counts = {
        doi_key: len(title_signatures)
        for doi_key, title_signatures in doi_titles.items()
        if len(title_signatures) > 1
    }
    return stats, unstable_doi_title_counts


def write_stats_csv(path: Path, stats: dict[str, int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(stats.keys()))
        writer.writeheader()
        writer.writerow(stats)


def write_review_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        with path.open("w", encoding="utf-8-sig", newline="") as f:
            f.write("")
        return

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_method_note(path: Path) -> None:
    lines = [
        "CNKI clean rebuild admission rules",
        "",
        "1. This stage is an admission-and-standardization step, not the final cross-database deduplication step.",
        "2. Keep a CNKI record when it contains at least one usable title field (title_cn_en or title or title_en), one publication-time field (publish_date or year), one journal field (journal_cn or journal_en), and one institution field (institution_extracted or institution or institution_norm).",
        "3. Do not require DOI and do not require journal_en specifically, because those source-specific completeness rules would remove otherwise usable Chinese records without improving bibliographic identity.",
        "4. Normalize standard DOI candidates from doi or registered_doi with the strict slash-required pattern 10.<prefix>/<suffix>.",
        "5. Do not auto-collapse CNKI rows at this stage. Audit checks showed that a small number of repeated DOI strings still point to different titles, so same-source DOI equality is not treated as a sufficient clean-stage deduplication rule.",
        "6. Export unstable DOI candidates for manual audit and exclude them from later DOI-based auto-merging in the rebuild probe.",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def clean_cnki_for_rebuild(
    input_path: Path,
    output_path: Path,
    stats_path: Path,
    note_path: Path,
    review_path: Path,
) -> None:
    maximize_csv_field_limit()

    stats, unstable_doi_title_counts = scan_cnki_input(input_path)
    unstable_doi_keys = set(unstable_doi_title_counts)

    stats["unstable_standard_doi_keys"] = len(unstable_doi_keys)
    stats["rows_with_unstable_standard_doi"] = 0
    stats["automatic_dedup_removed_rows"] = 0

    review_rows: list[dict[str, str]] = []
    output_header: list[str] = []

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with input_path.open("r", encoding="utf-8-sig", errors="replace", newline="") as rf, output_path.open(
        "w", encoding="utf-8-sig", newline=""
    ) as wf:
        reader = csv.DictReader(rf)
        if reader.fieldnames is None:
            raise ValueError(f"Input file has no header: {input_path}")

        output_header = list(reader.fieldnames)
        for column in ("title_cn_en", "publish_date", "institution_extracted", "institution_norm", "journal_cn_en"):
            if column not in output_header:
                output_header.append(column)

        writer = csv.DictWriter(wf, fieldnames=output_header)
        writer.writeheader()

        output_rows = 0
        for raw_row in reader:
            row = clean_row(raw_row)
            if admission_reasons(row):
                continue

            record = {column: text_or_empty(row.get(column, "")) for column in output_header}
            year = derive_year(row)
            institution = derive_institution(row)

            if not record["title_cn_en"]:
                record["title_cn_en"] = build_title_cn_en(row) or derive_title(row)
            record["year"] = clean_year(record.get("year", "")) or year

            normalized_publish_date = normalize_publish_date(record.get("publish_date", ""))
            record["publish_date"] = normalized_publish_date or (f"{record['year']}-00" if record["year"] else "")

            if not record["institution_extracted"]:
                record["institution_extracted"] = institution
            if not record["journal_cn_en"]:
                record["journal_cn_en"] = build_journal_cn_en(row) or derive_journal(row)

            writer.writerow(record)
            output_rows += 1

            standard_doi_key = normalize_standard_doi(record.get("doi", "")) or normalize_standard_doi(
                record.get("registered_doi", "")
            )
            if standard_doi_key and standard_doi_key in unstable_doi_keys:
                stats["rows_with_unstable_standard_doi"] += 1
                review_rows.append(
                    {
                        "review_type": "unstable_standard_doi",
                        "standard_doi_key": standard_doi_key,
                        "title_signature_count": str(unstable_doi_title_counts[standard_doi_key]),
                        "year": record["year"],
                        "title": derive_title(record),
                        "journal": derive_journal(record),
                        "author": pick_first_nonempty(record, "author", "author_en"),
                        "institution": institution,
                        "doi": record.get("doi", ""),
                        "registered_doi": record.get("registered_doi", ""),
                        "review_note": "Same strict DOI appears with multiple title signatures in CNKI; excluded from DOI-based auto-merge.",
                    }
                )

    stats["output_rows"] = output_rows

    write_stats_csv(stats_path, stats)
    write_method_note(note_path)
    write_review_csv(review_path, review_rows)

    print(f"input={input_path}")
    print(f"output={output_path}")
    print(f"stats_output={stats_path}")
    print(f"note_output={note_path}")
    print(f"review_output={review_path}")
    print(f"input_rows={stats['input_rows']}")
    print(f"kept_rows={stats['kept_rows']}")
    print(f"rows_with_candidate_standard_doi={stats['rows_with_candidate_standard_doi']}")
    print(f"unstable_standard_doi_keys={stats['unstable_standard_doi_keys']}")
    print(f"rows_with_unstable_standard_doi={stats['rows_with_unstable_standard_doi']}")
    print(f"output_rows={stats['output_rows']}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(DEFAULT_INPUT_PATH))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--stats-out", default=str(DEFAULT_STATS_PATH))
    parser.add_argument("--note-out", default=str(DEFAULT_NOTE_PATH))
    parser.add_argument("--review-out", default=str(DEFAULT_REVIEW_PATH))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    clean_cnki_for_rebuild(
        Path(args.input),
        Path(args.output),
        Path(args.stats_out),
        Path(args.note_out),
        Path(args.review_out),
    )
