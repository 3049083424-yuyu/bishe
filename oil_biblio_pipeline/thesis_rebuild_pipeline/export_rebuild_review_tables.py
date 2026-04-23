from __future__ import annotations

import argparse
import csv
import re
from collections import defaultdict
from pathlib import Path


REBUILD_ROOT = Path(r"D:\graduate\thesis_rebuild")
DEFAULT_INPUT_PATH = REBUILD_ROOT / "corpus" / "merged_clean_dual_key_probe_2011_2025.csv"
DEFAULT_WEAK_OUTPUT_PATH = REBUILD_ROOT / "qa" / "review_weak_similarity_probe_2011_2025.csv"
DEFAULT_CONFLICT_OUTPUT_PATH = REBUILD_ROOT / "qa" / "review_doi_conflicts_probe_2011_2025.csv"

INPUT_ENCODING = "utf-8-sig"
OUTPUT_ENCODING = "gb18030"

RE_WORD = re.compile(r"[\W_]+", re.UNICODE)


def compact(text: object) -> str:
    return " ".join(str(text or "").replace("\r", " ").replace("\n", " ").split())


def norm_text(text: str) -> str:
    return RE_WORD.sub("", compact(text).lower())


def pick_title_and_field(row: dict[str, str]) -> tuple[str, str]:
    for field in ("title_cn", "title_cn_en", "title_en"):
        value = compact(row.get(field, ""))
        if value:
            return value, field
    return "", ""


def pick_journal(row: dict[str, str]) -> str:
    return compact(row.get("journal_cn") or row.get("journal_en"))


def pick_first_author(row: dict[str, str]) -> tuple[str, str]:
    raw_cn = compact(row.get("author_cn", ""))
    raw_en = compact(row.get("author", ""))
    raw = raw_cn or raw_en
    first = re.split(r"[;；,，]", raw, maxsplit=1)[0].strip()
    return first, norm_text(first)


def group_source_combo(rows: list[dict[str, str]]) -> str:
    values: set[str] = set()
    for row in rows:
        combo = compact(row.get("source_db_group", ""))
        if not combo:
            continue
        for part in combo.split("|"):
            part = part.strip()
            if part:
                values.add(part)
    return "|".join(sorted(values))


def group_journal_combo(rows: list[dict[str, str]]) -> str:
    values = sorted({pick_journal(row) for row in rows if pick_journal(row)})
    return " | ".join(values)


def weak_similarity_group_key(row: dict[str, str]) -> str:
    if compact(row.get("standard_doi_key", "")):
        return ""

    title, _ = pick_title_and_field(row)
    title_key = norm_text(title)
    year = compact(row.get("year", ""))
    first_author, author_key = pick_first_author(row)
    if len(title_key) < 8 or not year or not first_author or not author_key:
        return ""
    return f"{title_key}|{year}|{author_key}"


def build_weak_rows(rows: list[dict[str, str]]) -> tuple[list[dict[str, str]], int]:
    groups: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        key = weak_similarity_group_key(row)
        if key:
            groups[key].append(row)

    sorted_groups = []
    for key, group_rows in groups.items():
        if len(group_rows) <= 1:
            continue
        title, _ = pick_title_and_field(group_rows[0])
        year = compact(group_rows[0].get("year", ""))
        first_author, _ = pick_first_author(group_rows[0])
        sorted_groups.append((key, year, title, first_author, group_rows))
    sorted_groups.sort(key=lambda item: (item[1], item[2], item[3], item[0]))

    out_rows: list[dict[str, str]] = []
    for idx, (key, _year, _title, _first_author, group_rows) in enumerate(sorted_groups, 1):
        group_id = f"W{idx:03d}"
        source_combo = group_source_combo(group_rows)
        journal_combo = group_journal_combo(group_rows)

        sorted_group_rows = sorted(
            group_rows,
            key=lambda row: (
                compact(row.get("source_db_group", "")),
                pick_journal(row),
                compact(row.get("doi", "")),
                compact(row.get("title_cn", "") or row.get("title_cn_en", "") or row.get("title_en", "")),
            ),
        )

        for row_idx, row in enumerate(sorted_group_rows, 1):
            title, title_field = pick_title_and_field(row)
            first_author, _ = pick_first_author(row)
            out_rows.append(
                {
                    "review_group_id": group_id,
                    "row_in_group": str(row_idx),
                    "review_type": "weak_similarity",
                    "group_size": str(len(group_rows)),
                    "group_source_combo": source_combo,
                    "group_journal_combo": journal_combo,
                    "weak_similarity_key": key,
                    "year": compact(row.get("year", "")),
                    "first_author": first_author,
                    "title": title,
                    "title_source_field": title_field,
                    "journal": pick_journal(row),
                    "source_db_group": compact(row.get("source_db_group", "")),
                    "source_db_primary": compact(row.get("source_db_primary", "")),
                    "doi": compact(row.get("doi", "")),
                    "registered_doi": compact(row.get("registered_doi", "")),
                    "standard_doi_key": compact(row.get("standard_doi_key", "")),
                    "meta_dedup_key": compact(row.get("meta_dedup_key", "")),
                    "dedup_group_size": compact(row.get("dedup_group_size", "")),
                    "dedup_match_basis": compact(row.get("dedup_match_basis", "")),
                    "cited_count": compact(row.get("cited_count", "")),
                    "institution": compact(row.get("institution", "")),
                    "institution_extracted": compact(row.get("institution_extracted", "")),
                    "institution_norm": compact(row.get("institution_norm", "")),
                    "review_note": "No standard DOI key; grouped by loose title + year + first author for manual review rather than auto-merging.",
                }
            )

    return out_rows, len(sorted_groups)


def build_conflict_rows(rows: list[dict[str, str]]) -> tuple[list[dict[str, str]], int]:
    groups: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        meta_key = compact(row.get("meta_dedup_key", ""))
        if meta_key:
            groups[meta_key].append(row)

    sorted_groups = []
    for meta_key, group_rows in groups.items():
        if len(group_rows) <= 1:
            continue
        std_keys = sorted(
            {
                compact(row.get("standard_doi_key", ""))
                for row in group_rows
                if compact(row.get("standard_doi_key", ""))
            }
        )
        if len(std_keys) <= 1:
            continue
        title, _ = pick_title_and_field(group_rows[0])
        year = compact(group_rows[0].get("year", ""))
        first_author, _ = pick_first_author(group_rows[0])
        sorted_groups.append((meta_key, year, title, first_author, std_keys, group_rows))
    sorted_groups.sort(key=lambda item: (item[1], item[2], item[3], item[0]))

    out_rows: list[dict[str, str]] = []
    for idx, (meta_key, _year, _title, _first_author, std_keys, group_rows) in enumerate(sorted_groups, 1):
        group_id = f"C{idx:03d}"
        source_combo = group_source_combo(group_rows)
        journal_combo = group_journal_combo(group_rows)

        sorted_group_rows = sorted(
            group_rows,
            key=lambda row: (
                compact(row.get("standard_doi_key", "")),
                compact(row.get("doi", "")),
                compact(row.get("source_db_group", "")),
            ),
        )

        for row_idx, row in enumerate(sorted_group_rows, 1):
            title, title_field = pick_title_and_field(row)
            first_author, _ = pick_first_author(row)
            out_rows.append(
                {
                    "review_group_id": group_id,
                    "row_in_group": str(row_idx),
                    "review_type": "doi_conflict",
                    "group_size": str(len(group_rows)),
                    "distinct_standard_doi_count": str(len(std_keys)),
                    "group_source_combo": source_combo,
                    "group_journal_combo": journal_combo,
                    "meta_dedup_key": meta_key,
                    "year": compact(row.get("year", "")),
                    "first_author": first_author,
                    "title": title,
                    "title_source_field": title_field,
                    "journal": pick_journal(row),
                    "source_db_group": compact(row.get("source_db_group", "")),
                    "source_db_primary": compact(row.get("source_db_primary", "")),
                    "doi": compact(row.get("doi", "")),
                    "registered_doi": compact(row.get("registered_doi", "")),
                    "standard_doi_key": compact(row.get("standard_doi_key", "")),
                    "dedup_group_size": compact(row.get("dedup_group_size", "")),
                    "dedup_match_basis": compact(row.get("dedup_match_basis", "")),
                    "cited_count": compact(row.get("cited_count", "")),
                    "institution": compact(row.get("institution", "")),
                    "institution_extracted": compact(row.get("institution_extracted", "")),
                    "institution_norm": compact(row.get("institution_norm", "")),
                    "review_note": "Same strict meta key but multiple distinct standard DOI keys; kept separate for manual review.",
                }
            )

    return out_rows, len(sorted_groups)


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        with path.open("w", encoding=OUTPUT_ENCODING, newline="") as f:
            f.write("")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding=OUTPUT_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def export_review_tables(
    input_path: Path,
    weak_output_path: Path,
    conflict_output_path: Path,
) -> None:
    with input_path.open("r", encoding=INPUT_ENCODING, newline="") as f:
        rows = list(csv.DictReader(f))

    weak_rows, weak_group_count = build_weak_rows(rows)
    conflict_rows, conflict_group_count = build_conflict_rows(rows)

    write_csv(weak_output_path, weak_rows)
    write_csv(conflict_output_path, conflict_rows)

    print(f"input={input_path}")
    print(f"weak_output={weak_output_path}")
    print(f"weak_group_count={weak_group_count}")
    print(f"weak_row_count={len(weak_rows)}")
    print(f"conflict_output={conflict_output_path}")
    print(f"conflict_group_count={conflict_group_count}")
    print(f"conflict_row_count={len(conflict_rows)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(DEFAULT_INPUT_PATH))
    parser.add_argument("--weak-output", default=str(DEFAULT_WEAK_OUTPUT_PATH))
    parser.add_argument("--conflict-output", default=str(DEFAULT_CONFLICT_OUTPUT_PATH))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    export_review_tables(
        Path(args.input),
        Path(args.weak_output),
        Path(args.conflict_output),
    )
