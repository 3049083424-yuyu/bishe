from __future__ import annotations

import argparse
import csv
import re
import unicodedata
from collections import Counter


INPUT_PATH = r"D:/\u6bd5\u4e1a\u8bba\u6587/merged_clean_doi_required.csv"
OUTPUT_PATH = r"D:/\u6bd5\u4e1a\u8bba\u6587/institution_name_table_doi_required.csv"

SOURCE_ORDER = ["CNKI", "WOS", "CSCD"]

RE_MULTI_SPACE = re.compile(r"\s+")
RE_WOS_AUTHOR = re.compile(r"\[[^\]]+\]\s*")
RE_EN_TITLE_SUFFIX = re.compile(
    r",?\s*(Distinguished Professor|Associate Professor|Assistant Professor|Professor|Senior Engineer|Engineer|Senior Researcher|Researcher|Lecturer|PhD|MSc|BSc)(?:[,\s].*)?$",
    re.I,
)
RE_CN_TITLE_SUFFIX = re.compile(
    r"[,\s]*(?:\u6559\u6388|\u526f\u6559\u6388|\u8bb2\u5e08|\u52a9\u6559|\u5de5\u7a0b\u5e08|\u9ad8\u7ea7\u5de5\u7a0b\u5e08|\u7814\u7a76\u5458|\u526f\u7814\u7a76\u5458|\u535a\u58eb|\u7855\u58eb)(?:[,\s].*)?$"
)

ROLE_ONLY_VALUES = {
    "\u6559\u5e08",
    "\u8bb2\u5e08",
    "\u52a9\u6559",
    "\u6559\u6388",
    "\u526f\u6559\u6388",
    "\u5de5\u7a0b\u5e08",
    "\u9ad8\u7ea7\u5de5\u7a0b\u5e08",
    "\u7814\u7a76\u5458",
    "\u526f\u7814\u7a76\u5458",
    "\u535a\u58eb",
    "\u7855\u58eb",
}

EN_SUBUNIT_HINTS = (
    "department",
    "dept",
    "faculty",
    "school",
    "college",
    "lab",
    "laboratory",
    "centre",
    "center",
    "key laboratory",
    "key lab",
    "state key lab",
)

EN_ORG_HINTS = (
    "university",
    "universiti",
    "univ",
    "institute",
    "inst",
    "academy",
    "company",
    "corporation",
    "corp",
    "oilfield",
    "hospital",
    "petrochina",
    "sinopec",
    "cnooc",
    "cnpc",
    "research center",
    "research centre",
)


def compact_text(value: str) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    text = text.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    text = text.replace("\u3000", " ").replace("\xa0", " ")
    text = RE_MULTI_SPACE.sub(" ", text)
    return text.strip().strip(",;\u3001\u3002.")


def clean_name(value: str) -> str:
    text = compact_text(value)
    if not text:
        return ""
    text = text.split("!")[0].strip()
    text = RE_CN_TITLE_SUFFIX.sub("", text).strip().strip(",;\u3001\u3002.")
    text = RE_EN_TITLE_SUFFIX.sub("", text).strip().strip(",;\u3001\u3002.")
    text = compact_text(text)
    if not text or text in ROLE_ONLY_VALUES:
        return ""
    return text


def split_cnki(value: str) -> list[str]:
    text = compact_text(value)
    if not text:
        return []
    parts = [clean_name(part) for part in re.split(r"\s*\|\s*|\s*;\s*|\s*\uff1b\s*", text) if part.strip()]
    return [part for part in parts if part]


def split_merged(value: str) -> list[str]:
    text = compact_text(value)
    if not text:
        return []
    parts = [clean_name(part) for part in re.split(r"\s*\|\s*", text) if part.strip()]
    return [part for part in parts if part]


def split_wos(value: str) -> list[str]:
    text = compact_text(value)
    if not text:
        return []
    text = RE_WOS_AUTHOR.sub("", text)
    blocks = [block.strip() for block in re.split(r"\s*;\s*", text) if block.strip()]
    institutions: list[str] = []
    for block in blocks:
        first = block.split(",", 1)[0].strip()
        name = clean_name(first)
        if name:
            institutions.append(name)
    return institutions


def contains_any(text: str, hints: tuple[str, ...]) -> bool:
    lowered = text.lower()
    return any(hint in lowered for hint in hints)


def split_cscd(value: str) -> list[str]:
    text = compact_text(value)
    if not text:
        return []

    blocks = [block.strip() for block in re.split(r"\.\s*;\s*|\.\s*$", text) if block.strip()]
    institutions: list[str] = []

    for block in blocks:
        parts = [compact_text(part) for part in block.split(",") if compact_text(part)]
        if len(parts) < 2:
            continue

        candidate = parts[1]
        if ";" in candidate:
            candidate = candidate.split(";")[-1].strip()

        if contains_any(candidate, EN_SUBUNIT_HINTS) and len(parts) >= 3 and contains_any(parts[2], EN_ORG_HINTS):
            candidate = parts[2]

        name = clean_name(candidate)
        if name:
            institutions.append(name)

    return institutions


def extract_institutions(source_db: str, institution: str, institution_extracted: str) -> list[str]:
    if source_db == "MERGED":
        return split_merged(institution_extracted or institution)
    if source_db == "CNKI":
        return split_cnki(institution_extracted or institution)
    if source_db == "WOS":
        return split_wos(institution)
    if source_db == "CSCD":
        return split_cscd(institution)
    return []


def write_output(rows: list[tuple[str, int, dict[str, int]]], out_path: str) -> None:
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["institution_name", "count", "cnki_count", "wos_count", "cscd_count"])
        for name, total, source_counts in rows:
            writer.writerow(
                [
                    name,
                    total,
                    source_counts.get("CNKI", 0),
                    source_counts.get("WOS", 0),
                    source_counts.get("CSCD", 0),
                ]
            )


def build_table(input_path: str, output_path: str) -> None:
    total_counter: Counter[str] = Counter()
    source_counter: dict[str, Counter[str]] = {source: Counter() for source in SOURCE_ORDER}

    with open(input_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            source_db = compact_text(row.get("source_db", ""))
            source_db_primary = compact_text(row.get("source_db_primary", "")) or source_db
            institution = row.get("institution", "")
            institution_extracted = row.get("institution_extracted", "")
            names = extract_institutions(source_db, institution, institution_extracted)

            for name in sorted(set(names)):
                total_counter[name] += 1
                if source_db_primary in source_counter:
                    source_counter[source_db_primary][name] += 1

    merged_rows: list[tuple[str, int, dict[str, int]]] = []
    for name, total in total_counter.most_common():
        merged_rows.append(
            (
                name,
                total,
                {source: source_counter[source][name] for source in SOURCE_ORDER},
            )
        )

    write_output(merged_rows, output_path)

    print(f"unique_institutions={len(merged_rows)}")
    print(f"output={output_path}")
    print("top10:")
    for name, total, _ in merged_rows[:10]:
        print(f"{name}\t{total}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=INPUT_PATH)
    parser.add_argument("--output", default=OUTPUT_PATH)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    build_table(args.input, args.output)
