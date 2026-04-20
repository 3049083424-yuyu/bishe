from __future__ import annotations

import argparse
import csv
import re
import unicodedata
from pathlib import Path


INPUT_PATH = Path(r"D:\毕业论文\institution_name_table_doi_required_2011_2025.csv")
MASTER_PATH = Path(r"D:\毕业论文\institution_name_table_doi_required_trans_norm.csv")
OUTPUT_PATH = Path(r"D:\毕业论文\institution_name_table_doi_required_trans_norm_2011_2025.csv")

INPUT_ENCODING = "utf-8-sig"
MASTER_ENCODING = "gb18030"
OUTPUT_ENCODING = "gb18030"

SPECIAL_CHAR_MAP = str.maketrans(
    {
        "ı": "i",
        "İ": "i",
        "ł": "l",
        "Ł": "l",
        "ø": "o",
        "Ø": "o",
        "ð": "d",
        "Ð": "d",
        "þ": "th",
        "Þ": "th",
        "ß": "ss",
        "æ": "ae",
        "Æ": "ae",
        "œ": "oe",
        "Œ": "oe",
    }
)

MANUAL_FALLBACKS: dict[str, tuple[str, str]] = {
    "Pontifıcia Univ Catolica Rio de Janeiro PUC": ("里约热内卢天主教大学", "里约热内卢天主教大学"),
    "Pontificia Univ Catolica Rio de Janeiro PUC": ("里约热内卢天主教大学", "里约热内卢天主教大学"),
    "Wyzsza Szkoła Bankowa Poznaniu": ("波兹南高等银行学校", "波兹南高等银行学校"),
    "Wyzsza Szkola Bankowa Poznaniu": ("波兹南高等银行学校", "波兹南高等银行学校"),
    "Lentatek Uzay Havacılık & Teknol AS": ("Lentatek航天航空与技术股份公司", "Lentatek航天航空与技术股份公司"),
    "Lentatek Uzay Havacilik & Teknol AS": ("Lentatek航天航空与技术股份公司", "Lentatek航天航空与技术股份公司"),
}

RE_ASCII = re.compile(r"[A-Za-z]")
RE_CJK = re.compile(r"[\u4e00-\u9fff]")

EXACT_BAD_NORMS = {
    "",
    "大学",
    "学院",
    "研究所",
    "研究院",
    "实验室",
    "中心",
    "公司",
    "国家大学",
    "技术大学",
    "大学技术",
    "中国大学",
    "China大学",
    "Fed大学",
    "Pontificia大学",
    "Amer大学",
    "农业大学",
    "中心大学",
    "医学院",
    "研究大学",
}

GENERIC_CJK_PARTS = {
    "大学",
    "学院",
    "研究所",
    "研究院",
    "实验室",
    "中心",
    "公司",
    "医院",
    "国家大学",
    "技术大学",
    "农业大学",
    "医科大学",
    "工业大学",
    "理工大学",
    "研究大学",
}


def strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text.translate(SPECIAL_CHAR_MAP))
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def canonical_key(text: str) -> str:
    value = strip_accents(text).lower().replace("&", " and ")
    for ch in "()[]{}\\/,-_.;:'\"?":
        value = value.replace(ch, " ")
    return " ".join(value.split())


def is_bad_generic_norm(norm: str) -> bool:
    cleaned = norm.strip()
    if cleaned in EXACT_BAD_NORMS:
        return True
    if not RE_ASCII.search(cleaned):
        return False
    cjk_only = "".join(RE_CJK.findall(cleaned))
    return bool(cjk_only) and cjk_only in GENERIC_CJK_PARTS


def load_master_map(master_path: Path) -> tuple[dict[str, tuple[str, str]], dict[str, tuple[str, str]]]:
    exact_mapping: dict[str, tuple[str, str]] = {}
    canonical_pairs: dict[str, set[tuple[str, str]]] = {}
    with master_path.open("r", encoding=MASTER_ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            institution_name = (row.get("institution_name") or "").strip()
            if not institution_name:
                continue
            pair = (
                (row.get("institution_trans") or "").strip(),
                (row.get("institution_norm") or "").strip(),
            )
            exact_mapping[institution_name] = pair
            key = canonical_key(institution_name)
            canonical_pairs.setdefault(key, set()).add(pair)

    canonical_mapping = {
        key: next(iter(pairs))
        for key, pairs in canonical_pairs.items()
        if len(pairs) == 1
    }
    return exact_mapping, canonical_mapping


def build_table(input_path: Path, master_path: Path, output_path: Path) -> None:
    master_exact_map, master_canonical_map = load_master_map(master_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows = 0
    exact_matched = 0
    canonical_matched = 0
    missing = 0
    fallback_norm_rows = 0

    with input_path.open("r", encoding=INPUT_ENCODING, newline="") as src, output_path.open(
        "w",
        encoding=OUTPUT_ENCODING,
        newline="",
    ) as dst:
        reader = csv.DictReader(src)
        fieldnames = list(reader.fieldnames or [])
        for extra in ("institution_trans", "institution_norm"):
            if extra not in fieldnames:
                fieldnames.append(extra)

        writer = csv.DictWriter(dst, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            rows += 1
            institution_name = (row.get("institution_name") or "").strip()
            trans, norm = master_exact_map.get(institution_name, ("", ""))
            if trans or norm:
                exact_matched += 1
            else:
                trans, norm = MANUAL_FALLBACKS.get(institution_name, ("", ""))
                if trans or norm:
                    canonical_matched += 1
                else:
                    trans, norm = master_canonical_map.get(canonical_key(institution_name), ("", ""))
                    if trans or norm:
                        canonical_matched += 1
                    else:
                        missing += 1
            if norm and is_bad_generic_norm(norm):
                norm = institution_name
                fallback_norm_rows += 1
            row["institution_trans"] = trans
            row["institution_norm"] = norm
            writer.writerow(row)

    print(f"rows={rows}")
    print(f"exact_matched_rows={exact_matched}")
    print(f"canonical_matched_rows={canonical_matched}")
    print(f"missing_rows={missing}")
    print(f"fallback_norm_rows={fallback_norm_rows}")
    print(f"output={output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(INPUT_PATH))
    parser.add_argument("--master", default=str(MASTER_PATH))
    parser.add_argument("--output", default=str(OUTPUT_PATH))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    build_table(Path(args.input), Path(args.master), Path(args.output))
