from __future__ import annotations

import argparse
import csv
import difflib
import importlib.util
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests


REBUILD_ROOT = Path(r"D:\graduate\thesis_rebuild")
DEFAULT_SOURCE_PATH = REBUILD_ROOT / "institution_eval" / "institution_name_table_dual_key_trans_norm_2011_2025.csv"
DEFAULT_OUTPUT_PATH = REBUILD_ROOT / "qa" / "institution_wikidata_lookup_rules.csv"
ENCODING = "gb18030"

PIPELINE_ROOT = Path(__file__).resolve().parent.parent
BUILD_SCRIPT = PIPELINE_ROOT / "thesis_rebuild_pipeline" / "build_institution_trans_norm_rebuild.py"

HEADERS = {"User-Agent": "Mozilla/5.0"}
WIKIDATA_API = "https://www.wikidata.org/w/api.php"

ASCII_RE = re.compile(r"[A-Za-z]")
BAD_TRANSLIT_RE = re.compile(
    r"(艾|比|西|迪|伊|吉|提|维|欧|恩|皮|开|齐|优|吾艾|艾弗|艾姆|艾勒){6,}"
)

TYPE_HINTS = (
    "university",
    "college",
    "institute",
    "laboratory",
    "research",
    "academy",
    "company",
    "centre",
    "center",
    "organization",
    "organisation",
    "council",
    "school",
    "facility",
    "observatory",
)

BAD_DESC_HINTS = (
    "scientific article",
    "sports team",
    "headquarters",
    "disambiguation",
    "album",
    "film",
    "song",
    "family name",
)

SPECIAL_QUERY_MAP: dict[str, list[str]] = {
    "Middle E Tech Univ": ["Middle East Technical University"],
    "Middle East Tech Univ": ["Middle East Technical University"],
    "GFZ German Res Ctr Geosci": ["GFZ German Research Centre for Geosciences"],
    "Consejo Nacl Invest Cient & Tecn": ["Consejo Nacional de Investigaciones Cientificas y Tecnicas"],
    "Inst Carboquim ICB CSIC": ["Instituto de Carboquimica"],
    "Tecnol Monterrey": ["Monterrey Institute of Technology and Higher Education"],
    "Natl Inst Clean & Low Carbon Energy": ["National Institute of Clean and Low Carbon Energy"],
    "Skolkovo Inst Sci & Technol": ["Skolkovo Institute of Science and Technology"],
    "Karadeniz Tech Univ": ["Karadeniz Technical University"],
    "Tech Univ Bergakad Freiberg": ["TU Bergakademie Freiberg"],
    "VTT Tech Res Ctr Finland Ltd": ["VTT Technical Research Centre of Finland"],
    "CSIR Indian Inst Petr": ["CSIR-Indian Institute of Petroleum"],
    "CSIR Indian Inst Chem Technol": ["CSIR-Indian Institute of Chemical Technology"],
    "King Fahd Univ Petr & Minerals KFUPM": ["King Fahd University of Petroleum and Minerals"],
    "Natl Inst Adv Ind Sci & Technol": ["National Institute of Advanced Industrial Science and Technology"],
    "Royal Inst Technol KTH": ["KTH Royal Institute of Technology"],
    "Korea Inst Sci & Technol KIST": ["Korea Institute of Science and Technology"],
    "Karlsruhe Inst Technol KIT": ["Karlsruhe Institute of Technology"],
    "Sungkyunkwan Univ SKKU": ["Sungkyunkwan University"],
    "Ulsan Natl Inst Sci & Technol": ["Ulsan National Institute of Science and Technology"],
    "Korea Adv Inst Sci & Technol KAIST": ["Korea Advanced Institute of Science and Technology"],
    "Pohang Univ Sci & Technol POSTECH": ["Pohang University of Science and Technology"],
    "Northwestern Polytech Univ": ["Northwestern Polytechnical University"],
    "Wroclaw Univ Sci & Technol": ["Wroclaw University of Science and Technology"],
    "Univ Wisconsin Madison": ["University of Wisconsin-Madison"],
    "Univ Fed Fluminense": ["Fluminense Federal University"],
    "Univ Publ Navarra": ["Public University of Navarre"],
    "Univ Eastern Finland": ["University of Eastern Finland"],
}

INSTITUTION_TYPE_EQUIV: dict[str, tuple[str, ...]] = {
    "university": ("university", "college", "higher education", "school"),
    "institute": ("institute", "research institute", "facility"),
    "academy": ("academy", "academy of sciences", "research institute"),
    "laboratory": ("laboratory", "lab", "facility"),
    "center": ("center", "centre", "research center", "research centre", "facility"),
    "company": ("company", "corporation", "enterprise", "firm", "oil company", "service company"),
    "council": ("council", "organization", "organisation", "research council"),
    "hospital": ("hospital",),
}


def load_module(path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载模块: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


bp = load_module(BUILD_SCRIPT, "wikidata_lookup_build_script")
tn = bp.load_module(bp.TRANSLATE_SCRIPT, "wikidata_lookup_translate_module", stub_pandas=True)
bp.extend_translation_resources(tn)


def compact_text(text: str) -> str:
    return " ".join(str(text or "").replace("\u00a0", " ").split())


def normalize_ascii_for_match(text: str) -> str:
    value = (
        compact_text(text)
        .replace("Concepción", "Concepcion")
        .replace("Tecnológico", "Tecnologico")
        .replace("Científicas", "Cientificas")
        .replace("Carboquímica", "Carboquimica")
        .replace("São", "Sao")
        .replace("Sáo", "Sao")
    )
    return tn.canonical_en_key(value)


def title_tokens(text: str) -> str:
    return " ".join(
        token.capitalize() if token not in {"of", "and", "for", "the", "in", "at", "on", "de", "la"} else token
        for token in text.split()
    )


def candidate_queries(name: str) -> list[str]:
    if name in SPECIAL_QUERY_MAP:
        return SPECIAL_QUERY_MAP[name]

    canon = normalize_ascii_for_match(name)
    tokens = canon.split()
    if not tokens:
        return [name]

    queries: list[str] = [title_tokens(canon)]

    if tokens[:2] == ["university", "federal"] and len(tokens) > 2:
        queries.append("Federal University of " + title_tokens(" ".join(tokens[2:])))
        queries.append(title_tokens(" ".join(tokens[2:])) + " Federal University")
    if tokens[0] == "university" and len(tokens) > 1:
        queries.append("University of " + title_tokens(" ".join(tokens[1:])))
    if tokens[-1] == "university" and len(tokens) > 1:
        queries.append(title_tokens(" ".join(tokens[:-1])) + " University")
        if "polytech" in name.lower():
            queries.append(title_tokens(" ".join(tokens[:-1])) + " Polytechnical University")
    if tokens[-2:] in (["academy", "science"], ["academy", "sciences"]) and len(tokens) > 2:
        queries.append(title_tokens(" ".join(tokens[:-2])) + " Academy of Sciences")
    if tokens[:2] == ["federal", "university"] and len(tokens) > 2:
        queries.append("Federal University of " + title_tokens(" ".join(tokens[2:])))
    if tokens[-2:] == ["federal", "university"] and len(tokens) > 2:
        queries.append(title_tokens(" ".join(tokens[:-2])) + " Federal University")
    if tokens[:2] == ["institute", "technology"] and len(tokens) > 2:
        queries.append("Institute of Technology " + title_tokens(" ".join(tokens[2:])))
    if tokens[:1] == ["institute"] and len(tokens) > 1:
        queries.append("Institute of " + title_tokens(" ".join(tokens[1:])))
    if tokens[-2:] == ["medical", "university"] and len(tokens) > 2:
        queries.append(title_tokens(" ".join(tokens[:-2])) + " Medical University")
    if tokens[-2:] == ["normal", "university"] and len(tokens) > 2:
        queries.append(title_tokens(" ".join(tokens[:-2])) + " Normal University")
    if tokens[-2:] == ["technology", "university"] and len(tokens) > 2:
        queries.append(title_tokens(" ".join(tokens[:-2])) + " Technology University")
        queries.append(title_tokens(" ".join(tokens[:-2])) + " Technical University")
    if tokens[-2:] == ["national", "laboratory"] and len(tokens) > 2:
        queries.append(title_tokens(" ".join(tokens[:-2])) + " National Laboratory")
    if tokens[-2:] == ["research", "council"] and len(tokens) > 2:
        queries.append(title_tokens(" ".join(tokens[:-2])) + " Research Council")

    queries.append(compact_text(name))
    return list(dict.fromkeys(query for query in queries if query))


def infer_expected_types(name: str) -> set[str]:
    canon = normalize_ascii_for_match(name)
    expected: set[str] = set()
    if " university " in f" {canon} " or canon.startswith("university ") or canon.endswith(" university"):
        expected.add("university")
    if " institute " in f" {canon} " or canon.startswith("institute "):
        expected.add("institute")
    if " academy " in f" {canon} ":
        expected.add("academy")
    if " laboratory " in f" {canon} " or canon.endswith(" lab"):
        expected.add("laboratory")
    if " center " in f" {canon} " or " centre " in f" {canon} " or canon.endswith(" ctr"):
        expected.add("center")
    if any(token in canon.split() for token in ("company", "corp", "corporation", "limited", "ltd", "inc")):
        expected.add("company")
    if " council " in f" {canon} ":
        expected.add("council")
    if " hospital " in f" {canon} ":
        expected.add("hospital")
    return expected


def has_institution_cue(name: str) -> bool:
    if name in SPECIAL_QUERY_MAP:
        return True
    return bool(infer_expected_types(name))


def score_result(raw_name: str, query: str, item: dict) -> tuple[int, str, str, str]:
    label = item.get("display", {}).get("label", {}).get("value") or item.get("label", "") or ""
    description = item.get("display", {}).get("description", {}).get("value") or item.get("description", "") or ""
    match_text = item.get("match", {}).get("text", "") or " ".join(item.get("aliases", [])[:1]) or ""

    score = 0
    if tn.has_cjk(label):
        score += 40
    if any(hint in description.lower() for hint in TYPE_HINTS) or any(hint in match_text.lower() for hint in TYPE_HINTS):
        score += 20
    if any(hint in description.lower() for hint in BAD_DESC_HINTS):
        score -= 25

    expected_types = infer_expected_types(raw_name)
    if expected_types:
        expected_hit = False
        combined = f"{description} {match_text}".lower()
        for expected in expected_types:
            if any(term in combined for term in INSTITUTION_TYPE_EQUIV[expected]):
                expected_hit = True
                break
        score += 15 if expected_hit else -30

    raw_norm = normalize_ascii_for_match(raw_name)
    query_norm = normalize_ascii_for_match(query)
    match_norm = normalize_ascii_for_match(match_text)
    score += int(35 * max(
        difflib.SequenceMatcher(None, raw_norm, match_norm).ratio(),
        difflib.SequenceMatcher(None, query_norm, match_norm).ratio(),
    ))

    return score, label, description, match_text


def is_candidate(row: dict[str, str], min_count: int) -> bool:
    name = row.get("institution_name", "")
    count = int(float(row.get("count") or 0))
    trans = row.get("institution_trans", "")
    norm = row.get("institution_norm", "")
    method = row.get("candidate_method", "")

    if count < min_count or not ASCII_RE.search(name) or not has_institution_cue(name):
        return False
    if method in {"自动补译中文化", "低置信自动补译"}:
        return True
    if ASCII_RE.search(trans) or ASCII_RE.search(norm):
        return True
    if BAD_TRANSLIT_RE.search(trans) or BAD_TRANSLIT_RE.search(norm):
        return True
    return False


def fetch_best_match(name: str) -> tuple[str, str, str] | None:
    best: tuple[int, str, str, str] | None = None
    for query in candidate_queries(name):
        data = None
        for attempt in range(5):
            response = requests.get(
                WIKIDATA_API,
                params={
                    "action": "wbsearchentities",
                    "format": "json",
                    "language": "en",
                    "uselang": "zh-cn",
                    "type": "item",
                    "limit": 8,
                    "search": query,
                },
                headers=HEADERS,
                timeout=20,
            )
            if response.status_code == 200:
                data = response.json()
                break
            if response.status_code in {429, 500, 502, 503, 504}:
                time.sleep(1.5 * (attempt + 1))
                continue
            response.raise_for_status()
        if not data:
            continue

        for item in data.get("search", []):
            candidate = score_result(name, query, item)
            if best is None or candidate > best:
                best = candidate

    if best is None:
        return None

    score, label, description, match_text = best
    if score < 58 or not tn.has_cjk(label):
        return None
    note = f"Wikidata匹配：{match_text}；说明：{description}"
    return label, label, note


def build_lookup(source_path: Path, output_path: Path, min_count: int, max_rows: int | None, workers: int) -> None:
    candidates: list[tuple[int, str]] = []
    with source_path.open("r", encoding=ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if is_candidate(row, min_count):
                candidates.append((int(float(row.get("count") or 0)), row.get("institution_name", "")))

    candidates.sort(reverse=True)
    if max_rows is not None:
        candidates = candidates[:max_rows]

    rows: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_best_match, name): (count, name) for count, name in candidates}
        for future in as_completed(futures):
            count, name = futures[future]
            try:
                result = future.result()
            except Exception:
                result = None
            if not result:
                continue
            trans, norm, note = result
            rows.append(
                {
                    "机构原始名称": name,
                    "机构译名": trans,
                    "机构标准名": norm,
                    "规则来源": "Wikidata自动核名",
                    "备注": note,
                    "_count": str(count),
                }
            )

    rows.sort(key=lambda row: (-int(row["_count"]), row["机构原始名称"]))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["机构原始名称", "机构译名", "机构标准名", "规则来源", "备注"],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row[key] for key in writer.fieldnames})

    print(f"candidates={len(candidates)}")
    print(f"matched_rows={len(rows)}")
    print(f"output={output_path}")
    print("preview:")
    for row in rows[:20]:
        print(
            f"{row['机构原始名称']} -> {row['机构译名']} | {row['机构标准名']} | {row['备注']}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default=str(DEFAULT_SOURCE_PATH))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--min-count", type=int, default=3)
    parser.add_argument("--max-rows", type=int, default=None)
    parser.add_argument("--workers", type=int, default=2)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    build_lookup(
        source_path=Path(args.source),
        output_path=Path(args.output),
        min_count=args.min_count,
        max_rows=args.max_rows,
        workers=args.workers,
    )
