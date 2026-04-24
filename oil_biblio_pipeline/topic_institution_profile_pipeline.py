from __future__ import annotations

import argparse
import csv
import re
from collections import Counter, defaultdict
from pathlib import Path

import matplotlib
import numpy as np

from build_institution_name_table import extract_institutions
from topic_evolution_pipeline import (
    INPUT_ENCODING,
    OUTPUT_ENCODING,
    PERIODS,
    build_document_signature,
    build_output_paths,
    compact_text,
    period_label_from_year,
)

matplotlib.use("Agg")
import matplotlib.pyplot as plt


BASE_DIR = Path(r"D:/毕业论文")
DEFAULT_MERGED_PATH = BASE_DIR / "merged_clean_doi_required_2011_2025.csv"
DEFAULT_TRANS_NORM_PATH = BASE_DIR / "institution_name_table_doi_required_trans_norm_2011_2025.csv"
DEFAULT_TOPSIS_PATH = BASE_DIR / "institution_topsis_score_top100_2011_2025_zh.csv"
DEFAULT_REVIEW_PATH = Path("")
DEFAULT_TOPIC_OUTPUT_DIR = BASE_DIR / "topic_evolution_doi_required_2011_2025"
DEFAULT_DATASET_TAG = "doi_required"
DEFAULT_DATASET_LABEL = "DOI主键版本"

TARGET_TYPES = ("高校/科研院所", "企业研发中心", "国际组织", "政府机构")
TARGET_LEVELS = ("头部引领型", "中坚创新型", "特色细分型")

STRONG_ACADEMIC_KEYWORDS_CN = ("大学", "学院")
STRONG_ACADEMIC_KEYWORDS_EN = ("university", "college", "school")
ACADEMIC_KEYWORDS_CN = ("大学", "学院", "科学院", "研究院", "研究所", "实验室", "研究中心")
ACADEMIC_KEYWORDS_EN = ("university", "college", "school", "academy", "institute", "laboratory", "research center", "research centre")

ENTERPRISE_MARKERS = ("petrochina", "sinopec", "cnooc", "cnpc", "shell", "exxon", "chevron", "aramco", "totalenergies")
ENTERPRISE_KEYWORDS_CN = ("公司", "集团", "股份有限公司", "有限责任公司", "油田", "石化", "石油局", "勘探局", "工程公司")
ENTERPRISE_KEYWORDS_EN = ("company", "corporation", "corp", "co ltd", "co", "limited", "inc", "plc", "oilfield", "petrochemical")

INTERNATIONAL_PHRASES_EN = (
    "international energy agency",
    "organization of the petroleum exporting countries",
    "world petroleum council",
    "society of petroleum engineers",
    "international association of oil & gas producers",
)
INTERNATIONAL_PHRASES_CN = (
    "国际能源署",
    "石油输出国组织",
    "世界石油理事会",
    "国际石油工程师协会",
    "国际石油天然气生产者协会",
)
INTERNATIONAL_ABBREVIATIONS = {"iea", "opec", "wpc", "spe", "iogp"}
GOVERNMENT_EXACT_NAMES = {
    "中国教育部",
    "教育部",
    "中华人民共和国教育部",
}
INSTITUTION_KEYWORDS_CN = tuple(dict.fromkeys(STRONG_ACADEMIC_KEYWORDS_CN + ACADEMIC_KEYWORDS_CN + ENTERPRISE_KEYWORDS_CN + ("协会", "学会", "署", "委员会", "理事会", "中心", "实验室")))
INSTITUTION_KEYWORDS_EN = tuple(
    dict.fromkeys(
        STRONG_ACADEMIC_KEYWORDS_EN
        + ACADEMIC_KEYWORDS_EN
        + ENTERPRISE_KEYWORDS_EN
        + ("association", "society", "agency", "committee", "council", "center", "centre", "laboratory", "lab", "group")
    )
)
QUOTE_CHARS = "\"'“”‘’`"
RE_SPLIT_NAME_PARTS = re.compile(r"\s*[,;/\\]+\s*")
RE_ADDRESS_HINT = re.compile(r"(?<![a-z])(?:road|rd|street|st|avenue|ave|boulevard|blvd|lane|ln|apt|apartment|building|bldg|room|floor)(?![a-z])")
RE_ADDRESS_STYLE_NAME = re.compile(r"^(?:no\s*)?\d+[a-z0-9]*(?:\s+[a-z0-9]+){0,4}$")
RE_NUMERIC_SUBUNIT = re.compile(r"^\d+(?:st|nd|rd|th)?\s+")
RE_LEADING_DIGITS = re.compile(r"^\d{2,}")
RE_LEADING_PARENTHETICAL_LOCATION = re.compile(r"^[\(\uff08]([^\)\uff09]{1,12})[\)\uff09](.+)$")
GENERIC_SUBUNIT_HINTS = ("drilling", "exploration", "production", "team", "plant", "factory", "workshop", "branch", "crew", "unit", "company", "co", "division")
NOISE_MARKERS_CN = ("编辑部", "编委会", "指导委员会", "项目部", "工程科", "研究科", "办公室")
NOISE_MARKERS_EN = ("editorial office", "editorial board", "committee office", "working group")
MANUAL_INSTITUTION_ALIASES = {
    "china university of petroleum beijing": "中国石油大学（北京）",
    "china university of petroleum east china": "中国石油大学（华东）",
    "china university of petroleum qingdao": "中国石油大学（华东）",
    "china university of petroleum huadong": "中国石油大学（华东）",
    "sinopec lubricant company": "中国石油化工股份有限公司",
    "sinopec lubricant co": "中国石油化工股份有限公司",
    "sinopec lubricant co ltd": "中国石油化工股份有限公司",
}

SAFE_MAPPING_METHODS = {
    "人工精确匹配",
    "人工规则生成",
    "人工规范名匹配",
    "规则稳定译名",
}
BLOCKED_MAPPING_SENTINEL = "__blocked_norm__"

GOVERNMENT_EXACT_NAMES.update(
    {
        "\u4e2d\u56fd\u6559\u80b2\u90e8",
        "\u6559\u80b2\u90e8",
        "\u4e2d\u534e\u4eba\u6c11\u5171\u548c\u56fd\u6559\u80b2\u90e8",
        "\u4e2d\u56fd\u81ea\u7136\u8d44\u6e90\u90e8",
        "\u4e2d\u534e\u4eba\u6c11\u5171\u548c\u56fd\u81ea\u7136\u8d44\u6e90\u90e8",
        "\u4e2d\u534e\u4eba\u6c11\u5171\u548c\u56fd\u5e94\u6025\u7ba1\u7406\u90e8",
        "\u4e2d\u534e\u4eba\u6c11\u5171\u548c\u56fd\u751f\u6001\u73af\u5883\u90e8",
        "\u56fd\u5bb6\u6d77\u6d0b\u5c40",
        "\u7f8e\u56fd\u56fd\u5bb6\u6807\u51c6\u4e0e\u6280\u672f\u7814\u7a76\u9662",
        "\u52a0\u62ff\u5927\u519c\u4e1a\u4e0e\u519c\u4e1a\u98df\u54c1\u90e8",
        "\u6cf0\u56fd\u56fd\u5bb6\u79d1\u5b66\u6280\u672f\u53d1\u5c55\u7f72",
        "\u65e5\u672c\u79d1\u5b66\u6280\u672f\u632f\u5174\u673a\u6784",
    }
)

INSTITUTION_TYPE_ALIASES = {
    "\u4f01\u4e1a\u7814\u53d1\u673a\u6784": "\u4f01\u4e1a\u7814\u53d1\u4e2d\u5fc3",
}

DIRECT_CANONICAL_ALIASES = {
    "(\u5317\u4eac)\u4e2d\u56fd\u77f3\u6cb9\u5927\u5b66": "\u4e2d\u56fd\u77f3\u6cb9\u5927\u5b66\uff08\u5317\u4eac\uff09",
    "(\u5317\u4eac)\u4e2d\u56fd\u77ff\u4e1a\u5927\u5b66": "\u4e2d\u56fd\u77ff\u4e1a\u5927\u5b66\uff08\u5317\u4eac\uff09",
    "(\u5f90\u5dde)\u4e2d\u56fd\u77ff\u4e1a\u5927\u5b66": "\u4e2d\u56fd\u77ff\u4e1a\u5927\u5b66",
    "(\u6b66\u6c49)\u4e2d\u56fd\u5730\u8d28\u5927\u5b66": "\u4e2d\u56fd\u5730\u8d28\u5927\u5b66\uff08\u6b66\u6c49\uff09",
    "\uff08\u5317\u4eac\uff09\u4e2d\u56fd\u77f3\u6cb9\u5927\u5b66": "\u4e2d\u56fd\u77f3\u6cb9\u5927\u5b66\uff08\u5317\u4eac\uff09",
    "\uff08\u5317\u4eac\uff09\u4e2d\u56fd\u77ff\u4e1a\u5927\u5b66": "\u4e2d\u56fd\u77ff\u4e1a\u5927\u5b66\uff08\u5317\u4eac\uff09",
    "\uff08\u5f90\u5dde\uff09\u4e2d\u56fd\u77ff\u4e1a\u5927\u5b66": "\u4e2d\u56fd\u77ff\u4e1a\u5927\u5b66",
    "\uff08\u6b66\u6c49\uff09\u4e2d\u56fd\u5730\u8d28\u5927\u5b66": "\u4e2d\u56fd\u5730\u8d28\u5927\u5b66\uff08\u6b66\u6c49\uff09",
}

OUTPUT_FILE_TEMPLATES = {
    "institution_profile_classification": "institution_profile_classification_{dataset_tag}_2011_2025.csv",
    "type_distribution": "topic_distribution_by_institution_type_{dataset_tag}_2011_2025.csv",
    "level_distribution": "topic_distribution_by_institution_level_{dataset_tag}_2011_2025.csv",
    "type_heatmap": "topic_distribution_heatmap_by_institution_type_{dataset_tag}_2011_2025.png",
    "level_heatmap": "topic_distribution_heatmap_by_institution_level_{dataset_tag}_2011_2025.png",
    "summary": "topic_institution_profile_summary_{dataset_tag}_2011_2025.txt",
}


def build_profile_output_paths(output_dir: Path, dataset_tag: str) -> dict[str, Path]:
    return {
        key: output_dir / template.format(dataset_tag=dataset_tag)
        for key, template in OUTPUT_FILE_TEMPLATES.items()
    }


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding=OUTPUT_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_text(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding=OUTPUT_ENCODING)


def normalize_institution_type_label(value: object) -> str:
    text = compact_text(value)
    return INSTITUTION_TYPE_ALIASES.get(text, text)


def apply_direct_canonical_alias(value: object) -> str:
    text = compact_text(value)
    return DIRECT_CANONICAL_ALIASES.get(text, text)


def english_tokens(text: str) -> set[str]:
    return set(re.findall(r"[a-z][a-z0-9&-]*", text.lower()))


def normalize_lookup_key(value: object) -> str:
    text = compact_text(value)
    text = text.replace("（", "(").replace("）", ")")
    text = text.strip(QUOTE_CHARS + "[]{}")
    text = text.replace("&", " and ")
    text = text.lower()
    text = re.sub(r"[\s\-_/.,;:()]+", " ", text)
    return text.strip()


MANUAL_INSTITUTION_ALIASES.update(
    {
        normalize_lookup_key("\uff08\u5317\u4eac\uff09\u4e2d\u56fd\u77f3\u6cb9\u5927\u5b66"): "\u4e2d\u56fd\u77f3\u6cb9\u5927\u5b66\uff08\u5317\u4eac\uff09",
        normalize_lookup_key("(\u5317\u4eac)\u4e2d\u56fd\u77f3\u6cb9\u5927\u5b66"): "\u4e2d\u56fd\u77f3\u6cb9\u5927\u5b66\uff08\u5317\u4eac\uff09",
        normalize_lookup_key("\u4e2d\u56fd\u77f3\u6cb9\u5927\u5b66\uff08\u5317\u4eac\uff09"): "\u4e2d\u56fd\u77f3\u6cb9\u5927\u5b66\uff08\u5317\u4eac\uff09",
        normalize_lookup_key("\uff08\u5317\u4eac\uff09\u4e2d\u56fd\u77ff\u4e1a\u5927\u5b66"): "\u4e2d\u56fd\u77ff\u4e1a\u5927\u5b66\uff08\u5317\u4eac\uff09",
        normalize_lookup_key("(\u5317\u4eac)\u4e2d\u56fd\u77ff\u4e1a\u5927\u5b66"): "\u4e2d\u56fd\u77ff\u4e1a\u5927\u5b66\uff08\u5317\u4eac\uff09",
        normalize_lookup_key("\u4e2d\u56fd\u77ff\u4e1a\u5927\u5b66\uff08\u5317\u4eac\uff09"): "\u4e2d\u56fd\u77ff\u4e1a\u5927\u5b66\uff08\u5317\u4eac\uff09",
        normalize_lookup_key("\uff08\u5f90\u5dde\uff09\u4e2d\u56fd\u77ff\u4e1a\u5927\u5b66"): "\u4e2d\u56fd\u77ff\u4e1a\u5927\u5b66",
        normalize_lookup_key("(\u5f90\u5dde)\u4e2d\u56fd\u77ff\u4e1a\u5927\u5b66"): "\u4e2d\u56fd\u77ff\u4e1a\u5927\u5b66",
        normalize_lookup_key("\u4e2d\u56fd\u77ff\u4e1a\u5927\u5b66\uff08\u5f90\u5dde\uff09"): "\u4e2d\u56fd\u77ff\u4e1a\u5927\u5b66",
        normalize_lookup_key("\uff08\u6b66\u6c49\uff09\u4e2d\u56fd\u5730\u8d28\u5927\u5b66"): "\u4e2d\u56fd\u5730\u8d28\u5927\u5b66\uff08\u6b66\u6c49\uff09",
        normalize_lookup_key("(\u6b66\u6c49)\u4e2d\u56fd\u5730\u8d28\u5927\u5b66"): "\u4e2d\u56fd\u5730\u8d28\u5927\u5b66\uff08\u6b66\u6c49\uff09",
        normalize_lookup_key("\u4e2d\u56fd\u5730\u8d28\u5927\u5b66\uff08\u6b66\u6c49\uff09"): "\u4e2d\u56fd\u5730\u8d28\u5927\u5b66\uff08\u6b66\u6c49\uff09",
    }
)


def strip_institution_quotes(value: object) -> str:
    text = compact_text(value)
    for quote in QUOTE_CHARS:
        text = text.replace(quote, "")
    return compact_text(text)


def contains_english_keyword(text: str, keyword: str) -> bool:
    normalized_text = normalize_lookup_key(text)
    normalized_keyword = normalize_lookup_key(keyword)
    if not normalized_text or not normalized_keyword:
        return False
    if " " in normalized_keyword:
        return normalized_keyword in normalized_text
    return re.search(rf"(?<![a-z0-9]){re.escape(normalized_keyword)}(?![a-z0-9])", normalized_text) is not None


def has_institution_anchor(name: str) -> bool:
    text = strip_institution_quotes(name)
    if not text:
        return False
    lowered = text.lower()
    if english_tokens(lowered) & INTERNATIONAL_ABBREVIATIONS:
        return True
    if any(keyword in text for keyword in INSTITUTION_KEYWORDS_CN):
        return True
    return any(contains_english_keyword(lowered, keyword) for keyword in INSTITUTION_KEYWORDS_EN)


def institution_anchor_strength(name: str) -> int:
    text = strip_institution_quotes(name)
    lowered = text.lower()
    strong_cn = ("大学", "研究院", "研究所", "公司", "集团", "油田", "协会", "学会", "理事会", "委员会")
    strong_en = ("university", "academy", "institute", "company", "corporation", "group", "oilfield", "association", "society", "agency", "council", "committee")
    medium_cn = ("学院", "实验室", "中心")
    medium_en = ("college", "school", "laboratory", "lab", "center", "centre")
    if any(keyword in text for keyword in strong_cn):
        return 3
    if any(contains_english_keyword(lowered, keyword) for keyword in strong_en):
        return 3
    if any(keyword in text for keyword in medium_cn):
        return 2
    if any(contains_english_keyword(lowered, keyword) for keyword in medium_en):
        return 2
    return 1


def is_mixed_translation_artifact(name: str) -> bool:
    text = compact_text(name)
    if not re.search(r"[\u4e00-\u9fff]", text):
        return False
    english_fragments = re.findall(r"[A-Za-z]{2,}", text)
    return any(not (fragment.isupper() and len(fragment) <= 5) for fragment in english_fragments)


def is_address_like_name(name: str) -> bool:
    normalized = normalize_lookup_key(name)
    if not normalized:
        return True
    if RE_ADDRESS_HINT.search(normalized):
        return True
    if RE_ADDRESS_STYLE_NAME.fullmatch(normalized) and not has_institution_anchor(name):
        return True
    return False


def is_numeric_subunit_name(name: str) -> bool:
    normalized = normalize_lookup_key(name)
    if not normalized or not RE_NUMERIC_SUBUNIT.match(normalized):
        return False
    return any(contains_english_keyword(normalized, hint) for hint in GENERIC_SUBUNIT_HINTS)


def has_noise_marker(name: str) -> bool:
    text = strip_institution_quotes(name)
    normalized = normalize_lookup_key(text)
    if RE_LEADING_DIGITS.match(normalized):
        return True
    if any(marker in text for marker in NOISE_MARKERS_CN):
        return True
    return any(contains_english_keyword(normalized, marker) for marker in NOISE_MARKERS_EN)


def swap_leading_parenthetical_location(name: str) -> str:
    text = strip_institution_quotes(name)
    match = RE_LEADING_PARENTHETICAL_LOCATION.match(text)
    if not match:
        return ""
    location = compact_text(match.group(1))
    body = compact_text(match.group(2))
    if not location or not body:
        return ""
    return f"{body}\uff08{location}\uff09"


def lookup_mapped_institution_name(name: str, raw_to_norm: dict[str, str]) -> str:
    exact = compact_text(raw_to_norm.get(compact_text(name), ""))
    if exact:
        return exact
    normalized_key = normalize_lookup_key(name)
    normalized = compact_text(raw_to_norm.get(normalized_key, ""))
    if normalized:
        return normalized
    if raw_to_norm and len(raw_to_norm) <= 1000:
        for raw_key, mapped_value in raw_to_norm.items():
            if normalize_lookup_key(raw_key) == normalized_key:
                normalized = compact_text(mapped_value)
                if normalized:
                    return normalized
    swapped = swap_leading_parenthetical_location(name)
    if swapped:
        swapped_key = normalize_lookup_key(swapped)
        swapped_mapped = compact_text(raw_to_norm.get(swapped, "")) or compact_text(raw_to_norm.get(swapped_key, ""))
        if swapped_mapped:
            return swapped_mapped
        alias_swapped = MANUAL_INSTITUTION_ALIASES.get(swapped_key, "")
        if alias_swapped:
            return alias_swapped
    return MANUAL_INSTITUTION_ALIASES.get(normalized_key, "")


def score_resolved_institution_name(original: str, candidate: str, resolved: str) -> tuple[int, int, int]:
    score = 0
    if compact_text(resolved) != compact_text(candidate):
        score += 4
    if re.search(r"[\u4e00-\u9fff]", resolved):
        score += 2
    score += institution_anchor_strength(resolved)
    if candidate != original:
        score += 1
    if not any(separator in resolved for separator in (",", ";", "/", "\\")):
        score += 1
    if len(resolved) < len(original):
        score += 1
    if is_mixed_translation_artifact(resolved):
        score -= 5
    return (score, -len(resolved), -len(candidate))


def clean_standardized_institution_name(name: str, raw_to_norm: dict[str, str]) -> str:
    text = apply_direct_canonical_alias(strip_institution_quotes(name))
    if len(text) < 2 or not re.search(r"[A-Za-z\u4e00-\u9fff]", text):
        return ""

    candidates: list[str] = [text]
    if any(separator in text for separator in (",", ";", "/", "\\")):
        parts = [strip_institution_quotes(part) for part in RE_SPLIT_NAME_PARTS.split(text) if strip_institution_quotes(part)]
        candidates.extend(reversed(parts))

    best_name = ""
    best_score = (-1, 0, 0)
    seen: set[str] = set()
    for candidate in candidates:
        candidate = strip_institution_quotes(candidate)
        if not candidate:
            continue
        normalized_candidate = normalize_lookup_key(candidate)
        if normalized_candidate in seen:
            continue
        seen.add(normalized_candidate)

        mapped_name = lookup_mapped_institution_name(candidate, raw_to_norm)
        if mapped_name == BLOCKED_MAPPING_SENTINEL:
            continue
        if not mapped_name and not re.search(r"[\u4e00-\u9fff]", candidate):
            continue
        resolved = strip_institution_quotes(mapped_name or candidate)
        if is_mixed_translation_artifact(resolved) and not is_mixed_translation_artifact(candidate):
            resolved = candidate
        if len(resolved) < 2 or not re.search(r"[A-Za-z\u4e00-\u9fff]", resolved):
            continue
        if (
            is_address_like_name(resolved)
            or is_numeric_subunit_name(resolved)
            or has_noise_marker(resolved)
            or is_mixed_translation_artifact(resolved)
        ):
            continue
        if not has_institution_anchor(resolved):
            continue

        score = score_resolved_institution_name(text, candidate, resolved)
        if score > best_score:
            best_name = resolved
            best_score = score
    return best_name


def is_valid_institution_name(name: str) -> bool:
    text = strip_institution_quotes(name)
    if len(text) < 2 or not re.search(r"[A-Za-z\u4e00-\u9fff]", text):
        return False
    if is_address_like_name(text) or is_numeric_subunit_name(text) or has_noise_marker(text) or is_mixed_translation_artifact(text):
        return False
    return has_institution_anchor(text)


def extract_norm_names_from_row(row: dict[str, str], raw_to_norm: dict[str, str]) -> list[str]:
    institution_norm = compact_text(row.get("institution_norm", ""))
    if institution_norm:
        names = extract_institutions("MERGED", "", institution_norm)
        cleaned_names = {
            clean_standardized_institution_name(name, raw_to_norm)
            for name in names
        }
        cleaned_names.discard("")
        if cleaned_names:
            return sorted(cleaned_names)

    raw_names = extract_institutions(
        compact_text(row.get("source_db", "")),
        compact_text(row.get("institution", "")),
        compact_text(row.get("institution_extracted", "")),
    )
    cleaned_names = {
        clean_standardized_institution_name(raw_name, raw_to_norm)
        for raw_name in raw_names
    }
    cleaned_names.discard("")
    return sorted(cleaned_names)


def classify_institution_type(name: str) -> tuple[str, str]:
    text = strip_institution_quotes(name)
    lowered = text.lower()
    if not text:
        return "其他", "空值"
    if text in GOVERNMENT_EXACT_NAMES or text.endswith("教育部"):
        return "政府机构", "政府机构特例:教育部"
    for keyword in STRONG_ACADEMIC_KEYWORDS_CN:
        if keyword in text:
            return "高校/科研院所", f"高校强特征:{keyword}"
    for keyword in STRONG_ACADEMIC_KEYWORDS_EN:
        if contains_english_keyword(lowered, keyword):
            return "高校/科研院所", f"高校强特征:{keyword}"
    for marker in ENTERPRISE_MARKERS:
        if contains_english_keyword(lowered, marker):
            return "企业研发中心", f"企业标记:{marker}"
    for keyword in ENTERPRISE_KEYWORDS_CN:
        if keyword in text:
            return "企业研发中心", f"企业关键词:{keyword}"
    for keyword in ENTERPRISE_KEYWORDS_EN:
        if contains_english_keyword(lowered, keyword):
            return "企业研发中心", f"企业英文关键词:{keyword}"
    for marker in INTERNATIONAL_PHRASES_CN:
        if marker in text:
            return "国际组织", f"国际组织短语:{marker}"
    for marker in INTERNATIONAL_PHRASES_EN:
        if contains_english_keyword(lowered, marker):
            return "国际组织", f"国际组织短语:{marker}"
    if english_tokens(lowered) & INTERNATIONAL_ABBREVIATIONS:
        abbr = sorted(english_tokens(lowered) & INTERNATIONAL_ABBREVIATIONS)[0]
        return "国际组织", f"国际组织缩写:{abbr}"
    for keyword in ACADEMIC_KEYWORDS_CN:
        if keyword in text:
            return "高校/科研院所", f"高校科研关键词:{keyword}"
    for keyword in ACADEMIC_KEYWORDS_EN:
        if contains_english_keyword(lowered, keyword):
            return "高校/科研院所", f"高校科研关键词:{keyword}"
    return "其他", "未命中规则"


def load_raw_to_norm_map(path: Path) -> dict[str, str]:
    mapping: dict[str, str] = {}
    with path.open("r", encoding=OUTPUT_ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_name = compact_text(row.get("institution_name", ""))
            trans_name = compact_text(row.get("institution_trans", ""))
            norm_name = compact_text(row.get("institution_norm", ""))
            method = compact_text(row.get("candidate_method", ""))
            safe_mapping = bool(
                norm_name
                and (
                    method in SAFE_MAPPING_METHODS
                    or (re.search(r"[\u4e00-\u9fff]", raw_name or "") and re.search(r"[\u4e00-\u9fff]", norm_name))
                )
            )
            if not safe_mapping:
                for key in (trans_name, norm_name):
                    if key and key not in mapping:
                        mapping[key] = BLOCKED_MAPPING_SENTINEL
                        mapping[normalize_lookup_key(key)] = BLOCKED_MAPPING_SENTINEL
                continue
            for key in (raw_name, trans_name, norm_name):
                if key and norm_name:
                    mapping[key] = norm_name
                    mapping[normalize_lookup_key(key)] = norm_name
    return mapping


def load_top100_profile(path: Path) -> dict[str, dict[str, object]]:
    profile: dict[str, dict[str, object]] = {}
    with path.open("r", encoding=OUTPUT_ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            institution = compact_text(row.get("标准化机构名称", ""))
            rank_text = compact_text(row.get("TOPSIS综合排名", ""))
            score_text = compact_text(row.get("TOPSIS综合得分", ""))
            if institution and rank_text.isdigit():
                profile[institution] = {
                    "topsis_rank": int(rank_text),
                    "topsis_score": float(score_text) if score_text else 0.0,
                    "institution_level": compact_text(row.get("机构层级", "")),
                }
    return profile


def load_review_overrides(path: Path) -> dict[str, dict[str, object]]:
    if not path or str(path).strip() in {"", "."} or not path.exists() or path.is_dir():
        return {}
    overrides: dict[str, dict[str, object]] = {}
    with path.open("r", encoding=OUTPUT_ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            institution = compact_text(row.get("标准化机构名称", ""))
            if institution:
                overrides[institution] = {
                    "institution_type": normalize_institution_type_label(row.get("机构类型", "")),
                    "classification_basis": compact_text(row.get("分类依据", "")),
                    "institution_level": compact_text(row.get("机构层级", "")),
                    "topsis_rank": int(compact_text(row.get("TOPSIS综合排名", "0")) or 0),
                    "topsis_score": float(compact_text(row.get("TOPSIS综合得分", "0")) or 0.0),
                }
    return overrides


def load_topic_label_map(path: Path) -> dict[tuple[str, str], str]:
    label_map: dict[tuple[str, str], str] = {}
    with path.open("r", encoding=OUTPUT_ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            period = compact_text(row.get("阶段", ""))
            topic_id = compact_text(row.get("主题编号", ""))
            topic_label = compact_text(row.get("主题标签", ""))
            if period and topic_id:
                label_map[(period, topic_id)] = topic_label
    return label_map


def load_topic_assignment_map(path: Path) -> dict[str, dict[str, str]]:
    assignment_map: dict[str, dict[str, str]] = {}
    with path.open("r", encoding=OUTPUT_ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            signature = compact_text(row.get("文献签名", ""))
            if signature:
                assignment_map[signature] = {
                    "阶段": compact_text(row.get("阶段", "")),
                    "主题编号": compact_text(row.get("主导主题编号", "")),
                    "主题标签": compact_text(row.get("主导主题标签", "")),
                }
    return assignment_map


def plot_heatmaps(aggregate_rows: list[dict[str, object]], category_field: str, categories: list[str], out_path: Path, title_prefix: str) -> None:
    if not categories:
        return
    plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False
    figure, axes = plt.subplots(1, len(PERIODS), figsize=(18, 5), constrained_layout=True)
    if len(PERIODS) == 1:
        axes = [axes]
    for ax, (period_label, _, _) in zip(axes, PERIODS):
        rows = [row for row in aggregate_rows if row["阶段"] == period_label]
        topic_ids = sorted({str(row["主题编号"]) for row in rows})
        matrix = np.full((len(categories), len(topic_ids)), np.nan)
        for i, category in enumerate(categories):
            for j, topic_id in enumerate(topic_ids):
                match = next((row for row in rows if row[category_field] == category and str(row["主题编号"]) == topic_id), None)
                if match is not None:
                    matrix[i, j] = float(match["主题占比"])
        image = ax.imshow(matrix, cmap="YlOrRd", aspect="auto", vmin=0, vmax=np.nanmax(matrix) if np.isfinite(matrix).any() else 0.2)
        ax.set_title(f"{title_prefix} {period_label}")
        ax.set_xticks(range(len(topic_ids)))
        ax.set_xticklabels(topic_ids)
        ax.set_yticks(range(len(categories)))
        ax.set_yticklabels(categories)
        for i in range(len(categories)):
            for j in range(len(topic_ids)):
                if np.isfinite(matrix[i, j]):
                    ax.text(j, i, f"{matrix[i, j]:.2f}", ha="center", va="center", fontsize=8)
        figure.colorbar(image, ax=ax, fraction=0.046, pad=0.04)
    figure.savefig(out_path, dpi=220, bbox_inches="tight")
    plt.close(figure)


def write_summary(
    out_path: Path,
    dataset_label: str,
    institution_rows: list[dict[str, object]],
    type_rows: list[dict[str, object]],
    level_rows: list[dict[str, object]],
) -> None:
    type_counter = Counter(row["机构类型"] for row in institution_rows)
    lines = [
        f"机构类型与机构层级主题分布说明（{dataset_label}，2011-2025）",
        "",
        "1. 机构类型分析基于双主键主题主分析输出，并覆盖全体有效标准化机构。",
        "2. 机构层级分析仅针对双主键 TOPSIS Top100 机构，按头部引领型、中坚创新型、特色细分型比较主题偏好。",
        "3. 同一篇文献若包含多个机构类型或多个机构层级，则分别计入对应类别，但在同一类别内只计一次。",
        "",
        "机构类型分类数量：",
    ]
    for institution_type, count in sorted(type_counter.items()):
        lines.append(f"- {institution_type}：{count}")
    lines.append("")
    lines.append(f"机构类型主题分布记录数：{len(type_rows)}")
    lines.append(f"机构层级主题分布记录数：{len(level_rows)}")
    write_text(out_path, lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--merged", default=str(DEFAULT_MERGED_PATH))
    parser.add_argument("--trans-norm", default=str(DEFAULT_TRANS_NORM_PATH))
    parser.add_argument("--topsis", default=str(DEFAULT_TOPSIS_PATH))
    parser.add_argument("--review", default=str(DEFAULT_REVIEW_PATH))
    parser.add_argument("--topic-output-dir", default=str(DEFAULT_TOPIC_OUTPUT_DIR))
    parser.add_argument("--dataset-tag", default=DEFAULT_DATASET_TAG)
    parser.add_argument("--dataset-label", default=DEFAULT_DATASET_LABEL)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    merged_path = Path(args.merged)
    trans_norm_path = Path(args.trans_norm)
    topsis_path = Path(args.topsis)
    review_path = Path(args.review) if str(args.review).strip() else Path("")
    topic_output_dir = Path(args.topic_output_dir)
    dataset_tag = str(args.dataset_tag).strip() or DEFAULT_DATASET_TAG
    dataset_label = str(args.dataset_label).strip() or DEFAULT_DATASET_LABEL
    topic_paths = build_output_paths(topic_output_dir, dataset_tag)
    output_paths = build_profile_output_paths(topic_output_dir, dataset_tag)

    raw_to_norm = load_raw_to_norm_map(trans_norm_path)
    top100_profile = load_top100_profile(topsis_path)
    review_overrides = load_review_overrides(review_path)
    topic_label_map = load_topic_label_map(topic_paths["topic_strength"])
    topic_assignment_map = load_topic_assignment_map(topic_paths["topic_assignment"])

    institution_profiles: dict[str, dict[str, object]] = {}
    institution_doc_counts: Counter[str] = Counter()
    institution_periods: dict[str, set[str]] = defaultdict(set)
    type_topic_counter_by_period = {label: defaultdict(Counter) for label, _, _ in PERIODS}
    type_total_counter_by_period = {label: Counter() for label, _, _ in PERIODS}
    level_topic_counter_by_period = {label: defaultdict(Counter) for label, _, _ in PERIODS}
    level_total_counter_by_period = {label: Counter() for label, _, _ in PERIODS}

    with merged_path.open("r", encoding=INPUT_ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            year_text = compact_text(row.get("year", ""))
            if not year_text.isdigit():
                continue
            period_label = period_label_from_year(int(year_text))
            if not period_label:
                continue
            assignment = topic_assignment_map.get(build_document_signature(row))
            if assignment is None or assignment.get("阶段") != period_label:
                continue
            topic_id = compact_text(assignment.get("主题编号", ""))
            if not topic_id:
                continue
            norm_names = extract_norm_names_from_row(row, raw_to_norm)
            document_types: set[str] = set()
            document_levels: set[str] = set()
            for norm_name in norm_names:
                override = review_overrides.get(norm_name, {})
                profile = top100_profile.get(norm_name, {})
                if override:
                    institution_type = str(override.get("institution_type", "其他"))
                    classification_basis = str(override.get("classification_basis", ""))
                    topsis_rank = int(override.get("topsis_rank", 0) or 0)
                    topsis_score = float(override.get("topsis_score", 0.0) or 0.0)
                    institution_level = str(override.get("institution_level", ""))
                else:
                    institution_type, classification_basis = classify_institution_type(norm_name)
                    topsis_rank = int(profile.get("topsis_rank", 0) or 0)
                    topsis_score = float(profile.get("topsis_score", 0.0) or 0.0)
                    institution_level = str(profile.get("institution_level", ""))
                institution_profiles[norm_name] = {
                    "标准化机构名称": norm_name,
                    "机构类型": institution_type,
                    "分类依据": classification_basis,
                    "是否Top100": 1 if topsis_rank else 0,
                    "TOPSIS综合排名": topsis_rank or "",
                    "TOPSIS综合得分": round(topsis_score, 6) if topsis_rank else "",
                    "机构层级": institution_level,
                }
                institution_doc_counts[norm_name] += 1
                institution_periods[norm_name].add(period_label)
                if institution_type in TARGET_TYPES:
                    document_types.add(institution_type)
                if institution_level in TARGET_LEVELS:
                    document_levels.add(institution_level)
            for institution_type in document_types:
                type_total_counter_by_period[period_label][institution_type] += 1
                type_topic_counter_by_period[period_label][institution_type][topic_id] += 1
            for institution_level in document_levels:
                level_total_counter_by_period[period_label][institution_level] += 1
                level_topic_counter_by_period[period_label][institution_level][topic_id] += 1

    institution_rows: list[dict[str, object]] = []
    for institution_name in sorted(institution_profiles):
        row = dict(institution_profiles[institution_name])
        row["关联文献数"] = int(institution_doc_counts[institution_name])
        row["涉及阶段数"] = len(institution_periods[institution_name])
        institution_rows.append(row)

    type_rows: list[dict[str, object]] = []
    level_rows: list[dict[str, object]] = []
    for period_label, _, _ in PERIODS:
        for institution_type in TARGET_TYPES:
            total_docs = int(type_total_counter_by_period[period_label][institution_type])
            if not total_docs:
                continue
            for topic_id, count in sorted(type_topic_counter_by_period[period_label][institution_type].items()):
                type_rows.append(
                    {
                        "阶段": period_label,
                        "机构类型": institution_type,
                        "主题编号": topic_id,
                        "主题标签": topic_label_map.get((period_label, topic_id), ""),
                        "主题论文数": int(count),
                        "机构类型文献总数": total_docs,
                        "主题占比": round(count / total_docs, 6),
                    }
                )
        for institution_level in TARGET_LEVELS:
            total_docs = int(level_total_counter_by_period[period_label][institution_level])
            if not total_docs:
                continue
            for topic_id, count in sorted(level_topic_counter_by_period[period_label][institution_level].items()):
                level_rows.append(
                    {
                        "阶段": period_label,
                        "机构层级": institution_level,
                        "主题编号": topic_id,
                        "主题标签": topic_label_map.get((period_label, topic_id), ""),
                        "主题论文数": int(count),
                        "机构层级文献总数": total_docs,
                        "主题占比": round(count / total_docs, 6),
                    }
                )

    type_categories = [item for item in TARGET_TYPES if any(row["机构类型"] == item for row in type_rows)]
    level_categories = [item for item in TARGET_LEVELS if any(row["机构层级"] == item for row in level_rows)]
    write_csv(output_paths["institution_profile_classification"], ["标准化机构名称", "机构类型", "分类依据", "关联文献数", "涉及阶段数", "是否Top100", "TOPSIS综合排名", "TOPSIS综合得分", "机构层级"], institution_rows)
    write_csv(output_paths["type_distribution"], ["阶段", "机构类型", "主题编号", "主题标签", "主题论文数", "机构类型文献总数", "主题占比"], type_rows)
    write_csv(output_paths["level_distribution"], ["阶段", "机构层级", "主题编号", "主题标签", "主题论文数", "机构层级文献总数", "主题占比"], level_rows)
    plot_heatmaps(type_rows, "机构类型", type_categories, output_paths["type_heatmap"], "机构类型主题分布")
    plot_heatmaps(level_rows, "机构层级", level_categories, output_paths["level_heatmap"], "机构层级主题分布")
    write_summary(output_paths["summary"], dataset_label, institution_rows, type_rows, level_rows)

    print(f"output_dir={topic_output_dir}")
    print(f"institution_count={len(institution_rows)}")
    print(f"type_rows={len(type_rows)}")
    print(f"level_rows={len(level_rows)}")


__all__ = [
    "classify_institution_type",
    "main",
]
