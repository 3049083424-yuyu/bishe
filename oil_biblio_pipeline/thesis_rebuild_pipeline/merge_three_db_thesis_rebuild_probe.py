from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from pathlib import Path


PIPELINE_ROOT = Path(__file__).resolve().parent.parent
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

from build_institution_name_table import clean_name, compact_text, extract_institutions
from build_top100_impact_indicators import canonical_country_from_block


REBUILD_ROOT = Path(r"D:\graduate\thesis_rebuild")

DEFAULT_CNKI_PATH = Path(r"D:\毕业论文\thesis_rebuild\corpus\cnki_clean_dedup.csv")
DEFAULT_WOS_PATH = Path(r"D:\BaiduNetdiskDownload\期刊数据\DBdata数据_2025_11_14.csv")
DEFAULT_CSCD_PATH = Path(r"D:\BaiduNetdiskDownload\期刊数据\CSCD数据_2025_11_14.csv")

DEFAULT_OUT_PATH = REBUILD_ROOT / "corpus" / "merged_clean_dual_key_probe_2011_2025.csv"
DEFAULT_STATS_PATH = REBUILD_ROOT / "corpus" / "dedup_stats_dual_key_probe_2011_2025.csv"
DEFAULT_NOTE_PATH = REBUILD_ROOT / "corpus" / "dedup_method_note_dual_key_probe_2011_2025.txt"

DEFAULT_CNKI_PATH = REBUILD_ROOT / "corpus" / "cnki_clean_rebuild.csv"
DEFAULT_WOS_PATH = Path("D:/BaiduNetdiskDownload/\u671f\u520a\u6570\u636e/DBdata\u6570\u636e_2025_11_14.csv")
DEFAULT_CSCD_PATH = Path("D:/BaiduNetdiskDownload/\u671f\u520a\u6570\u636e/CSCD\u6570\u636e_2025_11_14.csv")

SOURCE_ORDER = ("CNKI", "WOS", "CSCD")

YEAR_RE = re.compile(r"(19|20)\d{2}")
STD_DOI_RE = re.compile(r"(10\.\d{4,9}/\S+)", re.I)
RE_WOS_AUTHOR = re.compile(r"\[[^\]]+\]\s*")
RE_STRIP_PUNCT = re.compile(r"[\W_]+", re.UNICODE)

CORE_FIELDS = [
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
    "registered_doi",
    "year",
    "publish_date",
    "abstract_en",
    "abstract_cn",
    "keywords_en",
    "keywords_cn",
    "cited_count",
]
CORE_INDEX = {field: index for index, field in enumerate(CORE_FIELDS)}

OUT_FIELDS = CORE_FIELDS + [
    "source_db",
    "source_db_primary",
    "source_db_group",
    "dedup_group_size",
    "dedup_match_basis",
    "standard_doi_key",
    "meta_dedup_key",
    "institution_country_pairs",
]

FIELD_PRIORITIES = {
    "title_en": ("WOS", "CNKI", "CSCD"),
    "title_cn": ("CNKI", "CSCD", "WOS"),
    "title_cn_en": ("CNKI", "CSCD", "WOS"),
    "author": ("WOS", "CSCD", "CNKI"),
    "author_cn": ("CNKI", "CSCD", "WOS"),
    "journal_en": ("WOS", "CNKI", "CSCD"),
    "journal_cn": ("CNKI", "CSCD", "WOS"),
    "registered_doi": ("CNKI", "WOS", "CSCD"),
    "publish_date": ("WOS", "CNKI", "CSCD"),
    "abstract_en": ("WOS", "CNKI", "CSCD"),
    "abstract_cn": ("CNKI", "CSCD", "WOS"),
    "keywords_en": ("WOS", "CNKI", "CSCD"),
    "keywords_cn": ("CNKI", "CSCD", "WOS"),
    "cited_count": ("WOS", "CSCD", "CNKI"),
}

MATCH_SINGLE = 1
MATCH_STANDARD_DOI = 2
MATCH_META = 4


@dataclass(slots=True)
class SourceRecord:
    source: str
    values: tuple[str, ...]
    standard_doi_key: str
    meta_key: str
    institution_names: tuple[str, ...]
    institution_country_pairs: tuple[str, ...]
    nonempty_count: int
    text_weight: int


@dataclass(slots=True)
class Group:
    parent: int
    size: int
    match_mask: int
    standard_doi_key: str
    meta_key: str
    per_source: dict[str, SourceRecord]
    institution_names: set[str]
    institution_country_pairs: set[str]


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


def normalize_meta_text(value: object) -> str:
    text = text_or_empty(value).lower()
    if not text:
        return ""
    return RE_STRIP_PUNCT.sub("", text)


def pick_title_for_meta(values: tuple[str, ...]) -> str:
    title_en = values[CORE_INDEX["title_en"]]
    if title_en:
        return title_en

    title_cn_en = values[CORE_INDEX["title_cn_en"]]
    if title_cn_en:
        parts = [part.strip() for part in title_cn_en.split("|") if part.strip()]
        if parts:
            english_like = [part for part in parts if re.search(r"[a-zA-Z]", part)]
            if english_like:
                return max(english_like, key=len)
            return max(parts, key=len)

    return values[CORE_INDEX["title_cn"]]


def normalize_person_key(value: object) -> str:
    text = text_or_empty(value).lower()
    if not text:
        return ""

    first_author = re.split(r"[;；,，]", text, maxsplit=1)[0].strip()
    if re.search(r"[a-z]", first_author):
        tokens = [token for token in re.findall(r"[a-z0-9]+", first_author) if token]
        if not tokens:
            return ""
        if len(tokens) == 1:
            return tokens[0]
        return "|".join(sorted(tokens))

    return normalize_meta_text(first_author)


def normalize_first_author_key(source: str, row: dict[str, str]) -> str:
    if source == "CNKI":
        raw = pick_first_nonempty(row, "author_en", "author")
    elif source == "WOS":
        raw = pick_first_nonempty(row, "af", "au")
    else:
        raw = pick_first_nonempty(row, "au", "z2")
    return normalize_person_key(raw)


def pick_journal_for_meta(values: tuple[str, ...]) -> str:
    for field in ("journal_en", "journal_cn"):
        value = values[CORE_INDEX[field]]
        if value:
            return value
    return ""


def build_meta_key(title: str, year: str, author_key: str, journal: str) -> str:
    title_key = normalize_meta_text(title)
    journal_key = normalize_meta_text(journal)
    if len(title_key) < 8 or not year or not author_key or not journal_key:
        return ""
    return f"{title_key}|{year}|{author_key}|{journal_key}"


def build_title_signature(values: tuple[str, ...]) -> str:
    return normalize_meta_text(pick_title_for_meta(values))


def serialize_country_pairs(pairs: list[tuple[str, str]]) -> tuple[str, ...]:
    unique = {f"{name}@@{country}" for name, country in pairs if name}
    return tuple(sorted(unique))


def parse_wos_country_pairs(institution: str) -> list[tuple[str, str]]:
    text = compact_text(institution)
    if not text:
        return []

    text = RE_WOS_AUTHOR.sub("", text)
    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for block in [item.strip() for item in re.split(r"\s*;\s*", text) if item.strip()]:
        raw_name = clean_name(block.split(",", 1)[0].strip())
        if not raw_name:
            continue
        country = canonical_country_from_block(block)
        pair = (raw_name, country)
        if pair not in seen:
            pairs.append(pair)
            seen.add(pair)
    return pairs


def map_cnki_row(row: dict[str, str]) -> tuple[str, ...]:
    return (
        text_or_empty(row.get("title_en")),
        text_or_empty(row.get("title")),
        text_or_empty(row.get("title_cn_en")),
        "",
        text_or_empty(row.get("author")),
        text_or_empty(row.get("institution")),
        text_or_empty(row.get("institution_extracted")),
        text_or_empty(row.get("institution_norm")),
        text_or_empty(row.get("journal_en")),
        text_or_empty(row.get("journal_cn")),
        text_or_empty(row.get("doi")),
        text_or_empty(row.get("registered_doi")),
        clean_year(row.get("year")),
        normalize_publish_date(row.get("publish_date")),
        text_or_empty(row.get("abstract_en")),
        text_or_empty(row.get("abstract_cn")),
        text_or_empty(row.get("keywords_en")),
        text_or_empty(row.get("keywords_cn")),
        text_or_empty(row.get("cited_count")),
    )


def map_wos_row(row: dict[str, str]) -> tuple[str, ...]:
    publish_date = normalize_publish_date(
        f"{text_or_empty(row.get('pd'))} {text_or_empty(row.get('py'))}".strip()
    )
    return (
        text_or_empty(row.get("ti")),
        "",
        text_or_empty(row.get("ti")),
        pick_first_nonempty(row, "af", "au"),
        "",
        text_or_empty(row.get("c1")),
        "",
        "",
        text_or_empty(row.get("so")),
        "",
        text_or_empty(row.get("di")),
        "",
        clean_year(row.get("py")),
        publish_date,
        text_or_empty(row.get("ab")),
        "",
        text_or_empty(row.get("de")),
        "",
        text_or_empty(row.get("tc")),
    )


def map_cscd_row(row: dict[str, str]) -> tuple[str, ...]:
    title_parts = [text_or_empty(row.get(column)) for column in ("ti", "z1")]
    title_cn_en = " | ".join(part for part in title_parts if part)
    return (
        text_or_empty(row.get("ti")),
        text_or_empty(row.get("z1")),
        title_cn_en,
        text_or_empty(row.get("au")),
        text_or_empty(row.get("z2")),
        pick_first_nonempty(row, "c1", "z6"),
        "",
        "",
        text_or_empty(row.get("so")),
        text_or_empty(row.get("z3")),
        text_or_empty(row.get("di")),
        "",
        clean_year(row.get("py")),
        normalize_publish_date(row.get("py")),
        text_or_empty(row.get("ab")),
        text_or_empty(row.get("z4")),
        text_or_empty(row.get("de")),
        text_or_empty(row.get("z5")),
        text_or_empty(row.get("z9")),
    )


def iter_source_rows(source: str, source_paths: dict[str, Path]):
    if source == "CNKI":
        path = source_paths[source]
        mapper = map_cnki_row
    elif source == "WOS":
        path = source_paths[source]
        mapper = map_wos_row
    elif source == "CSCD":
        path = source_paths[source]
        mapper = map_cscd_row
    else:
        raise ValueError(f"Unknown source: {source}")

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row, mapper(row)


def candidate_standard_doi_key(values: tuple[str, ...]) -> str:
    raw_doi = values[CORE_INDEX["doi"]]
    raw_registered_doi = values[CORE_INDEX["registered_doi"]]
    return normalize_standard_doi(raw_doi) or normalize_standard_doi(raw_registered_doi)


def collect_unstable_standard_doi_keys(
    source_paths: dict[str, Path],
    year_start: int | None,
    year_end: int | None,
) -> dict[str, set[str]]:
    doi_titles = {source: defaultdict(set) for source in SOURCE_ORDER}

    for source in SOURCE_ORDER:
        for _raw_row, mapped_values in iter_source_rows(source, source_paths):
            year_value = mapped_values[CORE_INDEX["year"]]
            year_int = parse_year_int(year_value)
            if year_int is None:
                continue
            if year_start is not None and year_int < year_start:
                continue
            if year_end is not None and year_int > year_end:
                continue

            standard_doi_key = candidate_standard_doi_key(mapped_values)
            if not standard_doi_key:
                continue

            title_signature = build_title_signature(mapped_values)
            if not title_signature:
                continue
            doi_titles[source][standard_doi_key].add(title_signature)

    return {
        source: {doi for doi, titles in doi_titles[source].items() if len(titles) > 1}
        for source in SOURCE_ORDER
    }


def record_quality(values: tuple[str, ...]) -> tuple[int, int]:
    nonempty_count = sum(1 for value in values if value)
    text_weight = sum(len(value) for value in values if value)
    return nonempty_count, text_weight


def build_source_record(
    source: str,
    raw_row: dict[str, str],
    values: tuple[str, ...],
    unstable_standard_doi_keys: set[str],
) -> SourceRecord:
    candidate_standard_doi = candidate_standard_doi_key(values)
    if candidate_standard_doi in unstable_standard_doi_keys:
        standard_doi_key = ""
    else:
        standard_doi_key = candidate_standard_doi
    year = values[CORE_INDEX["year"]]
    meta_key = build_meta_key(
        pick_title_for_meta(values),
        year,
        normalize_first_author_key(source, raw_row),
        pick_journal_for_meta(values),
    )

    institution = values[CORE_INDEX["institution"]]
    institution_extracted = values[CORE_INDEX["institution_extracted"]]
    institution_names = tuple(sorted(set(extract_institutions(source, institution, institution_extracted))))

    if source == "WOS":
        country_pairs = serialize_country_pairs(parse_wos_country_pairs(institution))
    else:
        country_pairs = serialize_country_pairs([(name, "China") for name in institution_names])

    nonempty_count, text_weight = record_quality(values)
    return SourceRecord(
        source=source,
        values=values,
        standard_doi_key=standard_doi_key,
        meta_key=meta_key,
        institution_names=institution_names,
        institution_country_pairs=country_pairs,
        nonempty_count=nonempty_count,
        text_weight=text_weight,
    )


def find_root(parent: list[int], group_id: int) -> int:
    while parent[group_id] != group_id:
        parent[group_id] = parent[parent[group_id]]
        group_id = parent[group_id]
    return group_id


def better_record(candidate: SourceRecord, current: SourceRecord) -> bool:
    if candidate.nonempty_count != current.nonempty_count:
        return candidate.nonempty_count > current.nonempty_count
    if candidate.text_weight != current.text_weight:
        return candidate.text_weight > current.text_weight
    return candidate.standard_doi_key > current.standard_doi_key


def compatible_standard_doi(group: Group, record: SourceRecord) -> bool:
    if not group.standard_doi_key or not record.standard_doi_key:
        return True
    return group.standard_doi_key == record.standard_doi_key


def merge_group_into_root(
    root_id: int,
    other_id: int,
    parent: list[int],
    groups: list[Group | None],
) -> int:
    root_id = find_root(parent, root_id)
    other_id = find_root(parent, other_id)
    if root_id == other_id:
        return root_id

    root_group = groups[root_id]
    other_group = groups[other_id]
    if root_group is None or other_group is None:
        return root_id

    if other_group.size > root_group.size:
        root_id, other_id = other_id, root_id
        root_group, other_group = other_group, root_group

    parent[other_id] = root_id
    root_group.size += other_group.size
    root_group.match_mask |= other_group.match_mask

    if not root_group.standard_doi_key:
        root_group.standard_doi_key = other_group.standard_doi_key
    if not root_group.meta_key:
        root_group.meta_key = other_group.meta_key

    for source, record in other_group.per_source.items():
        current = root_group.per_source.get(source)
        if current is None or better_record(record, current):
            root_group.per_source[source] = record

    root_group.institution_names.update(other_group.institution_names)
    root_group.institution_country_pairs.update(other_group.institution_country_pairs)
    groups[other_id] = None
    return root_id


def match_mask_to_text(mask: int) -> str:
    parts: list[str] = []
    if mask & MATCH_STANDARD_DOI:
        parts.append("standard_doi")
    if mask & MATCH_META:
        parts.append("meta_key")
    if not parts:
        parts.append("single")
    return "|".join(parts)


def choose_primary_source(group: Group) -> str:
    for source in SOURCE_ORDER:
        if source in group.per_source:
            return source
    raise ValueError("Group has no source records")


def get_value(record: SourceRecord | None, field: str) -> str:
    if record is None:
        return ""
    return record.values[CORE_INDEX[field]]


def pick_field(group: Group, field: str) -> str:
    if field == "doi":
        if group.standard_doi_key:
            return group.standard_doi_key
        primary_source = choose_primary_source(group)
        return get_value(group.per_source.get(primary_source), "doi")

    if field in {"year", "institution", "institution_norm"}:
        primary_source = choose_primary_source(group)
        return get_value(group.per_source.get(primary_source), field)

    for source in FIELD_PRIORITIES.get(field, SOURCE_ORDER):
        value = get_value(group.per_source.get(source), field)
        if value:
            return value

    for source in SOURCE_ORDER:
        value = get_value(group.per_source.get(source), field)
        if value:
            return value
    return ""


def build_output_row(group: Group) -> dict[str, object]:
    primary_source = choose_primary_source(group)
    out = {field: "" for field in OUT_FIELDS}

    for field in CORE_FIELDS:
        out[field] = pick_field(group, field)

    merged_institutions = sorted(group.institution_names)
    out["institution_extracted"] = " | ".join(merged_institutions)

    if not out["title_cn_en"]:
        title_parts = [part for part in (out["title_cn"], out["title_en"]) if part]
        out["title_cn_en"] = " | ".join(title_parts)

    if not out["publish_date"]:
        out["publish_date"] = f"{out['year']}-00" if out["year"] else ""

    out["source_db"] = "MERGED"
    out["source_db_primary"] = primary_source
    out["source_db_group"] = "|".join(source for source in SOURCE_ORDER if source in group.per_source)
    out["dedup_group_size"] = group.size
    out["dedup_match_basis"] = match_mask_to_text(group.match_mask)
    out["standard_doi_key"] = group.standard_doi_key
    out["meta_dedup_key"] = group.meta_key
    out["institution_country_pairs"] = " || ".join(sorted(group.institution_country_pairs))
    return out


def write_stats_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "source_db",
        "original_rows",
        "year_kept_rows",
        "rows_with_candidate_standard_doi",
        "rows_with_unstable_standard_doi",
        "rows_with_standard_doi",
        "rows_with_meta_key",
        "new_group_rows",
        "merged_into_existing_rows",
        "primary_kept_groups",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_method_note(path: Path) -> None:
    lines = [
        "Probe rebuild dual-key deduplication rules for the 2011-2025 corpus",
        "",
        "1. Keep records whose publication year falls in 2011-2025.",
        "2. Build a strict standard DOI key from normalize(doi or registered_doi), and only accept DOI strings that match the slash-required pattern 10.<prefix>/<suffix>.",
        "3. If the same source reuses one strict DOI key for multiple normalized titles within the analysis window, mark that DOI key as unstable and exclude it from DOI-based auto-merging.",
        "4. Do not use source-local raw DOI-like strings as automatic merge keys.",
        "5. Build a metadata fallback key from normalized title + year + first author + journal.",
        "6. Merge two records when they share a usable standard DOI key, or when they can be safely connected by the metadata fallback key without conflicting standard DOI keys.",
        "7. If a record has no usable standard DOI key and its metadata key matches multiple existing groups, treat the fallback match as ambiguous and keep the record separate unless another key resolves the match.",
        "8. DOI conflicts and weak-similarity candidates are exported to review tables instead of being force-merged.",
        "9. Fuse fields within each deduplicated group using source-aware priorities: Chinese text prefers CNKI then CSCD, English text prefers WOS then CNKI, cited_count prefers WOS then CSCD then CNKI.",
        "10. Store merged institution names in institution_extracted and merged country pairs in institution_country_pairs for downstream institution and collaboration analysis.",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def merge_and_dedup(
    cnki_path: Path,
    wos_path: Path,
    cscd_path: Path,
    out_path: Path,
    stats_path: Path,
    note_path: Path,
    year_start: int | None = None,
    year_end: int | None = None,
) -> None:
    maximize_csv_field_limit()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    source_paths = {
        "CNKI": cnki_path,
        "WOS": wos_path,
        "CSCD": cscd_path,
    }
    unstable_standard_doi_keys = collect_unstable_standard_doi_keys(source_paths, year_start, year_end)

    source_stats = OrderedDict(
        (
            source,
            {
                "source_db": source,
                "original_rows": 0,
                "year_kept_rows": 0,
                "rows_with_candidate_standard_doi": 0,
                "rows_with_unstable_standard_doi": 0,
                "rows_with_standard_doi": 0,
                "rows_with_meta_key": 0,
                "new_group_rows": 0,
                "merged_into_existing_rows": 0,
                "primary_kept_groups": 0,
            },
        )
        for source in SOURCE_ORDER
    )

    parent: list[int] = []
    groups: list[Group | None] = []
    standard_doi_map: dict[str, int] = {}
    meta_key_map: dict[str, list[int]] = {}

    for source in SOURCE_ORDER:
        print(f"processing {source} ...")
        for raw_row, mapped_values in iter_source_rows(source, source_paths):
            source_stats[source]["original_rows"] += 1

            year_value = mapped_values[CORE_INDEX["year"]]
            year_int = parse_year_int(year_value)
            if year_int is None:
                continue
            if year_start is not None and year_int < year_start:
                continue
            if year_end is not None and year_int > year_end:
                continue

            source_stats[source]["year_kept_rows"] += 1
            candidate_standard_doi = candidate_standard_doi_key(mapped_values)
            if candidate_standard_doi:
                source_stats[source]["rows_with_candidate_standard_doi"] += 1
                if candidate_standard_doi in unstable_standard_doi_keys[source]:
                    source_stats[source]["rows_with_unstable_standard_doi"] += 1

            record = build_source_record(
                source,
                raw_row,
                mapped_values,
                unstable_standard_doi_keys[source],
            )

            if record.standard_doi_key:
                source_stats[source]["rows_with_standard_doi"] += 1
            if record.meta_key:
                source_stats[source]["rows_with_meta_key"] += 1

            candidate_roots: list[int] = []
            reasons = 0

            if record.standard_doi_key and record.standard_doi_key in standard_doi_map:
                candidate_roots.append(find_root(parent, standard_doi_map[record.standard_doi_key]))
                reasons |= MATCH_STANDARD_DOI

            if record.meta_key:
                meta_candidates = [
                    find_root(parent, group_id)
                    for group_id in meta_key_map.get(record.meta_key, [])
                    if groups[find_root(parent, group_id)] is not None
                ]
                compatible_meta_roots = []
                for group_id in meta_candidates:
                    group = groups[group_id]
                    if group is not None and compatible_standard_doi(group, record):
                        compatible_meta_roots.append(group_id)

                unique_meta_roots = sorted(set(compatible_meta_roots))
                if candidate_roots:
                    candidate_roots.extend(unique_meta_roots)
                    if unique_meta_roots:
                        reasons |= MATCH_META
                elif record.standard_doi_key:
                    candidate_roots.extend(unique_meta_roots)
                    if unique_meta_roots:
                        reasons |= MATCH_META
                elif len(unique_meta_roots) == 1:
                    candidate_roots.extend(unique_meta_roots)
                    reasons |= MATCH_META

            unique_candidates = sorted(set(candidate_roots))

            if not unique_candidates:
                group_id = len(groups)
                parent.append(group_id)
                groups.append(
                    Group(
                        parent=group_id,
                        size=1,
                        match_mask=MATCH_SINGLE,
                        standard_doi_key=record.standard_doi_key,
                        meta_key=record.meta_key,
                        per_source={source: record},
                        institution_names=set(record.institution_names),
                        institution_country_pairs=set(record.institution_country_pairs),
                    )
                )
                source_stats[source]["new_group_rows"] += 1
            else:
                group_id = unique_candidates[0]
                for other_id in unique_candidates[1:]:
                    group_id = merge_group_into_root(group_id, other_id, parent, groups)

                group = groups[group_id]
                if group is None:
                    raise RuntimeError("Merged group unexpectedly missing")

                group.size += 1
                group.match_mask |= reasons
                if not group.standard_doi_key:
                    group.standard_doi_key = record.standard_doi_key
                if not group.meta_key:
                    group.meta_key = record.meta_key

                current = group.per_source.get(source)
                if current is None or better_record(record, current):
                    group.per_source[source] = record

                group.institution_names.update(record.institution_names)
                group.institution_country_pairs.update(record.institution_country_pairs)
                source_stats[source]["merged_into_existing_rows"] += 1

            root_id = find_root(parent, group_id)
            root_group = groups[root_id]
            if root_group is None:
                raise RuntimeError("Root group unexpectedly missing")

            if record.standard_doi_key:
                standard_doi_map[record.standard_doi_key] = root_id
            if record.meta_key:
                meta_key_map.setdefault(record.meta_key, []).append(root_id)

    output_rows: list[dict[str, object]] = []
    for group_id, group in enumerate(groups):
        if group is None:
            continue
        root_id = find_root(parent, group_id)
        if root_id != group_id:
            continue

        primary_source = choose_primary_source(group)
        source_stats[primary_source]["primary_kept_groups"] += 1
        output_rows.append(build_output_row(group))

    output_rows.sort(
        key=lambda row: (
            int(text_or_empty(row["year"]) or 0),
            text_or_empty(row["title_en"]),
            text_or_empty(row["title_cn"]),
        )
    )

    with out_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUT_FIELDS)
        writer.writeheader()
        writer.writerows(output_rows)

    write_stats_csv(stats_path, list(source_stats.values()))
    write_method_note(note_path)

    print()
    print(f"year_range={year_start}-{year_end}")
    print(f"final_unique_rows={len(output_rows)}")
    print("unstable_standard_doi_keys:")
    for source in SOURCE_ORDER:
        print(f"{source}: {len(unstable_standard_doi_keys[source])}")
    print("primary_source_distribution:")
    for source in SOURCE_ORDER:
        print(f"{source}: {source_stats[source]['primary_kept_groups']}")
    print(f"merged_output={out_path}")
    print(f"stats_output={stats_path}")
    print(f"note_output={note_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cnki-in", default=str(DEFAULT_CNKI_PATH))
    parser.add_argument("--wos-in", default=str(DEFAULT_WOS_PATH))
    parser.add_argument("--cscd-in", default=str(DEFAULT_CSCD_PATH))
    parser.add_argument("--out", default=str(DEFAULT_OUT_PATH))
    parser.add_argument("--stats-out", default=str(DEFAULT_STATS_PATH))
    parser.add_argument("--note-out", default=str(DEFAULT_NOTE_PATH))
    parser.add_argument("--year-start", type=int, default=2011)
    parser.add_argument("--year-end", type=int, default=2025)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.year_start is not None and args.year_end is not None and args.year_start > args.year_end:
        raise ValueError("--year-start cannot be greater than --year-end")

    merge_and_dedup(
        Path(args.cnki_in),
        Path(args.wos_in),
        Path(args.cscd_in),
        Path(args.out),
        Path(args.stats_out),
        Path(args.note_out),
        year_start=args.year_start,
        year_end=args.year_end,
    )
