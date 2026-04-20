from __future__ import annotations

import argparse
import csv
import math
import re
import statistics
from collections import Counter, defaultdict
from pathlib import Path

from build_institution_name_table import clean_name, compact_text, extract_institutions


BASE_DATA_DIR = Path("D:/\u6bd5\u4e1a\u8bba\u6587")

NORM_FREQ_PATH = BASE_DATA_DIR / "institution_name_table_doi_required_norm_freq_2011_2025.csv"
TRANS_NORM_PATH = BASE_DATA_DIR / "institution_name_table_doi_required_trans_norm_2011_2025.csv"
MERGED_PATH = BASE_DATA_DIR / "merged_clean_doi_required_2011_2025.csv"

TOP100_OUTPUT_PATH = BASE_DATA_DIR / "institution_core_top100_2011_2025.csv"
IMPACT_OUTPUT_PATH = BASE_DATA_DIR / "institution_impact_indicator_top100_2011_2025.csv"

NORM_FREQ_ENCODING = "gb18030"
TRANS_NORM_ENCODING = "gb18030"
MERGED_ENCODING = "utf-8-sig"
OUTPUT_ENCODING = "gb18030"

STUDY_START_YEAR = 2011
STUDY_END_YEAR = 2025
STUDY_YEAR_COUNT = STUDY_END_YEAR - STUDY_START_YEAR + 1
YEAR_BUCKETS = (
    ("paper_count_2011_2015", 2011, 2015),
    ("paper_count_2016_2020", 2016, 2020),
    ("paper_count_2021_2025", 2021, 2025),
)

TOP100_FIELDS = ["rank_top100", "institution_norm", "count", "cnki_count", "wos_count", "cscd_count"]
IMPACT_FIELDS = [
    "rank_top100",
    "institution_norm",
    "occurrence_count",
    "occurrence_cnki_count",
    "occurrence_wos_count",
    "occurrence_cscd_count",
    "distinct_paper_count",
    "distinct_cnki_paper_count",
    "distinct_wos_paper_count",
    "distinct_cscd_paper_count",
    "first_paper_year",
    "latest_paper_year",
    "active_year_count",
    "avg_annual_paper_count",
    "paper_count_2011_2015",
    "paper_count_2016_2020",
    "paper_count_2021_2025",
    "recent_paper_ratio_2021_2025",
    "total_citations",
    "avg_citations_per_paper",
    "median_citations_per_paper",
    "max_citations",
    "h_index",
    "cited_paper_count",
    "cited_paper_ratio",
    "uncited_paper_count",
    "uncited_paper_ratio",
    "high_cited_paper_count",
    "high_cited_paper_ratio",
    "collaboration_paper_count",
    "collaboration_paper_ratio",
    "partner_institution_count",
    "avg_partner_institutions_per_collab_paper",
    "primary_country_region",
    "international_collaboration_paper_count",
    "international_collaboration_paper_ratio",
    "international_partner_institution_count",
    "partner_country_region_count",
]

RE_WOS_AUTHOR = re.compile(r"\[[^\]]+\]\s*")
RE_MULTI_SPACE = re.compile(r"\s+")
RE_CJK = re.compile(r"[\u4e00-\u9fff]")

COUNTRY_ALIASES = {
    "PEOPLES R CHINA": "China",
    "PEOPLES REPUBLIC OF CHINA": "China",
    "P R CHINA": "China",
    "PR CHINA": "China",
    "CHINA": "China",
    "HONG KONG": "Hong Kong",
    "MACAU": "Macau",
    "MACAO": "Macau",
    "TAIWAN": "Taiwan",
    "USA": "United States",
    "U S A": "United States",
    "UNITED STATES": "United States",
    "UNITED STATES OF AMERICA": "United States",
    "ENGLAND": "United Kingdom",
    "SCOTLAND": "United Kingdom",
    "WALES": "United Kingdom",
    "NORTHERN IRELAND": "United Kingdom",
    "UK": "United Kingdom",
    "UNITED KINGDOM": "United Kingdom",
    "U ARAB EMIRATES": "United Arab Emirates",
    "UNITED ARAB EMIRATES": "United Arab Emirates",
    "UAE": "United Arab Emirates",
    "VIET NAM": "Vietnam",
    "REPUBLIC OF KOREA": "South Korea",
    "KOREA": "South Korea",
    "SOUTH KOREA": "South Korea",
    "RUSSIAN FEDERATION": "Russia",
    "CZECH REPUBLIC": "Czechia",
    "IRAN": "Iran",
    "INDIA": "India",
    "GERMANY": "Germany",
    "CANADA": "Canada",
    "JAPAN": "Japan",
    "AUSTRALIA": "Australia",
    "SPAIN": "Spain",
    "FRANCE": "France",
    "RUSSIA": "Russia",
    "ITALY": "Italy",
    "BRAZIL": "Brazil",
    "SAUDI ARABIA": "Saudi Arabia",
    "NETHERLANDS": "Netherlands",
    "MALAYSIA": "Malaysia",
    "SWEDEN": "Sweden",
    "NORWAY": "Norway",
    "TURKEY": "Turkey",
    "SINGAPORE": "Singapore",
    "BELGIUM": "Belgium",
    "PORTUGAL": "Portugal",
    "MEXICO": "Mexico",
    "DENMARK": "Denmark",
    "POLAND": "Poland",
    "EGYPT": "Egypt",
    "THAILAND": "Thailand",
    "FINLAND": "Finland",
    "PAKISTAN": "Pakistan",
    "SWITZERLAND": "Switzerland",
    "AUSTRIA": "Austria",
    "GREECE": "Greece",
    "SOUTH AFRICA": "South Africa",
    "VIETNAM": "Vietnam",
    "ARGENTINA": "Argentina",
    "INDONESIA": "Indonesia",
    "NEW ZEALAND": "New Zealand",
    "QATAR": "Qatar",
    "OMAN": "Oman",
    "KUWAIT": "Kuwait",
    "IRAQ": "Iraq",
    "CHILE": "Chile",
    "COLOMBIA": "Colombia",
    "PERU": "Peru",
    "ROMANIA": "Romania",
    "HUNGARY": "Hungary",
    "IRELAND": "Ireland",
    "ISRAEL": "Israel",
    "BANGLADESH": "Bangladesh",
    "PHILIPPINES": "Philippines",
    "NIGERIA": "Nigeria",
    "ALGERIA": "Algeria",
    "MOROCCO": "Morocco",
    "SLOVAKIA": "Slovakia",
    "SLOVENIA": "Slovenia",
    "SERBIA": "Serbia",
    "CROATIA": "Croatia",
    "LUXEMBOURG": "Luxembourg",
    "KAZAKHSTAN": "Kazakhstan",
    "UZBEKISTAN": "Uzbekistan",
}
COUNTRY_SUFFIXES = sorted(COUNTRY_ALIASES, key=len, reverse=True)

COUNTRY_LABELS_ZH = {
    "China": "\u4e2d\u56fd",
    "Hong Kong": "\u4e2d\u56fd\u9999\u6e2f",
    "Macau": "\u4e2d\u56fd\u6fb3\u95e8",
    "Taiwan": "\u4e2d\u56fd\u53f0\u6e7e",
    "United States": "\u7f8e\u56fd",
    "United Kingdom": "\u82f1\u56fd",
    "Australia": "\u6fb3\u5927\u5229\u4e9a",
    "Canada": "\u52a0\u62ff\u5927",
    "Germany": "\u5fb7\u56fd",
    "Japan": "\u65e5\u672c",
    "South Korea": "\u97e9\u56fd",
    "Singapore": "\u65b0\u52a0\u5761",
    "Russia": "\u4fc4\u7f57\u65af",
    "France": "\u6cd5\u56fd",
    "Italy": "\u610f\u5927\u5229",
    "Spain": "\u897f\u73ed\u7259",
    "Netherlands": "\u8377\u5170",
    "Saudi Arabia": "\u6c99\u7279\u963f\u62c9\u4f2f",
    "India": "\u5370\u5ea6",
    "Malaysia": "\u9a6c\u6765\u897f\u4e9a",
    "Thailand": "\u6cf0\u56fd",
    "Iran": "\u4f0a\u6717",
    "United Arab Emirates": "\u963f\u8054\u914b",
    "Denmark": "\u4e39\u9ea6",
    "Norway": "\u632a\u5a01",
}


def to_int(value: object) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        return int(text)
    except ValueError:
        try:
            return int(float(text))
        except ValueError:
            return 0


def to_float(value: object) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def round4(value: float) -> float:
    return round(value, 4)


def h_index(citations: list[float]) -> int:
    ordered = sorted(citations, reverse=True)
    h = 0
    for i, citation in enumerate(ordered, 1):
        if citation >= i:
            h = i
        else:
            break
    return h


def percentile_nearest_rank(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(1, math.ceil(p * len(ordered)))
    return ordered[rank - 1]


def normalize_country_token(value: str) -> str:
    text = compact_text(value).strip(" ,;.")
    if not text:
        return ""
    text = text.replace(".", " ")
    text = text.replace("People's", "Peoples")
    text = text.replace("PEOPLE'S", "PEOPLES")
    text = RE_MULTI_SPACE.sub(" ", text)
    return text.strip().upper()


def match_country_suffix(text: str) -> str:
    for suffix in COUNTRY_SUFFIXES:
        if text == suffix or text.endswith(f" {suffix}") or text.endswith(f", {suffix}"):
            return COUNTRY_ALIASES[suffix]
    return ""


def canonical_country_from_block(block: str) -> str:
    normalized = normalize_country_token(block)
    if not normalized:
        return ""

    matched = match_country_suffix(normalized)
    if matched:
        return matched

    pieces = [piece.strip() for piece in compact_text(block).split(",") if piece.strip()]
    if pieces:
        matched = match_country_suffix(normalize_country_token(pieces[-1]))
        if matched:
            return matched

    tail_match = re.search(r"([A-Za-z][A-Za-z .'-]+)$", compact_text(block))
    if tail_match:
        tail = tail_match.group(1).strip()
        matched = match_country_suffix(normalize_country_token(tail))
        if matched:
            return matched
        return RE_MULTI_SPACE.sub(" ", tail)

    return ""


def country_label(country: str) -> str:
    if not country:
        return ""
    return COUNTRY_LABELS_ZH.get(country, country)


def load_top100(norm_freq_path: Path) -> list[dict[str, int | str]]:
    top100: list[dict[str, int | str]] = []
    with norm_freq_path.open("r", encoding=NORM_FREQ_ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        for rank, row in enumerate(reader, 1):
            if rank > 100:
                break
            top100.append(
                {
                    "rank_top100": rank,
                    "institution_norm": (row.get("institution_norm") or "").strip(),
                    "count": to_int(row.get("count")),
                    "cnki_count": to_int(row.get("cnki_count")),
                    "wos_count": to_int(row.get("wos_count")),
                    "cscd_count": to_int(row.get("cscd_count")),
                }
            )
    return top100


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding=OUTPUT_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def load_raw_to_norm_map(path: Path) -> dict[str, str]:
    mapping: dict[str, str] = {}
    with path.open("r", encoding=TRANS_NORM_ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_name = (row.get("institution_name") or "").strip()
            norm_name = (row.get("institution_norm") or "").strip()
            if raw_name:
                mapping[raw_name] = norm_name
    return mapping


def parse_serialized_country_pairs(value: str) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in [chunk.strip() for chunk in str(value or "").split("||") if chunk.strip()]:
        if "@@" in item:
            raw_name, country = item.split("@@", 1)
        else:
            raw_name, country = item, ""
        pair = (raw_name.strip(), country.strip())
        if pair[0] and pair not in seen:
            pairs.append(pair)
            seen.add(pair)
    return pairs


def parse_institution_country_pairs(
    source_db: str,
    institution: str,
    institution_extracted: str,
    institution_country_pairs: str = "",
) -> list[tuple[str, str]]:
    source = str(source_db or "").strip().upper()
    if source == "MERGED":
        pairs = parse_serialized_country_pairs(institution_country_pairs)
        if pairs:
            return pairs
        names = sorted(set(extract_institutions(source, institution, institution_extracted)))
        return [(name, "") for name in names if name]

    if source == "CNKI":
        names = sorted(set(extract_institutions(source, institution, institution_extracted)))
        return [(name, "China") for name in names if name]

    if source == "CSCD":
        names = sorted(set(extract_institutions(source, institution, institution_extracted)))
        return [(name, "China") for name in names if name]

    if source != "WOS":
        return []

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
        item = (raw_name, country)
        if item not in seen:
            pairs.append(item)
            seen.add(item)

    return pairs


def build_primary_country_map(
    merged_path: Path,
    raw_to_norm: dict[str, str],
    top100_rows: list[dict[str, int | str]],
) -> dict[str, str]:
    top100_set = {str(row["institution_norm"]) for row in top100_rows}
    tallies: dict[str, Counter[str]] = {institution: Counter() for institution in top100_set}

    with merged_path.open("r", encoding=MERGED_ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pairs = parse_institution_country_pairs(
                str(row.get("source_db") or ""),
                str(row.get("institution") or ""),
                str(row.get("institution_extracted") or ""),
                str(row.get("institution_country_pairs") or ""),
            )
            if not pairs:
                continue
            for raw_name, country in pairs:
                norm_name = raw_to_norm.get(raw_name, "").strip()
                if norm_name in tallies and country:
                    tallies[norm_name][country] += 1

    primary_country_map: dict[str, str] = {}
    for institution in top100_set:
        counter = tallies[institution]
        if counter:
            primary_country_map[institution] = counter.most_common(1)[0][0]
        elif RE_CJK.search(institution):
            primary_country_map[institution] = "China"
        else:
            primary_country_map[institution] = "Unknown"

    return primary_country_map


def build_indicators(
    merged_path: Path,
    raw_to_norm: dict[str, str],
    top100_rows: list[dict[str, int | str]],
    primary_country_map: dict[str, str],
) -> tuple[list[dict[str, object]], float]:
    top100_set = {str(row["institution_norm"]) for row in top100_rows}
    metrics: dict[str, dict[str, object]] = {
        institution: {
            "distinct_paper_count": 0,
            "distinct_cnki_paper_count": 0,
            "distinct_wos_paper_count": 0,
            "distinct_cscd_paper_count": 0,
            "total_citations": 0.0,
            "citations": [],
            "years": set(),
            "paper_count_2011_2015": 0,
            "paper_count_2016_2020": 0,
            "paper_count_2021_2025": 0,
            "cited_paper_count": 0,
            "uncited_paper_count": 0,
            "collaboration_paper_count": 0,
            "partners": set(),
            "partner_total": 0,
            "international_collaboration_paper_count": 0,
            "international_partners": set(),
            "partner_countries": set(),
        }
        for institution in top100_set
    }
    all_citations: list[float] = []
    papers_with_top100_hit = 0

    with merged_path.open("r", encoding=MERGED_ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            source_db = str(row.get("source_db") or "").strip().upper()
            source_db_primary = str(row.get("source_db_primary") or row.get("source_db") or "").strip().upper()
            cited_count = to_float(row.get("cited_count"))
            all_citations.append(cited_count)

            names = extract_institutions(
                source_db,
                str(row.get("institution") or ""),
                str(row.get("institution_extracted") or ""),
            )
            norm_names = {
                raw_to_norm.get(name, "").strip()
                for name in names
                if raw_to_norm.get(name, "").strip()
            }
            if not norm_names:
                continue

            top_hits = norm_names & top100_set
            if not top_hits:
                continue

            papers_with_top100_hit += 1
            year = to_int(row.get("year"))
            is_collaboration = len(norm_names) >= 2

            norm_country_map: dict[str, set[str]] = defaultdict(set)
            for raw_name, country in parse_institution_country_pairs(
                source_db,
                str(row.get("institution") or ""),
                str(row.get("institution_extracted") or ""),
                str(row.get("institution_country_pairs") or ""),
            ):
                norm_name = raw_to_norm.get(raw_name, "").strip()
                if norm_name and country:
                    norm_country_map[norm_name].add(country)

            for norm_name in norm_names:
                if norm_name in primary_country_map and not norm_country_map.get(norm_name):
                    primary_country = primary_country_map[norm_name]
                    if primary_country and primary_country != "Unknown":
                        norm_country_map[norm_name].add(primary_country)

            for institution in top_hits:
                bucket = metrics[institution]
                bucket["distinct_paper_count"] = int(bucket["distinct_paper_count"]) + 1

                source_key = f"distinct_{source_db_primary.lower()}_paper_count"
                if source_key in bucket:
                    bucket[source_key] = int(bucket[source_key]) + 1

                bucket["total_citations"] = float(bucket["total_citations"]) + cited_count
                bucket["citations"].append(cited_count)

                if STUDY_START_YEAR <= year <= STUDY_END_YEAR:
                    bucket["years"].add(year)
                    for field_name, start_year, end_year in YEAR_BUCKETS:
                        if start_year <= year <= end_year:
                            bucket[field_name] = int(bucket[field_name]) + 1

                if cited_count > 0:
                    bucket["cited_paper_count"] = int(bucket["cited_paper_count"]) + 1
                else:
                    bucket["uncited_paper_count"] = int(bucket["uncited_paper_count"]) + 1

                if is_collaboration:
                    partners = norm_names - {institution}
                    bucket["collaboration_paper_count"] = int(bucket["collaboration_paper_count"]) + 1
                    bucket["partners"].update(partners)
                    bucket["partner_total"] = int(bucket["partner_total"]) + len(partners)

                    home_country = primary_country_map.get(institution, "Unknown")
                    foreign_partners: set[str] = set()
                    foreign_countries: set[str] = set()

                    for partner in partners:
                        partner_countries = norm_country_map.get(partner, set())
                        for country in partner_countries:
                            if country and country != home_country:
                                foreign_partners.add(partner)
                                foreign_countries.add(country)

                    if foreign_countries:
                        bucket["international_collaboration_paper_count"] = (
                            int(bucket["international_collaboration_paper_count"]) + 1
                        )
                        bucket["international_partners"].update(foreign_partners)
                        bucket["partner_countries"].update(foreign_countries)

    high_cited_threshold = percentile_nearest_rank(all_citations, 0.9)
    result_rows: list[dict[str, object]] = []

    for row in top100_rows:
        institution = str(row["institution_norm"])
        bucket = metrics[institution]
        paper_count = int(bucket["distinct_paper_count"])
        citation_values: list[float] = list(bucket["citations"])
        total_citations = float(bucket["total_citations"])
        high_cited_count = sum(1 for value in citation_values if value >= high_cited_threshold)
        collaboration_paper_count = int(bucket["collaboration_paper_count"])
        international_collaboration_paper_count = int(bucket["international_collaboration_paper_count"])
        years = sorted(int(year) for year in bucket["years"])

        result_rows.append(
            {
                "rank_top100": row["rank_top100"],
                "institution_norm": institution,
                "occurrence_count": row["count"],
                "occurrence_cnki_count": row["cnki_count"],
                "occurrence_wos_count": row["wos_count"],
                "occurrence_cscd_count": row["cscd_count"],
                "distinct_paper_count": paper_count,
                "distinct_cnki_paper_count": int(bucket["distinct_cnki_paper_count"]),
                "distinct_wos_paper_count": int(bucket["distinct_wos_paper_count"]),
                "distinct_cscd_paper_count": int(bucket["distinct_cscd_paper_count"]),
                "first_paper_year": years[0] if years else 0,
                "latest_paper_year": years[-1] if years else 0,
                "active_year_count": len(years),
                "avg_annual_paper_count": round4(paper_count / STUDY_YEAR_COUNT) if paper_count else 0.0,
                "paper_count_2011_2015": int(bucket["paper_count_2011_2015"]),
                "paper_count_2016_2020": int(bucket["paper_count_2016_2020"]),
                "paper_count_2021_2025": int(bucket["paper_count_2021_2025"]),
                "recent_paper_ratio_2021_2025": (
                    round4(int(bucket["paper_count_2021_2025"]) / paper_count) if paper_count else 0.0
                ),
                "total_citations": round4(total_citations),
                "avg_citations_per_paper": round4(total_citations / paper_count) if paper_count else 0.0,
                "median_citations_per_paper": round4(statistics.median(citation_values)) if citation_values else 0.0,
                "max_citations": round4(max(citation_values)) if citation_values else 0.0,
                "h_index": h_index(citation_values),
                "cited_paper_count": int(bucket["cited_paper_count"]),
                "cited_paper_ratio": round4(int(bucket["cited_paper_count"]) / paper_count) if paper_count else 0.0,
                "uncited_paper_count": int(bucket["uncited_paper_count"]),
                "uncited_paper_ratio": (
                    round4(int(bucket["uncited_paper_count"]) / paper_count) if paper_count else 0.0
                ),
                "high_cited_paper_count": high_cited_count,
                "high_cited_paper_ratio": round4(high_cited_count / paper_count) if paper_count else 0.0,
                "collaboration_paper_count": collaboration_paper_count,
                "collaboration_paper_ratio": (
                    round4(collaboration_paper_count / paper_count) if paper_count else 0.0
                ),
                "partner_institution_count": len(bucket["partners"]),
                "avg_partner_institutions_per_collab_paper": (
                    round4(int(bucket["partner_total"]) / collaboration_paper_count)
                    if collaboration_paper_count
                    else 0.0
                ),
                "primary_country_region": country_label(primary_country_map.get(institution, "Unknown")),
                "international_collaboration_paper_count": international_collaboration_paper_count,
                "international_collaboration_paper_ratio": (
                    round4(international_collaboration_paper_count / paper_count) if paper_count else 0.0
                ),
                "international_partner_institution_count": len(bucket["international_partners"]),
                "partner_country_region_count": len(bucket["partner_countries"]),
            }
        )

    print(f"papers_with_top100_hit={papers_with_top100_hit}")
    return result_rows, high_cited_threshold


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--norm-freq", default=str(NORM_FREQ_PATH))
    parser.add_argument("--trans-norm", default=str(TRANS_NORM_PATH))
    parser.add_argument("--merged", default=str(MERGED_PATH))
    parser.add_argument("--top100-out", default=str(TOP100_OUTPUT_PATH))
    parser.add_argument("--impact-out", default=str(IMPACT_OUTPUT_PATH))
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    top100_rows = load_top100(Path(args.norm_freq))
    write_csv(Path(args.top100_out), TOP100_FIELDS, top100_rows)

    raw_to_norm = load_raw_to_norm_map(Path(args.trans_norm))
    primary_country_map = build_primary_country_map(Path(args.merged), raw_to_norm, top100_rows)
    impact_rows, high_cited_threshold = build_indicators(
        Path(args.merged),
        raw_to_norm,
        top100_rows,
        primary_country_map,
    )
    write_csv(Path(args.impact_out), IMPACT_FIELDS, impact_rows)

    print(f"top100_rows={len(top100_rows)}")
    print(f"high_cited_threshold_p90={high_cited_threshold}")
    print(f"top100_output={args.top100_out}")
    print(f"impact_output={args.impact_out}")


if __name__ == "__main__":
    main()
