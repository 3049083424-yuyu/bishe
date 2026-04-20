from __future__ import annotations

import csv
from pathlib import Path


BASE = Path(r"D:\毕业论文\version_dual_key_dedup_2011_2025")
ENC = "gb18030"


CORE_TOP100_HEADERS = {
    "rank_top100": "\u0054\u006f\u0070\u0031\u0030\u0030\u6392\u540d",
    "institution_norm": "\u6807\u51c6\u5316\u673a\u6784\u540d\u79f0",
    "count": "\u673a\u6784\u51fa\u73b0\u603b\u6b21\u6570",
    "cnki_count": "\u0043\u004e\u004b\u0049\u673a\u6784\u51fa\u73b0\u6b21\u6570",
    "wos_count": "\u0057\u004f\u0053\u673a\u6784\u51fa\u73b0\u6b21\u6570",
    "cscd_count": "\u0043\u0053\u0043\u0044\u673a\u6784\u51fa\u73b0\u6b21\u6570",
}

IMPACT_HEADERS = {
    "rank_top100": "\u0054\u006f\u0070\u0031\u0030\u0030\u6392\u540d",
    "institution_norm": "\u6807\u51c6\u5316\u673a\u6784\u540d\u79f0",
    "occurrence_count": "\u673a\u6784\u51fa\u73b0\u603b\u6b21\u6570",
    "occurrence_cnki_count": "\u0043\u004e\u004b\u0049\u673a\u6784\u51fa\u73b0\u6b21\u6570",
    "occurrence_wos_count": "\u0057\u004f\u0053\u673a\u6784\u51fa\u73b0\u6b21\u6570",
    "occurrence_cscd_count": "\u0043\u0053\u0043\u0044\u673a\u6784\u51fa\u73b0\u6b21\u6570",
    "distinct_paper_count": "\u53bb\u91cd\u8bba\u6587\u603b\u6570",
    "distinct_cnki_paper_count": "\u0043\u004e\u004b\u0049\u53bb\u91cd\u8bba\u6587\u6570",
    "distinct_wos_paper_count": "\u0057\u004f\u0053\u53bb\u91cd\u8bba\u6587\u6570",
    "distinct_cscd_paper_count": "\u0043\u0053\u0043\u0044\u53bb\u91cd\u8bba\u6587\u6570",
    "first_paper_year": "\u9996\u6b21\u53d1\u6587\u5e74\u4efd",
    "latest_paper_year": "\u6700\u8fd1\u53d1\u6587\u5e74\u4efd",
    "active_year_count": "\u6d3b\u8dc3\u5e74\u4efd\u6570",
    "avg_annual_paper_count": "\u5e74\u5747\u53d1\u6587\u91cf",
    "paper_count_2011_2015": "\u0032\u0030\u0031\u0031\u002d\u0032\u0030\u0031\u0035\u53d1\u6587\u91cf",
    "paper_count_2016_2020": "\u0032\u0030\u0031\u0036\u002d\u0032\u0030\u0032\u0030\u53d1\u6587\u91cf",
    "paper_count_2021_2025": "\u0032\u0030\u0032\u0031\u002d\u0032\u0030\u0032\u0035\u53d1\u6587\u91cf",
    "recent_paper_ratio_2021_2025": "\u8fd1\u4e94\u5e74\u53d1\u6587\u5360\u6bd4",
    "total_citations": "\u603b\u88ab\u5f15\u9891\u6b21",
    "avg_citations_per_paper": "\u7bc7\u5747\u88ab\u5f15\u9891\u6b21",
    "median_citations_per_paper": "\u7bc7\u5747\u88ab\u5f15\u4e2d\u4f4d\u6570",
    "max_citations": "\u5355\u7bc7\u6700\u9ad8\u88ab\u5f15\u9891\u6b21",
    "h_index": "\u0048\u6307\u6570",
    "cited_paper_count": "\u6709\u88ab\u5f15\u8bba\u6587\u6570",
    "cited_paper_ratio": "\u6709\u88ab\u5f15\u8bba\u6587\u5360\u6bd4",
    "uncited_paper_count": "\u672a\u88ab\u5f15\u8bba\u6587\u6570",
    "uncited_paper_ratio": "\u672a\u88ab\u5f15\u8bba\u6587\u5360\u6bd4",
    "high_cited_paper_count": "\u9ad8\u88ab\u5f15\u8bba\u6587\u6570",
    "high_cited_paper_ratio": "\u9ad8\u88ab\u5f15\u8bba\u6587\u5360\u6bd4",
    "collaboration_paper_count": "\u5408\u4f5c\u8bba\u6587\u6570",
    "collaboration_paper_ratio": "\u5408\u4f5c\u8bba\u6587\u5360\u6bd4",
    "partner_institution_count": "\u5408\u4f5c\u673a\u6784\u6570",
    "avg_partner_institutions_per_collab_paper": "\u5355\u7bc7\u5408\u4f5c\u8bba\u6587\u5e73\u5747\u5408\u4f5c\u673a\u6784\u6570",
    "primary_country_region": "\u673a\u6784\u4e3b\u5c5e\u56fd\u5bb6\u002f\u5730\u533a",
    "international_collaboration_paper_count": "\u56fd\u9645\u5408\u4f5c\u8bba\u6587\u6570",
    "international_collaboration_paper_ratio": "\u56fd\u9645\u5408\u4f5c\u8bba\u6587\u5360\u6bd4",
    "international_partner_institution_count": "\u56fd\u9645\u5408\u4f5c\u673a\u6784\u6570",
    "partner_country_region_count": "\u5408\u4f5c\u56fd\u5bb6\u002f\u5730\u533a\u6570",
}

TOPSIS_SCORE_HEADERS = {
    "rank_top100": "\u0054\u006f\u0070\u0031\u0030\u0030\u539f\u59cb\u5165\u9009\u6392\u540d",
    "institution_norm": "\u6807\u51c6\u5316\u673a\u6784\u540d\u79f0",
    "topsis_score": "\u0054\u004f\u0050\u0053\u0049\u0053\u7efc\u5408\u5f97\u5206",
    "topsis_rank": "\u0054\u004f\u0050\u0053\u0049\u0053\u7efc\u5408\u6392\u540d",
    "research_output_subscore": "\u79d1\u7814\u4ea7\u51fa\u5b50\u5f97\u5206",
    "academic_impact_subscore": "\u5b66\u672f\u5f71\u54cd\u5b50\u5f97\u5206",
    "collaboration_international_subscore": "\u5408\u4f5c\u4e0e\u56fd\u9645\u5316\u5b50\u5f97\u5206",
    "distinct_paper_count": "\u53bb\u91cd\u8bba\u6587\u603b\u6570",
    "recent_paper_ratio_2021_2025": "\u8fd1\u4e94\u5e74\u53d1\u6587\u5360\u6bd4",
    "h_index": "\u0048\u6307\u6570",
    "high_cited_paper_ratio": "\u9ad8\u88ab\u5f15\u8bba\u6587\u5360\u6bd4",
    "collaboration_paper_ratio": "\u5408\u4f5c\u8bba\u6587\u5360\u6bd4",
    "international_collaboration_paper_ratio": "\u56fd\u9645\u5408\u4f5c\u8bba\u6587\u5360\u6bd4",
    "partner_country_region_count": "\u5408\u4f5c\u56fd\u5bb6\u002f\u5730\u533a\u6570",
}

WEIGHT_HEADERS = [
    "\u6307\u6807\u5b57\u6bb5\u540d",
    "\u6307\u6807\u4e2d\u6587\u540d",
    "\u6240\u5c5e\u7ef4\u5ea6\u4ee3\u7801",
    "\u6240\u5c5e\u7ef4\u5ea6",
    "\u6307\u6807\u65b9\u5411",
    "\u5148\u9a8c\u6743\u91cd",
    "\u71b5\u6743\u6cd5\u6743\u91cd",
    "\u0043\u0052\u0049\u0054\u0049\u0043\u6cd5\u6743\u91cd",
    "\u7ec4\u5408\u6743\u91cd",
]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding=ENC, newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding=ENC, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def rewrite_simple(src_name: str, dst_name: str, mapping: dict[str, str]) -> None:
    rows = read_csv(BASE / src_name)
    out_rows = [{mapping[key]: row.get(key, "") for key in mapping} for row in rows]
    write_csv(BASE / dst_name, list(mapping.values()), out_rows)


def rewrite_weight_table() -> None:
    rows = read_csv(BASE / "institution_weight_scheme_top100_dual_key_2011_2025.csv")
    out_rows: list[dict[str, object]] = []
    for row in rows:
        out_rows.append(
            {
                "\u6307\u6807\u5b57\u6bb5\u540d": row.get("field", ""),
                "\u6307\u6807\u4e2d\u6587\u540d": row.get("label_zh", ""),
                "\u6240\u5c5e\u7ef4\u5ea6\u4ee3\u7801": row.get("dimension", ""),
                "\u6240\u5c5e\u7ef4\u5ea6": row.get("dimension_zh", ""),
                "\u6307\u6807\u65b9\u5411": "\u6b63\u5411" if row.get("direction", "") == "benefit" else row.get("direction", ""),
                "\u5148\u9a8c\u6743\u91cd": row.get("prior_weight", ""),
                "\u71b5\u6743\u6cd5\u6743\u91cd": row.get("entropy_weight", ""),
                "\u0043\u0052\u0049\u0054\u0049\u0043\u6cd5\u6743\u91cd": row.get("critic_weight", ""),
                "\u7ec4\u5408\u6743\u91cd": row.get("combined_weight", ""),
            }
        )
    write_csv(BASE / "institution_weight_scheme_top100_dual_key_2011_2025_zh.csv", WEIGHT_HEADERS, out_rows)


def main() -> None:
    rewrite_simple(
        "institution_core_top100_dual_key_2011_2025.csv",
        "institution_core_top100_dual_key_2011_2025_zh.csv",
        CORE_TOP100_HEADERS,
    )
    rewrite_simple(
        "institution_impact_indicator_top100_dual_key_2011_2025.csv",
        "institution_impact_indicator_top100_dual_key_2011_2025_zh.csv",
        IMPACT_HEADERS,
    )
    rewrite_simple(
        "institution_topsis_score_top100_dual_key_2011_2025.csv",
        "institution_topsis_score_top100_dual_key_2011_2025_zh.csv",
        TOPSIS_SCORE_HEADERS,
    )
    rewrite_weight_table()
    print("dual_key_zh_tables_refreshed=core,impact,score,weight")


if __name__ == "__main__":
    main()
