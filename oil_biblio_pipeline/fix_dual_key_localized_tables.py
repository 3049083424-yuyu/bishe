from __future__ import annotations

import csv
from pathlib import Path


ENCODING = "gb18030"

BASE_DIR = Path("D:/毕业论文/version_dual_key_dedup_2011_2025")


CORE_TOP100_HEADERS = {
    "rank_top100": "Top100排名",
    "institution_norm": "标准化机构名称",
    "count": "机构出现总次数",
    "cnki_count": "CNKI机构出现次数",
    "wos_count": "WOS机构出现次数",
    "cscd_count": "CSCD机构出现次数",
}

IMPACT_HEADERS = {
    "rank_top100": "Top100排名",
    "institution_norm": "标准化机构名称",
    "occurrence_count": "机构出现总次数",
    "occurrence_cnki_count": "CNKI机构出现次数",
    "occurrence_wos_count": "WOS机构出现次数",
    "occurrence_cscd_count": "CSCD机构出现次数",
    "distinct_paper_count": "去重论文总数",
    "distinct_cnki_paper_count": "CNKI去重论文数",
    "distinct_wos_paper_count": "WOS去重论文数",
    "distinct_cscd_paper_count": "CSCD去重论文数",
    "first_paper_year": "首次发文年份",
    "latest_paper_year": "最近发文年份",
    "active_year_count": "活跃年份数",
    "avg_annual_paper_count": "年均发文量",
    "paper_count_2011_2015": "2011-2015发文量",
    "paper_count_2016_2020": "2016-2020发文量",
    "paper_count_2021_2025": "2021-2025发文量",
    "recent_paper_ratio_2021_2025": "近五年发文占比",
    "total_citations": "总被引频次",
    "avg_citations_per_paper": "篇均被引频次",
    "median_citations_per_paper": "篇均被引中位数",
    "max_citations": "单篇最高被引频次",
    "h_index": "H指数",
    "cited_paper_count": "有被引论文数",
    "cited_paper_ratio": "有被引论文占比",
    "uncited_paper_count": "未被引论文数",
    "uncited_paper_ratio": "未被引论文占比",
    "high_cited_paper_count": "高被引论文数",
    "high_cited_paper_ratio": "高被引论文占比",
    "collaboration_paper_count": "合作论文数",
    "collaboration_paper_ratio": "合作论文占比",
    "partner_institution_count": "合作机构数",
    "avg_partner_institutions_per_collab_paper": "单篇合作论文平均合作机构数",
    "primary_country_region": "机构主属国家/地区",
    "international_collaboration_paper_count": "国际合作论文数",
    "international_collaboration_paper_ratio": "国际合作论文占比",
    "international_partner_institution_count": "国际合作机构数",
    "partner_country_region_count": "合作国家/地区数",
}

TOPSIS_SCORE_HEADERS = {
    "rank_top100": "Top100原始入选排名",
    "institution_norm": "标准化机构名称",
    "topsis_score": "TOPSIS综合得分",
    "topsis_rank": "TOPSIS综合排名",
    "research_output_subscore": "科研产出子得分",
    "academic_impact_subscore": "学术影响子得分",
    "collaboration_international_subscore": "合作与国际化子得分",
    "distinct_paper_count": "去重论文总数",
    "recent_paper_ratio_2021_2025": "近五年发文占比",
    "h_index": "H指数",
    "high_cited_paper_ratio": "高被引论文占比",
    "collaboration_paper_ratio": "合作论文占比",
    "international_collaboration_paper_ratio": "国际合作论文占比",
    "partner_country_region_count": "合作国家/地区数",
}

SELECTION_HEADERS = [
    "指标字段名",
    "指标中文名",
    "所属维度代码",
    "所属维度",
    "指标方向",
    "是否纳入TOPSIS",
    "筛选说明",
]

WEIGHT_HEADERS = [
    "指标字段名",
    "指标中文名",
    "所属维度代码",
    "所属维度",
    "指标方向",
    "先验权重",
    "熵权法权重",
    "CRITIC法权重",
    "组合权重",
]


def read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding=ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        return reader.fieldnames or [], list(reader)


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding=ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def rewrite_simple_table(source_name: str, target_name: str, header_map: dict[str, str]) -> None:
    _, rows = read_csv(BASE_DIR / source_name)
    rewritten = [{header_map[key]: row.get(key, "") for key in header_map} for row in rows]
    write_csv(BASE_DIR / target_name, list(header_map.values()), rewritten)


def normalize_direction(value: str) -> str:
    if value == "benefit":
        return "正向"
    if value == "cost":
        return "逆向"
    return value


def normalize_selected(value: str) -> str:
    return "是" if str(value).strip() in {"1", "true", "True", "是"} else "否"


def rewrite_selection_table() -> None:
    _, rows = read_csv(BASE_DIR / "institution_topsis_indicator_selection_top100_dual_key_2011_2025.csv")
    rewritten: list[dict[str, str]] = []
    for row in rows:
        rewritten.append(
            {
                "指标字段名": row.get("field", ""),
                "指标中文名": row.get("label_zh", ""),
                "所属维度代码": row.get("dimension", ""),
                "所属维度": row.get("dimension_zh", ""),
                "指标方向": normalize_direction(row.get("direction", "")),
                "是否纳入TOPSIS": normalize_selected(row.get("selected", "")),
                "筛选说明": row.get("selection_reason", ""),
            }
        )
    write_csv(
        BASE_DIR / "institution_topsis_indicator_selection_top100_dual_key_2011_2025_zh.csv",
        SELECTION_HEADERS,
        rewritten,
    )


def rewrite_weight_table() -> None:
    _, rows = read_csv(BASE_DIR / "institution_weight_scheme_top100_dual_key_2011_2025.csv")
    rewritten: list[dict[str, str]] = []
    for row in rows:
        rewritten.append(
            {
                "指标字段名": row.get("field", ""),
                "指标中文名": row.get("label_zh", ""),
                "所属维度代码": row.get("dimension", ""),
                "所属维度": row.get("dimension_zh", ""),
                "指标方向": normalize_direction(row.get("direction", "")),
                "先验权重": row.get("prior_weight", ""),
                "熵权法权重": row.get("entropy_weight", ""),
                "CRITIC法权重": row.get("critic_weight", ""),
                "组合权重": row.get("combined_weight", ""),
            }
        )
    write_csv(
        BASE_DIR / "institution_weight_scheme_top100_dual_key_2011_2025_zh.csv",
        WEIGHT_HEADERS,
        rewritten,
    )


def main() -> None:
    rewrite_simple_table(
        "institution_core_top100_dual_key_2011_2025.csv",
        "institution_core_top100_dual_key_2011_2025_zh.csv",
        CORE_TOP100_HEADERS,
    )
    rewrite_simple_table(
        "institution_impact_indicator_top100_dual_key_2011_2025.csv",
        "institution_impact_indicator_top100_dual_key_2011_2025_zh.csv",
        IMPACT_HEADERS,
    )
    rewrite_simple_table(
        "institution_topsis_score_top100_dual_key_2011_2025.csv",
        "institution_topsis_score_top100_dual_key_2011_2025_zh.csv",
        TOPSIS_SCORE_HEADERS,
    )
    rewrite_selection_table()
    rewrite_weight_table()
    print("dual_key_localized_tables=fixed")


if __name__ == "__main__":
    main()
