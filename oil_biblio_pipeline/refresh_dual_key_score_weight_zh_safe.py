from __future__ import annotations

import csv
from pathlib import Path


BASE = Path(r"D:\毕业论文\version_dual_key_dedup_2011_2025")
ENC = "gb18030"


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


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding=ENC, newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding=ENC, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def rewrite_score_table() -> None:
    src = BASE / "institution_topsis_score_top100_dual_key_2011_2025.csv"
    dst = BASE / "institution_topsis_score_top100_dual_key_2011_2025_zh.csv"
    rows = read_csv(src)
    out_rows = [{TOPSIS_SCORE_HEADERS[key]: row.get(key, "") for key in TOPSIS_SCORE_HEADERS} for row in rows]
    write_csv(dst, list(TOPSIS_SCORE_HEADERS.values()), out_rows)


def rewrite_weight_table() -> None:
    src = BASE / "institution_weight_scheme_top100_dual_key_2011_2025.csv"
    dst = BASE / "institution_weight_scheme_top100_dual_key_2011_2025_zh.csv"
    rows = read_csv(src)
    out_rows: list[dict[str, object]] = []
    for row in rows:
        out_rows.append(
            {
                "指标字段名": row.get("field", ""),
                "指标中文名": row.get("label_zh", ""),
                "所属维度代码": row.get("dimension", ""),
                "所属维度": row.get("dimension_zh", ""),
                "指标方向": "正向" if row.get("direction", "") == "benefit" else row.get("direction", ""),
                "先验权重": row.get("prior_weight", ""),
                "熵权法权重": row.get("entropy_weight", ""),
                "CRITIC法权重": row.get("critic_weight", ""),
                "组合权重": row.get("combined_weight", ""),
            }
        )
    write_csv(dst, WEIGHT_HEADERS, out_rows)


def main() -> None:
    rewrite_score_table()
    rewrite_weight_table()
    print("dual_key_zh_tables_refreshed=score,weight")


if __name__ == "__main__":
    main()
