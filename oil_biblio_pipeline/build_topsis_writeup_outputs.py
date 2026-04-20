from __future__ import annotations

import argparse
import csv
from pathlib import Path


BASE_DATA_DIR = Path("D:/毕业论文")

SCORE_PATH = BASE_DATA_DIR / "institution_topsis_score_top100_2011_2025.csv"
IMPACT_PATH = BASE_DATA_DIR / "institution_impact_indicator_top100_2011_2025.csv"
WEIGHT_PATH = BASE_DATA_DIR / "institution_weight_scheme_top100_2011_2025.csv"

TOP20_OUTPUT_PATH = BASE_DATA_DIR / "institution_topsis_top20_2011_2025.csv"
DIMENSION_OUTPUT_PATH = BASE_DATA_DIR / "institution_dimension_top10_2011_2025.csv"
DRAFT_OUTPUT_PATH = BASE_DATA_DIR / "thesis_method_results_draft_2011_2025.md"

INPUT_ENCODING = "gb18030"
OUTPUT_ENCODING = "gb18030"

ALIASES = {
    "institution_norm": "标准化机构名称",
    "primary_country_region": "主属国家/地区",
    "topsis_rank": "TOPSIS综合排名",
    "topsis_score": "TOPSIS综合得分",
    "research_output_subscore": "科研产出子得分",
    "academic_impact_subscore": "学术影响子得分",
    "collaboration_international_subscore": "合作与国际化子得分",
    "dominant_dimension": "主导优势维度",
    "distinct_paper_count": "去重论文总数",
    "h_index": "H指数",
    "high_cited_paper_ratio": "高被引论文占比",
    "international_collaboration_paper_ratio": "国际合作论文占比",
    "rank_top100": "Top100原始入选排名",
    "label_zh": "指标名称",
    "combined_weight": "组合权重",
}

TOP20_FIELDS = [
    "TOPSIS综合排名",
    "标准化机构名称",
    "主属国家/地区",
    "TOPSIS综合得分",
    "科研产出子得分",
    "学术影响子得分",
    "合作与国际化子得分",
    "主导优势维度",
    "去重论文总数",
    "H指数",
    "高被引论文占比",
    "国际合作论文占比",
]

DIMENSION_FIELDS = [
    "榜单类型",
    "榜内排名",
    "标准化机构名称",
    "主属国家/地区",
    "TOPSIS综合排名",
    "得分指标",
    "得分值",
    "备注",
]

DIMENSION_META = {
    "overall": ("综合排名", "topsis_score"),
    "research_output": ("科研产出维度", "research_output_subscore"),
    "academic_impact": ("学术影响维度", "academic_impact_subscore"),
    "collaboration_international": ("合作与国际化维度", "collaboration_international_subscore"),
}

DOMINANT_LABELS = {
    "research_output_subscore": "科研产出",
    "academic_impact_subscore": "学术影响",
    "collaboration_international_subscore": "合作与国际化",
}

SCORE_FIELD_NAMES = {
    "topsis_score": "TOPSIS综合得分",
    "research_output_subscore": "科研产出子得分",
    "academic_impact_subscore": "学术影响子得分",
    "collaboration_international_subscore": "合作与国际化子得分",
}


def to_float(value: object) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding=INPUT_ENCODING, newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding=OUTPUT_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def row_value(row: dict[str, str], field: str) -> str:
    return row.get(field, row.get(ALIASES.get(field, ""), ""))


def dominant_dimension(row: dict[str, str]) -> str:
    candidates = [
        "research_output_subscore",
        "academic_impact_subscore",
        "collaboration_international_subscore",
    ]
    best_field = max(candidates, key=lambda field: to_float(row_value(row, field)))
    return DOMINANT_LABELS[best_field]


def build_top20_rows(
    score_rows: list[dict[str, str]],
    impact_map: dict[str, dict[str, str]],
) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for row in score_rows[:20]:
        institution_name = row_value(row, "institution_norm")
        impact_row = impact_map.get(institution_name, {})
        result.append(
            {
                "TOPSIS综合排名": row_value(row, "topsis_rank"),
                "标准化机构名称": institution_name,
                "主属国家/地区": row_value(impact_row, "primary_country_region"),
                "TOPSIS综合得分": row_value(row, "topsis_score"),
                "科研产出子得分": row_value(row, "research_output_subscore"),
                "学术影响子得分": row_value(row, "academic_impact_subscore"),
                "合作与国际化子得分": row_value(row, "collaboration_international_subscore"),
                "主导优势维度": dominant_dimension(row),
                "去重论文总数": row_value(row, "distinct_paper_count"),
                "H指数": row_value(row, "h_index"),
                "高被引论文占比": row_value(row, "high_cited_paper_ratio"),
                "国际合作论文占比": row_value(row, "international_collaboration_paper_ratio"),
            }
        )
    return result


def build_dimension_rows(
    score_rows: list[dict[str, str]],
    impact_map: dict[str, dict[str, str]],
) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for list_type, (list_name, score_field) in DIMENSION_META.items():
        ranked = sorted(score_rows, key=lambda row: to_float(row_value(row, score_field)), reverse=True)[:10]
        for rank, row in enumerate(ranked, 1):
            institution_name = row_value(row, "institution_norm")
            impact_row = impact_map.get(institution_name, {})
            overall_rank = int(row_value(row, "topsis_rank"))
            remark = ""
            if list_type != "overall":
                if overall_rank - rank >= 20:
                    remark = "该机构在该维度表现明显优于其综合排名"
                elif rank <= 5 and overall_rank <= 20:
                    remark = "该机构在该维度与综合排名上均表现突出"

            result.append(
                {
                    "榜单类型": list_name,
                    "榜内排名": rank,
                    "标准化机构名称": institution_name,
                    "主属国家/地区": row_value(impact_row, "primary_country_region"),
                    "TOPSIS综合排名": row_value(row, "topsis_rank"),
                    "得分指标": SCORE_FIELD_NAMES[score_field],
                    "得分值": row_value(row, score_field),
                    "备注": remark,
                }
            )
    return result


def build_markdown_draft(
    score_rows: list[dict[str, str]],
    impact_map: dict[str, dict[str, str]],
    weight_rows: list[dict[str, str]],
) -> str:
    top20 = build_top20_rows(score_rows, impact_map)

    output_top5 = sorted(score_rows, key=lambda row: to_float(row_value(row, "research_output_subscore")), reverse=True)[:5]
    impact_top5 = sorted(score_rows, key=lambda row: to_float(row_value(row, "academic_impact_subscore")), reverse=True)[:5]
    collab_top5 = sorted(
        score_rows,
        key=lambda row: to_float(row_value(row, "collaboration_international_subscore")),
        reverse=True,
    )[:5]

    top10_names = "、".join(row["标准化机构名称"] for row in top20[:10])
    foreign_top20 = [
        row["标准化机构名称"]
        for row in top20
        if row["主属国家/地区"] != "中国"
    ]
    foreign_top20_text = "、".join(foreign_top20)

    weight_lines = "\n".join(
        f"- `{row_value(row, 'label_zh')}`：`{row_value(row, 'combined_weight')}`"
        for row in weight_rows
    )

    top10_table = "\n".join(
        f"| {row['TOPSIS综合排名']} | {row['标准化机构名称']} | {row['主属国家/地区']} | {row['TOPSIS综合得分']} | "
        f"{row['科研产出子得分']} | {row['学术影响子得分']} | {row['合作与国际化子得分']} |"
        for row in top20[:10]
    )

    output_list = "；".join(
        f"{idx}. {row_value(row, 'institution_norm')}（{row_value(row, 'research_output_subscore')}）"
        for idx, row in enumerate(output_top5, 1)
    )
    impact_list = "；".join(
        f"{idx}. {row_value(row, 'institution_norm')}（{row_value(row, 'academic_impact_subscore')}）"
        for idx, row in enumerate(impact_top5, 1)
    )
    collab_list = "；".join(
        f"{idx}. {row_value(row, 'institution_norm')}（{row_value(row, 'collaboration_international_subscore')}）"
        for idx, row in enumerate(collab_top5, 1)
    )

    top1 = top20[0]
    top2 = top20[1]
    second_tier_names = "、".join(row["标准化机构名称"] for row in top20[2:6])
    foreign_top20_preview = "、".join(foreign_top20[:4]) if foreign_top20 else "暂无"
    output_leaders = "、".join(row_value(row, "institution_norm") for row in output_top5[:3])
    impact_leaders = "、".join(row_value(row, "institution_norm") for row in impact_top5[:3])
    collab_leaders = "、".join(row_value(row, "institution_norm") for row in collab_top5[:3])
    impact_rank_gap_names = "、".join(
        row_value(row, "institution_norm")
        for idx, row in enumerate(impact_top5, 1)
        if int(row_value(row, "topsis_rank")) - idx >= 20
    ) or "部分机构"

    return f"""# 2011-2025年核心机构综合影响力评价写作草稿

## 一、研究方法

本研究以核心机构影响力指标表为基础数据，选取按机构出现频次排序的前100家核心机构作为评价对象。结合开题报告中“科研产出、学术影响、国际合作”三维框架，并结合现有数据可得性，对原始指标进行离散度与相关性检验后，最终保留7个指标进入综合评价模型：去重论文总数、近五年发文占比、H指数、高被引论文占比、合作论文占比、国际合作论文占比和合作国家/地区数。

在权重确定方面，本研究没有直接采用形式化德尔菲法。原因在于严格意义上的德尔菲法需要依托专家问卷或专家评分数据，而当前研究阶段尚未形成可追溯的专家咨询结果。若在没有专家调查的情况下直接人为赋权，容易削弱方法的严谨性。因此，本研究采用“维度均衡先验权重 + 熵权法 + CRITIC法”的组合赋权思路，并将三种方法得到的权重进行算术平均后归一化，形成最终组合权重。该处理既保留了开题报告强调的多方法融合思想，又保证了权重计算的可复现性和客观性。

最终组合权重如下：
{weight_lines}

在综合评价阶段，首先对全部正向指标进行标准化处理；其次，依据组合权重构建加权标准化决策矩阵；再次，确定正理想解与负理想解；最后，计算各机构到理想解和负理想解的距离，并据此获得TOPSIS综合贴近度得分。得分越高，说明机构越接近理想最优状态，其综合学术影响力越强。

## 二、综合评价结果

从TOPSIS综合评价结果看，2011-2025年核心机构的综合影响力排名前十位分别为：{top10_names}。其中，{top1['标准化机构名称']}以 `{top1['TOPSIS综合得分']}` 的综合得分位列第一，{top2['标准化机构名称']}以 `{top2['TOPSIS综合得分']}` 位列第二，二者构成样本中的第一梯队。{second_tier_names}紧随其后，显示出头部机构在该研究主题下的持续优势。

值得注意的是，综合排名前20位中不仅包含多数国内高校与科研机构，也出现了若干境外高水平大学，如 {foreign_top20_text}。这说明在当前研究主题下，国际化程度较高、学术影响质量较强的境外机构，虽然在发文规模上未必占据绝对优势，但能够依靠合作网络和高质量成果进入综合排名前列。

表：综合排名前10机构

| 排名 | 机构 | 主属国家/地区 | 综合得分 | 科研产出子得分 | 学术影响子得分 | 合作与国际化子得分 |
| --- | --- | --- | ---: | ---: | ---: | ---: |
{top10_table}

从综合排名结构看，可以归纳出三点特征。第一，规模优势仍然是综合影响力评价中的重要基础，{row_value(output_top5[0], 'institution_norm')}和{row_value(output_top5[1], 'institution_norm')}在科研产出维度上的突出表现显著支撑了其综合排名。第二，高水平综合性大学与科研机构在学术影响维度上更具均衡性，{impact_leaders}等机构凭借较高的H指数和高被引论文占比保持了较强竞争力。第三，行业特色高校与国际化机构共同进入综合前列，说明主题契合度与合作网络广度共同塑造了机构影响力结构。

## 三、分维度结果分析

从科研产出维度看，前五位机构分别为：{output_list}。这一结果表明，科研产出维度主要由发文规模驱动，国内大型科研院所和头部高校仍占据主导地位，其中{output_leaders}的优势尤为显著。

从学术影响维度看，前五位机构分别为：{impact_list}。这一结果说明，学术影响并不完全等同于发文规模。{impact_rank_gap_names}等机构虽然综合排名不一定居前，但在H指数和高被引论文占比方面表现突出，体现出“质量优先型”特征。

从合作与国际化维度看，前五位机构分别为：{collab_list}。该维度中境外机构优势尤为明显，{collab_leaders}等机构在国际合作论文占比和合作国家/地区覆盖面上表现突出，说明其在国际合作网络中的嵌入程度更高。相比之下，部分国内机构虽然综合排名靠前，但在国际合作维度上仍存在进一步提升空间。

## 四、典型发现

第一，{top1['标准化机构名称']}在综合评价中位居首位，其主导优势维度为{top1['主导优势维度']}，并在多项指标上保持较强均衡性，是样本中的头部机构代表。

第二，{top2['标准化机构名称']}综合排名位居第二，其在{top2['主导优势维度']}方面表现突出，说明主题契合度与长期积累对机构影响力具有重要支撑作用。

第三，{second_tier_names}等机构体现出“规模与质量并重”的特征，是综合型优势机构的代表。

第四，{foreign_top20_preview}等境外机构虽然在总体发文规模上不占绝对优势，但依靠较强的学术影响与国际合作表现，在综合排名中占据重要位置。这说明国际合作网络与成果质量是提升机构综合影响力的重要路径。

第五，{impact_rank_gap_names}在学术影响子维度上的名次显著高于其综合排名，表明部分机构虽然规模相对有限，但在高质量研究成果方面具有突出潜力，值得在后续分析中作为重点观察对象。
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--score", default=str(SCORE_PATH))
    parser.add_argument("--impact", default=str(IMPACT_PATH))
    parser.add_argument("--weight", default=str(WEIGHT_PATH))
    parser.add_argument("--top20-out", default=str(TOP20_OUTPUT_PATH))
    parser.add_argument("--dimension-out", default=str(DIMENSION_OUTPUT_PATH))
    parser.add_argument("--draft-out", default=str(DRAFT_OUTPUT_PATH))
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    score_rows = load_rows(Path(args.score))
    impact_rows = load_rows(Path(args.impact))
    weight_rows = load_rows(Path(args.weight))
    impact_map = {row_value(row, "institution_norm"): row for row in impact_rows}

    top20_rows = build_top20_rows(score_rows, impact_map)
    dimension_rows = build_dimension_rows(score_rows, impact_map)
    draft_text = build_markdown_draft(score_rows, impact_map, weight_rows)

    write_csv(Path(args.top20_out), TOP20_FIELDS, top20_rows)
    write_csv(Path(args.dimension_out), DIMENSION_FIELDS, dimension_rows)
    Path(args.draft_out).write_text(draft_text, encoding="utf-8")

    print(f"top20_output={args.top20_out}")
    print(f"dimension_output={args.dimension_out}")
    print(f"draft_output={args.draft_out}")


if __name__ == "__main__":
    main()
