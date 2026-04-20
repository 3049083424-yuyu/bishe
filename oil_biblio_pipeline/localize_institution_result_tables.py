from __future__ import annotations

import csv
from pathlib import Path


BASE_DIR = Path("D:/毕业论文")
ENCODING = "gb18030"


IMPACT_FIELD_MAP = {
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
    "primary_country_region": "主属国家/地区",
    "international_collaboration_paper_count": "国际合作论文数",
    "international_collaboration_paper_ratio": "国际合作论文占比",
    "international_partner_institution_count": "国际合作机构数",
    "partner_country_region_count": "合作国家/地区数",
}

CORE_TOP100_FIELD_MAP = {
    "rank_top100": "Top100排名",
    "institution_norm": "标准化机构名称",
    "count": "机构出现总次数",
    "cnki_count": "CNKI机构出现次数",
    "wos_count": "WOS机构出现次数",
    "cscd_count": "CSCD机构出现次数",
}

TOPSIS_SCORE_FIELD_MAP = {
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

TOP20_FIELD_MAP = {
    "topsis_rank": "TOPSIS综合排名",
    "institution_norm": "标准化机构名称",
    "primary_country_region": "主属国家/地区",
    "topsis_score": "TOPSIS综合得分",
    "research_output_subscore": "科研产出子得分",
    "academic_impact_subscore": "学术影响子得分",
    "collaboration_international_subscore": "合作与国际化子得分",
    "dominant_dimension": "主导优势维度",
    "distinct_paper_count": "去重论文总数",
    "h_index": "H指数",
    "high_cited_paper_ratio": "高被引论文占比",
    "international_collaboration_paper_ratio": "国际合作论文占比",
}

SELECTION_DIMENSIONS = {
    "distinct_paper_count": "科研产出",
    "recent_paper_ratio_2021_2025": "科研产出",
    "occurrence_count": "科研产出",
    "avg_annual_paper_count": "科研产出",
    "active_year_count": "科研产出",
    "h_index": "学术影响",
    "high_cited_paper_ratio": "学术影响",
    "total_citations": "学术影响",
    "avg_citations_per_paper": "学术影响",
    "median_citations_per_paper": "学术影响",
    "collaboration_paper_ratio": "合作与国际化",
    "international_collaboration_paper_ratio": "合作与国际化",
    "partner_country_region_count": "合作与国际化",
    "international_partner_institution_count": "合作与国际化",
    "avg_partner_institutions_per_collab_paper": "合作与国际化",
}

SCORE_FIELD_NAME_MAP = {
    "topsis_score": "TOPSIS综合得分",
    "research_output_subscore": "科研产出子得分",
    "academic_impact_subscore": "学术影响子得分",
    "collaboration_international_subscore": "合作与国际化子得分",
}

LIST_TYPE_NAME_MAP = {
    "overall": "综合排名",
    "research_output": "科研产出维度",
    "academic_impact": "学术影响维度",
    "collaboration_international": "合作与国际化维度",
}

EXPLANATIONS = {
    "Top100排名": "机构按进入核心机构或指标表时的既定排序得到的名次。",
    "Top100原始入选排名": "机构按原始出现频次进入Top100时的名次，不等同于TOPSIS综合排名。",
    "标准化机构名称": "经过机构翻译、清洗和标准化后的统一机构名称。",
    "机构出现总次数": "机构在全部机构条目中的总出现次数；若一篇论文有多个机构，每个机构各记1次。",
    "CNKI机构出现次数": "机构在CNKI来源论文中的机构条目出现次数。",
    "WOS机构出现次数": "机构在WOS来源论文中的机构条目出现次数。",
    "CSCD机构出现次数": "机构在CSCD来源论文中的机构条目出现次数。",
    "去重论文总数": "至少出现过该机构1次的去重论文篇数，同一篇论文中重复出现仅计1篇。",
    "CNKI去重论文数": "机构关联到的CNKI去重论文篇数。",
    "WOS去重论文数": "机构关联到的WOS去重论文篇数。",
    "CSCD去重论文数": "机构关联到的CSCD去重论文篇数。",
    "首次发文年份": "研究期内该机构最早出现的论文年份。",
    "最近发文年份": "研究期内该机构最近一次出现的论文年份。",
    "活跃年份数": "2011-2025年间该机构实际发文覆盖的年份数量。",
    "年均发文量": "去重论文总数除以15年得到的年均发文水平。",
    "2011-2015发文量": "机构在2011-2015阶段的去重论文数。",
    "2016-2020发文量": "机构在2016-2020阶段的去重论文数。",
    "2021-2025发文量": "机构在2021-2025阶段的去重论文数。",
    "近五年发文占比": "2021-2025发文量占机构去重论文总数的比例。",
    "总被引频次": "机构关联全部去重论文的被引频次之和。",
    "篇均被引频次": "总被引频次除以去重论文总数。",
    "篇均被引中位数": "机构全部去重论文被引频次的中位数。",
    "单篇最高被引频次": "机构关联论文中单篇论文的最高被引频次。",
    "H指数": "基于机构关联论文被引分布计算得到的机构H指数。",
    "有被引论文数": "被引频次大于0的去重论文篇数。",
    "有被引论文占比": "有被引论文数占去重论文总数的比例。",
    "未被引论文数": "被引频次等于0的去重论文篇数。",
    "未被引论文占比": "未被引论文数占去重论文总数的比例。",
    "高被引论文数": "达到或超过全体样本前10%被引阈值的论文篇数。",
    "高被引论文占比": "高被引论文数占去重论文总数的比例。",
    "合作论文数": "识别到2个及以上标准化机构共同署名的论文篇数。",
    "合作论文占比": "合作论文数占去重论文总数的比例。",
    "合作机构数": "与该机构共同出现过的不同标准化机构数量。",
    "单篇合作论文平均合作机构数": "合作论文中每篇论文平均涉及的其他合作机构数量。",
    "主属国家/地区": "根据机构地址信息识别出的机构主要所属国家或地区。",
    "国际合作论文数": "合作机构中存在不同国家/地区机构时记为1篇国际合作论文。",
    "国际合作论文占比": "国际合作论文数占去重论文总数的比例。",
    "国际合作机构数": "与该机构发生国际合作关系的不同境外机构数量。",
    "合作国家/地区数": "与该机构形成合作关系的国家或地区数量。",
    "TOPSIS综合得分": "机构基于组合权重和TOPSIS方法计算得到的综合贴近度得分，值越大表示综合影响力越强。",
    "TOPSIS综合排名": "机构按TOPSIS综合得分降序得到的综合排名。",
    "科研产出子得分": "机构在科研产出维度上的标准化综合得分。",
    "学术影响子得分": "机构在学术影响维度上的标准化综合得分。",
    "合作与国际化子得分": "机构在合作与国际化维度上的标准化综合得分。",
    "主导优势维度": "该机构三类子得分中数值最高的优势维度。",
    "指标名称": "进入筛选或赋权过程的指标中文名称。",
    "指标维度": "指标所属的一级评价维度。",
    "指标方向": "指标对综合评价的方向属性；本轮入模指标均为正向指标。",
    "是否纳入TOPSIS": "该指标是否最终进入TOPSIS综合评价模型。",
    "筛选说明": "说明该指标被保留或被剔除的主要原因。",
    "先验权重": "按三大维度等权、维度内均分得到的先验权重。",
    "熵权法权重": "根据指标离散程度计算得到的客观权重。",
    "CRITIC法权重": "综合考虑指标变异程度和与其他指标冲突程度得到的客观权重。",
    "榜单类型": "该行所属的榜单类别，如综合排名、科研产出维度等。",
    "榜内排名": "机构在当前榜单中的名次。",
    "得分指标": "当前榜单对应的得分字段或评价指标。",
    "得分值": "机构在当前榜单对应指标上的具体得分。",
    "备注": "对机构在该榜单中的表现特征做出的简要说明。",
}


def read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding=ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return reader.fieldnames or [], rows


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding=ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def get_value(row: dict[str, str], mapping: dict[str, str], english_key: str) -> str:
    chinese_key = mapping[english_key]
    return row.get(english_key, row.get(chinese_key, ""))


def localize_simple_table(filename: str, mapping: dict[str, str]) -> None:
    path = BASE_DIR / filename
    _, rows = read_csv(path)
    new_rows: list[dict[str, object]] = []
    for row in rows:
        new_rows.append({mapping[key]: get_value(row, mapping, key) for key in mapping})
    write_csv(path, list(mapping.values()), new_rows)


def localize_selection_table() -> None:
    path = BASE_DIR / "institution_topsis_indicator_selection_top100_2011_2025.csv"
    _, rows = read_csv(path)
    fieldnames = ["指标名称", "指标维度", "指标方向", "是否纳入TOPSIS", "筛选说明"]
    new_rows: list[dict[str, object]] = []
    for row in rows:
        original_field = row.get("field", "")
        chinese_name = (
            row.get("label_zh", "")
            or row.get("指标名称", "")
            or IMPACT_FIELD_MAP.get(original_field, original_field)
        )
        dimension = row.get("dimension_zh", "") or row.get("指标维度", "") or SELECTION_DIMENSIONS.get(original_field, "")
        direction_raw = row.get("direction", "") or row.get("指标方向", "")
        direction = "正向" if direction_raw in ("benefit", "正向") else direction_raw
        selected_raw = row.get("selected", "") or row.get("是否纳入TOPSIS", "")
        selected = "是" if str(selected_raw).strip() in ("1", "是", "true", "True") else "否"
        new_rows.append(
            {
                "指标名称": chinese_name,
                "指标维度": dimension,
                "指标方向": direction,
                "是否纳入TOPSIS": selected,
                "筛选说明": row.get("selection_reason", row.get("筛选说明", "")),
            }
        )
    write_csv(path, fieldnames, new_rows)


def localize_weight_table() -> None:
    path = BASE_DIR / "institution_weight_scheme_top100_2011_2025.csv"
    _, rows = read_csv(path)
    fieldnames = ["指标名称", "指标维度", "指标方向", "先验权重", "熵权法权重", "CRITIC法权重", "组合权重"]
    new_rows: list[dict[str, object]] = []
    for row in rows:
        field_code = row.get("field", "")
        chinese_name = (
            row.get("label_zh", "")
            or row.get("指标名称", "")
            or IMPACT_FIELD_MAP.get(field_code, field_code)
        )
        dimension = row.get("dimension_zh", "") or row.get("指标维度", "") or SELECTION_DIMENSIONS.get(field_code, "")
        direction_raw = row.get("direction", "") or row.get("指标方向", "")
        direction = "正向" if direction_raw in ("benefit", "正向") else direction_raw
        new_rows.append(
            {
                "指标名称": chinese_name,
                "指标维度": dimension,
                "指标方向": direction,
                "先验权重": row.get("prior_weight", row.get("先验权重", "")),
                "熵权法权重": row.get("entropy_weight", row.get("熵权法权重", "")),
                "CRITIC法权重": row.get("critic_weight", row.get("CRITIC法权重", "")),
                "组合权重": row.get("combined_weight", row.get("组合权重", "")),
            }
        )
    write_csv(path, fieldnames, new_rows)


def localize_dimension_table() -> None:
    path = BASE_DIR / "institution_dimension_top10_2011_2025.csv"
    _, rows = read_csv(path)
    fieldnames = ["榜单类型", "榜内排名", "标准化机构名称", "主属国家/地区", "TOPSIS综合排名", "得分指标", "得分值", "备注"]
    new_rows: list[dict[str, object]] = []
    for row in rows:
        list_type = (
            row.get("list_type_zh", "")
            or row.get("榜单类型", "")
            or LIST_TYPE_NAME_MAP.get(row.get("list_type", ""), row.get("list_type", ""))
        )
        score_field = row.get("score_field", "") or row.get("得分指标", "")
        score_name = SCORE_FIELD_NAME_MAP.get(score_field, row.get("得分指标", score_field))
        new_rows.append(
            {
                "榜单类型": list_type,
                "榜内排名": row.get("rank_in_list", row.get("榜内排名", "")),
                "标准化机构名称": row.get("institution_norm", row.get("标准化机构名称", "")),
                "主属国家/地区": row.get("primary_country_region", row.get("主属国家/地区", "")),
                "TOPSIS综合排名": row.get("topsis_rank", row.get("TOPSIS综合排名", "")),
                "得分指标": score_name,
                "得分值": row.get("score_value", row.get("得分值", "")),
                "备注": row.get("remark", row.get("备注", "")),
            }
        )
    write_csv(path, fieldnames, new_rows)


def build_explanation_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []

    table_fields = [
        ("institution_core_top100_2011_2025.csv", CORE_TOP100_FIELD_MAP),
        ("institution_impact_indicator_top100_2011_2025.csv", IMPACT_FIELD_MAP),
        ("institution_topsis_score_top100_2011_2025.csv", TOPSIS_SCORE_FIELD_MAP),
        ("institution_topsis_top20_2011_2025.csv", TOP20_FIELD_MAP),
    ]
    for filename, mapping in table_fields:
        for english_name, chinese_name in mapping.items():
            rows.append(
                {
                    "结果表文件": filename,
                    "中文字段名": chinese_name,
                    "原英文字段名": english_name,
                    "字段含义": EXPLANATIONS.get(chinese_name, ""),
                }
            )

    extra_rows = [
        ("institution_topsis_indicator_selection_top100_2011_2025.csv", "指标名称", "", EXPLANATIONS["指标名称"]),
        ("institution_topsis_indicator_selection_top100_2011_2025.csv", "指标维度", "", EXPLANATIONS["指标维度"]),
        ("institution_topsis_indicator_selection_top100_2011_2025.csv", "指标方向", "", EXPLANATIONS["指标方向"]),
        ("institution_topsis_indicator_selection_top100_2011_2025.csv", "是否纳入TOPSIS", "", EXPLANATIONS["是否纳入TOPSIS"]),
        ("institution_topsis_indicator_selection_top100_2011_2025.csv", "筛选说明", "", EXPLANATIONS["筛选说明"]),
        ("institution_weight_scheme_top100_2011_2025.csv", "指标名称", "", EXPLANATIONS["指标名称"]),
        ("institution_weight_scheme_top100_2011_2025.csv", "指标维度", "", EXPLANATIONS["指标维度"]),
        ("institution_weight_scheme_top100_2011_2025.csv", "指标方向", "", EXPLANATIONS["指标方向"]),
        ("institution_weight_scheme_top100_2011_2025.csv", "先验权重", "prior_weight", EXPLANATIONS["先验权重"]),
        ("institution_weight_scheme_top100_2011_2025.csv", "熵权法权重", "entropy_weight", EXPLANATIONS["熵权法权重"]),
        ("institution_weight_scheme_top100_2011_2025.csv", "CRITIC法权重", "critic_weight", EXPLANATIONS["CRITIC法权重"]),
        ("institution_weight_scheme_top100_2011_2025.csv", "组合权重", "combined_weight", "先验权重、熵权法权重与CRITIC法权重三者平均后归一化得到的最终权重。"),
        ("institution_dimension_top10_2011_2025.csv", "榜单类型", "list_type/list_type_zh", EXPLANATIONS["榜单类型"]),
        ("institution_dimension_top10_2011_2025.csv", "榜内排名", "rank_in_list", EXPLANATIONS["榜内排名"]),
        ("institution_dimension_top10_2011_2025.csv", "标准化机构名称", "institution_norm", EXPLANATIONS["标准化机构名称"]),
        ("institution_dimension_top10_2011_2025.csv", "主属国家/地区", "primary_country_region", EXPLANATIONS["主属国家/地区"]),
        ("institution_dimension_top10_2011_2025.csv", "TOPSIS综合排名", "topsis_rank", EXPLANATIONS["TOPSIS综合排名"]),
        ("institution_dimension_top10_2011_2025.csv", "得分指标", "score_field", EXPLANATIONS["得分指标"]),
        ("institution_dimension_top10_2011_2025.csv", "得分值", "score_value", EXPLANATIONS["得分值"]),
        ("institution_dimension_top10_2011_2025.csv", "备注", "remark", EXPLANATIONS["备注"]),
    ]
    for filename, chinese_name, english_name, meaning in extra_rows:
        rows.append(
            {
                "结果表文件": filename,
                "中文字段名": chinese_name,
                "原英文字段名": english_name,
                "字段含义": meaning,
            }
        )
    return rows


def write_explanation_files() -> None:
    rows = build_explanation_rows()
    fieldnames = ["结果表文件", "中文字段名", "原英文字段名", "字段含义"]
    write_csv(BASE_DIR / "result_table_field_explanation_2011_2025.csv", fieldnames, rows)

    impact_rows = []
    for english_name, chinese_name in IMPACT_FIELD_MAP.items():
        impact_rows.append(
            {
                "中文字段名": chinese_name,
                "原英文字段名": english_name,
                "指标维度": (
                    "基础标识"
                    if chinese_name in ("Top100排名", "标准化机构名称")
                    else "科研产出"
                    if chinese_name in (
                        "机构出现总次数",
                        "CNKI机构出现次数",
                        "WOS机构出现次数",
                        "CSCD机构出现次数",
                        "去重论文总数",
                        "CNKI去重论文数",
                        "WOS去重论文数",
                        "CSCD去重论文数",
                        "首次发文年份",
                        "最近发文年份",
                        "活跃年份数",
                        "年均发文量",
                        "2011-2015发文量",
                        "2016-2020发文量",
                        "2021-2025发文量",
                        "近五年发文占比",
                    )
                    else "学术影响"
                    if chinese_name in (
                        "总被引频次",
                        "篇均被引频次",
                        "篇均被引中位数",
                        "单篇最高被引频次",
                        "H指数",
                        "有被引论文数",
                        "有被引论文占比",
                        "未被引论文数",
                        "未被引论文占比",
                        "高被引论文数",
                        "高被引论文占比",
                    )
                    else "合作与国际化"
                ),
                "含义与计算口径": EXPLANATIONS.get(chinese_name, ""),
            }
        )
    write_csv(
        BASE_DIR / "institution_impact_indicator_top100_2011_2025_field_guide.csv",
        ["中文字段名", "原英文字段名", "指标维度", "含义与计算口径"],
        impact_rows,
    )


def main() -> None:
    localize_simple_table("institution_core_top100_2011_2025.csv", CORE_TOP100_FIELD_MAP)
    localize_simple_table("institution_impact_indicator_top100_2011_2025.csv", IMPACT_FIELD_MAP)
    localize_simple_table("institution_topsis_score_top100_2011_2025.csv", TOPSIS_SCORE_FIELD_MAP)
    localize_simple_table("institution_topsis_top20_2011_2025.csv", TOP20_FIELD_MAP)
    localize_selection_table()
    localize_weight_table()
    localize_dimension_table()
    write_explanation_files()
    print("localized_result_tables=done")


if __name__ == "__main__":
    main()
