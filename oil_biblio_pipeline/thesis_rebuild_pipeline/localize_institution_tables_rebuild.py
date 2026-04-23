from __future__ import annotations

import csv
from pathlib import Path


REBUILD_ROOT = Path(r"D:\graduate\thesis_rebuild")
INSTITUTION_DIR = REBUILD_ROOT / "institution_eval"
QA_DIR = REBUILD_ROOT / "qa"
DELIVERY_ROOT = REBUILD_ROOT / "delivery_zh"
DELIVERY_INSTITUTION_DIR = DELIVERY_ROOT / "institution_eval"
DELIVERY_QA_DIR = DELIVERY_ROOT / "qa"

OUTPUT_ENCODING = "gb18030"

METHOD_VALUE_MAP = {
    "manual_exact": "人工精确匹配",
    "manual_generated": "人工规则生成",
    "manual_canonical": "人工规范名匹配",
    "auto_word_level": "自动分词翻译",
    "raw_fallback": "原文回退保留",
    "ambiguous_abbrev": "缩写释义待判定",
}

REASON_VALUE_MAP = {
    "empty_trans_or_norm": "译名或标准名为空",
    "norm_not_cjk": "标准名仍含英文或非中文主名",
    "high_freq_auto_result": "高频自动结果需人工复核",
    "top_freq_needs_review": "核心高频机构需人工复核",
    "generic_enterprise_alias": "企业别名或内部研发单元需复核",
    "ambiguous_abbrev": "缩写信息不足，需结合原始地址或国家字段复核",
    "same_name_country_disambiguation": "同名机构已按原英文名区分国家，建议抽样复核",
}


def read_csv_guess(path: Path) -> tuple[list[str], list[dict[str, str]], str]:
    for encoding in ("utf-8-sig", "utf-8", "gb18030"):
        try:
            with path.open("r", encoding=encoding, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                return list(reader.fieldnames or []), rows, encoding
        except Exception:
            continue
    raise RuntimeError(f"Unable to read CSV with known encodings: {path}")


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding=OUTPUT_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def map_rows(rows: list[dict[str, str]], field_map: dict[str, str], value_fn=None) -> list[dict[str, object]]:
    out_rows: list[dict[str, object]] = []
    for row in rows:
        out_row: dict[str, object] = {}
        for source_key, target_key in field_map.items():
            value = row.get(source_key, "")
            if value_fn is not None:
                value = value_fn(source_key, value)
            out_row[target_key] = value
        out_rows.append(out_row)
    return out_rows


def selection_value_fn(field: str, value: str) -> str:
    text = str(value or "").strip()
    if field == "direction":
        return "正向" if text == "benefit" else text
    if field == "selected":
        return "是" if text in {"1", "true", "True"} else "否"
    return text


def weight_value_fn(field: str, value: str) -> str:
    text = str(value or "").strip()
    if field == "direction":
        return "正向" if text == "benefit" else text
    return text


def trans_norm_value_fn(field: str, value: str) -> str:
    text = str(value or "").strip()
    if field == "candidate_review_flag":
        return "是" if text in {"1", "true", "True", "是"} else "否"
    if field == "candidate_method":
        return METHOD_VALUE_MAP.get(text, text)
    if field == "candidate_review_reason":
        parts = [part.strip() for part in text.replace("；", "|").split("|") if part.strip()]
        return "；".join(REASON_VALUE_MAP.get(part, part) for part in parts)
    return text


RAW_TABLE_MAP = {
    "institution_name": "机构原始名称",
    "count": "总频次",
    "cnki_count": "CNKI频次",
    "wos_count": "WOS频次",
    "cscd_count": "CSCD频次",
}

TRANS_NORM_MAP = {
    "institution_name": "机构原始名称",
    "count": "总频次",
    "cnki_count": "CNKI频次",
    "wos_count": "WOS频次",
    "cscd_count": "CSCD频次",
    "institution_trans": "机构译名",
    "institution_norm": "机构标准名",
    "candidate_method": "候选生成方式",
    "candidate_review_flag": "是否建议复核",
    "candidate_review_reason": "复核原因",
}

TRANS_NORM_REVIEW_MAP = {
    "institution_name": "机构原始名称",
    "count": "总频次",
    "cnki_count": "CNKI频次",
    "wos_count": "WOS频次",
    "cscd_count": "CSCD频次",
    "institution_trans": "机构译名",
    "institution_norm": "机构标准名",
    "candidate_method": "候选生成方式",
    "candidate_review_reason": "复核原因",
}

NORM_FREQ_MAP = {
    "institution_norm": "机构标准名",
    "count": "总频次",
    "cnki_count": "CNKI频次",
    "wos_count": "WOS频次",
    "cscd_count": "CSCD频次",
}

CORE_TOP100_MAP = {
    "rank_top100": "Top100入选排名",
    "institution_norm": "标准化机构名称",
    "count": "机构出现总次数",
    "cnki_count": "CNKI机构出现次数",
    "wos_count": "WOS机构出现次数",
    "cscd_count": "CSCD机构出现次数",
}

IMPACT_MAP = {
    "rank_top100": "Top100入选排名",
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

SELECTION_MAP = {
    "field": "指标代码",
    "label_zh": "指标名称",
    "dimension": "维度代码",
    "dimension_zh": "指标维度",
    "direction": "指标方向",
    "selected": "是否纳入TOPSIS",
    "selection_reason": "筛选说明",
}

WEIGHT_MAP = {
    "field": "指标代码",
    "label_zh": "指标名称",
    "dimension": "维度代码",
    "dimension_zh": "指标维度",
    "direction": "指标方向",
    "prior_weight": "先验权重",
    "entropy_weight": "熵权法权重",
    "critic_weight": "CRITIC法权重",
    "combined_weight": "组合权重",
}

TOPSIS_SCORE_MAP = {
    "rank_top100": "Top100入选排名",
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


TABLE_SPECS = [
    (
        INSTITUTION_DIR / "institution_name_table_dual_key_2011_2025.csv",
        DELIVERY_INSTITUTION_DIR / "institution_name_table_dual_key_2011_2025_zh.csv",
        RAW_TABLE_MAP,
        None,
    ),
    (
        INSTITUTION_DIR / "institution_name_table_dual_key_trans_norm_2011_2025.csv",
        DELIVERY_INSTITUTION_DIR / "institution_name_table_dual_key_trans_norm_2011_2025_zh.csv",
        TRANS_NORM_MAP,
        trans_norm_value_fn,
    ),
    (
        INSTITUTION_DIR / "institution_name_table_dual_key_norm_freq_2011_2025.csv",
        DELIVERY_INSTITUTION_DIR / "institution_name_table_dual_key_norm_freq_2011_2025_zh.csv",
        NORM_FREQ_MAP,
        None,
    ),
    (
        INSTITUTION_DIR / "institution_core_top100_dual_key_2011_2025.csv",
        DELIVERY_INSTITUTION_DIR / "institution_core_top100_dual_key_2011_2025_zh.csv",
        CORE_TOP100_MAP,
        None,
    ),
    (
        INSTITUTION_DIR / "institution_impact_indicator_top100_dual_key_2011_2025.csv",
        DELIVERY_INSTITUTION_DIR / "institution_impact_indicator_top100_dual_key_2011_2025_zh.csv",
        IMPACT_MAP,
        None,
    ),
    (
        INSTITUTION_DIR / "institution_topsis_indicator_selection_top100_dual_key_2011_2025.csv",
        DELIVERY_INSTITUTION_DIR / "institution_topsis_indicator_selection_top100_dual_key_2011_2025_zh.csv",
        SELECTION_MAP,
        selection_value_fn,
    ),
    (
        INSTITUTION_DIR / "institution_weight_scheme_top100_dual_key_2011_2025.csv",
        DELIVERY_INSTITUTION_DIR / "institution_weight_scheme_top100_dual_key_2011_2025_zh.csv",
        WEIGHT_MAP,
        weight_value_fn,
    ),
    (
        INSTITUTION_DIR / "institution_topsis_score_top100_dual_key_2011_2025.csv",
        DELIVERY_INSTITUTION_DIR / "institution_topsis_score_top100_dual_key_2011_2025_zh.csv",
        TOPSIS_SCORE_MAP,
        None,
    ),
    (
        INSTITUTION_DIR / "institution_topsis_top20_dual_key_2011_2025.csv",
        DELIVERY_INSTITUTION_DIR / "institution_topsis_top20_dual_key_2011_2025_zh.csv",
        None,
        None,
    ),
    (
        INSTITUTION_DIR / "institution_dimension_top10_dual_key_2011_2025.csv",
        DELIVERY_INSTITUTION_DIR / "institution_dimension_top10_dual_key_2011_2025_zh.csv",
        None,
        None,
    ),
    (
        INSTITUTION_DIR / "institution_type_review_top100_dual_key_2011_2025.csv",
        DELIVERY_INSTITUTION_DIR / "institution_type_review_top100_dual_key_2011_2025_zh.csv",
        None,
        None,
    ),
    (
        QA_DIR / "institution_trans_norm_review_dual_key_2011_2025.csv",
        DELIVERY_QA_DIR / "institution_trans_norm_review_dual_key_2011_2025_zh.csv",
        TRANS_NORM_REVIEW_MAP,
        trans_norm_value_fn,
    ),
]


def localize_table(input_path: Path, output_path: Path, field_map: dict[str, str] | None, value_fn) -> None:
    fieldnames, rows, _ = read_csv_guess(input_path)
    if field_map is None:
        write_csv(output_path, fieldnames, rows)
        return
    out_rows = map_rows(rows, field_map, value_fn=value_fn)
    write_csv(output_path, list(field_map.values()), out_rows)


def build_mapping_preview() -> None:
    _, rows, _ = read_csv_guess(INSTITUTION_DIR / "institution_name_table_dual_key_trans_norm_2011_2025.csv")
    rows.sort(key=lambda row: int(row.get("count", "0") or 0), reverse=True)
    preview_rows = rows[:500]
    out_rows = map_rows(preview_rows, TRANS_NORM_MAP, value_fn=trans_norm_value_fn)
    write_csv(
        DELIVERY_INSTITUTION_DIR / "institution_mapping_top500_for_review_2011_2025_zh.csv",
        list(TRANS_NORM_MAP.values()),
        out_rows,
    )


def main() -> None:
    for input_path, output_path, field_map, value_fn in TABLE_SPECS:
        localize_table(input_path, output_path, field_map, value_fn)
        print(f"localized={output_path}")
    build_mapping_preview()
    print(f"localized={DELIVERY_INSTITUTION_DIR / 'institution_mapping_top500_for_review_2011_2025_zh.csv'}")


if __name__ == "__main__":
    main()
