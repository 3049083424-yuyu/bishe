from __future__ import annotations

import csv
import re
from collections import defaultdict
from pathlib import Path


BASE_DIR = Path(r"D:\毕业论文\version_dual_key_dedup_2011_2025")
ENCODING = "gb18030"

RESIDUAL_DETAIL_PATH = BASE_DIR / "audit_residual_duplicate_candidates_dual_key_2011_2025.csv"
CONFLICT_DETAIL_PATH = BASE_DIR / "audit_doi_title_conflict_dual_key_2011_2025.csv"
RESIDUAL_SUMMARY_PATH = BASE_DIR / "audit_residual_duplicate_candidates_dual_key_2011_2025_group_summary.csv"
CONFLICT_SUMMARY_PATH = BASE_DIR / "audit_doi_title_conflict_dual_key_2011_2025_group_summary.csv"

RE_PUNCT = re.compile(r"[\s\.,;:!?\-_/\\'\"()（）\[\]【】·&]+")

RESIDUAL_REVIEW_FIELDS = [
    "机器预判分类",
    "机器预判置信度",
    "人工审查优先级",
    "机器预判依据",
    "建议处理动作",
]

CONFLICT_REVIEW_FIELDS = [
    "机器预判分类",
    "机器预判置信度",
    "人工审查优先级",
    "机器预判依据",
    "建议处理动作",
]


def compact(text: object) -> str:
    return " ".join(str(text or "").replace("\r", " ").replace("\n", " ").split())


def normalize_key(text: object) -> str:
    value = compact(text).lower()
    value = RE_PUNCT.sub("", value)
    return value


def read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding=ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        return list(reader.fieldnames or []), list(reader)


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding=ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def group_rows(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row["审查组编号"]].append(row)
    return dict(sorted(grouped.items()))


def residual_group_overview(rows: list[dict[str, str]]) -> dict[str, object]:
    journal_keys = {normalize_key(row["期刊"]) for row in rows if compact(row["期刊"])}
    inst_keys = {normalize_key(row["机构标准化字段"]) for row in rows if compact(row["机构标准化字段"])}
    return {
        "group_id": rows[0]["审查组编号"],
        "row_count": len(rows),
        "same_source": rows[0]["同来源库"] == "是",
        "same_journal_flag": rows[0]["同期刊"] == "是",
        "source_combo": rows[0]["组来源库组合"],
        "journal_combo": rows[0]["组期刊组合"],
        "same_normalized_journal": len(journal_keys) == 1,
        "institution_variant_count": len(inst_keys),
        "title": rows[0]["标题"],
        "first_author": rows[0]["第一作者"],
        "institutions": " | ".join(sorted({compact(row["机构标准化字段"]) for row in rows if compact(row["机构标准化字段"])})),
        "dois": " | ".join(sorted({compact(row["doi原值"]) for row in rows if compact(row["doi原值"])})),
    }


def classify_residual_group(rows: list[dict[str, str]]) -> dict[str, str]:
    info = residual_group_overview(rows)
    source_combo = info["source_combo"]

    if "|" in source_combo and info["same_normalized_journal"]:
        return {
            "机器预判分类": "建议合并",
            "机器预判置信度": "高",
            "人工审查优先级": "高",
            "机器预判依据": "跨库记录同题名、同第一作者、同年份，且期刊规范后视为同刊，属于高置信跨库重复。",
            "建议处理动作": "建议将组内记录合并为1条，并优先保留字段更完整的记录。",
        }

    if info["same_source"] and info["same_normalized_journal"] and info["institution_variant_count"] <= 1:
        return {
            "机器预判分类": "建议合并",
            "机器预判置信度": "高",
            "人工审查优先级": "中",
            "机器预判依据": "同库同刊、同题名、同第一作者，且机构标准化字段一致，极像同一论文的重复入库。",
            "建议处理动作": "建议将组内记录合并为1条，并保留信息更完整的记录。",
        }

    if info["same_source"] and info["same_normalized_journal"] and info["institution_variant_count"] > 1:
        return {
            "机器预判分类": "需人工判断",
            "机器预判置信度": "中",
            "人工审查优先级": "高",
            "机器预判依据": "同库同刊、同题名、同第一作者，但机构标准化字段存在差异，较像同文多次入库，也可能是机构写法或署名层级不同导致。",
            "建议处理动作": "建议优先人工核对题名页、作者全名单和机构原文后，再决定是否合并。",
        }

    if info["same_source"] and not info["same_normalized_journal"]:
        return {
            "机器预判分类": "建议保留",
            "机器预判置信度": "中",
            "人工审查优先级": "中",
            "机器预判依据": "同题名同作者但来自不同期刊，更像转载、资讯摘编或不同发表场景，不宜直接视为重复论文。",
            "建议处理动作": "建议暂时保留，不直接合并；如有需要再人工核对正文内容。",
        }

    return {
        "机器预判分类": "需人工判断",
        "机器预判置信度": "中",
        "人工审查优先级": "高",
        "机器预判依据": "当前组合不属于高置信自动判断场景，仍需人工核对来源和题名细节。",
        "建议处理动作": "建议逐条核对后决定是否合并。",
    }


def conflict_group_overview(rows: list[dict[str, str]]) -> dict[str, object]:
    title = compact(rows[0]["标题"])
    title_lower = title.lower()
    journal_keys = {normalize_key(row["期刊"]) for row in rows if compact(row["期刊"])}
    markers = []
    for token in (
        "retraction",
        "erratum",
        "correction",
        "corrigendum",
        "award",
        "awards",
        "eulogy",
        "obituary",
        "editorial",
        "preface",
        "introduction",
        "confronting racism",
    ):
        if token in title_lower:
            markers.append(token)
    if "(vol " in title_lower or " vol " in title_lower:
        markers.append("vol-reference")
    return {
        "group_id": rows[0]["审查组编号"],
        "row_count": len(rows),
        "source_combo": rows[0]["组来源库组合"],
        "journal_combo": rows[0]["组期刊组合"],
        "same_normalized_journal": len(journal_keys) == 1,
        "title": title,
        "first_author": rows[0]["第一作者"],
        "marker_hits": markers,
        "dois": " | ".join(sorted({compact(row["标准doi键"]) for row in rows if compact(row["标准doi键"])})),
    }


def classify_conflict_group(rows: list[dict[str, str]]) -> dict[str, str]:
    info = conflict_group_overview(rows)

    if not info["same_normalized_journal"]:
        return {
            "机器预判分类": "建议保留",
            "机器预判置信度": "高",
            "人工审查优先级": "中",
            "机器预判依据": "同题名记录分属不同期刊，且标准DOI彼此冲突，不应直接合并。",
            "建议处理动作": "建议保留为独立记录，仅在论文中说明存在跨期刊同题名现象。",
        }

    if info["marker_hits"]:
        marker_text = "、".join(info["marker_hits"])
        return {
            "机器预判分类": "建议保留",
            "机器预判置信度": "高",
            "人工审查优先级": "中",
            "机器预判依据": f"标题含有 {marker_text} 等勘误/更正/纪念性标记，较像不同类型记录或后续版本，不宜直接合并。",
            "建议处理动作": "建议保留为独立记录，并作为DOI/题名冲突样本说明。",
        }

    return {
        "机器预判分类": "需人工判断",
        "机器预判置信度": "中",
        "人工审查优先级": "高",
        "机器预判依据": "同刊同题名但标准DOI不同，算法已为避免误并而保留，需要人工核对是否为重复入库、勘误或版本记录。",
        "建议处理动作": "建议优先人工核对WOS原始记录、卷期页码和摘要后再决定是否保留。",
    }


def annotate_rows(rows: list[dict[str, str]], classifier) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    grouped = group_rows(rows)
    annotated_rows: list[dict[str, str]] = []
    summary_rows: list[dict[str, str]] = []

    for group_id, group_rows_list in grouped.items():
        decision = classifier(group_rows_list)
        first = group_rows_list[0]

        title = compact(first.get("标题", ""))
        first_author = compact(first.get("第一作者", ""))
        source_combo = compact(first.get("组来源库组合", ""))
        journal_combo = compact(first.get("组期刊组合", ""))
        institution_combo = " | ".join(sorted({compact(row.get("机构标准化字段", "")) for row in group_rows_list if compact(row.get("机构标准化字段", ""))}))
        doi_combo = " | ".join(sorted({compact(row.get("doi原值", "")) or compact(row.get("标准doi键", "")) for row in group_rows_list if compact(row.get("doi原值", "")) or compact(row.get("标准doi键", ""))}))

        summary_rows.append(
            {
                "审查组编号": group_id,
                "候选类型": compact(first.get("候选类型", "")),
                "组内记录数": str(len(group_rows_list)),
                "组来源库组合": source_combo,
                "组期刊组合": journal_combo,
                "标题概览": title,
                "第一作者": first_author,
                "机构标准化概览": institution_combo,
                "DOI概览": doi_combo,
                **decision,
            }
        )

        for row in group_rows_list:
            annotated = dict(row)
            annotated.update(decision)
            annotated_rows.append(annotated)

    return annotated_rows, summary_rows


def ensure_fieldnames(base_fields: list[str], extra_fields: list[str]) -> list[str]:
    merged = list(base_fields)
    for field in extra_fields:
        if field not in merged:
            merged.append(field)
    return merged


def main() -> None:
    residual_fields, residual_rows = read_csv(RESIDUAL_DETAIL_PATH)
    conflict_fields, conflict_rows = read_csv(CONFLICT_DETAIL_PATH)

    residual_annotated_rows, residual_summary_rows = annotate_rows(residual_rows, classify_residual_group)
    conflict_annotated_rows, conflict_summary_rows = annotate_rows(conflict_rows, classify_conflict_group)

    write_csv(
        RESIDUAL_DETAIL_PATH,
        ensure_fieldnames(residual_fields, RESIDUAL_REVIEW_FIELDS),
        residual_annotated_rows,
    )
    write_csv(
        CONFLICT_DETAIL_PATH,
        ensure_fieldnames(conflict_fields, CONFLICT_REVIEW_FIELDS),
        conflict_annotated_rows,
    )
    write_csv(RESIDUAL_SUMMARY_PATH, list(residual_summary_rows[0].keys()), residual_summary_rows)
    write_csv(CONFLICT_SUMMARY_PATH, list(conflict_summary_rows[0].keys()), conflict_summary_rows)

    print(f"residual_detail={RESIDUAL_DETAIL_PATH}")
    print(f"residual_groups={len(residual_summary_rows)}")
    print(f"residual_summary={RESIDUAL_SUMMARY_PATH}")
    print(f"conflict_detail={CONFLICT_DETAIL_PATH}")
    print(f"conflict_groups={len(conflict_summary_rows)}")
    print(f"conflict_summary={CONFLICT_SUMMARY_PATH}")


if __name__ == "__main__":
    main()
