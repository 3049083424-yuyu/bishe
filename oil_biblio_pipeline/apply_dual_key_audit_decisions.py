from __future__ import annotations

import csv
from collections import Counter, defaultdict
from pathlib import Path

from export_dual_key_audit_review_tables import pick_first_author, pick_title_and_field, residual_group_key


BASE_DIR = Path(r"D:\毕业论文\version_dual_key_dedup_2011_2025")
MERGED_PATH = BASE_DIR / "merged_clean_dual_key_dedup_2011_2025.csv"
RESIDUAL_DETAIL_PATH = BASE_DIR / "audit_residual_duplicate_candidates_dual_key_2011_2025.csv"
RESIDUAL_SUMMARY_PATH = BASE_DIR / "audit_residual_duplicate_candidates_dual_key_2011_2025_group_summary.csv"
CONFLICT_DETAIL_PATH = BASE_DIR / "audit_doi_title_conflict_dual_key_2011_2025.csv"
CONFLICT_SUMMARY_PATH = BASE_DIR / "audit_doi_title_conflict_dual_key_2011_2025_group_summary.csv"
ACTION_LOG_PATH = BASE_DIR / "audit_final_disposition_dual_key_2011_2025.csv"

INPUT_ENCODING = "utf-8-sig"
OUTPUT_ENCODING = "gb18030"

SOURCE_ORDER = ["CNKI", "WOS", "CSCD"]
SOURCE_PRIORITY = {"WOS": 3, "CNKI": 2, "CSCD": 1}
MANUAL_RESIDUAL_MERGE_GROUPS = {"R035", "R036", "R039", "R044", "R055", "R057", "R068"}


def compact(text: object) -> str:
    return " ".join(str(text or "").replace("\r", " ").replace("\n", " ").split())


def to_int(value: object) -> int:
    text = compact(value)
    if not text:
        return 0
    try:
        return int(text)
    except ValueError:
        try:
            return int(float(text))
        except ValueError:
            return 0


def read_dict_csv(path: Path, encoding: str = OUTPUT_ENCODING) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding=encoding, newline="") as f:
        reader = csv.DictReader(f)
        return list(reader.fieldnames or []), list(reader)


def write_dict_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]], encoding: str = OUTPUT_ENCODING) -> None:
    with path.open("w", encoding=encoding, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def group_rows(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row["审查组编号"]].append(row)
    return dict(sorted(grouped.items()))


def scan_residual_candidate_keys() -> Counter[str]:
    counter: Counter[str] = Counter()
    with MERGED_PATH.open("r", encoding=INPUT_ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = residual_group_key(row)
            if key:
                counter[key] += 1
    return counter


def collect_residual_group_rows(valid_keys: set[str]) -> tuple[list[str], dict[str, list[dict[str, str]]]]:
    fieldnames: list[str] = []
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    with MERGED_PATH.open("r", encoding=INPUT_ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        for row in reader:
            key = residual_group_key(row)
            if key in valid_keys:
                grouped[key].append(dict(row))
    return fieldnames, grouped


def sort_residual_groups(grouped: dict[str, list[dict[str, str]]]) -> list[tuple[str, list[dict[str, str]]]]:
    ordered = []
    for key, rows in grouped.items():
        if len(rows) <= 1:
            continue
        title, _ = pick_title_and_field(rows[0])
        year = compact(rows[0].get("year", ""))
        first_author, _ = pick_first_author(rows[0])
        ordered.append((key, year, title, first_author, rows))
    ordered.sort(key=lambda item: (item[1], item[2], item[3], item[0]))
    return [(key, rows) for key, _year, _title, _first_author, rows in ordered]


def row_richness_score(row: dict[str, str]) -> tuple[int, int, int, int]:
    nonempty_count = sum(1 for value in row.values() if compact(value))
    text_weight = sum(len(compact(value)) for value in row.values() if compact(value))
    source_group_count = len([part for part in compact(row.get("source_db_group", "")).split("|") if part])
    source_bonus = SOURCE_PRIORITY.get(compact(row.get("source_db_primary", "")).upper(), 0)
    return (nonempty_count, text_weight, source_group_count, source_bonus)


def split_tokens(value: str, separator: str) -> list[str]:
    text = compact(value)
    if not text:
        return []
    return [part.strip() for part in text.split(separator) if part.strip()]


def merge_unique_tokens(values: list[str], separator: str) -> str:
    seen: set[str] = set()
    merged: list[str] = []
    for value in values:
        for token in split_tokens(value, separator):
            if token not in seen:
                seen.add(token)
                merged.append(token)
    return f"{separator}".join(merged) if separator == " || " else f" {separator} ".join(merged)


def build_merged_row(rows: list[dict[str, str]], fieldnames: list[str]) -> tuple[dict[str, str], int]:
    scored = sorted(
        enumerate(rows),
        key=lambda item: row_richness_score(item[1]),
        reverse=True,
    )
    keep_idx = scored[0][0]
    merged = dict(rows[keep_idx])

    ordered_rows = [rows[idx] for idx, _row in scored]

    for field in fieldnames:
        if field in {"institution_extracted", "institution_country_pairs", "source_db", "source_db_primary", "source_db_group", "dedup_group_size", "dedup_match_basis"}:
            continue
        if compact(merged.get(field, "")):
            continue
        for row in ordered_rows:
            candidate = compact(row.get(field, ""))
            if candidate:
                merged[field] = candidate
                break

    merged["institution_extracted"] = merge_unique_tokens([row.get("institution_extracted", "") for row in rows], "|")
    merged["institution_country_pairs"] = merge_unique_tokens([row.get("institution_country_pairs", "") for row in rows], "||")

    merged["source_db"] = "MERGED"
    source_groups = {
        part.strip().upper()
        for row in rows
        for part in compact(row.get("source_db_group", "")).split("|")
        if part.strip()
    }
    merged["source_db_group"] = "|".join(source for source in SOURCE_ORDER if source in source_groups)
    merged["source_db_primary"] = compact(rows[keep_idx].get("source_db_primary", "")).upper() or compact(rows[keep_idx].get("source_db", "")).upper() or "CNKI"
    merged["dedup_group_size"] = str(sum(max(1, to_int(row.get("dedup_group_size", ""))) for row in rows))
    merged["dedup_match_basis"] = "post_audit_residual_merge"

    if not compact(merged.get("publish_date", "")) and compact(merged.get("year", "")):
        merged["publish_date"] = f"{compact(merged['year'])}-00"

    if not compact(merged.get("title_cn_en", "")):
        title_parts = [compact(merged.get("title_cn", "")), compact(merged.get("title_en", ""))]
        merged["title_cn_en"] = " | ".join([part for part in title_parts if part])

    return merged, keep_idx


def choose_final_residual_action(group_id: str, machine_class: str) -> str:
    if group_id in MANUAL_RESIDUAL_MERGE_GROUPS:
        return "合并去重"
    if machine_class == "建议合并":
        return "合并去重"
    return "保留原状"


def annotate_final_decisions(
    residual_detail_rows: list[dict[str, str]],
    residual_summary_rows: list[dict[str, str]],
    conflict_detail_rows: list[dict[str, str]],
    conflict_summary_rows: list[dict[str, str]],
    residual_keep_seq: dict[str, int],
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    for row in residual_summary_rows:
        final_action = choose_final_residual_action(row["审查组编号"], row["机器预判分类"])
        row["最终处置"] = final_action
        row["是否执行到语料"] = "是"
        row["最终处理说明"] = (
            "按最终审查决定执行合并，语料中仅保留信息更完整的1条记录。"
            if final_action == "合并去重"
            else "按最终审查决定保留原状，不对语料做删除。"
        )
        row["保留组内序号"] = str(residual_keep_seq.get(row["审查组编号"], "")) if final_action == "合并去重" else ""

    for row in residual_detail_rows:
        final_action = choose_final_residual_action(row["审查组编号"], row["机器预判分类"])
        keep_seq = residual_keep_seq.get(row["审查组编号"])
        row["最终处置"] = final_action
        row["是否执行到语料"] = "是"
        row["最终是否保留"] = "是" if (final_action == "保留原状" or str(keep_seq) == row["组内序号"]) else "否"
        row["最终处理说明"] = (
            "该组按最终审查决定合并去重；本行作为保留记录写回语料。"
            if final_action == "合并去重" and str(keep_seq) == row["组内序号"]
            else "该组按最终审查决定合并去重；本行作为重复记录从语料中移除。"
            if final_action == "合并去重"
            else "该组按最终审查决定保留原状。"
        )

    for row in conflict_summary_rows:
        row["最终处置"] = "保留原状"
        row["是否执行到语料"] = "是"
        row["最终处理说明"] = "标准DOI存在冲突且缺乏足够证据证明应合并，最终统一保留。"

    for row in conflict_detail_rows:
        row["最终处置"] = "保留原状"
        row["是否执行到语料"] = "是"
        row["最终是否保留"] = "是"
        row["最终处理说明"] = "标准DOI存在冲突且缺乏足够证据证明应合并，最终统一保留。"

    return residual_detail_rows, residual_summary_rows, conflict_detail_rows, conflict_summary_rows


def write_updated_audit_tables(
    residual_detail_fields: list[str],
    residual_detail_rows: list[dict[str, str]],
    residual_summary_fields: list[str],
    residual_summary_rows: list[dict[str, str]],
    conflict_detail_fields: list[str],
    conflict_detail_rows: list[dict[str, str]],
    conflict_summary_fields: list[str],
    conflict_summary_rows: list[dict[str, str]],
) -> None:
    residual_detail_fields = residual_detail_fields + [field for field in ["最终处置", "是否执行到语料", "最终是否保留", "最终处理说明"] if field not in residual_detail_fields]
    residual_summary_fields = residual_summary_fields + [field for field in ["最终处置", "是否执行到语料", "最终处理说明", "保留组内序号"] if field not in residual_summary_fields]
    conflict_detail_fields = conflict_detail_fields + [field for field in ["最终处置", "是否执行到语料", "最终是否保留", "最终处理说明"] if field not in conflict_detail_fields]
    conflict_summary_fields = conflict_summary_fields + [field for field in ["最终处置", "是否执行到语料", "最终处理说明"] if field not in conflict_summary_fields]

    write_dict_csv(RESIDUAL_DETAIL_PATH, residual_detail_fields, residual_detail_rows)
    write_dict_csv(RESIDUAL_SUMMARY_PATH, residual_summary_fields, residual_summary_rows)
    write_dict_csv(CONFLICT_DETAIL_PATH, conflict_detail_fields, conflict_detail_rows)
    write_dict_csv(CONFLICT_SUMMARY_PATH, conflict_summary_fields, conflict_summary_rows)


def main() -> None:
    residual_key_counts = scan_residual_candidate_keys()
    candidate_keys = {key for key, count in residual_key_counts.items() if count > 1}
    fieldnames, grouped_rows = collect_residual_group_rows(candidate_keys)
    ordered_groups = sort_residual_groups(grouped_rows)

    group_id_to_key: dict[str, str] = {}
    merged_row_by_group_id: dict[str, dict[str, str]] = {}
    merge_group_ids: set[str] = set()
    keep_group_ids: set[str] = set()
    residual_keep_seq: dict[str, int] = {}
    action_log_rows: list[dict[str, str]] = []

    residual_summary_fields, residual_summary_rows = read_dict_csv(RESIDUAL_SUMMARY_PATH)
    residual_detail_fields, residual_detail_rows = read_dict_csv(RESIDUAL_DETAIL_PATH)
    conflict_summary_fields, conflict_summary_rows = read_dict_csv(CONFLICT_SUMMARY_PATH)
    conflict_detail_fields, conflict_detail_rows = read_dict_csv(CONFLICT_DETAIL_PATH)
    residual_machine_class = {row["审查组编号"]: row["机器预判分类"] for row in residual_summary_rows}

    for idx, (group_key, rows) in enumerate(ordered_groups, 1):
        group_id = f"R{idx:03d}"
        group_id_to_key[group_id] = group_key
        final_action = choose_final_residual_action(group_id, residual_machine_class.get(group_id, ""))
        if final_action == "合并去重":
            merge_group_ids.add(group_id)
            merged_row, keep_idx = build_merged_row(rows, fieldnames)
            merged_row_by_group_id[group_id] = merged_row
            sorted_rows = sorted(
                rows,
                key=lambda row: (
                    compact(row.get("source_db_group", "")),
                    compact(row.get("journal_cn", "") or row.get("journal_en", "")),
                    compact(row.get("doi", "")),
                    compact(row.get("title_cn", "") or row.get("title_cn_en", "") or row.get("title_en", "")),
                ),
            )
            keep_row = rows[keep_idx]
            keep_seq = 1
            for seq, row in enumerate(sorted_rows, 1):
                if row is keep_row:
                    keep_seq = seq
                    break
            residual_keep_seq[group_id] = keep_seq
            title, _ = pick_title_and_field(rows[0])
            first_author, _ = pick_first_author(rows[0])
            action_log_rows.append(
                {
                    "审查组编号": group_id,
                    "候选类型": "残留重复候选",
                    "最终处置": final_action,
                    "组内记录数": str(len(rows)),
                    "保留组内序号": str(keep_seq),
                    "年份": compact(rows[0].get("year", "")),
                    "标题": title,
                    "第一作者": first_author,
                    "来源库组合": " | ".join(sorted({compact(row.get('source_db_group', '')) for row in rows if compact(row.get('source_db_group', ''))})),
                    "处理说明": "按最终审查决定执行合并，写回1条融合后的记录。",
                }
            )
        else:
            keep_group_ids.add(group_id)
            title, _ = pick_title_and_field(rows[0])
            first_author, _ = pick_first_author(rows[0])
            action_log_rows.append(
                {
                    "审查组编号": group_id,
                    "候选类型": "残留重复候选",
                    "最终处置": final_action,
                    "组内记录数": str(len(rows)),
                    "保留组内序号": "",
                    "年份": compact(rows[0].get("year", "")),
                    "标题": title,
                    "第一作者": first_author,
                    "来源库组合": " | ".join(sorted({compact(row.get('source_db_group', '')) for row in rows if compact(row.get('source_db_group', ''))})),
                    "处理说明": "按最终审查决定保留原状，不执行删除。",
                }
            )

    for row in conflict_summary_rows:
        action_log_rows.append(
            {
                "审查组编号": row["审查组编号"],
                "候选类型": row["候选类型"],
                "最终处置": "保留原状",
                "组内记录数": row["组内记录数"],
                "保留组内序号": "",
                "年份": "",
                "标题": row["标题概览"],
                "第一作者": row["第一作者"],
                "来源库组合": row["组来源库组合"],
                "处理说明": "标准DOI冲突样本最终统一保留，不执行删除。",
            }
        )

    residual_detail_rows, residual_summary_rows, conflict_detail_rows, conflict_summary_rows = annotate_final_decisions(
        residual_detail_rows,
        residual_summary_rows,
        conflict_detail_rows,
        conflict_summary_rows,
        residual_keep_seq,
    )
    write_updated_audit_tables(
        residual_detail_fields,
        residual_detail_rows,
        residual_summary_fields,
        residual_summary_rows,
        conflict_detail_fields,
        conflict_detail_rows,
        conflict_summary_fields,
        conflict_summary_rows,
    )

    merge_keys = {group_id_to_key[group_id]: group_id for group_id in merge_group_ids}
    final_row_count = 0
    removed_row_count = 0
    written_merge_groups: set[str] = set()

    temp_output = MERGED_PATH.with_name(MERGED_PATH.stem + ".tmp.csv")
    with MERGED_PATH.open("r", encoding=INPUT_ENCODING, newline="") as src, temp_output.open("w", encoding=INPUT_ENCODING, newline="") as dst:
        reader = csv.DictReader(src)
        fieldnames = list(reader.fieldnames or [])
        writer = csv.DictWriter(dst, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            key = residual_group_key(row)
            if key in merge_keys:
                group_id = merge_keys[key]
                if group_id not in written_merge_groups:
                    writer.writerow(merged_row_by_group_id[group_id])
                    written_merge_groups.add(group_id)
                    final_row_count += 1
                else:
                    removed_row_count += 1
                continue

            writer.writerow(row)
            final_row_count += 1

    temp_output.replace(MERGED_PATH)

    action_log_fields = [
        "审查组编号",
        "候选类型",
        "最终处置",
        "组内记录数",
        "保留组内序号",
        "年份",
        "标题",
        "第一作者",
        "来源库组合",
        "处理说明",
    ]
    write_dict_csv(ACTION_LOG_PATH, action_log_fields, action_log_rows)

    print(f"merged_path={MERGED_PATH}")
    print(f"residual_merge_groups={len(merge_group_ids)}")
    print(f"residual_keep_groups={len(keep_group_ids)}")
    print(f"conflict_keep_groups={len(conflict_summary_rows)}")
    print(f"removed_rows={removed_row_count}")
    print(f"final_rows={final_row_count}")
    print(f"action_log={ACTION_LOG_PATH}")


if __name__ == "__main__":
    main()
