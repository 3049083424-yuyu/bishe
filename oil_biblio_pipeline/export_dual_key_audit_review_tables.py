from __future__ import annotations

import csv
import re
from collections import defaultdict
from pathlib import Path


BASE_DIR = Path(r"D:\毕业论文\version_dual_key_dedup_2011_2025")
INPUT_PATH = BASE_DIR / "merged_clean_dual_key_dedup_2011_2025.csv"
RESIDUAL_OUTPUT_PATH = BASE_DIR / "audit_residual_duplicate_candidates_dual_key_2011_2025.csv"
CONFLICT_OUTPUT_PATH = BASE_DIR / "audit_doi_title_conflict_dual_key_2011_2025.csv"

INPUT_ENCODING = "utf-8-sig"
OUTPUT_ENCODING = "gb18030"

RE_WORD = re.compile(r"\W+", re.UNICODE)


def compact(text: object) -> str:
    return " ".join(str(text or "").replace("\r", " ").replace("\n", " ").split())


def norm_text(text: str) -> str:
    return RE_WORD.sub("", compact(text).lower())


def pick_title_and_field(row: dict[str, str]) -> tuple[str, str]:
    for field in ("title_cn", "title_cn_en", "title_en"):
        value = compact(row.get(field, ""))
        if value:
            return value, field
    return "", ""


def pick_journal(row: dict[str, str]) -> str:
    return compact(row.get("journal_cn") or row.get("journal_en"))


def pick_first_author(row: dict[str, str]) -> tuple[str, str]:
    raw_cn = compact(row.get("author_cn", ""))
    raw_en = compact(row.get("author", ""))
    raw = raw_cn or raw_en
    first = re.split(r"[;；|,，/]", raw, maxsplit=1)[0].strip()
    return first, norm_text(first)


def group_source_combo(rows: list[dict[str, str]]) -> str:
    values = sorted({compact(row.get("source_db_group", "")) for row in rows if compact(row.get("source_db_group", ""))})
    return "|".join(values)


def group_journal_combo(rows: list[dict[str, str]]) -> str:
    values = sorted({pick_journal(row) for row in rows if pick_journal(row)})
    return " | ".join(values)


def yes_no(flag: bool) -> str:
    return "是" if flag else "否"


def residual_group_key(row: dict[str, str]) -> str:
    if compact(row.get("standard_doi_key", "")) or compact(row.get("meta_dedup_key", "")):
        return ""

    title, _ = pick_title_and_field(row)
    title_key = norm_text(title)
    year = compact(row.get("year", ""))
    first_author, author_key = pick_first_author(row)
    if len(title_key) < 8 or not year or not first_author or not author_key:
        return ""
    return f"{title_key}|{year}|{author_key}"


def build_residual_rows(rows: list[dict[str, str]]) -> tuple[list[dict[str, str]], int]:
    groups: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        key = residual_group_key(row)
        if key:
            groups[key].append(row)

    residual_groups = []
    for key, group_rows in groups.items():
        if len(group_rows) > 1:
            title, _ = pick_title_and_field(group_rows[0])
            year = compact(group_rows[0].get("year", ""))
            first_author, _ = pick_first_author(group_rows[0])
            residual_groups.append((key, year, title, first_author, group_rows))

    residual_groups.sort(key=lambda item: (item[1], item[2], item[3], item[0]))

    out_rows: list[dict[str, str]] = []
    for idx, (key, _year, _title, _first_author, group_rows) in enumerate(residual_groups, 1):
        group_id = f"R{idx:03d}"
        same_source = len({compact(row.get("source_db_group", "")) for row in group_rows}) == 1
        same_journal = len({pick_journal(row) for row in group_rows}) == 1
        source_combo = group_source_combo(group_rows)
        journal_combo = group_journal_combo(group_rows)

        sorted_group_rows = sorted(
            group_rows,
            key=lambda row: (
                compact(row.get("source_db_group", "")),
                pick_journal(row),
                compact(row.get("doi", "")),
                compact(row.get("title_cn", "") or row.get("title_cn_en", "") or row.get("title_en", "")),
            ),
        )

        for row_idx, row in enumerate(sorted_group_rows, 1):
            title, title_field = pick_title_and_field(row)
            first_author, _ = pick_first_author(row)
            out_rows.append(
                {
                    "审查组编号": group_id,
                    "组内序号": str(row_idx),
                    "候选类型": "残留重复候选",
                    "组内记录数": str(len(group_rows)),
                    "同来源库": yes_no(same_source),
                    "同期刊": yes_no(same_journal),
                    "组来源库组合": source_combo,
                    "组期刊组合": journal_combo,
                    "宽松候选键": key,
                    "年份": compact(row.get("year", "")),
                    "第一作者": first_author,
                    "标题": title,
                    "标题来源字段": title_field,
                    "期刊": pick_journal(row),
                    "当前记录来源库组合": compact(row.get("source_db_group", "")),
                    "当前主来源库": compact(row.get("source_db_primary", "")),
                    "doi原值": compact(row.get("doi", "")),
                    "标准doi键": compact(row.get("standard_doi_key", "")),
                    "元数据去重键": compact(row.get("meta_dedup_key", "")),
                    "当前去重组大小": compact(row.get("dedup_group_size", "")),
                    "当前去重匹配依据": compact(row.get("dedup_match_basis", "")),
                    "被引频次": compact(row.get("cited_count", "")),
                    "机构原始字段": compact(row.get("institution", "")),
                    "机构抽取字段": compact(row.get("institution_extracted", "")),
                    "机构标准化字段": compact(row.get("institution_norm", "")),
                    "备注": "无标准DOI且无英文元数据键；按题名+年份+第一作者宽松规则识别为疑似重复",
                }
            )

    return out_rows, len(residual_groups)


def build_conflict_rows(rows: list[dict[str, str]]) -> tuple[list[dict[str, str]], int]:
    groups: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        meta_key = compact(row.get("meta_dedup_key", ""))
        if meta_key:
            groups[meta_key].append(row)

    conflict_groups = []
    for meta_key, group_rows in groups.items():
        if len(group_rows) <= 1:
            continue
        std_keys = sorted({compact(row.get("standard_doi_key", "")) for row in group_rows if compact(row.get("standard_doi_key", ""))})
        if len(std_keys) <= 1:
            continue
        title, _ = pick_title_and_field(group_rows[0])
        year = compact(group_rows[0].get("year", ""))
        first_author, _ = pick_first_author(group_rows[0])
        conflict_groups.append((meta_key, year, title, first_author, std_keys, group_rows))

    conflict_groups.sort(key=lambda item: (item[1], item[2], item[3], item[0]))

    out_rows: list[dict[str, str]] = []
    for idx, (meta_key, _year, _title, _first_author, std_keys, group_rows) in enumerate(conflict_groups, 1):
        group_id = f"C{idx:03d}"
        source_combo = group_source_combo(group_rows)
        journal_combo = group_journal_combo(group_rows)

        sorted_group_rows = sorted(
            group_rows,
            key=lambda row: (
                compact(row.get("standard_doi_key", "")),
                compact(row.get("doi", "")),
                compact(row.get("source_db_group", "")),
            ),
        )

        for row_idx, row in enumerate(sorted_group_rows, 1):
            title, title_field = pick_title_and_field(row)
            first_author, _ = pick_first_author(row)
            out_rows.append(
                {
                    "审查组编号": group_id,
                    "组内序号": str(row_idx),
                    "候选类型": "DOI/题名冲突样本",
                    "组内记录数": str(len(group_rows)),
                    "冲突标准DOI数": str(len(std_keys)),
                    "组来源库组合": source_combo,
                    "组期刊组合": journal_combo,
                    "meta_dedup_key": meta_key,
                    "年份": compact(row.get("year", "")),
                    "第一作者": first_author,
                    "标题": title,
                    "标题来源字段": title_field,
                    "期刊": pick_journal(row),
                    "当前记录来源库组合": compact(row.get("source_db_group", "")),
                    "当前主来源库": compact(row.get("source_db_primary", "")),
                    "doi原值": compact(row.get("doi", "")),
                    "标准doi键": compact(row.get("standard_doi_key", "")),
                    "元数据去重键": compact(row.get("meta_dedup_key", "")),
                    "当前去重组大小": compact(row.get("dedup_group_size", "")),
                    "当前去重匹配依据": compact(row.get("dedup_match_basis", "")),
                    "被引频次": compact(row.get("cited_count", "")),
                    "机构原始字段": compact(row.get("institution", "")),
                    "机构抽取字段": compact(row.get("institution_extracted", "")),
                    "机构标准化字段": compact(row.get("institution_norm", "")),
                    "备注": "同meta_key下出现多个标准DOI；算法为避免误并已保留为独立记录",
                }
            )

    return out_rows, len(conflict_groups)


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    if not rows:
        raise ValueError(f"No rows to write: {path}")
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding=OUTPUT_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    with INPUT_PATH.open("r", encoding=INPUT_ENCODING, newline="") as f:
        rows = list(csv.DictReader(f))

    residual_rows, residual_group_count = build_residual_rows(rows)
    conflict_rows, conflict_group_count = build_conflict_rows(rows)

    write_csv(RESIDUAL_OUTPUT_PATH, residual_rows)
    write_csv(CONFLICT_OUTPUT_PATH, conflict_rows)

    print(f"input={INPUT_PATH}")
    print(f"residual_output={RESIDUAL_OUTPUT_PATH}")
    print(f"residual_group_count={residual_group_count}")
    print(f"residual_row_count={len(residual_rows)}")
    print(f"conflict_output={CONFLICT_OUTPUT_PATH}")
    print(f"conflict_group_count={conflict_group_count}")
    print(f"conflict_row_count={len(conflict_rows)}")


if __name__ == "__main__":
    main()
