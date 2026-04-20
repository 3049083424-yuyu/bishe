from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path

from topic_evolution_pipeline import (
    OUTPUT_ENCODING,
    build_output_paths,
    canonicalize_topic_term,
    topic_label_from_terms,
    write_summary,
)


PROFILE_TEMPLATES = {
    "type_distribution": "topic_distribution_by_institution_type_{dataset_tag}_2011_2025.csv",
    "level_distribution": "topic_distribution_by_institution_level_{dataset_tag}_2011_2025.csv",
}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding=OUTPUT_ENCODING, newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding=OUTPUT_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            output.append(value)
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--dataset-tag", required=True)
    parser.add_argument("--dataset-label", required=True)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    dataset_tag = str(args.dataset_tag).strip()
    dataset_label = str(args.dataset_label).strip()
    output_paths = build_output_paths(output_dir, dataset_tag)
    profile_paths = {
        key: output_dir / template.format(dataset_tag=dataset_tag)
        for key, template in PROFILE_TEMPLATES.items()
    }

    keyword_rows = read_csv(output_paths["keyword_distribution"])
    grouped_keywords: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in keyword_rows:
        row["关键词"] = canonicalize_topic_term(row["关键词"])
        grouped_keywords[(row["阶段"], row["主题编号"])].append(row)

    label_map: dict[tuple[str, str], str] = {}
    representative_map: dict[tuple[str, str], str] = {}
    for key, rows in grouped_keywords.items():
        rows.sort(key=lambda item: int(item["关键词排名"]))
        cleaned_keywords = dedupe_keep_order([row["关键词"] for row in rows])
        label = topic_label_from_terms(cleaned_keywords)
        representative = " | ".join(cleaned_keywords[:12])
        label_map[key] = label
        representative_map[key] = representative
        for row in rows:
            row["主题标签"] = label

    write_csv(
        output_paths["keyword_distribution"],
        list(keyword_rows[0].keys()) if keyword_rows else [],
        keyword_rows,
    )

    strength_rows = read_csv(output_paths["topic_strength"])
    for row in strength_rows:
        key = (row["阶段"], row["主题编号"])
        if key in label_map:
            row["主题标签"] = label_map[key]
            row["代表关键词"] = representative_map[key]
        else:
            cleaned_keywords = dedupe_keep_order(
                [canonicalize_topic_term(item.strip()) for item in row["代表关键词"].split("|")]
            )
            row["主题标签"] = topic_label_from_terms(cleaned_keywords)
            row["代表关键词"] = " | ".join(cleaned_keywords[:12])
    write_csv(output_paths["topic_strength"], list(strength_rows[0].keys()) if strength_rows else [], strength_rows)

    assignment_rows = read_csv(output_paths["topic_assignment"])
    for row in assignment_rows:
        key = (row["阶段"], row["主导主题编号"])
        if key in label_map:
            row["主导主题标签"] = label_map[key]
    write_csv(output_paths["topic_assignment"], list(assignment_rows[0].keys()) if assignment_rows else [], assignment_rows)

    path_rows = read_csv(output_paths["evolution_paths"])
    for row in path_rows:
        for period in ("2011-2015", "2016-2020", "2021-2025"):
            topic_id = row.get(f"{period}主题编号", "")
            if topic_id:
                row[f"{period}主题标签"] = label_map.get((period, topic_id), row.get(f"{period}主题标签", ""))
    write_csv(output_paths["evolution_paths"], list(path_rows[0].keys()) if path_rows else [], path_rows)

    high_freq_rows = read_csv(output_paths["high_frequency_terms"])
    for row in high_freq_rows:
        row["词项"] = canonicalize_topic_term(row["词项"])
    write_csv(output_paths["high_frequency_terms"], list(high_freq_rows[0].keys()) if high_freq_rows else [], high_freq_rows)

    for path in profile_paths.values():
        rows = read_csv(path)
        for row in rows:
            key = (row["阶段"], row["主题编号"])
            if key in label_map:
                row["主题标签"] = label_map[key]
        if rows:
            write_csv(path, list(rows[0].keys()), rows)

    preprocess_rows = read_csv(output_paths["preprocess_stats"])
    selection_rows = read_csv(output_paths["model_selection"])
    refreshed_strength_rows = read_csv(output_paths["topic_strength"])
    refreshed_path_rows = read_csv(output_paths["evolution_paths"])
    write_summary(
        output_paths["summary"],
        dataset_label,
        preprocess_rows,
        selection_rows,
        refreshed_strength_rows,
        refreshed_path_rows,
    )


if __name__ == "__main__":
    main()
