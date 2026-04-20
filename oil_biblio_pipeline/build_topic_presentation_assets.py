from __future__ import annotations

import argparse
import csv
import math
import re
from collections import Counter, defaultdict
from pathlib import Path

import matplotlib
import numpy as np
from wordcloud import WordCloud

from topic_evolution_pipeline import (
    HIGH_FREQ_TERM_COUNT,
    INPUT_ENCODING,
    OUTPUT_ENCODING,
    PERIODS,
    MIN_TOKEN_COUNT,
    build_output_paths,
    choose_abstract,
    clean_presentation_topic_term,
    compact_text,
    is_petroleum_relevant,
    period_label_from_year,
    preprocess_tokens,
)

matplotlib.use("Agg")
import matplotlib.pyplot as plt


DEFAULT_FONT_PATH = Path(r"C:/Windows/Fonts/msyh.ttc")
RE_EN = re.compile(r"[A-Za-z]")

PRESENTATION_OUTPUT_TEMPLATES = {
    "wordcloud_terms": "topic_wordcloud_terms_{dataset_tag}_2011_2025.csv",
    "top20_terms": "topic_stage_top20_terms_{dataset_tag}_2011_2025.csv",
    "topic_overview": "topic_stage_topic_overview_{dataset_tag}_2011_2025.csv",
    "summary": "topic_presentation_assets_summary_{dataset_tag}_2011_2025.txt",
    "wordcloud_panel": "topic_wordcloud_panel_{dataset_tag}_2011_2025.png",
}


def build_presentation_output_paths(output_dir: Path, dataset_tag: str) -> dict[str, Path]:
    return {
        key: output_dir / template.format(dataset_tag=dataset_tag)
        for key, template in PRESENTATION_OUTPUT_TEMPLATES.items()
    }


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding=OUTPUT_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_text(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding=OUTPUT_ENCODING)


def collect_presentation_term_stats(input_path: Path) -> dict[str, dict[str, Counter[str] | int]]:
    stats: dict[str, dict[str, Counter[str] | int]] = {
        label: {
            "total_counter": Counter(),
            "doc_counter": Counter(),
            "document_count": 0,
        }
        for label, _, _ in PERIODS
    }
    with input_path.open("r", encoding=INPUT_ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            year_text = compact_text(row.get("year", ""))
            if not year_text.isdigit():
                continue
            period_label = period_label_from_year(int(year_text))
            if not period_label:
                continue
            abstract_text, language = choose_abstract(row)
            if not abstract_text or not is_petroleum_relevant(row, abstract_text):
                continue
            tokens = preprocess_tokens(abstract_text, language)
            if len(tokens) < MIN_TOKEN_COUNT:
                continue
            cleaned_tokens = [
                term
                for token in tokens
                for term in [clean_presentation_topic_term(token)]
                if term and not RE_EN.search(term)
            ]
            if not cleaned_tokens:
                continue
            stats[period_label]["document_count"] = int(stats[period_label]["document_count"]) + 1
            stats[period_label]["total_counter"].update(cleaned_tokens)
            stats[period_label]["doc_counter"].update(set(cleaned_tokens))
    return stats


def build_ranked_term_rows(stats: dict[str, dict[str, Counter[str] | int]], limit: int = HIGH_FREQ_TERM_COUNT) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for period_label, _, _ in PERIODS:
        total_counter = stats[period_label]["total_counter"]
        doc_counter = stats[period_label]["doc_counter"]
        if not isinstance(total_counter, Counter) or not isinstance(doc_counter, Counter):
            continue
        ranked = sorted(total_counter.items(), key=lambda item: (-item[1], -doc_counter[item[0]], item[0]))[:limit]
        max_frequency = ranked[0][1] if ranked else 1
        for rank, (term, total_frequency) in enumerate(ranked, start=1):
            rows.append(
                {
                    "阶段": period_label,
                    "阶段内排序": rank,
                    "词项": term,
                    "总词频": int(total_frequency),
                    "文献频次": int(doc_counter[term]),
                    "词云权重": round(total_frequency / max_frequency, 6) if max_frequency else 0.0,
                    "进入词频统计文献数": int(stats[period_label]["document_count"]),
                }
            )
    return rows


def build_topic_overview_rows(topic_strength_path: Path, keyword_distribution_path: Path) -> list[dict[str, object]]:
    with topic_strength_path.open("r", encoding=OUTPUT_ENCODING, newline="") as f:
        source_rows = list(csv.DictReader(f))
    with keyword_distribution_path.open("r", encoding=OUTPUT_ENCODING, newline="") as f:
        keyword_rows = list(csv.DictReader(f))

    cleaned_keyword_map: dict[tuple[str, str], list[str]] = defaultdict(list)
    for row in sorted(keyword_rows, key=lambda item: (str(item["阶段"]), str(item["主题编号"]), int(item["关键词排名"]))):
        key = (str(row["阶段"]), str(row["主题编号"]))
        cleaned_keyword = clean_presentation_topic_term(str(row["关键词"]))
        if not cleaned_keyword or RE_EN.search(cleaned_keyword):
            continue
        if cleaned_keyword not in cleaned_keyword_map[key]:
            cleaned_keyword_map[key].append(cleaned_keyword)

    grouped_rows: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in source_rows:
        grouped_rows[str(row["阶段"])].append(row)
    output_rows: list[dict[str, object]] = []
    for period_label, _, _ in PERIODS:
        rows = grouped_rows.get(period_label, [])
        rows.sort(key=lambda item: (-float(item["主题强度"]), item["主题编号"]))
        for rank, row in enumerate(rows, start=1):
            cleaned_keywords = cleaned_keyword_map.get((period_label, str(row["主题编号"])), [])
            output_rows.append(
                {
                    "阶段": period_label,
                    "阶段内主题强度排序": rank,
                    "主题编号": row["主题编号"],
                    "主题标签": row["主题标签"],
                    "主题强度": row["主题强度"],
                    "主题文献数": row["主题文献数"],
                    "阶段文献总数": row["阶段文献总数"],
                    "平均主题概率": row["平均主题概率"],
                    "代表关键词": " | ".join(cleaned_keywords[:8]) if cleaned_keywords else row["代表关键词"],
                }
            )
    return output_rows


def render_wordcloud_images(
    ranked_rows: list[dict[str, object]],
    output_dir: Path,
    dataset_tag: str,
    dataset_label: str,
    font_path: Path,
) -> list[Path]:
    font_path = font_path if font_path.exists() else DEFAULT_FONT_PATH
    if not font_path.exists():
        raise FileNotFoundError(f"未找到可用中文字体文件: {font_path}")

    rows_by_period: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in ranked_rows:
        rows_by_period[str(row["阶段"])].append(row)

    period_images: list[tuple[str, np.ndarray]] = []
    output_paths: list[Path] = []
    for period_label, _, _ in PERIODS:
        rows = rows_by_period.get(period_label, [])
        frequencies = {str(row["词项"]): float(row["总词频"]) for row in rows[:80]}
        wordcloud = WordCloud(
            font_path=str(font_path),
            width=1800,
            height=1100,
            background_color="white",
            colormap="YlGnBu",
            prefer_horizontal=0.85,
            max_words=80,
            random_state=42,
        ).generate_from_frequencies(frequencies or {"暂无有效词项": 1.0})
        image_array = wordcloud.to_array()
        period_images.append((period_label, image_array))

        single_path = output_dir / f"topic_wordcloud_{period_label.replace('-', '_')}_{dataset_tag}_2011_2025.png"
        plt.figure(figsize=(10, 6))
        plt.imshow(image_array, interpolation="bilinear")
        plt.axis("off")
        plt.title(f"{period_label} 主题词云（{dataset_label}）", fontproperties="Microsoft YaHei")
        plt.tight_layout()
        plt.savefig(single_path, dpi=220, bbox_inches="tight")
        plt.close()
        output_paths.append(single_path)

    panel_path = output_dir / PRESENTATION_OUTPUT_TEMPLATES["wordcloud_panel"].format(dataset_tag=dataset_tag)
    fig, axes = plt.subplots(1, len(PERIODS), figsize=(18, 6), constrained_layout=True)
    for ax, (period_label, image_array) in zip(np.atleast_1d(axes), period_images):
        ax.imshow(image_array, interpolation="bilinear")
        ax.axis("off")
        ax.set_title(period_label)
    fig.savefig(panel_path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    output_paths.append(panel_path)
    return output_paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--dataset-tag", required=True)
    parser.add_argument("--dataset-label", required=True)
    parser.add_argument("--font-path", default=str(DEFAULT_FONT_PATH))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    dataset_tag = str(args.dataset_tag).strip()
    dataset_label = str(args.dataset_label).strip()
    font_path = Path(args.font_path)

    output_paths = build_output_paths(output_dir, dataset_tag)
    presentation_paths = build_presentation_output_paths(output_dir, dataset_tag)

    term_stats = collect_presentation_term_stats(input_path)
    ranked_rows = build_ranked_term_rows(term_stats, limit=HIGH_FREQ_TERM_COUNT)
    cleaned_high_freq_rows = [
        {
            "阶段": row["阶段"],
            "词项": row["词项"],
            "总词频": row["总词频"],
            "文献频次": row["文献频次"],
        }
        for row in ranked_rows
    ]
    write_csv(output_paths["high_frequency_terms"], ["阶段", "词项", "总词频", "文献频次"], cleaned_high_freq_rows)
    write_csv(
        presentation_paths["wordcloud_terms"],
        ["阶段", "阶段内排序", "词项", "总词频", "文献频次", "词云权重", "进入词频统计文献数"],
        ranked_rows,
    )
    write_csv(
        presentation_paths["top20_terms"],
        ["阶段", "阶段内排序", "词项", "总词频", "文献频次", "词云权重", "进入词频统计文献数"],
        [row for row in ranked_rows if int(row["阶段内排序"]) <= 20],
    )

    topic_overview_rows = build_topic_overview_rows(output_paths["topic_strength"], output_paths["keyword_distribution"])
    write_csv(
        presentation_paths["topic_overview"],
        ["阶段", "阶段内主题强度排序", "主题编号", "主题标签", "主题强度", "主题文献数", "阶段文献总数", "平均主题概率", "代表关键词"],
        topic_overview_rows,
    )

    image_paths = render_wordcloud_images(ranked_rows, output_dir, dataset_tag, dataset_label, font_path)
    summary_lines = [
        f"主题展示层材料生成完成（{dataset_label}，2011-2025）",
        "",
        "1. 高频词统计基于双主键主题分析使用的同一份入模语料进行重算。",
        "2. 统计过程沿用摘要筛选、石油领域过滤和分词逻辑，并额外执行展示层全中文化与泛词清洗。",
        "3. 已同步覆盖原高频词表，并生成词云数据源、阶段 Top20 词表和阶段主题概览表。",
        "",
        "各阶段进入词频统计文献数：",
    ]
    for period_label, _, _ in PERIODS:
        summary_lines.append(f"- {period_label}: {int(term_stats[period_label]['document_count'])} 篇")
    summary_lines.append("")
    summary_lines.append("输出文件：")
    summary_lines.extend(f"- {path}" for path in [output_paths["high_frequency_terms"], presentation_paths["wordcloud_terms"], presentation_paths["top20_terms"], presentation_paths["topic_overview"], *image_paths])
    write_text(presentation_paths["summary"], summary_lines)

    print(f"output_dir={output_dir}")
    for period_label, _, _ in PERIODS:
        print(f"{period_label}\tdocs={int(term_stats[period_label]['document_count'])}")


if __name__ == "__main__":
    main()
