from __future__ import annotations

import argparse
import pickle
from collections import Counter
from pathlib import Path

import jieba

import topic_evolution_pipeline as tp


CACHE_DIRNAME = "_runtime_cache"


def cache_dir(output_dir: Path) -> Path:
    return output_dir / CACHE_DIRNAME


def preprocess_cache_path(output_dir: Path, dataset_tag: str) -> Path:
    return cache_dir(output_dir) / f"topic_preprocess_cache_{dataset_tag}_2011_2025.pkl"


def period_cache_path(output_dir: Path, dataset_tag: str, period_label: str) -> Path:
    slug = period_label.replace("-", "_")
    return cache_dir(output_dir) / f"topic_period_result_{slug}_{dataset_tag}_2011_2025.pkl"


def save_pickle(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)


def load_pickle(path: Path) -> object:
    with path.open("rb") as f:
        return pickle.load(f)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True, choices=("preprocess", "period", "finalize"))
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--dataset-tag", required=True)
    parser.add_argument("--dataset-label", required=True)
    parser.add_argument("--period")
    return parser


def init_jieba() -> None:
    for word in sorted(tp.ZH_CUSTOM_WORDS):
        jieba.add_word(word)


def run_preprocess(input_path: Path, output_dir: Path, dataset_tag: str) -> None:
    init_jieba()
    tp.log_progress(f"分步预处理启动：input={input_path}，output_dir={output_dir}，dataset_tag={dataset_tag}")
    period_documents: dict[str, list[str]] = {label: [] for label, _, _ in tp.PERIODS}
    period_source_rows: dict[str, list[dict[str, object]]] = {label: [] for label, _, _ in tp.PERIODS}
    preprocess_stats: dict[str, dict[str, object]] = {
        label: {
            "阶段": label,
            "年份范围": label,
            "阶段文献数": 0,
            "有摘要文献数": 0,
            "英文摘要文献数": 0,
            "中文摘要文献数": 0,
            "通过领域过滤文献数": 0,
            "进入建模文献数": 0,
            "平均有效词数": 0.0,
            "词汇表规模": 0,
        }
        for label, _, _ in tp.PERIODS
    }
    token_counter: Counter[str] = Counter()
    scanned_rows = 0

    with input_path.open("r", encoding=tp.INPUT_ENCODING, newline="") as f:
        reader = tp.csv.DictReader(f)
        for row in reader:
            scanned_rows += 1
            if scanned_rows % 50000 == 0:
                tp.log_progress(f"预处理扫描进度：已读取 {scanned_rows} 篇文献")
            year_text = tp.compact_text(row.get("year", ""))
            if not year_text.isdigit():
                continue
            period_label = tp.period_label_from_year(int(year_text))
            if not period_label:
                continue
            preprocess_stats[period_label]["阶段文献数"] = int(preprocess_stats[period_label]["阶段文献数"]) + 1
            abstract_text, language = tp.choose_abstract(row)
            if not abstract_text:
                continue
            preprocess_stats[period_label]["有摘要文献数"] = int(preprocess_stats[period_label]["有摘要文献数"]) + 1
            abstract_field = "英文摘要文献数" if language == "en" else "中文摘要文献数"
            preprocess_stats[period_label][abstract_field] = int(preprocess_stats[period_label][abstract_field]) + 1
            if not tp.is_petroleum_relevant(row, abstract_text):
                continue
            preprocess_stats[period_label]["通过领域过滤文献数"] = int(preprocess_stats[period_label]["通过领域过滤文献数"]) + 1
            tokens = tp.preprocess_tokens(abstract_text, language)
            if len(tokens) < tp.MIN_TOKEN_COUNT:
                continue
            token_counter[period_label] += len(tokens)
            preprocess_stats[period_label]["进入建模文献数"] = int(preprocess_stats[period_label]["进入建模文献数"]) + 1
            period_documents[period_label].append(" ".join(tokens))
            period_source_rows[period_label].append(
                {
                    "文献签名": tp.build_document_signature(row),
                    "年份": year_text,
                    "来源库": tp.compact_text(row.get("source_db_primary", "")) or tp.compact_text(row.get("source_db", "")),
                    "DOI": tp.compact_text(row.get("doi", "")),
                    "标题": tp.compact_text(row.get("title_cn", "")) or tp.compact_text(row.get("title_en", "")),
                    "第一作者": tp.compact_text(row.get("author_cn", "")) or tp.compact_text(row.get("author", "")),
                }
            )

    for period_label, _, _ in tp.PERIODS:
        tp.log_progress(
            f"{period_label}：预处理完成，阶段文献数={preprocess_stats[period_label]['阶段文献数']}，"
            f"有摘要={preprocess_stats[period_label]['有摘要文献数']}，"
            f"通过领域过滤={preprocess_stats[period_label]['通过领域过滤文献数']}，"
            f"进入建模={preprocess_stats[period_label]['进入建模文献数']}"
        )

    payload = {
        "period_documents": period_documents,
        "period_source_rows": period_source_rows,
        "preprocess_stats": preprocess_stats,
        "token_counter": dict(token_counter),
    }
    save_pickle(preprocess_cache_path(output_dir, dataset_tag), payload)
    tp.log_progress(f"预处理缓存写出完成：{preprocess_cache_path(output_dir, dataset_tag)}")


def run_period(output_dir: Path, dataset_tag: str, period_label: str) -> None:
    payload = load_pickle(preprocess_cache_path(output_dir, dataset_tag))
    assert isinstance(payload, dict)
    period_documents = payload["period_documents"]
    preprocess_stats = payload["preprocess_stats"]
    token_counter = payload["token_counter"]

    documents = list(period_documents[period_label])
    if not documents:
        raise ValueError(f"{period_label} 无可建模文献。")

    doc_count = int(preprocess_stats[period_label]["进入建模文献数"])
    avg_effective_tokens = round(float(token_counter.get(period_label, 0)) / doc_count if doc_count else 0.0, 4)
    selection_rows, selected_topic_count = tp.evaluate_topic_counts(period_label, documents)
    result = tp.fit_period_model(period_label, documents, selected_topic_count)
    period_payload = {
        "period_label": period_label,
        "selection_rows": selection_rows,
        "selected_topic_count": selected_topic_count,
        "avg_effective_tokens": avg_effective_tokens,
        "result": result,
    }
    save_pickle(period_cache_path(output_dir, dataset_tag, period_label), period_payload)
    tp.log_progress(
        f"{period_label}：阶段结果缓存写出完成，主题数={selected_topic_count}，"
        f"词汇表规模={result['matrix_shape'][1]}，cache={period_cache_path(output_dir, dataset_tag, period_label)}"
    )


def run_finalize(output_dir: Path, dataset_tag: str, dataset_label: str) -> None:
    preprocess_payload = load_pickle(preprocess_cache_path(output_dir, dataset_tag))
    assert isinstance(preprocess_payload, dict)
    period_documents = preprocess_payload["period_documents"]
    period_source_rows = preprocess_payload["period_source_rows"]
    preprocess_stats = preprocess_payload["preprocess_stats"]
    output_paths = tp.build_output_paths(output_dir, dataset_tag)

    selection_rows: list[dict[str, object]] = []
    fitted_results: dict[str, dict[str, object]] = {}
    all_topic_keyword_rows: list[dict[str, object]] = []
    all_topic_strength_rows: list[dict[str, object]] = []
    all_high_freq_rows: list[dict[str, object]] = []
    all_topic_assignment_rows: list[dict[str, object]] = []
    selected_topic_counts: dict[str, int] = {}

    for period_label, _, _ in tp.PERIODS:
        period_payload = load_pickle(period_cache_path(output_dir, dataset_tag, period_label))
        assert isinstance(period_payload, dict)
        preprocess_stats[period_label]["平均有效词数"] = period_payload["avg_effective_tokens"]
        preprocess_stats[period_label]["词汇表规模"] = int(period_payload["result"]["matrix_shape"][1])
        selection_rows.extend(period_payload["selection_rows"])
        selected_topic_counts[period_label] = int(period_payload["selected_topic_count"])
        fitted_results[period_label] = period_payload["result"]
        all_topic_keyword_rows.extend(period_payload["result"]["topic_keyword_rows"])
        all_topic_strength_rows.extend(period_payload["result"]["topic_strength_rows"])
        all_high_freq_rows.extend(period_payload["result"]["high_freq_rows"])
        topic_label_map = {str(row["主题编号"]): str(row["主题标签"]) for row in period_payload["result"]["topic_strength_rows"]}
        for source_row, topic_index, topic_probability in zip(
            period_source_rows[period_label],
            period_payload["result"]["dominant_topics"].tolist(),
            period_payload["result"]["dominant_probabilities"].tolist(),
        ):
            topic_id = f"T{int(topic_index) + 1:02d}"
            all_topic_assignment_rows.append(
                {
                    "阶段": period_label,
                    "文献签名": source_row["文献签名"],
                    "年份": source_row["年份"],
                    "来源库": source_row["来源库"],
                    "DOI": source_row["DOI"],
                    "标题": source_row["标题"],
                    "第一作者": source_row["第一作者"],
                    "主导主题编号": topic_id,
                    "主导主题标签": topic_label_map.get(topic_id, ""),
                    "主导主题概率": round(float(topic_probability), 6),
                }
            )

    tp.log_progress("开始汇总阶段结果并计算演化路径")
    left_period, middle_period, right_period = [label for label, _, _ in tp.PERIODS]
    match_12, similarity_rows_12 = tp.match_topics_between_periods(
        left_period,
        middle_period,
        fitted_results[left_period]["topic_vectors"],
        fitted_results[middle_period]["topic_vectors"],
    )
    match_23, similarity_rows_23 = tp.match_topics_between_periods(
        middle_period,
        right_period,
        fitted_results[middle_period]["topic_vectors"],
        fitted_results[right_period]["topic_vectors"],
    )
    topic_strength_by_period = {
        period_label: fitted_results[period_label]["topic_strength_rows"]
        for period_label, _, _ in tp.PERIODS
    }
    path_rows = tp.build_evolution_paths(topic_strength_by_period, match_12, match_23)
    preprocess_rows = [preprocess_stats[label] for label, _, _ in tp.PERIODS]

    tp.write_csv(output_paths["preprocess_stats"], ["阶段", "年份范围", "阶段文献数", "有摘要文献数", "英文摘要文献数", "中文摘要文献数", "通过领域过滤文献数", "进入建模文献数", "平均有效词数", "词汇表规模"], preprocess_rows)
    tp.write_csv(output_paths["model_selection"], ["阶段", "候选主题数", "样本文献数", "词汇表规模", "困惑度", "一致性得分", "困惑度排名", "一致性排名", "综合排序值", "是否选中"], selection_rows)
    tp.write_csv(output_paths["keyword_distribution"], ["阶段", "主题编号", "主题标签", "主题强度", "平均主题概率", "关键词排名", "关键词", "词项概率"], all_topic_keyword_rows)
    tp.write_csv(output_paths["topic_strength"], ["阶段", "主题编号", "主题标签", "主题文献数", "阶段文献总数", "主题强度", "平均主题概率", "代表关键词"], all_topic_strength_rows)
    tp.write_csv(output_paths["high_frequency_terms"], ["阶段", "词项", "总词频", "文献频次"], all_high_freq_rows)
    tp.write_csv(output_paths["topic_assignment"], ["阶段", "文献签名", "年份", "来源库", "DOI", "标题", "第一作者", "主导主题编号", "主导主题标签", "主导主题概率"], all_topic_assignment_rows)
    tp.write_csv(output_paths["similarity_links"], ["左阶段", "左主题编号", "右阶段", "右主题编号", "主题相似度", "纳入演化路径"], similarity_rows_12 + similarity_rows_23)
    tp.write_csv(output_paths["evolution_paths"], ["演化路径编号", "2011-2015主题编号", "2011-2015主题标签", "2011-2015主题强度", "2016-2020主题编号", "2016-2020主题标签", "2016-2020主题强度", "2021-2025主题编号", "2021-2025主题标签", "2021-2025主题强度"], path_rows)
    tp.plot_topic_intensity(path_rows, output_paths["intensity_curve"], dataset_label)
    tp.write_summary(output_paths["summary"], dataset_label, preprocess_rows, selection_rows, all_topic_strength_rows, path_rows)
    tp.log_progress(f"阶段结果汇总写出完成：output_dir={output_dir}")
    for period_label, _, _ in tp.PERIODS:
        tp.log_progress(f"{period_label}：docs={len(period_documents[period_label])}，selected_k={selected_topic_counts[period_label]}")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    dataset_tag = str(args.dataset_tag).strip()
    dataset_label = str(args.dataset_label).strip()
    mode = str(args.mode).strip()
    period_label = str(args.period or "").strip()

    if mode == "preprocess":
        run_preprocess(input_path, output_dir, dataset_tag)
        return

    if mode == "period":
        if period_label not in {label for label, _, _ in tp.PERIODS}:
            raise ValueError("--period 必须是 2011-2015、2016-2020、2021-2025 之一。")
        run_period(output_dir, dataset_tag, period_label)
        return

    run_finalize(output_dir, dataset_tag, dataset_label)


if __name__ == "__main__":
    main()
