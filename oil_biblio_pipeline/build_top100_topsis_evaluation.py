from __future__ import annotations

import argparse
import csv
import math
from pathlib import Path
from statistics import mean, pstdev


BASE_DATA_DIR = Path("D:/\u6bd5\u4e1a\u8bba\u6587")

INPUT_PATH = BASE_DATA_DIR / "institution_impact_indicator_top100_2011_2025.csv"
SELECTION_OUTPUT_PATH = BASE_DATA_DIR / "institution_topsis_indicator_selection_top100_2011_2025.csv"
WEIGHT_OUTPUT_PATH = BASE_DATA_DIR / "institution_weight_scheme_top100_2011_2025.csv"
SCORE_OUTPUT_PATH = BASE_DATA_DIR / "institution_topsis_score_top100_2011_2025.csv"

INPUT_ENCODING = "gb18030"
OUTPUT_ENCODING = "gb18030"

FIELD_ALIASES = {
    "rank_top100": "Top100排名",
    "institution_norm": "标准化机构名称",
    "distinct_paper_count": "去重论文总数",
    "recent_paper_ratio_2021_2025": "近五年发文占比",
    "h_index": "H指数",
    "high_cited_paper_ratio": "高被引论文占比",
    "collaboration_paper_ratio": "合作论文占比",
    "international_collaboration_paper_ratio": "国际合作论文占比",
    "partner_country_region_count": "合作国家/地区数",
}

INDICATORS = [
    {
        "field": "distinct_paper_count",
        "label_zh": "\u53bb\u91cd\u8bba\u6587\u603b\u6570",
        "dimension": "research_output",
        "dimension_zh": "\u79d1\u7814\u4ea7\u51fa",
        "direction": "benefit",
        "selection_reason": "\u4ee3\u8868\u673a\u6784\u5728\u6b63\u5f0f\u8bed\u6599\u4e2d\u7684\u53d1\u6587\u89c4\u6a21\uff0c\u4f5c\u4e3a\u6838\u5fc3\u4ea7\u51fa\u6307\u6807\u3002",
    },
    {
        "field": "recent_paper_ratio_2021_2025",
        "label_zh": "\u8fd1\u4e94\u5e74\u53d1\u6587\u5360\u6bd4",
        "dimension": "research_output",
        "dimension_zh": "\u79d1\u7814\u4ea7\u51fa",
        "direction": "benefit",
        "selection_reason": "\u53cd\u6620\u673a\u6784\u8fd1\u671f\u4ea7\u51fa\u6d3b\u8dc3\u5ea6\u548c\u6301\u7eed\u6027\uff0c\u8865\u5145\u89c4\u6a21\u6307\u6807\u3002",
    },
    {
        "field": "h_index",
        "label_zh": "H\u6307\u6570",
        "dimension": "academic_impact",
        "dimension_zh": "\u5b66\u672f\u5f71\u54cd",
        "direction": "benefit",
        "selection_reason": "\u517c\u987e\u53d1\u6587\u4e0e\u88ab\u5f15\u8868\u73b0\uff0c\u662f\u673a\u6784\u5b66\u672f\u5f71\u54cd\u529b\u7684\u7ecf\u5178\u6307\u6807\u3002",
    },
    {
        "field": "high_cited_paper_ratio",
        "label_zh": "\u9ad8\u88ab\u5f15\u8bba\u6587\u5360\u6bd4",
        "dimension": "academic_impact",
        "dimension_zh": "\u5b66\u672f\u5f71\u54cd",
        "direction": "benefit",
        "selection_reason": "\u53cd\u6620\u673a\u6784\u9ad8\u5f71\u54cd\u6210\u679c\u7684\u8f93\u51fa\u6bd4\u91cd\uff0c\u5951\u5408\u5f00\u9898\u62a5\u544a\u4e2d\u7684\u9ad8\u88ab\u5f15\u7ef4\u5ea6\u3002",
    },
    {
        "field": "collaboration_paper_ratio",
        "label_zh": "\u5408\u4f5c\u8bba\u6587\u5360\u6bd4",
        "dimension": "collaboration_international",
        "dimension_zh": "\u5408\u4f5c\u4e0e\u56fd\u9645\u5316",
        "direction": "benefit",
        "selection_reason": "\u53cd\u6620\u673a\u6784\u7814\u7a76\u5408\u4f5c\u503e\u5411\uff0c\u662f\u5408\u4f5c\u7f51\u7edc\u7ef4\u5ea6\u7684\u57fa\u7840\u6307\u6807\u3002",
    },
    {
        "field": "international_collaboration_paper_ratio",
        "label_zh": "\u56fd\u9645\u5408\u4f5c\u8bba\u6587\u5360\u6bd4",
        "dimension": "collaboration_international",
        "dimension_zh": "\u5408\u4f5c\u4e0e\u56fd\u9645\u5316",
        "direction": "benefit",
        "selection_reason": "\u53cd\u6620\u673a\u6784\u56fd\u9645\u5408\u4f5c\u5f3a\u5ea6\uff0c\u5bf9\u5e94\u5f00\u9898\u62a5\u544a\u4e2d\u7684\u56fd\u9645\u5408\u4f5c\u7ef4\u5ea6\u3002",
    },
    {
        "field": "partner_country_region_count",
        "label_zh": "\u5408\u4f5c\u56fd\u5bb6/\u5730\u533a\u6570",
        "dimension": "collaboration_international",
        "dimension_zh": "\u5408\u4f5c\u4e0e\u56fd\u9645\u5316",
        "direction": "benefit",
        "selection_reason": "\u53cd\u6620\u56fd\u9645\u5408\u4f5c\u8986\u76d6\u9762\u4e0e\u5916\u90e8\u8fde\u63a5\u5e7f\u5ea6\u3002",
    },
]

SCREENING_NOTES = {
    "occurrence_count": "\u4e0e distinct_paper_count \u9ad8\u5ea6\u91cd\u5408\uff08r=0.9992\uff09\uff0c\u4e0d\u91cd\u590d\u7eb3\u5165\u3002",
    "avg_annual_paper_count": "\u4e0e distinct_paper_count \u5b58\u5728\u76f4\u63a5\u6362\u7b97\u5173\u7cfb\uff0c\u4e0d\u5355\u72ec\u5165\u6a21\u3002",
    "active_year_count": "\u5728 Top100 \u673a\u6784\u4e2d\u53d8\u5f02\u8f83\u4f4e\uff08CV=0.0681\uff09\uff0c\u533a\u5206\u5ea6\u4e0d\u8db3\u3002",
    "total_citations": "\u4e0e distinct_paper_count \u76f8\u5173\u6027\u8f83\u9ad8\uff08r=0.9198\uff09\uff0c\u5bb9\u6613\u91cd\u590d\u53cd\u6620\u89c4\u6a21\u6548\u5e94\u3002",
    "avg_citations_per_paper": "\u4e0e high_cited_paper_ratio \u9ad8\u5ea6\u76f8\u5173\uff08r=0.9775\uff09\uff0c\u672c\u8f6e\u4f18\u5148\u4fdd\u7559\u9ad8\u88ab\u5f15\u8d28\u91cf\u6307\u6807\u3002",
    "median_citations_per_paper": "\u4e0e high_cited_paper_ratio \u76f8\u5173\u6027\u8f83\u9ad8\uff0c\u4e14\u4e0e h_index \u90e8\u5206\u91cd\u53e0\uff0c\u672c\u8f6e\u672a\u7eb3\u5165\u3002",
    "international_partner_institution_count": "\u4e0e partner_country_region_count \u9ad8\u5ea6\u76f8\u5173\uff08r=0.8290\uff09\uff0c\u672c\u8f6e\u4f18\u5148\u4fdd\u7559\u56fd\u5bb6/\u5730\u533a\u8986\u76d6\u5ea6\u6307\u6807\u3002",
    "avg_partner_institutions_per_collab_paper": "\u4e0e collaboration_paper_ratio \u76f8\u5173\u6027\u8f83\u9ad8\uff08r=0.7316\uff09\uff0c\u672c\u8f6e\u672a\u7eb3\u5165\u3002",
}

SELECTION_FIELDS = [
    "field",
    "label_zh",
    "dimension",
    "dimension_zh",
    "direction",
    "selected",
    "selection_reason",
]
WEIGHT_FIELDS = [
    "field",
    "label_zh",
    "dimension",
    "dimension_zh",
    "direction",
    "prior_weight",
    "entropy_weight",
    "critic_weight",
    "combined_weight",
]
SCORE_FIELDS_PREFIX = [
    "rank_top100",
    "institution_norm",
    "topsis_score",
    "topsis_rank",
    "research_output_subscore",
    "academic_impact_subscore",
    "collaboration_international_subscore",
]


def to_float(value: object) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def round6(value: float) -> float:
    return round(value, 6)


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding=INPUT_ENCODING, newline="") as f:
        return list(csv.DictReader(f))


def row_value(row: dict[str, str], field: str) -> str:
    return row.get(field, row.get(FIELD_ALIASES.get(field, ""), ""))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding=OUTPUT_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def benefit_minmax(values: list[float]) -> list[float]:
    min_value = min(values)
    max_value = max(values)
    if math.isclose(max_value, min_value):
        return [1.0 for _ in values]
    return [(value - min_value) / (max_value - min_value) for value in values]


def vector_normalize(values: list[float]) -> list[float]:
    denominator = math.sqrt(sum(value * value for value in values))
    if math.isclose(denominator, 0.0):
        return [0.0 for _ in values]
    return [value / denominator for value in values]


def pearson_corr(xs: list[float], ys: list[float]) -> float:
    mx = mean(xs)
    my = mean(ys)
    sx = sum((x - mx) ** 2 for x in xs)
    sy = sum((y - my) ** 2 for y in ys)
    if math.isclose(sx, 0.0) or math.isclose(sy, 0.0):
        return 0.0
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    return cov / math.sqrt(sx * sy)


def normalize_weights(weight_map: dict[str, float]) -> dict[str, float]:
    total = sum(weight_map.values())
    if math.isclose(total, 0.0):
        even = 1.0 / len(weight_map)
        return {key: even for key in weight_map}
    return {key: value / total for key, value in weight_map.items()}


def build_prior_weights() -> dict[str, float]:
    dimension_groups: dict[str, list[str]] = {}
    for indicator in INDICATORS:
        dimension_groups.setdefault(indicator["dimension"], []).append(indicator["field"])

    dimension_weight = 1.0 / len(dimension_groups)
    result: dict[str, float] = {}
    for fields in dimension_groups.values():
        within_weight = dimension_weight / len(fields)
        for field in fields:
            result[field] = within_weight
    return normalize_weights(result)


def build_entropy_weights(normalized_matrix: dict[str, list[float]]) -> dict[str, float]:
    n = len(next(iter(normalized_matrix.values())))
    k = 1.0 / math.log(n)
    raw_weights: dict[str, float] = {}
    epsilon = 1e-12

    for field, values in normalized_matrix.items():
        shifted = [value + epsilon for value in values]
        total = sum(shifted)
        proportions = [value / total for value in shifted]
        entropy = -k * sum(p * math.log(p) for p in proportions)
        raw_weights[field] = 1.0 - entropy

    return normalize_weights(raw_weights)


def build_critic_weights(normalized_matrix: dict[str, list[float]]) -> dict[str, float]:
    fields = list(normalized_matrix)
    raw_weights: dict[str, float] = {}

    for field in fields:
        values = normalized_matrix[field]
        std_value = pstdev(values)
        conflict = 0.0
        for other in fields:
            if other == field:
                continue
            conflict += 1.0 - pearson_corr(values, normalized_matrix[other])
        raw_weights[field] = std_value * conflict

    return normalize_weights(raw_weights)


def build_selection_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for indicator in INDICATORS:
        rows.append(
            {
                "field": indicator["field"],
                "label_zh": indicator["label_zh"],
                "dimension": indicator["dimension"],
                "dimension_zh": indicator["dimension_zh"],
                "direction": indicator["direction"],
                "selected": 1,
                "selection_reason": indicator["selection_reason"],
            }
        )

    for field, note in SCREENING_NOTES.items():
        rows.append(
            {
                "field": field,
                "label_zh": "",
                "dimension": "",
                "dimension_zh": "",
                "direction": "",
                "selected": 0,
                "selection_reason": note,
            }
        )

    return rows


def build_dimension_subscores(
    normalized_matrix: dict[str, list[float]],
    combined_weights: dict[str, float],
) -> dict[str, list[float]]:
    dimension_groups: dict[str, list[str]] = {}
    for indicator in INDICATORS:
        dimension_groups.setdefault(indicator["dimension"], []).append(indicator["field"])

    result: dict[str, list[float]] = {}
    for dimension, fields in dimension_groups.items():
        within_weights = normalize_weights({field: combined_weights[field] for field in fields})
        values: list[float] = []
        row_count = len(normalized_matrix[fields[0]])
        for i in range(row_count):
            score = sum(normalized_matrix[field][i] * within_weights[field] for field in fields)
            values.append(score)
        result[dimension] = values
    return result


def build_topsis_scores(raw_matrix: dict[str, list[float]], combined_weights: dict[str, float]) -> list[float]:
    vector_matrix = {field: vector_normalize(values) for field, values in raw_matrix.items()}
    weighted_matrix = {
        field: [value * combined_weights[field] for value in values]
        for field, values in vector_matrix.items()
    }

    positive_ideal = {field: max(values) for field, values in weighted_matrix.items()}
    negative_ideal = {field: min(values) for field, values in weighted_matrix.items()}

    row_count = len(next(iter(weighted_matrix.values())))
    scores: list[float] = []
    for i in range(row_count):
        distance_pos = math.sqrt(
            sum((weighted_matrix[field][i] - positive_ideal[field]) ** 2 for field in weighted_matrix)
        )
        distance_neg = math.sqrt(
            sum((weighted_matrix[field][i] - negative_ideal[field]) ** 2 for field in weighted_matrix)
        )
        if math.isclose(distance_pos + distance_neg, 0.0):
            scores.append(0.0)
        else:
            scores.append(distance_neg / (distance_pos + distance_neg))
    return scores


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(INPUT_PATH))
    parser.add_argument("--selection-out", default=str(SELECTION_OUTPUT_PATH))
    parser.add_argument("--weight-out", default=str(WEIGHT_OUTPUT_PATH))
    parser.add_argument("--score-out", default=str(SCORE_OUTPUT_PATH))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = load_rows(Path(args.input))

    selected_fields = [indicator["field"] for indicator in INDICATORS]
    raw_matrix = {field: [to_float(row_value(row, field)) for row in rows] for field in selected_fields}
    normalized_matrix = {field: benefit_minmax(values) for field, values in raw_matrix.items()}

    prior_weights = build_prior_weights()
    entropy_weights = build_entropy_weights(normalized_matrix)
    critic_weights = build_critic_weights(normalized_matrix)
    combined_weights = normalize_weights(
        {
            field: (prior_weights[field] + entropy_weights[field] + critic_weights[field]) / 3.0
            for field in selected_fields
        }
    )

    selection_rows = build_selection_rows()
    weight_rows: list[dict[str, object]] = []
    for indicator in INDICATORS:
        field = indicator["field"]
        weight_rows.append(
            {
                "field": field,
                "label_zh": indicator["label_zh"],
                "dimension": indicator["dimension"],
                "dimension_zh": indicator["dimension_zh"],
                "direction": indicator["direction"],
                "prior_weight": round6(prior_weights[field]),
                "entropy_weight": round6(entropy_weights[field]),
                "critic_weight": round6(critic_weights[field]),
                "combined_weight": round6(combined_weights[field]),
            }
        )

    dimension_subscores = build_dimension_subscores(normalized_matrix, combined_weights)
    topsis_scores = build_topsis_scores(raw_matrix, combined_weights)

    score_rows: list[dict[str, object]] = []
    for i, row in enumerate(rows):
        out_row: dict[str, object] = {
            "rank_top100": row_value(row, "rank_top100"),
            "institution_norm": row_value(row, "institution_norm"),
            "topsis_score": round6(topsis_scores[i]),
            "research_output_subscore": round6(dimension_subscores["research_output"][i]),
            "academic_impact_subscore": round6(dimension_subscores["academic_impact"][i]),
            "collaboration_international_subscore": round6(
                dimension_subscores["collaboration_international"][i]
            ),
        }
        for field in selected_fields:
            out_row[field] = row_value(row, field)
        score_rows.append(out_row)

    score_rows.sort(key=lambda item: float(item["topsis_score"]), reverse=True)
    for rank, row in enumerate(score_rows, 1):
        row["topsis_rank"] = rank

    score_fields = SCORE_FIELDS_PREFIX + selected_fields

    write_csv(Path(args.selection_out), SELECTION_FIELDS, selection_rows)
    write_csv(Path(args.weight_out), WEIGHT_FIELDS, weight_rows)
    write_csv(Path(args.score_out), score_fields, score_rows)

    print(f"input_rows={len(rows)}")
    print(f"selected_indicator_count={len(selected_fields)}")
    print(f"selection_output={args.selection_out}")
    print(f"weight_output={args.weight_out}")
    print(f"score_output={args.score_out}")
    print("combined_weights:")
    for row in weight_rows:
        print(f"{row['field']}\t{row['combined_weight']}")


if __name__ == "__main__":
    main()
