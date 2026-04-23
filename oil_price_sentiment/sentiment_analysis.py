from __future__ import annotations

import argparse
import ast
import csv
import re
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence


DEFAULT_INPUT_PATH = Path(r"D:\tokenized_data.csv")
DEFAULT_BASE_POSITIVE_PATH = Path(r"D:\positive-words.txt")
DEFAULT_BASE_NEGATIVE_PATH = Path(r"D:\negative-words.txt")
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[1] / "oil_price_sentiment"
DEFAULT_MARKET_LEXICON_FILENAME = "expanded_sentiment_lexicon.csv"

TITLE_TOKEN_RE = re.compile(r"[a-z]+(?:-[a-z]+)?")
NEGATIONS = {
    "no",
    "not",
    "never",
    "none",
    "neither",
    "nor",
    "without",
    "hardly",
    "rarely",
    "seldom",
}
INTENSIFIERS: Dict[str, float] = {
    "very": 1.3,
    "strongly": 1.4,
    "sharply": 1.5,
    "steeply": 1.5,
    "significantly": 1.4,
    "substantially": 1.4,
    "deeply": 1.4,
    "severely": 1.5,
    "materially": 1.3,
    "further": 1.2,
}
DIMINISHERS: Dict[str, float] = {
    "slightly": 0.7,
    "modestly": 0.75,
    "marginally": 0.8,
    "partly": 0.85,
}
TITLE_WEIGHT = 1.5
NORMALIZATION_ALPHA = 3.0
BULLISH_LABEL_THRESHOLD = 0.10
BEARISH_LABEL_THRESHOLD = -0.10


@dataclass
class ScoreBreakdown:
    raw_score: float = 0.0
    positive_mass: float = 0.0
    negative_mass: float = 0.0
    positive_hits: int = 0
    negative_hits: int = 0


def load_word_list(path: Path) -> List[str]:
    words: List[str] = []
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            word = line.strip().lower()
            if not word or word.startswith(";"):
                continue
            words.append(word)
    return words


def parse_tokens(cell: str) -> List[str]:
    try:
        tokens = ast.literal_eval(cell)
    except (SyntaxError, ValueError):
        return []
    if not isinstance(tokens, list):
        return []
    return [str(token).strip().lower() for token in tokens if str(token).strip()]


def tokenize_title(title: str) -> List[str]:
    return TITLE_TOKEN_RE.findall((title or "").lower())


def build_sentiment_lexicon(positive_words: Sequence[str], negative_words: Sequence[str]) -> Dict[str, float]:
    lexicon: Dict[str, float] = {}
    for word in positive_words:
        lexicon[word] = 1.0
    for word in negative_words:
        lexicon[word] = -1.0
    return lexicon


def load_market_lexicon(path: Path) -> Dict[str, float]:
    lexicon: Dict[str, float] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            word = (row.get("word") or "").strip().lower()
            polarity = (row.get("polarity") or "").strip().lower()
            if not word or polarity not in {"positive", "negative"}:
                continue
            lexicon[word] = 1.0 if polarity == "positive" else -1.0
    return lexicon


def score_tokens(tokens: Sequence[str], lexicon: Dict[str, float]) -> tuple[ScoreBreakdown, Counter[str], Counter[str]]:
    breakdown = ScoreBreakdown()
    positive_terms: Counter[str] = Counter()
    negative_terms: Counter[str] = Counter()

    for index, token in enumerate(tokens):
        base_score = lexicon.get(token)
        if base_score is None:
            continue

        score = base_score
        window = tokens[max(0, index - 3) : index]

        if any(prev in NEGATIONS for prev in window):
            score *= -1.0

        for prev in window:
            score *= INTENSIFIERS.get(prev, 1.0)
            score *= DIMINISHERS.get(prev, 1.0)

        breakdown.raw_score += score
        if score >= 0:
            breakdown.positive_hits += 1
            breakdown.positive_mass += abs(score)
            positive_terms[token] += 1
        else:
            breakdown.negative_hits += 1
            breakdown.negative_mass += abs(score)
            negative_terms[token] += 1

    return breakdown, positive_terms, negative_terms


def merge_breakdowns(body: ScoreBreakdown, title: ScoreBreakdown) -> ScoreBreakdown:
    return ScoreBreakdown(
        raw_score=body.raw_score + title.raw_score * TITLE_WEIGHT,
        positive_mass=body.positive_mass + title.positive_mass * TITLE_WEIGHT,
        negative_mass=body.negative_mass + title.negative_mass * TITLE_WEIGHT,
        positive_hits=body.positive_hits + title.positive_hits,
        negative_hits=body.negative_hits + title.negative_hits,
    )


def normalize_score(breakdown: ScoreBreakdown) -> float:
    denominator = breakdown.positive_mass + breakdown.negative_mass + NORMALIZATION_ALPHA
    if denominator <= 0:
        return 0.0
    return breakdown.raw_score / denominator


def label_score(score: float) -> str:
    if score >= BULLISH_LABEL_THRESHOLD:
        return "bullish"
    if score <= BEARISH_LABEL_THRESHOLD:
        return "bearish"
    return "neutral"


def resolve_default_lexicon_paths(output_dir: Path) -> tuple[Path, Path]:
    expanded_positive = output_dir / "expanded_positive_words.txt"
    expanded_negative = output_dir / "expanded_negative_words.txt"
    if expanded_positive.exists() and expanded_negative.exists():
        return expanded_positive, expanded_negative
    return DEFAULT_BASE_POSITIVE_PATH, DEFAULT_BASE_NEGATIVE_PATH


def write_summary(
    *,
    output_path: Path,
    input_path: Path,
    lexicon_mode: str,
    lexicon_source: Path,
    row_count: int,
    label_counter: Counter[str],
    mean_score: float,
    top_positive_terms: Counter[str],
    top_negative_terms: Counter[str],
) -> None:
    lines = [
        "Oil-price sentiment scoring summary",
        f"Input dataset: {input_path}",
        f"Lexicon mode: {lexicon_mode}",
        f"Lexicon source: {lexicon_source}",
        f"Rows processed: {row_count}",
        f"Mean normalized score: {mean_score:.6f}",
        f"Bullish articles: {label_counter['bullish']}",
        f"Neutral articles: {label_counter['neutral']}",
        f"Bearish articles: {label_counter['bearish']}",
        "",
        "Top bullish matched terms:",
    ]
    for word, count in top_positive_terms.most_common(20):
        lines.append(f"{word}: {count}")

    lines.append("")
    lines.append("Top bearish matched terms:")
    for word, count in top_negative_terms.most_common(20):
        lines.append(f"{word}: {count}")

    output_path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Score each oil-price news article with a lexicon-based sentiment method."
    )
    parser.add_argument("--input", default=str(DEFAULT_INPUT_PATH), help="Tokenized news CSV path")
    parser.add_argument(
        "--positive-lexicon",
        default=None,
        help="Positive lexicon path. If omitted, prefer expanded lexicon in the output directory.",
    )
    parser.add_argument(
        "--negative-lexicon",
        default=None,
        help="Negative lexicon path. If omitted, prefer expanded lexicon in the output directory.",
    )
    parser.add_argument(
        "--market-lexicon-csv",
        default=None,
        help="Oil-price oriented market lexicon CSV. Defaults to expanded_sentiment_lexicon.csv in the output directory.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory to write sentiment results",
    )
    parser.add_argument(
        "--output-file",
        default="news_sentiment.csv",
        help="Output CSV filename inside the output directory",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        raise SystemExit(f"Input CSV not found: {input_path}")

    default_positive_path, default_negative_path = resolve_default_lexicon_paths(output_dir)
    positive_path = Path(args.positive_lexicon) if args.positive_lexicon else default_positive_path
    negative_path = Path(args.negative_lexicon) if args.negative_lexicon else default_negative_path
    market_lexicon_path = (
        Path(args.market_lexicon_csv)
        if args.market_lexicon_csv
        else output_dir / DEFAULT_MARKET_LEXICON_FILENAME
    )

    if market_lexicon_path.exists():
        lexicon = load_market_lexicon(market_lexicon_path)
        lexicon_mode = "market_expansion"
        lexicon_source = market_lexicon_path
    else:
        if not positive_path.exists():
            raise SystemExit(f"Positive lexicon not found: {positive_path}")
        if not negative_path.exists():
            raise SystemExit(f"Negative lexicon not found: {negative_path}")
        positive_words = load_word_list(positive_path)
        negative_words = load_word_list(negative_path)
        lexicon = build_sentiment_lexicon(positive_words, negative_words)
        lexicon_mode = "plain_positive_negative"
        lexicon_source = positive_path

    try:
        csv.field_size_limit(sys.maxsize)
    except (OverflowError, ValueError):
        csv.field_size_limit(10_000_000)

    output_path = output_dir / args.output_file
    summary_path = output_dir / "sentiment_summary.txt"

    label_counter: Counter[str] = Counter()
    matched_positive_terms: Counter[str] = Counter()
    matched_negative_terms: Counter[str] = Counter()
    row_count = 0
    score_total = 0.0

    fieldnames = [
        "news_id",
        "topic",
        "subtitle",
        "title",
        "date",
        "token_count",
        "title_token_count",
        "bullish_hits",
        "bearish_hits",
        "bullish_mass",
        "bearish_mass",
        "evidence_mass",
        "raw_score",
        "sentiment_score",
        "sentiment_label",
    ]

    with input_path.open("r", encoding="utf-8-sig", newline="") as rf, output_path.open(
        "w", encoding="utf-8-sig", newline=""
    ) as wf:
        reader = csv.DictReader(rf)
        writer = csv.DictWriter(wf, fieldnames=fieldnames)
        writer.writeheader()

        for row_count, row in enumerate(reader, start=1):
            body_tokens = parse_tokens(row.get("tokens", ""))
            title_tokens = tokenize_title(row.get("title", ""))

            body_breakdown, body_pos_terms, body_neg_terms = score_tokens(body_tokens, lexicon)
            title_breakdown, title_pos_terms, title_neg_terms = score_tokens(title_tokens, lexicon)
            combined = merge_breakdowns(body_breakdown, title_breakdown)

            normalized_score = normalize_score(combined)
            sentiment_label = label_score(normalized_score)

            label_counter[sentiment_label] += 1
            score_total += normalized_score
            matched_positive_terms.update(body_pos_terms)
            matched_positive_terms.update(title_pos_terms)
            matched_negative_terms.update(body_neg_terms)
            matched_negative_terms.update(title_neg_terms)

            writer.writerow(
                {
                    "news_id": row_count,
                    "topic": row.get("topic", ""),
                    "subtitle": row.get("subtitle", ""),
                    "title": row.get("title", ""),
                    "date": row.get("date", ""),
                    "token_count": len(body_tokens),
                    "title_token_count": len(title_tokens),
                    "bullish_hits": combined.positive_hits,
                    "bearish_hits": combined.negative_hits,
                    "bullish_mass": f"{combined.positive_mass:.4f}",
                    "bearish_mass": f"{combined.negative_mass:.4f}",
                    "evidence_mass": f"{combined.positive_mass + combined.negative_mass:.4f}",
                    "raw_score": f"{combined.raw_score:.4f}",
                    "sentiment_score": f"{normalized_score:.6f}",
                    "sentiment_label": sentiment_label,
                }
            )

    mean_score = score_total / row_count if row_count else 0.0
    write_summary(
        output_path=summary_path,
        input_path=input_path,
        lexicon_mode=lexicon_mode,
        lexicon_source=lexicon_source,
        row_count=row_count,
        label_counter=label_counter,
        mean_score=mean_score,
        top_positive_terms=matched_positive_terms,
        top_negative_terms=matched_negative_terms,
    )

    print(
        "Sentiment scoring complete. "
        f"rows={row_count}, "
        f"mean_score={mean_score:.6f}, "
        f"bullish={label_counter['bullish']}, "
        f"neutral={label_counter['neutral']}, "
        f"bearish={label_counter['bearish']}, "
        f"output={output_path}"
    )


if __name__ == "__main__":
    main()
