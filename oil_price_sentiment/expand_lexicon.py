from __future__ import annotations

import argparse
import ast
import csv
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple


DEFAULT_INPUT_PATH = Path(r"D:\tokenized_data.csv")
DEFAULT_POSITIVE_PATH = Path(r"D:\positive-words.txt")
DEFAULT_NEGATIVE_PATH = Path(r"D:\negative-words.txt")
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[1] / "oil_price_sentiment"


@dataclass(frozen=True)
class DomainTerm:
    word: str
    polarity: str
    category: str
    rationale: str


DOMAIN_TERM_SPECS: Tuple[DomainTerm, ...] = (
    DomainTerm("bullish", "positive", "price_move", "Directly signals an upward oil-price view."),
    DomainTerm("rally", "positive", "price_move", "Directly signals a rising oil-price move."),
    DomainTerm("rallying", "positive", "price_move", "Directly signals a rising oil-price move."),
    DomainTerm("rebound", "positive", "price_move", "Indicates recovery after a drop."),
    DomainTerm("rebounded", "positive", "price_move", "Indicates recovery after a drop."),
    DomainTerm("surge", "positive", "price_move", "Signals sharp upward movement in prices."),
    DomainTerm("surged", "positive", "price_move", "Signals sharp upward movement in prices."),
    DomainTerm("spike", "positive", "price_move", "Signals abrupt upward movement in prices."),
    DomainTerm("spiked", "positive", "price_move", "Signals abrupt upward movement in prices."),
    DomainTerm("jump", "positive", "price_move", "Signals abrupt upward movement in prices."),
    DomainTerm("jumped", "positive", "price_move", "Signals abrupt upward movement in prices."),
    DomainTerm("tight", "positive", "supply_balance", "Often indicates a tightening supply-demand balance."),
    DomainTerm("tightening", "positive", "supply_balance", "Often indicates a tightening supply-demand balance."),
    DomainTerm("tightness", "positive", "supply_balance", "Often indicates a tightening supply-demand balance."),
    DomainTerm("shortage", "positive", "supply_balance", "Supply shortages are usually bullish for oil prices."),
    DomainTerm("shortages", "positive", "supply_balance", "Supply shortages are usually bullish for oil prices."),
    DomainTerm("shortfall", "positive", "supply_balance", "Supply shortfalls are usually bullish for oil prices."),
    DomainTerm("shortfalls", "positive", "supply_balance", "Supply shortfalls are usually bullish for oil prices."),
    DomainTerm("draw", "positive", "inventory", "Inventory draws typically support oil prices."),
    DomainTerm("draws", "positive", "inventory", "Inventory draws typically support oil prices."),
    DomainTerm("drawdown", "positive", "inventory", "Inventory drawdowns typically support oil prices."),
    DomainTerm("drawdowns", "positive", "inventory", "Inventory drawdowns typically support oil prices."),
    DomainTerm("outage", "positive", "supply_disruption", "Production or transport outages are usually bullish."),
    DomainTerm("outages", "positive", "supply_disruption", "Production or transport outages are usually bullish."),
    DomainTerm("disruption", "positive", "supply_disruption", "Supply disruptions are usually bullish."),
    DomainTerm("disruptions", "positive", "supply_disruption", "Supply disruptions are usually bullish."),
    DomainTerm("disrupted", "positive", "supply_disruption", "Supply disruptions are usually bullish."),
    DomainTerm("sanction", "positive", "geopolitics", "Sanctions often tighten oil supply."),
    DomainTerm("sanctions", "positive", "geopolitics", "Sanctions often tighten oil supply."),
    DomainTerm("embargo", "positive", "geopolitics", "Embargoes often tighten oil supply."),
    DomainTerm("blockade", "positive", "geopolitics", "Blockades can threaten oil shipments."),
    DomainTerm("blockades", "positive", "geopolitics", "Blockades can threaten oil shipments."),
    DomainTerm("escalation", "positive", "geopolitics", "Escalation raises geopolitical risk premiums."),
    DomainTerm("escalate", "positive", "geopolitics", "Escalation raises geopolitical risk premiums."),
    DomainTerm("escalated", "positive", "geopolitics", "Escalation raises geopolitical risk premiums."),
    DomainTerm("attack", "positive", "geopolitics", "Attacks on energy infrastructure tend to be bullish."),
    DomainTerm("attacks", "positive", "geopolitics", "Attacks on energy infrastructure tend to be bullish."),
    DomainTerm("attacked", "positive", "geopolitics", "Attacks on energy infrastructure tend to be bullish."),
    DomainTerm("cut", "positive", "production_policy", "Output cuts are usually bullish for oil prices."),
    DomainTerm("cuts", "positive", "production_policy", "Output cuts are usually bullish for oil prices."),
    DomainTerm("curb", "positive", "production_policy", "Supply curbs are usually bullish for oil prices."),
    DomainTerm("curbs", "positive", "production_policy", "Supply curbs are usually bullish for oil prices."),
    DomainTerm("curtailed", "positive", "production_policy", "Curtailments usually tighten supply."),
    DomainTerm("deficit", "positive", "supply_balance", "Market deficits are usually bullish."),
    DomainTerm("deficits", "positive", "supply_balance", "Market deficits are usually bullish."),
    DomainTerm("premium", "positive", "risk_premium", "Risk premiums often accompany upward price pressure."),
    DomainTerm("premiums", "positive", "risk_premium", "Risk premiums often accompany upward price pressure."),
    DomainTerm("squeeze", "positive", "supply_balance", "Supply squeezes often accompany upward price pressure."),
    DomainTerm("squeezed", "positive", "supply_balance", "Supply squeezes often accompany upward price pressure."),
    DomainTerm("bearish", "negative", "price_move", "Directly signals a downward oil-price view."),
    DomainTerm("slump", "negative", "price_move", "Directly signals a falling oil-price move."),
    DomainTerm("slumped", "negative", "price_move", "Directly signals a falling oil-price move."),
    DomainTerm("slumping", "negative", "price_move", "Directly signals a falling oil-price move."),
    DomainTerm("plunge", "negative", "price_move", "Signals sharp downward movement in prices."),
    DomainTerm("plunged", "negative", "price_move", "Signals sharp downward movement in prices."),
    DomainTerm("drop", "negative", "price_move", "Signals downward movement in prices."),
    DomainTerm("drops", "negative", "price_move", "Signals downward movement in prices."),
    DomainTerm("dropped", "negative", "price_move", "Signals downward movement in prices."),
    DomainTerm("decline", "negative", "price_move", "Signals downward movement in prices."),
    DomainTerm("declines", "negative", "price_move", "Signals downward movement in prices."),
    DomainTerm("declined", "negative", "price_move", "Signals downward movement in prices."),
    DomainTerm("crash", "negative", "price_move", "Signals abrupt downward movement in prices."),
    DomainTerm("crashed", "negative", "price_move", "Signals abrupt downward movement in prices."),
    DomainTerm("glut", "negative", "supply_balance", "A supply glut is usually bearish."),
    DomainTerm("oversupply", "negative", "supply_balance", "Oversupply is usually bearish."),
    DomainTerm("oversupplied", "negative", "supply_balance", "Oversupply is usually bearish."),
    DomainTerm("surplus", "negative", "supply_balance", "A supply surplus is usually bearish."),
    DomainTerm("build", "negative", "inventory", "Inventory builds are usually bearish."),
    DomainTerm("builds", "negative", "inventory", "Inventory builds are usually bearish."),
    DomainTerm("ceasefire", "negative", "geopolitics", "Ceasefires often reduce geopolitical oil risk premium."),
    DomainTerm("truce", "negative", "geopolitics", "Truces often reduce geopolitical oil risk premium."),
    DomainTerm("slowdown", "negative", "demand", "Demand slowdowns are usually bearish."),
    DomainTerm("recession", "negative", "demand", "Recession risk is usually bearish for oil demand."),
    DomainTerm("weak", "negative", "demand", "Weak demand or prices are usually bearish."),
    DomainTerm("weaker", "negative", "demand", "Weak demand or prices are usually bearish."),
    DomainTerm("weakness", "negative", "demand", "Weak demand or prices are usually bearish."),
    DomainTerm("easing", "negative", "supply_balance", "Supply concerns easing is usually bearish."),
    DomainTerm("abundant", "negative", "supply_balance", "Abundant supply is usually bearish."),
    DomainTerm("abundance", "negative", "supply_balance", "Abundant supply is usually bearish."),
    DomainTerm("downturn", "negative", "demand", "Economic downturns are usually bearish."),
    DomainTerm("contraction", "negative", "demand", "Demand contraction is usually bearish."),
    DomainTerm("dampen", "negative", "demand", "Demand destruction language is usually bearish."),
    DomainTerm("dampened", "negative", "demand", "Demand destruction language is usually bearish."),
    DomainTerm("discount", "negative", "pricing", "Discounted crude signals weak market conditions."),
    DomainTerm("discounts", "negative", "pricing", "Discounted crude signals weak market conditions."),
    DomainTerm("slack", "negative", "demand", "Slack demand or market conditions are usually bearish."),
)


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


def count_corpus_terms(input_path: Path) -> Tuple[Counter[str], Counter[str], int]:
    try:
        csv.field_size_limit(sys.maxsize)
    except (OverflowError, ValueError):
        csv.field_size_limit(10_000_000)

    token_frequency: Counter[str] = Counter()
    document_frequency: Counter[str] = Counter()
    row_count = 0

    with input_path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            row_count += 1
            tokens = parse_tokens(row.get("tokens", ""))
            token_frequency.update(tokens)
            document_frequency.update(set(tokens))

    return token_frequency, document_frequency, row_count


def build_expansion_rows(
    *,
    base_positive: Sequence[str],
    base_negative: Sequence[str],
    token_frequency: Counter[str],
    document_frequency: Counter[str],
    min_frequency: int,
) -> List[dict]:
    base_pos = set(base_positive)
    base_neg = set(base_negative)

    rows: List[dict] = []
    for term in DOMAIN_TERM_SPECS:
        corpus_frequency = token_frequency.get(term.word, 0)
        doc_frequency = document_frequency.get(term.word, 0)
        if corpus_frequency < min_frequency:
            continue

        if term.word in base_pos and term.polarity == "positive":
            action = "already_positive"
        elif term.word in base_neg and term.polarity == "negative":
            action = "already_negative"
        elif term.word in base_pos and term.polarity == "negative":
            action = "reclassified_from_positive"
        elif term.word in base_neg and term.polarity == "positive":
            action = "reclassified_from_negative"
        else:
            action = "added"

        rows.append(
            {
                "word": term.word,
                "polarity": term.polarity,
                "category": term.category,
                "corpus_frequency": corpus_frequency,
                "document_frequency": doc_frequency,
                "action": action,
                "rationale": term.rationale,
            }
        )

    rows.sort(key=lambda item: (item["polarity"], item["category"], item["word"]))
    return rows


def merge_lexicons(
    *,
    base_positive: Iterable[str],
    base_negative: Iterable[str],
    expansion_rows: Sequence[dict],
) -> Tuple[List[str], List[str]]:
    positive = set(base_positive)
    negative = set(base_negative)

    for row in expansion_rows:
        word = row["word"]
        if row["polarity"] == "positive":
            positive.add(word)
            negative.discard(word)
        else:
            negative.add(word)
            positive.discard(word)

    return sorted(positive), sorted(negative)


def write_word_list(path: Path, words: Sequence[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        for word in words:
            fh.write(f"{word}\n")


def write_csv(path: Path, rows: Sequence[dict]) -> None:
    fieldnames = [
        "word",
        "polarity",
        "category",
        "corpus_frequency",
        "document_frequency",
        "action",
        "rationale",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_summary(
    *,
    output_path: Path,
    input_path: Path,
    row_count: int,
    base_positive_count: int,
    base_negative_count: int,
    merged_positive_count: int,
    merged_negative_count: int,
    expansion_rows: Sequence[dict],
) -> None:
    added_positive = sum(1 for row in expansion_rows if row["polarity"] == "positive")
    added_negative = sum(1 for row in expansion_rows if row["polarity"] == "negative")
    reclassified = sum(1 for row in expansion_rows if row["action"].startswith("reclassified"))

    lines = [
        "Oil-price lexicon expansion summary",
        f"Input dataset: {input_path}",
        f"Corpus rows processed: {row_count}",
        f"Base positive lexicon size: {base_positive_count}",
        f"Base negative lexicon size: {base_negative_count}",
        f"Merged positive lexicon size: {merged_positive_count}",
        f"Merged negative lexicon size: {merged_negative_count}",
        f"Expanded positive terms retained from corpus: {added_positive}",
        f"Expanded negative terms retained from corpus: {added_negative}",
        f"Reclassified terms: {reclassified}",
        "",
        "The expanded lexicon is oil-price oriented:",
        "positive = bullish / upward oil-price pressure",
        "negative = bearish / downward oil-price pressure",
    ]
    output_path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Expand a sentiment lexicon for oil-price news using a "
            "corpus-verified domain term list."
        )
    )
    parser.add_argument("--input", default=str(DEFAULT_INPUT_PATH), help="Tokenized news CSV path")
    parser.add_argument(
        "--positive-lexicon",
        default=str(DEFAULT_POSITIVE_PATH),
        help="Base positive lexicon path",
    )
    parser.add_argument(
        "--negative-lexicon",
        default=str(DEFAULT_NEGATIVE_PATH),
        help="Base negative lexicon path",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory to write expanded lexicon files",
    )
    parser.add_argument(
        "--min-frequency",
        type=int,
        default=10,
        help="Minimum corpus term frequency required to keep a domain term",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)

    input_path = Path(args.input)
    positive_path = Path(args.positive_lexicon)
    negative_path = Path(args.negative_lexicon)
    output_dir = Path(args.output_dir)

    if not input_path.exists():
        raise SystemExit(f"Input CSV not found: {input_path}")
    if not positive_path.exists():
        raise SystemExit(f"Positive lexicon not found: {positive_path}")
    if not negative_path.exists():
        raise SystemExit(f"Negative lexicon not found: {negative_path}")

    output_dir.mkdir(parents=True, exist_ok=True)

    base_positive = load_word_list(positive_path)
    base_negative = load_word_list(negative_path)
    token_frequency, document_frequency, row_count = count_corpus_terms(input_path)

    expansion_rows = build_expansion_rows(
        base_positive=base_positive,
        base_negative=base_negative,
        token_frequency=token_frequency,
        document_frequency=document_frequency,
        min_frequency=args.min_frequency,
    )
    merged_positive, merged_negative = merge_lexicons(
        base_positive=base_positive,
        base_negative=base_negative,
        expansion_rows=expansion_rows,
    )

    expanded_positive_path = output_dir / "expanded_positive_words.txt"
    expanded_negative_path = output_dir / "expanded_negative_words.txt"
    expansion_csv_path = output_dir / "expanded_sentiment_lexicon.csv"
    summary_path = output_dir / "lexicon_expansion_summary.txt"

    write_word_list(expanded_positive_path, merged_positive)
    write_word_list(expanded_negative_path, merged_negative)
    write_csv(expansion_csv_path, expansion_rows)
    write_summary(
        output_path=summary_path,
        input_path=input_path,
        row_count=row_count,
        base_positive_count=len(base_positive),
        base_negative_count=len(base_negative),
        merged_positive_count=len(merged_positive),
        merged_negative_count=len(merged_negative),
        expansion_rows=expansion_rows,
    )

    print(
        "Lexicon expansion complete. "
        f"rows={row_count}, "
        f"expanded_terms={len(expansion_rows)}, "
        f"positive_lexicon={len(merged_positive)}, "
        f"negative_lexicon={len(merged_negative)}, "
        f"output_dir={output_dir}"
    )


if __name__ == "__main__":
    main()
