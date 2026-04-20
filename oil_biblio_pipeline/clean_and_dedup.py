from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Set, Tuple


REQUIRED_FIELDS = [
    "title_cn_en",
    "publish_date",
    "institution",
    "journal_en",
]

PRIMARY_KEY_FIELDS = ["doi", "title_cn_en", "publish_date"]


@dataclass
class CleanStats:
    input_files: int
    input_rows: int
    dropped_missing_required: int
    dropped_duplicates: int
    output_rows: int


def iter_data_files(input_path: Path) -> Iterator[Path]:
    if input_path.is_file():
        yield input_path
        return
    # 默认只处理 normalize_institutions 产生的 data_*.csv
    for p in sorted(input_path.glob("data_*.csv")):
        if p.is_file():
            yield p


def clean_and_dedup(
    *,
    input_path: Path,
    out_file: Path,
    encoding: str,
    delimiter: str,
) -> CleanStats:
    out_file.parent.mkdir(parents=True, exist_ok=True)

    # 放宽单字段长度限制，避免长摘要/全文报错
    try:
        csv.field_size_limit(sys.maxsize)
    except (OverflowError, ValueError):
        csv.field_size_limit(10_000_000)

    seen_keys: Set[Tuple[str, str, str]] = set()

    input_files = 0
    input_rows = 0
    dropped_missing_required = 0
    dropped_duplicates = 0
    output_rows = 0

    writer: Optional[csv.DictWriter] = None
    header: List[str] = []

    with out_file.open("w", encoding="utf-8-sig", newline="") as wf:
        for path in iter_data_files(input_path):
            input_files += 1
            with path.open("r", encoding=encoding, errors="replace", newline="") as rf:
                reader = csv.DictReader(rf, delimiter=delimiter)
                if reader.fieldnames is None:
                    continue

                if not header:
                    header = list(reader.fieldnames)
                    for col in REQUIRED_FIELDS:
                        if col not in header:
                            header.append(col)
                    writer = csv.DictWriter(wf, fieldnames=header, delimiter=delimiter)
                    writer.writeheader()

                assert writer is not None

                for rec in reader:
                    input_rows += 1

                    # 统一 strip，避免只是空格的情况
                    for k, v in list(rec.items()):
                        if isinstance(v, str):
                            rec[k] = v.strip()

                    # 必须非空字段过滤
                    if any(not (rec.get(col) or "").strip() for col in REQUIRED_FIELDS):
                        dropped_missing_required += 1
                        continue

                    key = tuple((rec.get(col) or "").strip() for col in PRIMARY_KEY_FIELDS)
                    if key in seen_keys:
                        dropped_duplicates += 1
                        continue
                    seen_keys.add(key)

                    # 确保缺失字段有空字符串占位
                    for col in header:
                        if col not in rec:
                            rec[col] = ""

                    writer.writerow(rec)
                    output_rows += 1

    return CleanStats(
        input_files=input_files,
        input_rows=input_rows,
        dropped_missing_required=dropped_missing_required,
        dropped_duplicates=dropped_duplicates,
        output_rows=output_rows,
    )


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Clean & deduplicate normalized CNKI data "
            "(composite key: doi + title_cn_en + publish_date)"
        )
    )
    p.add_argument(
        "--input",
        required=True,
        help="Input: a directory containing data_*.csv from normalize_institutions",
    )
    p.add_argument(
        "--output",
        required=True,
        help="Output merged cleaned CSV file path",
    )
    p.add_argument(
        "--encoding",
        default="utf-8",
        help="Input encoding (utf-8/gb18030)",
    )
    p.add_argument(
        "--delimiter",
        default=",",
        help="CSV delimiter (default: ,)",
    )
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = _parse_args(argv)
    input_path = Path(args.input)
    out_file = Path(args.output)

    if not input_path.exists():
        raise SystemExit(f"Input not found: {input_path}")

    stats = clean_and_dedup(
        input_path=input_path,
        out_file=out_file,
        encoding=args.encoding,
        delimiter=args.delimiter,
    )

    print(
        "Clean & dedup done. "
        f"files={stats.input_files}, "
        f"rows_in={stats.input_rows}, "
        f"dropped_missing_required={stats.dropped_missing_required}, "
        f"dropped_duplicates={stats.dropped_duplicates}, "
        f"rows_out={stats.output_rows}, "
        f"output={out_file}"
    )


if __name__ == "__main__":
    main()

