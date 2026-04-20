from __future__ import annotations

import argparse
import csv
import math
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple


@dataclass(frozen=True)
class SplitStats:
    input_path: str
    outdir: str
    total_rows: int
    total_files: int


def _ensure_outdir(outdir: Path) -> None:
    outdir.mkdir(parents=True, exist_ok=True)


def _safe_filename(s: str) -> str:
    s = str(s).strip()
    for ch in ['<', '>', ':', '"', "/", "\\", "|", "?", "*"]:
        s = s.replace(ch, "_")
    s = s.replace("\n", " ").replace("\r", " ").strip()
    return s or "unknown"


def split_by_rows(
    *,
    input_path: Path,
    outdir: Path,
    rows_per_file: int,
    encoding: str,
    delimiter: str,
) -> SplitStats:
    _ensure_outdir(outdir)

    total_rows = 0
    file_idx = 0

    # 放宽单字段长度限制，避免长摘要/全文触发 field limit 错误
    try:
        csv.field_size_limit(sys.maxsize)
    except (OverflowError, ValueError):
        csv.field_size_limit(10_000_000)

    # errors="replace" 以防止个别非法字节导致整个文件读取失败
    with input_path.open("r", encoding=encoding, errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)
        header = next(reader, None)
        if header is None:
            return SplitStats(str(input_path), str(outdir), 0, 0)

        batch: List[List[str]] = []
        for row in reader:
            total_rows += 1
            batch.append(row)
            if len(batch) >= rows_per_file:
                file_idx += 1
                out_path = outdir / f"{input_path.stem}.part{file_idx:04d}.csv"
                with out_path.open("w", encoding="utf-8-sig", newline="") as wf:
                    w = csv.writer(wf)
                    w.writerow(header)
                    w.writerows(batch)
                batch = []

        if batch:
            file_idx += 1
            out_path = outdir / f"{input_path.stem}.part{file_idx:04d}.csv"
            with out_path.open("w", encoding="utf-8-sig", newline="") as wf:
                w = csv.writer(wf)
                w.writerow(header)
                w.writerows(batch)

    return SplitStats(str(input_path), str(outdir), total_rows, file_idx)


def split_by_year(
    *,
    input_path: Path,
    outdir: Path,
    year_col: str,
    encoding: str,
    delimiter: str,
    keep_unknown: bool,
) -> SplitStats:
    _ensure_outdir(outdir)

    writers: Dict[str, Tuple[csv.writer, object]] = {}
    total_rows = 0

    def get_writer(year_key: str, header: List[str]) -> csv.writer:
        if year_key in writers:
            return writers[year_key][0]
        out_path = outdir / f"{input_path.stem}.year_{_safe_filename(year_key)}.csv"
        wf = out_path.open("w", encoding="utf-8-sig", newline="")
        w = csv.writer(wf)
        w.writerow(header)
        writers[year_key] = (w, wf)
        return w

    # 同样放宽字段长度 + 使用 errors="replace" 提高鲁棒性
    try:
        csv.field_size_limit(sys.maxsize)
    except (OverflowError, ValueError):
        csv.field_size_limit(10_000_000)

    with input_path.open("r", encoding=encoding, errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        if reader.fieldnames is None:
            return SplitStats(str(input_path), str(outdir), 0, 0)

        header = list(reader.fieldnames)
        if year_col not in header:
            raise SystemExit(
                f"year_col '{year_col}' not found. Available columns: {header}"
            )

        for rec in reader:
            total_rows += 1
            y = (rec.get(year_col) or "").strip()
            if not y:
                if not keep_unknown:
                    continue
                y = "unknown"
            w = get_writer(y, header)
            w.writerow([rec.get(c, "") for c in header])

    for _, wf in writers.values():
        wf.close()

    return SplitStats(str(input_path), str(outdir), total_rows, len(writers))


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Split huge CSV into smaller files.")
    p.add_argument("--input", required=True, help="Input CSV file path")
    p.add_argument("--outdir", required=True, help="Output directory")
    p.add_argument(
        "--encoding",
        default="utf-8",
        help="Input file encoding (try utf-8 or gb18030)",
    )
    p.add_argument("--delimiter", default=",", help="CSV delimiter (default: ,)")

    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--rows-per-file", type=int, help="Split by fixed row count")
    mode.add_argument("--year-col", help="Split by year column name (e.g., year/py)")

    p.add_argument(
        "--keep-unknown-year",
        action="store_true",
        help="When splitting by year, keep rows with missing year as year_unknown.csv",
    )
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = _parse_args(argv)
    input_path = Path(args.input)
    outdir = Path(args.outdir)

    if not input_path.exists():
        raise SystemExit(f"Input not found: {input_path}")

    if args.rows_per_file:
        stats = split_by_rows(
            input_path=input_path,
            outdir=outdir,
            rows_per_file=args.rows_per_file,
            encoding=args.encoding,
            delimiter=args.delimiter,
        )
    else:
        stats = split_by_year(
            input_path=input_path,
            outdir=outdir,
            year_col=args.year_col,
            encoding=args.encoding,
            delimiter=args.delimiter,
            keep_unknown=bool(args.keep_unknown_year),
        )

    print(
        f"OK. rows={stats.total_rows}, files={stats.total_files}, outdir={stats.outdir}"
    )


if __name__ == "__main__":
    main()

