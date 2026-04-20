from __future__ import annotations

import argparse
import csv
import re
import sys
import unicodedata
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple


_RE_MULTI_SPACE = re.compile(r"\s+")
_RE_BRACKET = re.compile(r"[\(\（\[\【].*?[\)\）\]\】]")
_RE_EMAIL = re.compile(r"\b[\w.\-+]+@[\w.\-]+\.\w+\b", re.I)


def _norm_unicode(s: str) -> str:
    # NFKC handles full-width/half-width, compatibility forms.
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\u00a0", " ").replace("\u200b", "")
    return s


def _basic_cleanup(s: str) -> str:
    s = _norm_unicode(s)
    s = s.strip().strip(";").strip(",").strip("，").strip("；")
    s = _RE_EMAIL.sub("", s)
    s = _RE_BRACKET.sub("", s)
    s = s.replace("&", " and ")
    s = s.replace("，", ",").replace("；", ";")
    s = s.replace("．", ".").replace("·", " ")
    s = _RE_MULTI_SPACE.sub(" ", s).strip()
    return s


def normalize_institution_name(raw: str) -> str:
    """
    Heuristic, language-agnostic normalization:
    - unicode NFKC
    - remove emails / bracketed notes
    - unify punctuation
    - drop obvious sub-units (dept/lab/school) when present
    """
    s = _basic_cleanup(raw)
    if not s:
        return ""

    # Common English expansions
    repl = {
        r"\buniv\b\.?": "university",
        r"\bdept\b\.?": "",
        r"\bdepartment\b": "",
        r"\bsch\b\.?": "school",
        r"\binst\b\.?": "institute",
        r"\bctr\b\.?": "center",
        r"\bcent\b\.?": "center",
        r"\bco\b\.?": "company",
        r"\bcorp\b\.?": "corporation",
        r"\bltd\b\.?": "ltd",
        r"\binc\b\.?": "inc",
    }
    lowered = " " + s.lower() + " "
    for pat, rep in repl.items():
        lowered = re.sub(pat, f" {rep} ", lowered, flags=re.I)
    lowered = _RE_MULTI_SPACE.sub(" ", lowered).strip()

    # Chinese: remove common sub-units to keep organization-level name
    cn_subunits = [
        "学院",
        "系",
        "研究所",
        "研究院",
        "实验室",
        "中心",
        "分院",
        "分公司",
        "分部",
        "厂",
        "处",
        "科",
        "室",
        "队",
        "站",
    ]
    s2 = lowered
    for token in cn_subunits:
        # only remove if it looks like a trailing sub-unit (keep core org)
        s2 = re.sub(rf"{re.escape(token)}.*$", token, s2)

    s2 = _RE_MULTI_SPACE.sub(" ", s2).strip(" ,;")
    return s2


def split_institution_field(value: str) -> List[str]:
    """
    Split an institution/address field into candidate institution strings.
    Works for simple `institution` fields and for `c1`-like address fields
    (best-effort; you'll still get some noise to filter later).
    """
    s = _basic_cleanup(value)
    if not s:
        return []

    # WOS C1 sometimes uses: "[Author] addr; [Author] addr; ..."
    s = re.sub(r"\[[^\]]+\]\s*", "", s)

    # First split by ';' (most common record separator)
    parts = [p.strip() for p in s.split(";") if p.strip()]
    out: List[str] = []
    for p in parts:
        # Then split by ',' but keep multi-comma addresses by grouping
        # Heuristic: institution tends to be first 1-2 comma segments
        segs = [x.strip() for x in p.split(",") if x.strip()]
        if not segs:
            continue
        if len(segs) == 1:
            out.append(segs[0])
        else:
            # try to capture the institution-like prefix
            out.append(segs[0])
            # Some sources put "University, School/Dept" -> keep first two
            if any(k in segs[1].lower() for k in ["university", "institute", "academy", "company", "corp", "ltd"]):
                out.append(f"{segs[0]}, {segs[1]}")
    return [x for x in out if x]


def iter_csv_files(input_path: Path) -> Iterator[Path]:
    if input_path.is_file():
        yield input_path
        return
    for p in sorted(input_path.glob("*.csv")):
        yield p


def _read_csv_header(path: Path, encoding: str, delimiter: str) -> List[str]:
    # errors="replace" to be robust to mixed encodings
    with path.open("r", encoding=encoding, errors="replace", newline="") as f:
        r = csv.reader(f, delimiter=delimiter)
        header = next(r, None)
        return header or []


def normalize_dataset(
    *,
    input_path: Path,
    outdir: Path,
    encoding: str,
    delimiter: str,
    inst_col: Optional[str],
    c1_col: Optional[str],
    max_rows_per_file: Optional[int],
) -> None:
    outdir.mkdir(parents=True, exist_ok=True)

    raw_counter: Counter[str] = Counter()
    norm_counter: Counter[str] = Counter()
    raw_to_norm: Dict[str, str] = {}

    # 如果存在手工维护的自定义映射表，优先加载并使用
    custom_map_path = outdir / "institution_map_custom.csv"
    if custom_map_path.exists():
        with custom_map_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            # 兼容两种表头：raw, normalized 或 name, normalized
            for row in reader:
                raw = (row.get("raw") or row.get("name") or "").strip()
                norm = (row.get("normalized") or "").strip()
                if raw and norm:
                    raw_to_norm[raw] = norm

    def extract_values(rec: Dict[str, str]) -> List[str]:
        vals: List[str] = []
        if inst_col and rec.get(inst_col):
            vals.extend(split_institution_field(rec[inst_col]))
        if c1_col and rec.get(c1_col):
            vals.extend(split_institution_field(rec[c1_col]))
        return [v for v in vals if v]

    file_idx = 0
    # 提高单字段长度上限，避免长摘要/全文导致的 field limit 错误
    try:
        csv.field_size_limit(sys.maxsize)
    except (OverflowError, ValueError):
        # 在某些平台上 sys.maxsize 可能过大，退一步使用一个较大的常数
        csv.field_size_limit(10_000_000)

    for in_file in iter_csv_files(input_path):
        file_idx += 1
        out_file = outdir / f"data_{in_file.stem}.csv"

        # errors="replace" 防止因个别非法字节中断整个批处理
        with in_file.open("r", encoding=encoding, errors="replace", newline="") as rf, out_file.open(
            "w", encoding="utf-8-sig", newline=""
        ) as wf:
            reader = csv.DictReader(rf, delimiter=delimiter)
            if reader.fieldnames is None:
                continue

            header = list(reader.fieldnames)
            extra_cols = ["institution_extracted", "institution_norm"]
            writer = csv.DictWriter(wf, fieldnames=header + extra_cols, delimiter=delimiter)
            writer.writeheader()

            n = 0
            for rec in reader:
                n += 1
                if max_rows_per_file and n > max_rows_per_file:
                    break

                insts = extract_values(rec)
                insts = [i for i in insts if i]

                extracted = " | ".join(insts[:50])  # avoid extreme rows

                norms: List[str] = []
                for raw in insts:
                    raw_clean = _basic_cleanup(raw)
                    if not raw_clean:
                        continue
                    raw_counter[raw_clean] += 1
                    if raw_clean not in raw_to_norm:
                        raw_to_norm[raw_clean] = normalize_institution_name(raw_clean)
                    norm = raw_to_norm[raw_clean]
                    if norm:
                        norm_counter[norm] += 1
                        norms.append(norm)

                rec_out = dict(rec)
                rec_out["institution_extracted"] = extracted
                rec_out["institution_norm"] = " | ".join(sorted(set(norms))[:50])
                writer.writerow(rec_out)

    # write outputs
    def write_counter(path: Path, counter: Counter[str]) -> None:
        with path.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(["name", "count"])
            for name, cnt in counter.most_common():
                w.writerow([name, cnt])

    write_counter(outdir / "institutions_raw.csv", raw_counter)
    write_counter(outdir / "institutions_norm.csv", norm_counter)

    with (outdir / "institution_map.csv").open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["raw", "normalized"])
        for raw, norm in sorted(raw_to_norm.items(), key=lambda x: (-raw_counter[x[0]], x[0])):
            w.writerow([raw, norm])


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extract & normalize institution names.")
    p.add_argument("--input", required=True, help="Input CSV file or a directory of CSVs")
    p.add_argument("--outdir", required=True, help="Output directory")
    p.add_argument("--encoding", default="utf-8", help="Input encoding (utf-8/gb18030)")
    p.add_argument("--delimiter", default=",", help="CSV delimiter (default: ,)")
    p.add_argument("--inst-col", default=None, help="Institution column name (e.g., institution)")
    p.add_argument("--c1-col", default=None, help="Address column name (e.g., c1)")
    p.add_argument(
        "--max-rows-per-file",
        type=int,
        default=None,
        help="Debug limit: only process first N rows of each file",
    )
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = _parse_args(argv)
    input_path = Path(args.input)
    outdir = Path(args.outdir)

    if not input_path.exists():
        raise SystemExit(f"Input not found: {input_path}")

    if not args.inst_col and not args.c1_col:
        raise SystemExit("Provide --inst-col and/or --c1-col")

    normalize_dataset(
        input_path=input_path,
        outdir=outdir,
        encoding=args.encoding,
        delimiter=args.delimiter,
        inst_col=args.inst_col,
        c1_col=args.c1_col,
        max_rows_per_file=args.max_rows_per_file,
    )
    print(f"OK. Outputs written to: {outdir}")


if __name__ == "__main__":
    main()

