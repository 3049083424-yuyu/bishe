from __future__ import annotations

import argparse
import sys
from pathlib import Path


PIPELINE_ROOT = Path(__file__).resolve().parent.parent
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

from build_institution_name_table import build_table


REBUILD_ROOT = Path(r"D:\graduate\thesis_rebuild")

DEFAULT_INPUT_PATH = REBUILD_ROOT / "corpus" / "merged_clean_dual_key_dedup_2011_2025.csv"
DEFAULT_OUTPUT_PATH = REBUILD_ROOT / "institution_eval" / "institution_name_table_dual_key_2011_2025.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(DEFAULT_INPUT_PATH))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    build_table(args.input, args.output)
