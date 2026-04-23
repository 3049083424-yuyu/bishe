from __future__ import annotations

import argparse
import csv
from pathlib import Path


REBUILD_ROOT = Path(r"D:\graduate\thesis_rebuild")
DEFAULT_INPUT_PATH = REBUILD_ROOT / "institution_eval" / "institution_topsis_score_top100_dual_key_2011_2025.csv"
DEFAULT_OUTPUT_PATH = REBUILD_ROOT / "institution_eval" / "institution_type_review_top100_dual_key_2011_2025.csv"

ENCODING = "gb18030"

STRONG_ACADEMIC_KEYWORDS_CN = (
    "大学",
    "学院",
)
STRONG_ACADEMIC_KEYWORDS_EN = (
    "university",
    "college",
    "school",
)

ENTERPRISE_KEYWORDS_CN = (
    "公司",
    "集团",
    "股份有限公司",
    "有限责任公司",
    "油田",
    "石化",
    "石油局",
    "勘探局",
    "工程公司",
    "工程技术公司",
)
ENTERPRISE_MARKERS = (
    "中国石油",
    "中国石化",
    "中海油",
    "中国海油",
    "petrochina",
    "sinopec",
    "cnooc",
    "cnpc",
    "shell",
    "exxon",
    "chevron",
    "bp",
    "aramco",
    "totalenergies",
    "conocophillips",
    "halliburton",
    "schlumberger",
    "baker hughes",
)
ACADEMIC_KEYWORDS_CN = (
    "大学",
    "学院",
    "科学院",
    "研究院",
    "研究所",
    "实验室",
    "研究中心",
)
ACADEMIC_KEYWORDS_EN = (
    "university",
    "college",
    "school",
    "academy",
    "institute",
    "laboratory",
    "research center",
    "research centre",
)
INTERNATIONAL_MARKERS = (
    "international energy agency",
    "organization of the petroleum exporting countries",
    "world petroleum council",
    "united nations",
    "world bank",
    "international association",
    "international organization",
    "iea",
    "opec",
    "国际能源署",
    "欧佩克",
    "世界石油理事会",
    "联合国",
    "世界银行",
    "国际组织",
)
EXTRA_ENTERPRISE_CN = (
    "研究院有限公司",
    "技术公司",
    "装备公司",
    "工程技术研究院",
    "分公司",
)
EXTRA_ACADEMIC_CN = (
    "国家重点实验室",
    "国家工程实验室",
    "国家工程研究中心",
    "教育部重点实验室",
    "省部共建",
)
GOVERNMENT_EXACT_NAMES = {
    "中国教育部",
    "教育部",
    "中华人民共和国教育部",
    "中国自然资源部",
    "中华人民共和国自然资源部",
    "美国国家标准与技术研究院",
}


def classify_level(rank: int) -> str:
    if 1 <= rank <= 20:
        return "头部引领型"
    if 21 <= rank <= 50:
        return "中坚创新型"
    if 51 <= rank <= 100:
        return "特色细分型"
    return ""


def detect_institution_type(name: str) -> tuple[str, str]:
    text = str(name or "").strip()
    lowered = text.lower()
    if not text:
        return "其他", "空值"
    if text in GOVERNMENT_EXACT_NAMES or text.endswith("教育部"):
        return "政府机构", "政府机构特例:教育部"

    for marker in INTERNATIONAL_MARKERS:
        if marker in lowered or marker in text:
            return "国际组织", f"国际组织标记:{marker}"

    for keyword in STRONG_ACADEMIC_KEYWORDS_CN:
        if keyword in text:
            return "高校/科研院所", f"高校强特征:{keyword}"
    for keyword in STRONG_ACADEMIC_KEYWORDS_EN:
        if keyword in lowered:
            return "高校/科研院所", f"高校强特征:{keyword}"

    for marker in ENTERPRISE_MARKERS:
        if marker in lowered:
            return "企业研发机构", f"企业标记:{marker}"
    for keyword in ENTERPRISE_KEYWORDS_CN + EXTRA_ENTERPRISE_CN:
        if keyword in text:
            return "企业研发机构", f"企业关键词:{keyword}"
    for keyword in ("company", "corporation", "corp", "co ltd", "limited", "inc", "plc", "oilfield", "petrochemical"):
        if keyword in lowered:
            return "企业研发机构", f"企业英文关键词:{keyword}"

    for keyword in ACADEMIC_KEYWORDS_CN + EXTRA_ACADEMIC_CN:
        if keyword in text:
            return "高校/科研院所", f"高校科研关键词:{keyword}"
    for keyword in ACADEMIC_KEYWORDS_EN:
        if keyword in lowered:
            return "高校/科研院所", f"高校科研英文关键词:{keyword}"

    return "其他", "未命中规则"


def row_value(row: dict[str, str], *keys: str) -> str:
    for key in keys:
        value = str(row.get(key, "")).strip()
        if value:
            return value
    return ""


def build_review_rows(input_path: Path) -> list[dict[str, object]]:
    with input_path.open("r", encoding=ENCODING, newline="") as f:
        rows = list(csv.DictReader(f))

    out_rows: list[dict[str, object]] = []
    for row in rows:
        institution = row_value(row, "institution_norm", "标准化机构名称")
        rank_text = row_value(row, "topsis_rank", "TOPSIS综合排名")
        score_text = row_value(row, "topsis_score", "TOPSIS综合得分")
        rank = int(rank_text) if rank_text.isdigit() else 0
        institution_type, basis = detect_institution_type(institution)
        out_rows.append(
            {
                "TOPSIS综合排名": rank,
                "标准化机构名称": institution,
                "TOPSIS综合得分": score_text,
                "机构层级": classify_level(rank),
                "机构类型": institution_type,
                "分类依据": basis,
                "是否建议人工复核": 1 if institution_type == "其他" else 0,
            }
        )

    out_rows.sort(key=lambda item: int(item["TOPSIS综合排名"]))
    return out_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(DEFAULT_INPUT_PATH))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_path = Path(args.output)
    rows = build_review_rows(Path(args.input))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding=ENCODING, newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "TOPSIS综合排名",
                "标准化机构名称",
                "TOPSIS综合得分",
                "机构层级",
                "机构类型",
                "分类依据",
                "是否建议人工复核",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"output={output_path}")
    print(f"rows={len(rows)}")


if __name__ == "__main__":
    main()
