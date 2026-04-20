from __future__ import annotations

import csv
from pathlib import Path


BASE_DIR = Path(r"D:\毕业论文\version_dual_key_dedup_2011_2025")
INPUT_PATH = BASE_DIR / "institution_topsis_score_top100_dual_key_2011_2025_zh.csv"
OUTPUT_PATH = BASE_DIR / "institution_type_review_top100_dual_key_2011_2025.csv"
ENCODING = "gb18030"


TARGET_TYPES = ("高校/科研院所", "企业研发中心", "国际组织")
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
            return "企业研发中心", f"企业标记:{marker}"
    for keyword in ENTERPRISE_KEYWORDS_CN + EXTRA_ENTERPRISE_CN:
        if keyword in text:
            return "企业研发中心", f"企业关键词:{keyword}"
    for keyword in ("company", "corporation", "corp", "co ltd", "limited", "inc", "plc", "oilfield", "petrochemical"):
        if keyword in lowered:
            return "企业研发中心", f"企业英文关键词:{keyword}"

    for keyword in ACADEMIC_KEYWORDS_CN + EXTRA_ACADEMIC_CN:
        if keyword in text:
            return "高校/科研院所", f"高校科研关键词:{keyword}"
    for keyword in ACADEMIC_KEYWORDS_EN:
        if keyword in lowered:
            return "高校/科研院所", f"高校科研英文关键词:{keyword}"

    return "其他", "未命中规则"


def main() -> None:
    with INPUT_PATH.open("r", encoding=ENCODING, newline="") as f:
        rows = list(csv.DictReader(f))

    out_rows: list[dict[str, object]] = []
    for row in rows:
        institution = str(row.get("标准化机构名称", "")).strip()
        rank_text = str(row.get("TOPSIS综合排名", "")).strip()
        score_text = str(row.get("TOPSIS综合得分", "")).strip()
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

    with OUTPUT_PATH.open("w", encoding=ENCODING, newline="") as f:
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
        writer.writerows(out_rows)

    print(f"output={OUTPUT_PATH}")
    print(f"rows={len(out_rows)}")


if __name__ == "__main__":
    main()
