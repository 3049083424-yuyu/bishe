from __future__ import annotations

import csv
from pathlib import Path


INPUT_PATH = Path(
    r"D:\毕业论文\version_dual_key_dedup_2011_2025\institution_name_table_dual_key_trans_norm_2011_2025.csv"
)
ENCODING = "gb18030"

CUP_BEIJING = "\u4e2d\u56fd\u77f3\u6cb9\u5927\u5b66\uff08\u5317\u4eac\uff09"
CUP_EC = "\u4e2d\u56fd\u77f3\u6cb9\u5927\u5b66\uff08\u534e\u4e1c\uff09"
CUP_BASE = "\u4e2d\u56fd\u77f3\u6cb9\u5927\u5b66"

TSINGHUA = "\u6e05\u534e\u5927\u5b66"
TSINGHUA_BERKELEY_SZ = "\u6e05\u534e-\u4f2f\u514b\u5229\u6df1\u5733\u5b66\u9662"
TSINGHUA_SZ_GRAD = "\u6e05\u534e\u5927\u5b66\u6df1\u5733\u56fd\u9645\u7814\u7a76\u751f\u9662"
TSINGHUA_INFO_LAB = "\u6e05\u534e\u5927\u5b66\u4fe1\u606f\u79d1\u5b66\u4e0e\u6280\u672f\u56fd\u5bb6\u5b9e\u9a8c\u5ba4"
TSINGHUA_SC_ENERGY = "\u6e05\u534e\u56db\u5ddd\u80fd\u6e90\u4e92\u8054\u7f51\u7814\u7a76\u9662"
TSINGHUA_DG_CENTER = "\u6e05\u534e\u4e1c\u839e\u521b\u65b0\u4e2d\u5fc3"
TSINGHUA_INNOV_CENTER = "\u6e05\u534e\u521b\u65b0\u4e2d\u5fc3"

CUMT = "\u4e2d\u56fd\u77ff\u4e1a\u5927\u5b66"
CUMT_BEIJING = "\u4e2d\u56fd\u77ff\u4e1a\u5927\u5b66\uff08\u5317\u4eac\uff09"

XIAN_PETRO = "\u897f\u5b89\u77f3\u6cb9\u5927\u5b66"
SW_PETRO = "\u897f\u5357\u77f3\u6cb9\u5927\u5b66"


def replace_parent_prefix(text: str, base: str, target: str) -> str:
    text = text.strip()
    if not text:
        return target
    variants = [f"{base}\uff08\u5317\u4eac\uff09", f"{base}\uff08\u534e\u4e1c\uff09", base]
    for prefix in variants:
        if text.startswith(prefix):
            return target + text[len(prefix) :]
    return text


def classify_cup(name: str, trans: str, norm: str) -> tuple[str, str] | None:
    low_name = name.lower().strip()
    low = " ".join([name, trans, norm]).lower()
    if not (
        "china university of petroleum" in low
        or "china univ petr" in low
        or "china univ petro" in low
        or "petroleum university of china" in low
        or CUP_BASE in name
        or norm.strip() in {CUP_BASE, CUP_BEIJING, CUP_EC}
        or trans.strip().startswith(CUP_BASE)
    ):
        return None

    east_keys = [
        "east china",
        "eastchina",
        "(east)",
        "(east china",
        " east)",
        " e china",
        " eastern",
        " china east",
        "eastb china",
        "huadong",
        "qingdao",
        "dongying",
        "shandong",
        "\u534e\u4e1c",
        "\u9752\u5c9b",
        "\u4e1c\u8425",
        "\u5c71\u4e1c",
    ]
    beijing_keys = [
        "beijing",
        "karamay",
        "\u5317\u4eac",
        "\u514b\u62c9\u739b\u4f9d",
    ]

    target = CUP_BEIJING
    east_aliases = {
        "china univ petr east",
        "china univ petr e",
        "china univ petr eastern",
        "china univ petr china east",
    }
    if low_name in east_aliases or any(key in low for key in east_keys):
        target = CUP_EC
    elif any(key in low for key in beijing_keys):
        target = CUP_BEIJING

    new_trans = trans.strip()
    if new_trans in {"", CUP_BASE, CUP_BEIJING, CUP_EC}:
        new_trans = target
    elif new_trans.startswith(CUP_BASE):
        new_trans = replace_parent_prefix(new_trans, CUP_BASE, target)

    generic_markers = [
        "school",
        "college",
        "department",
        "lab",
        "laboratory",
        "institute",
        "center",
        "centre",
        "faculty",
        "key laboratory",
        "state key",
    ]
    if new_trans.startswith(target):
        suffix = new_trans[len(target) :]
        if any(ch.isascii() and ch.isalpha() for ch in suffix) and not any(marker in low_name for marker in generic_markers):
            new_trans = target

    return new_trans, target


def classify_tsinghua(name: str, trans: str, norm: str) -> tuple[str, str] | None:
    low_name = name.lower().strip()
    low_text = " ".join([name, trans, norm]).lower()
    if not (
        "tsinghua" in low_text
        or "qinghua" in low_text
        or "\u6e05\u534e" in name
        or "\u6e05\u534e" in trans
        or "\u6e05\u534e" in norm
    ):
        return None

    exclusion_keys = [
        " co ",
        " co.",
        " company",
        " ltd",
        " limited",
        " corp",
        " corporation",
        " planning",
        " design",
        " urban planning",
        " shuimuqinghua",
        " guohuan",
        " vehicle safety",
        " 35 qinghua east rd",
        " qinghua east rd",
    ]
    if any(key in low_name for key in exclusion_keys):
        return None

    explicit_map = {
        "tsinghua univ": TSINGHUA,
        "tsinghua university": TSINGHUA,
        "natl tsinghua univ": TSINGHUA,
        "tsinghua berkeley shenzhen inst": TSINGHUA_BERKELEY_SZ,
        "tsinghua univ shenzhen": TSINGHUA_SZ_GRAD,
        "tsinghua shenzhen int grad sch": TSINGHUA_SZ_GRAD,
        "tsinghua natl lab informat sci & technol": TSINGHUA_INFO_LAB,
        "tsinghua sichuan energy internet res inst": TSINGHUA_SC_ENERGY,
        "tsinghua innovat ctr dongguan": TSINGHUA_DG_CENTER,
        "tsinghua innovat ctr": TSINGHUA_INNOV_CENTER,
        "inst tsinghua univ": TSINGHUA,
        "res inst tsinghua univ shenzhen": TSINGHUA_SZ_GRAD,
        "inet of tsinghua university": TSINGHUA,
        "tsinghua univ beijing": TSINGHUA,
        "tsinghua univ zhejiang": TSINGHUA,
        "tsinghua univ tbsi": TSINGHUA,
        "tsinghua univ thuai": TSINGHUA,
    }
    if low_name in explicit_map:
        return explicit_map[low_name], TSINGHUA

    broader_keys = [
        "tsinghua univ",
        "tsinghua university",
        "natl tsinghua",
        " of tsinghua university",
        " in tsinghua university",
        "tsinghua shenzhen",
        "tsinghua berkeley",
        "tsinghua natl lab",
        "inst tsinghua",
        "res inst tsinghua",
        "tsinghua innovat",
        "inet of tsinghua",
    ]
    if any(key in low_name for key in broader_keys):
        new_trans = trans.strip()
        if not new_trans or "tsinghua" in new_trans.lower() or "qinghua" in new_trans.lower():
            new_trans = TSINGHUA
        return new_trans, TSINGHUA

    return None


def classify_cumt(name: str, trans: str, norm: str) -> tuple[str, str] | None:
    low = " ".join([name, trans, norm]).lower()
    if not (
        "china university of mining" in low
        or "china univ min" in low
        or CUMT in name
        or CUMT in trans
        or CUMT in norm
    ):
        return None

    target = ""
    if "beijing" in low or "cumtb" in low or "\u5317\u4eac" in name or "\u5317\u4eac" in trans:
        target = CUMT_BEIJING
    elif "xuzhou" in low or "\u5f90\u5dde" in name or "\u5f90\u5dde" in trans:
        target = CUMT
    else:
        return None

    new_trans = trans.strip()
    if (
        not new_trans
        or "chinauniversity" in new_trans.lower()
        or "china univ" in new_trans.lower()
        or new_trans in {CUMT, CUMT_BEIJING}
    ):
        new_trans = target
    elif new_trans.startswith(CUMT) and target == CUMT_BEIJING and not new_trans.startswith(CUMT_BEIJING):
        new_trans = CUMT_BEIJING + new_trans[len(CUMT) :]

    return new_trans, target


def classify_petro_variants(name: str) -> tuple[str, str] | None:
    key = name.lower().strip()
    mapping = {
        "xi'an petroleum university": (XIAN_PETRO, XIAN_PETRO),
        "xian petroleum university": (XIAN_PETRO, XIAN_PETRO),
        "south west petroleum university": (SW_PETRO, SW_PETRO),
        "southwest petroleum university": (SW_PETRO, SW_PETRO),
        "southwest university of petroleum": (SW_PETRO, SW_PETRO),
        "southwest petroleum university;;southwest petroleum university": (SW_PETRO, SW_PETRO),
    }
    return mapping.get(key)


def main() -> None:
    with INPUT_PATH.open("r", encoding=ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])

    changed_rows = 0
    family_counts = {"cup": 0, "tsinghua": 0, "cumt": 0, "petro_variants": 0}

    for row in rows:
        name = (row.get("institution_name") or "").strip()
        trans = (row.get("institution_trans") or "").strip()
        norm = (row.get("institution_norm") or "").strip()

        new_pair = None
        family = ""

        for family, classifier in (
            ("cup", lambda: classify_cup(name, trans, norm)),
            ("tsinghua", lambda: classify_tsinghua(name, trans, norm)),
            ("cumt", lambda: classify_cumt(name, trans, norm)),
            ("petro_variants", lambda: classify_petro_variants(name)),
        ):
            new_pair = classifier()
            if new_pair:
                break

        if not new_pair:
            continue

        new_trans, new_norm = new_pair
        if new_trans != trans or new_norm != norm:
            row["institution_trans"] = new_trans
            row["institution_norm"] = new_norm
            changed_rows += 1
            family_counts[family] += 1

    with INPUT_PATH.open("w", encoding=ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"changed_rows={changed_rows}")
    for family, count in family_counts.items():
        print(f"{family}_changed={count}")
    print(f"output={INPUT_PATH}")


if __name__ == "__main__":
    main()
