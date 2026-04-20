from __future__ import annotations

import csv
import re
from pathlib import Path


CURRENT_PATH = Path(r"D:\毕业论文\version_dual_key_dedup_2011_2025\institution_name_table_dual_key_trans_norm_2011_2025.csv")
ENCODING = "gb18030"

RE_PRIVATE = re.compile(r"[\ue000-\uf8ff\U00010000-\U0010ffff]")
RE_SPACE = re.compile(r"\s+")
RE_CAMEL = re.compile(r"([a-z])([A-Z])")


def has_broken_chars(text: str) -> bool:
    return bool(RE_PRIVATE.search(text))


def strip_broken_chars(text: str) -> str:
    cleaned = RE_PRIVATE.sub("", text or "")
    cleaned = RE_SPACE.sub(" ", cleaned).strip()
    return cleaned


def canonical(text: str) -> str:
    value = strip_broken_chars(text)
    value = RE_CAMEL.sub(r"\1 \2", value)
    value = value.lower().replace("&", " and ")
    for ch in "()[]{}\\/,-_.;:'\"?":
        value = value.replace(ch, " ")
    value = value.replace("∥", " ")
    return " ".join(value.split())


MANUAL_NAME_PAIRS: list[tuple[str, tuple[str, str]]] = [
    ("Wuxi Institute of Petroleum Geology", ("无锡石油地质研究所", "无锡石油地质研究所")),
    ("Wuxi Petroleum Geology Institute", ("无锡石油地质研究所", "无锡石油地质研究所")),
    ("Wuxi Res Inst Petr Geol", ("无锡石油地质研究所", "无锡石油地质研究所")),
    ("Dalian Institute of Chemical Physics", ("中国科学院大连化学物理研究所", "中国科学院大连化学物理研究所")),
    (
        "Dalian Institute of Chemical Physics, Chinese Academy of Sciences",
        ("中国科学院大连化学物理研究所", "中国科学院大连化学物理研究所"),
    ),
    ("Lanzhou Institute of Chemical Physics", ("中国科学院兰州化学物理研究所", "中国科学院兰州化学物理研究所")),
    (
        "Lanzhou Institute of Chemical Physics of Chinese Academy of Sciences",
        ("中国科学院兰州化学物理研究所", "中国科学院兰州化学物理研究所"),
    ),
    ("Chengdu Institute of Geology and Mineral Resources", ("成都地质矿产研究所", "成都地质矿产研究所")),
    (
        "Natl Engn Res Ctr Oil & Gas Drilling & Complet Tec",
        ("油气钻井完井技术国家工程研究中心", "油气钻井完井技术国家工程研究中心"),
    ),
    ("Southwest Oil & Gasfield Company", ("中国石油西南油气田公司", "中国石油西南油气田公司")),
    ("Southwest Oil & Gas Field Company", ("中国石油西南油气田公司", "中国石油西南油气田公司")),
    ("Petro China Southwest Oil & Gasfield Co", ("中国石油西南油气田公司", "中国石油西南油气田公司")),
    ("PetroChina Southwest Oil & Gasfield Co", ("中国石油西南油气田公司", "中国石油西南油气田公司")),
    ("PetroChina Southwest Oil & Gas Field Company", ("中国石油西南油气田公司", "中国石油西南油气田公司")),
    (
        "Exploration and Development Research Institute of Southwest Oil & Gasfield Company",
        ("中国石油西南油气田公司勘探开发研究院", "中国石油西南油气田公司勘探开发研究院"),
    ),
    (
        "Exploration and Development Research Institute of PetroChina Southwest Oil & Gasfield Company",
        ("中国石油西南油气田公司勘探开发研究院", "中国石油西南油气田公司勘探开发研究院"),
    ),
    (
        "Research Institute of Petroleum Exploration & Development-Northwest",
        ("中国石油勘探开发研究院西北分院", "中国石油勘探开发研究院"),
    ),
    (
        "PetroChina Research Institute of Petroleum Exploration & Development-Northwest",
        ("中国石油勘探开发研究院西北分院", "中国石油勘探开发研究院"),
    ),
    (
        "Langfang Filial of Research Institute of Petroleum Exploration and Development",
        ("中国石油勘探开发研究院廊坊分院", "中国石油勘探开发研究院"),
    ),
    ("PetroChina Turpan-Hami Oilfield Company", ("中国石油吐哈油田公司", "中国石油吐哈油田公司")),
    ("PetroChina Turpan Hami Oilfield Company", ("中国石油吐哈油田公司", "中国石油吐哈油田公司")),
    ("PetroChina Turpan Hami Oilfield Co", ("中国石油吐哈油田公司", "中国石油吐哈油田公司")),
    ("Petrochina Turpan Hami Oilfield Co", ("中国石油吐哈油田公司", "中国石油吐哈油田公司")),
    ("Petro China TurpanHami Oilfield Company", ("中国石油吐哈油田公司", "中国石油吐哈油田公司")),
    ("Petro China Turpan Hami Oilfield Company", ("中国石油吐哈油田公司", "中国石油吐哈油田公司")),
    ("Turpan-Hami Oilfield Company of PetroChina", ("中国石油吐哈油田公司", "中国石油吐哈油田公司")),
    ("PetroChina", ("中国石油天然气股份有限公司", "中国石油天然气股份有限公司")),
    ("Daqing Oilfield Limited Company", ("大庆油田有限责任公司", "大庆油田有限责任公司")),
    ("Liaoning Shihua University", ("辽宁石油化工大学", "辽宁石油化工大学")),
    ("School of Engineering of Sun Yat-Sen University", ("中山大学工学院", "中山大学工学院")),
    ("Geoscience Centre of the University of Gottingen", ("哥廷根大学地球科学中心", "哥廷根大学地球科学中心")),
    ("Geoscience Centre of the University of Gttingen", ("哥廷根大学地球科学中心", "哥廷根大学地球科学中心")),
    ("Northwest Oilfield Branch", ("中国石化西北油田分公司", "中国石化西北油田分公司")),
    (
        "Laboratory of Seismology and Physics of Earths Interior",
        ("地震学与地球内部物理实验室", "地震学与地球内部物理实验室"),
    ),
]

MANUAL_MAP = {canonical(name): pair for name, pair in MANUAL_NAME_PAIRS}


def main() -> None:
    with CURRENT_PATH.open("r", encoding=ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])

    changed_rows = 0
    broken_cleaned_rows = 0
    manual_hit_rows = 0
    manual_changed_rows = 0

    for row in rows:
        old_name = row.get("institution_name", "")
        old_trans = row.get("institution_trans", "")
        old_norm = row.get("institution_norm", "")

        if any(has_broken_chars(text) for text in (old_name, old_trans, old_norm)):
            broken_cleaned_rows += 1

        row["institution_name"] = strip_broken_chars(old_name)
        row["institution_trans"] = strip_broken_chars(old_trans)
        row["institution_norm"] = strip_broken_chars(old_norm)

        pair = MANUAL_MAP.get(canonical(row.get("institution_name", "")))
        if pair:
            manual_hit_rows += 1
            if row["institution_trans"] != pair[0] or row["institution_norm"] != pair[1]:
                row["institution_trans"], row["institution_norm"] = pair
                manual_changed_rows += 1

        if (
            row.get("institution_name", "") != old_name
            or row.get("institution_trans", "") != old_trans
            or row.get("institution_norm", "") != old_norm
        ):
            changed_rows += 1

    with CURRENT_PATH.open("w", encoding=ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"output={CURRENT_PATH}")
    print(f"rows={len(rows)}")
    print(f"changed_rows={changed_rows}")
    print(f"broken_cleaned_rows={broken_cleaned_rows}")
    print(f"manual_hit_rows={manual_hit_rows}")
    print(f"manual_changed_rows={manual_changed_rows}")


if __name__ == "__main__":
    main()
