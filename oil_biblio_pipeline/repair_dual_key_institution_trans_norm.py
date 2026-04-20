from __future__ import annotations

import csv
import difflib
import math
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path


CURRENT_PATH = Path(r"D:\毕业论文\version_dual_key_dedup_2011_2025\institution_name_table_dual_key_trans_norm_2011_2025.csv")
MASTER_PATH = Path(r"D:\毕业论文\institution_name_table_doi_required_trans_norm.csv")

ENCODING = "gb18030"

SPECIAL_CHAR_MAP = str.maketrans(
    {
        "ı": "i",
        "İ": "i",
        "ł": "l",
        "Ł": "l",
        "ø": "o",
        "Ø": "o",
        "ð": "d",
        "Ð": "d",
        "þ": "th",
        "Þ": "th",
        "ß": "ss",
        "æ": "ae",
        "Æ": "ae",
        "œ": "oe",
        "Œ": "oe",
    }
)

ABBR_MAP = {
    "univ": "university",
    "acad": "academy",
    "sci": "sciences",
    "petr": "petroleum",
    "geosci": "geosciences",
    "technol": "technology",
    "res": "research",
    "inst": "institute",
    "explorat": "exploration",
    "dev": "development",
    "prod": "production",
    "engn": "engineering",
    "geophys": "geophysical",
    "natl": "national",
    "lab": "laboratory",
    "co": "company",
    "ltd": "limited",
    "dept": "department",
    "sch": "school",
    "tech": "technology",
}

STOPWORDS = {"of", "the", "and", "in", "for", "at", "on"}
GENERIC_TOKENS = {
    "university",
    "institute",
    "research",
    "company",
    "limited",
    "center",
    "branch",
    "laboratory",
    "technology",
    "petroleum",
    "oilfield",
    "china",
    "academy",
    "sciences",
    "school",
    "college",
    "department",
}

SPECIAL_EQUIV = {
    "china university petroleum beijing": "china university petroleum beijing",
    "beijing china university petroleum": "china university petroleum beijing",
    "china university petroleum east china": "china university petroleum east china",
    "china university petroleum huadong": "china university petroleum east china",
    "china university petroleum qingdao": "china university petroleum east china",
    "china university geosciences beijing": "china university geosciences beijing",
    "china university geosciences wuhan": "china university geosciences wuhan",
    "china university mining technology beijing": "china university mining technology beijing",
    "china university mining technology xuzhou": "china university mining technology xuzhou",
}

MANUAL_EXACT: dict[str, tuple[str, str]] = {
    "CNOOC China Limited": ("中海石油（中国）有限公司", "中海石油（中国）有限公司"),
    "Zhanjiang Branch of CNOOC Ltd": ("中海石油（中国）有限公司湛江分公司", "中海石油（中国）有限公司湛江分公司"),
    "Shenzhen Branch of CNOOC Ltd": ("中海石油（中国）有限公司深圳分公司", "中海石油（中国）有限公司深圳分公司"),
    "Shenzhen Branch of CNOOC": ("中海石油（中国）有限公司深圳分公司", "中海石油（中国）有限公司深圳分公司"),
    "Shanghai Branch of CNOOC Ltd": ("中海石油（中国）有限公司上海分公司", "中海石油（中国）有限公司上海分公司"),
    "CNOOC (China) Tianjin Branch": ("中海石油（中国）有限公司天津分公司", "中海石油（中国）有限公司天津分公司"),
    "CNOOC ( China) Tianjin Branch": ("中海石油（中国）有限公司天津分公司", "中海石油（中国）有限公司天津分公司"),
    "CNOOC Research Center": ("中海油研究总院", "中海油研究总院"),
    "Research Institute of CNOOC": ("中海油研究总院", "中海油研究总院"),
    "CNOOC Research Institute Co": ("中海油研究总院有限责任公司", "中海油研究总院"),
    "CNOOC Gas & Power Group": ("中海石油气电集团有限责任公司", "中海石油气电集团有限责任公司"),
    "China Oilfield Services Limited": ("中海油田服务股份有限公司", "中海油田服务股份有限公司"),
    "SINOPEC Beijing Research Institute of Chemical Industry": ("中国石化北京化工研究院", "中国石化北京化工研究院"),
    "Sinopec Beijing Research Institute of Chemical Industry": ("中国石化北京化工研究院", "中国石化北京化工研究院"),
    "Sinopec Geophysical Research Institute": ("中国石化地球物理研究院", "中国石化地球物理研究院"),
    "SINOPEC Research Institute of Petroleum Processing Co": ("中国石化石油化工科学研究院有限公司", "中国石化石油化工科学研究院"),
    "SINOPEC Exploration & Production Research Institute": ("中国石化石油勘探开发研究院", "中国石化石油勘探开发研究院"),
    "Sinopec Exploration & Production Research Institute": ("中国石化石油勘探开发研究院", "中国石化石油勘探开发研究院"),
    "Sinopec Petroleum Exploration and Production Research Institute": ("中国石化石油勘探开发研究院", "中国石化石油勘探开发研究院"),
    "SINOPEC Petroleum Exploration and Production Research Institute": ("中国石化石油勘探开发研究院", "中国石化石油勘探开发研究院"),
    "SINOPEC Petroleum Exploration & Production Research Institute": ("中国石化石油勘探开发研究院", "中国石化石油勘探开发研究院"),
    "Sinopec Petroleum Exploration & Production Research Institute": ("中国石化石油勘探开发研究院", "中国石化石油勘探开发研究院"),
    "Sinopec Northwest Oilfield Company": ("中国石化西北油田分公司", "中国石化西北油田分公司"),
    "Sinopec Shengli Oilfield Company": ("中国石化胜利油田分公司", "中国石化胜利油田分公司"),
    "SINOPEC Shengli Oilfield Company": ("中国石化胜利油田分公司", "中国石化胜利油田分公司"),
    "Sinopec Exploration Company": ("中国石化勘探分公司", "中国石化勘探分公司"),
    "SINOPEC Shanghai Research Institute of Petrochemical Technology": ("中国石化上海石油化工研究院", "中国石化上海石油化工研究院"),
    "SINOPEC International Petroleum Exploration and Production Corporation": ("中国石化国际石油勘探开发有限公司", "中国石化国际石油勘探开发有限公司"),
    "Sinopec International Petroleum Exploration and Production Corporation": ("中国石化国际石油勘探开发有限公司", "中国石化国际石油勘探开发有限公司"),
    "SINOPEC Engineering Incorporation": ("中国石化工程建设有限公司", "中国石化工程建设有限公司"),
    "China Petroleum & Chemical Corporation": ("中国石油化工股份有限公司", "中国石油化工股份有限公司"),
    "PetroChina Changqing Oilfield Company": ("中国石油长庆油田公司", "中国石油长庆油田公司"),
    "PetroChina Xinjiang Oilfield Company": ("中国石油新疆油田公司", "中国石油新疆油田公司"),
    "PetroChina Liaohe Oilfield Company": ("中国石油辽河油田公司", "中国石油辽河油田公司"),
    "PetroChina Hangzhou Research Institute of Geology": ("中国石油杭州地质研究院", "中国石油杭州地质研究院"),
    "PetroChina Southwest Oil & Gasfield Company": ("中国石油西南油气田公司", "中国石油西南油气田公司"),
    "PetroChina Southwest Oil & Gas Field Company": ("中国石油西南油气田公司", "中国石油西南油气田公司"),
    "PetroChina Southwest Oil and Gas Field Company": ("中国石油西南油气田公司", "中国石油西南油气田公司"),
    "PetroChina Jidong Oilfield Company": ("中国石油冀东油田公司", "中国石油冀东油田公司"),
    "PetroChina Petrochemical Research Institute": ("中国石油石油化工研究院", "中国石油石油化工研究院"),
    "PetroChina Jilin Oilfield Company": ("中国石油吉林油田公司", "中国石油吉林油田公司"),
    "PetroChina Zhejiang Oilfield Company": ("中国石油浙江油田公司", "中国石油浙江油田公司"),
    "PetroChina Tuha Oilfield Company": ("中国石油吐哈油田公司", "中国石油吐哈油田公司"),
    "CNPC Greatwall Drilling Company": ("中国石油集团长城钻探工程有限公司", "中国石油集团长城钻探工程有限公司"),
    "CNPC Drilling Research Institute": ("中国石油集团钻井工程技术研究院", "中国石油集团钻井工程技术研究院"),
    "Exploration and Development Research Institute of Daqing Oilfield Company Ltd": ("大庆油田勘探开发研究院", "大庆油田勘探开发研究院"),
    "Exploration and Development Research Institute of Daqing Oilfield Co Ltd": ("大庆油田勘探开发研究院", "大庆油田勘探开发研究院"),
    "Exploration and Development Research Institute of Daqing Oilfield Co. Ltd": ("大庆油田勘探开发研究院", "大庆油田勘探开发研究院"),
    "Petroleum Exploration and Production Research Institute": ("石油勘探开发研究院", "石油勘探开发研究院"),
    "Petroleum Exploration & Production Research Institute": ("石油勘探开发研究院", "石油勘探开发研究院"),
    "Petroleum Exploration and Development Research Institute": ("石油勘探开发研究院", "石油勘探开发研究院"),
    "Research Institute of Petroleum Exploration and Production": ("石油勘探开发研究院", "石油勘探开发研究院"),
    "Research Institute of Petroleum Exploration & Production": ("石油勘探开发研究院", "石油勘探开发研究院"),
    "Research Institute of Natural Gas Technology": ("天然气技术研究院", "天然气技术研究院"),
    "Drilling Technology Research Institute": ("钻井技术研究院", "钻井技术研究院"),
    "Research Institute of Petroleum Engineering": ("石油工程研究院", "石油工程研究院"),
    "Research Institute of Engineering Technology": ("工程技术研究院", "工程技术研究院"),
    "Research Institute of Experiment and Detection": ("试验检测研究院", "试验检测研究院"),
    "Research Institute of Oil Production Technology": ("采油技术研究院", "采油技术研究院"),
    "Oil Production Technology Research Institute": ("采油技术研究院", "采油技术研究院"),
    "Research Institute of Oil and Gas Technology": ("油气技术研究院", "油气技术研究院"),
    "Research Institute of Petroleum Engineering Technology": ("石油工程技术研究院", "石油工程技术研究院"),
    "Geological Scientific Research Institute": ("地质科学研究院", "地质科学研究院"),
    "Geological Research Institute": ("地质研究院", "地质研究院"),
    "Hangzhou Research Institute of Geology": ("杭州地质研究院", "杭州地质研究院"),
    "Hangzhou Institute of Geology": ("杭州地质研究院", "杭州地质研究院"),
    "Hangzhou Institute of Petroleum Geology": ("杭州石油地质研究院", "杭州石油地质研究院"),
    "Wuxi Institute of Petroleum Geology": ("无锡石油地质研究所", "无锡石油地质研究所"),
    "Research Institute of Geological Exploration and Development": ("地质勘探开发研究院", "地质勘探开发研究院"),
    "Research Institute of Lanzhou Petrochemical Company": ("兰州石化公司研究院", "兰州石化公司研究院"),
    "Research Institute of Shaanxi Yanchang Petroleum (Group) Co": ("陕西延长石油（集团）研究院", "陕西延长石油（集团）研究院"),
    "Institute of Geophysics and Geomatics": ("地球物理与测绘研究所", "地球物理与测绘研究所"),
    "Institute of Porous Flow and Fluid Mechanics": ("渗流流体力学研究所", "渗流流体力学研究所"),
    "Institute of Exploration and Development": ("勘探开发研究院", "勘探开发研究院"),
    "Institute of Petroleum Exploration and Development": ("石油勘探开发研究院", "石油勘探开发研究院"),
    "Institute of Oil & Gas": ("油气研究所", "油气研究所"),
    "Institute of Oil and Gas": ("油气研究所", "油气研究所"),
    "Institute of Petrochemical Technology": ("石油化工技术研究所", "石油化工技术研究所"),
    "Key Laboratory of Petroleum Resources": ("石油资源重点实验室", "石油资源重点实验室"),
    "Key Laboratory of Exploration Technologies for Oil and Gas Resources": ("油气资源勘探技术重点实验室", "油气资源勘探技术重点实验室"),
    "Key Laboratory of Tectonics and Petroleum Resources": ("构造与油气资源重点实验室", "构造与油气资源重点实验室"),
    "Research Institute of Unconventional Petroleum and Renewable Energy": ("非常规油气与新能源研究院", "非常规油气与新能源研究院"),
    "Unconventional Natural Gas Institute": ("非常规天然气研究院", "非常规天然气研究院"),
    "Beijing University of Chemical Technology": ("北京化工大学", "北京化工大学"),
    "Qingdao University of Science and Technology": ("青岛科技大学", "青岛科技大学"),
    "Qingdao University of Science & Technology": ("青岛科技大学", "青岛科技大学"),
    "Chongqing University of Science and Technology": ("重庆科技大学", "重庆科技大学"),
    "Chongqing University of Science & Technology": ("重庆科技大学", "重庆科技大学"),
    "Nanjing University of Science and Technology": ("南京理工大学", "南京理工大学"),
    "Shenyang University of Chemical Technology": ("沈阳化工大学", "沈阳化工大学"),
    "Central South University": ("中南大学", "中南大学"),
    "Henan Polytechnic University": ("河南理工大学", "河南理工大学"),
    "University of Science and Technology Beijing": ("北京科技大学", "北京科技大学"),
    "University of Science & Technology Beijing": ("北京科技大学", "北京科技大学"),
    "Wuhan University of Science and Technology": ("武汉科技大学", "武汉科技大学"),
    "Anhui University of Science and Technology": ("安徽理工大学", "安徽理工大学"),
    "Hunan University of Science and Technology": ("湖南科技大学", "湖南科技大学"),
    "Kunming University of Science and Technology": ("昆明理工大学", "昆明理工大学"),
    "Xi'an University of Science and Technology": ("西安科技大学", "西安科技大学"),
    "Xi'an Petroleum University": ("西安石油大学", "西安石油大学"),
    "Xi'an Shiyou University": ("西安石油大学", "西安石油大学"),
    "University of Science and Technology of China": ("中国科学技术大学", "中国科学技术大学"),
    "University of Electronic Science and Technology of China": ("电子科技大学", "电子科技大学"),
    "Tianjin University of Science and Technology": ("天津科技大学", "天津科技大学"),
    "Tianjin University of Science & Technology": ("天津科技大学", "天津科技大学"),
    "Suzhou University of Science and Technology": ("苏州科技大学", "苏州科技大学"),
    "Guangdong University of Petrochemical Technology": ("广东石油化工学院", "广东石油化工学院"),
    "Liaoning University of Petroleum & Chemical Technology": ("辽宁石油化工大学", "辽宁石油化工大学"),
    "Liaoning Technical University": ("辽宁工程技术大学", "辽宁工程技术大学"),
    "Changchun Institute of Applied Chemistry": ("中国科学院长春应用化学研究所", "中国科学院长春应用化学研究所"),
    "Institute of Process Engineering": ("中国科学院过程工程研究所", "中国科学院过程工程研究所"),
    "Technical Institute of Physics and Chemistry": ("中国科学院理化技术研究所", "中国科学院理化技术研究所"),
    "Chinese Academy of Sciences": ("中国科学院", "中国科学院"),
}

CHINESE_PARENT_RULES: list[tuple[re.Pattern[str], tuple[str, str]]] = [
    (re.compile(r"中国石油大学[（(]华东[)）]"), ("中国石油大学（华东）", "中国石油大学（华东）")),
    (re.compile(r"中国石油大学[（(]北京[)）]"), ("中国石油大学（北京）", "中国石油大学（北京）")),
    (re.compile(r"中国地质大学[（(]北京[)）]"), ("中国地质大学（北京）", "中国地质大学（北京）")),
    (re.compile(r"中国地质大学[（(]武汉[)）]"), ("中国地质大学（武汉）", "中国地质大学（武汉）")),
    (re.compile(r"西南石油大学"), ("西南石油大学", "西南石油大学")),
    (re.compile(r"清华大学"), ("清华大学", "清华大学")),
    (re.compile(r"中国科学院"), ("中国科学院", "中国科学院")),
]

NOISE_NUMBER = re.compile(r"^\d+$")
NO_PATTERN = re.compile(r"^No\.?\s*(\d+)\s+(Oil|Gas)\s+Production\s+Plant(?:\s+of\s+(.+))?$", re.I)


def strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text.translate(SPECIAL_CHAR_MAP))
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def alias_key(text: str) -> str:
    value = strip_accents(text).lower().replace("&", " and ")
    for ch in "()[]{}\\/,-_.;:'\"?":
        value = value.replace(ch, " ")
    tokens: list[str] = []
    for token in value.split():
        token = ABBR_MAP.get(token, token)
        if token in STOPWORDS:
            continue
        tokens.append(token)
    key = " ".join(tokens)
    return SPECIAL_EQUIV.get(key, key)


def significant_tokens(key: str) -> list[str]:
    tokens = [token for token in key.split() if token not in GENERIC_TOKENS]
    return tokens or key.split()


def load_master_data() -> tuple[dict[str, tuple[str, str]], dict[str, tuple[str, str]], dict[str, Counter], dict[str, set[str]], Counter]:
    exact_map: dict[str, tuple[str, str]] = {}
    alias_counters: dict[str, Counter] = defaultdict(Counter)
    alias_index: dict[str, set[str]] = defaultdict(set)
    token_freq: Counter = Counter()

    with MASTER_PATH.open("r", encoding=ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("institution_name") or "").strip()
            if not name:
                continue
            trans = (row.get("institution_trans") or "").strip()
            norm = (row.get("institution_norm") or "").strip()
            if not trans or not norm:
                continue
            pair = (trans, norm)
            count = int(float(row.get("count") or 0) or 1)
            exact_map[name] = pair
            key = alias_key(name)
            alias_counters[key][pair] += max(count, 1)

    dominant_map = {key: counter.most_common(1)[0][0] for key, counter in alias_counters.items()}
    for key in dominant_map:
        for token in significant_tokens(key):
            alias_index[token].add(key)
            token_freq[token] += 1
    return exact_map, dominant_map, alias_counters, alias_index, token_freq


def pick_candidate(counter: Counter) -> tuple[str, str] | None:
    if not counter:
        return None
    best = counter.most_common(2)
    pair, top_weight = best[0]
    if len(best) == 1:
        return pair
    second_weight = best[1][1]
    if top_weight >= second_weight * 2 or top_weight - second_weight >= 20:
        return pair
    return pair if top_weight >= 100 else None


def build_dominant_map(alias_counters: dict[str, Counter]) -> dict[str, tuple[str, str]]:
    result: dict[str, tuple[str, str]] = {}
    for key, counter in alias_counters.items():
        pair = pick_candidate(counter)
        if pair:
            result[key] = pair
    return result


def detect_chinese_parent(name: str) -> tuple[str, str] | None:
    for pattern, pair in CHINESE_PARENT_RULES:
        if pattern.search(name):
            return pair
    return None


def num_to_cn(num: int) -> str:
    digits = {0: "零", 1: "一", 2: "二", 3: "三", 4: "四", 5: "五", 6: "六", 7: "七", 8: "八", 9: "九", 10: "十"}
    if num in digits:
        return digits[num]
    if num < 20:
        return "十" + digits[num - 10]
    tens, ones = divmod(num, 10)
    text = digits[tens] + "十"
    if ones:
        text += digits[ones]
    return text


def manual_generated(name: str) -> tuple[str, str] | None:
    if name in MANUAL_EXACT:
        return MANUAL_EXACT[name]

    chinese_parent = detect_chinese_parent(name)
    if chinese_parent:
        return chinese_parent

    match = NO_PATTERN.match(name)
    if match:
        number = int(match.group(1))
        resource = "采油厂" if match.group(2).lower() == "oil" else "采气厂"
        label = f"第{num_to_cn(number)}{resource}"
        return label, label

    return None


def match_by_containment(key: str, dominant_map: dict[str, tuple[str, str]], alias_index: dict[str, set[str]], token_freq: Counter) -> tuple[str, str] | None:
    tokens = significant_tokens(key)
    candidates: set[str] = set()
    for token in sorted(tokens, key=lambda item: (token_freq.get(item, math.inf), item))[:3]:
        candidates.update(alias_index.get(token, set()))
    ordered = sorted(candidates, key=lambda item: (-len(item.split()), -len(item)))
    for candidate in ordered:
        if candidate not in dominant_map:
            continue
        if len(candidate.split()) >= 3 and candidate in key:
            return dominant_map[candidate]
    return None


def match_by_fuzzy(key: str, dominant_map: dict[str, tuple[str, str]], alias_index: dict[str, set[str]], token_freq: Counter) -> tuple[str, str] | None:
    tokens = significant_tokens(key)
    candidates: set[str] = set()
    for token in sorted(tokens, key=lambda item: (token_freq.get(item, math.inf), item))[:4]:
        candidates.update(alias_index.get(token, set()))
    if not candidates:
        return None

    scored: list[tuple[float, str]] = []
    for candidate in candidates:
        if candidate not in dominant_map:
            continue
        score = difflib.SequenceMatcher(None, key, candidate).ratio()
        if score >= 0.88:
            scored.append((score, candidate))
    if not scored:
        return None

    scored.sort(reverse=True)
    best_score, best_key = scored[0]
    second_score = scored[1][0] if len(scored) > 1 else 0.0
    if best_score >= 0.97 or (best_score >= 0.93 and best_score - second_score >= 0.015):
        return dominant_map[best_key]
    return None


def fallback_pair(name: str) -> tuple[str, str]:
    if NOISE_NUMBER.fullmatch(name):
        return name, name
    return name, name


def repair_file() -> None:
    exact_map, dominant_raw_map, alias_counters, alias_index, token_freq = load_master_data()
    dominant_map = build_dominant_map(alias_counters)

    rows: list[dict[str, str]] = []
    method_counter: Counter = Counter()
    remaining_after = 0

    with CURRENT_PATH.open("r", encoding=ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        for row in reader:
            trans = (row.get("institution_trans") or "").strip()
            norm = (row.get("institution_norm") or "").strip()
            if trans and norm:
                rows.append(row)
                method_counter["kept_existing"] += 1
                continue

            name = (row.get("institution_name") or "").strip()
            pair = manual_generated(name)
            method = "manual"
            if not pair:
                pair = exact_map.get(name)
                method = "exact_master"
            if not pair:
                pair = dominant_map.get(alias_key(name))
                method = "dominant_alias"
            if not pair:
                pair = match_by_containment(alias_key(name), dominant_map, alias_index, token_freq)
                method = "containment"
            if not pair:
                pair = match_by_fuzzy(alias_key(name), dominant_map, alias_index, token_freq)
                method = "fuzzy"
            if not pair:
                pair = fallback_pair(name)
                method = "raw_fallback"

            row["institution_trans"], row["institution_norm"] = pair
            if not row["institution_trans"] or not row["institution_norm"]:
                remaining_after += 1
            method_counter[method] += 1
            rows.append(row)

    with CURRENT_PATH.open("w", encoding=ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"output={CURRENT_PATH}")
    print(f"rows={len(rows)}")
    for method, count in method_counter.most_common():
        print(f"{method}={count}")
    print(f"remaining_empty={remaining_after}")


if __name__ == "__main__":
    repair_file()
