"""
三库合并去重脚本
输入：知网(cnki_clean_dedup.csv) + WOS(DBdata数据_2025_11_14.csv) + CSCD(CSCD数据_2025_11_14.csv)
输出：D:\毕业论文\merged_clean.csv
去重主键：doi优先，doi为空时用 title_en + year
"""

import pandas as pd
import numpy as np
import re

# ── 路径配置 ──────────────────────────────────────────────
CNKI_PATH = r"D:\毕业论文\cnki_clean_dedup.csv"
WOS_PATH  = r"D:\毕业论文\DBdata数据_2025_11_14.csv"
CSCD_PATH = r"D:\毕业论文\CSCD数据_2025_11_14.csv"
OUT_PATH  = r"D:\毕业论文\merged_clean.csv"

# 统一输出字段
OUT_COLS = [
    "title_en", "title_cn", "title_cn_en",
    "author", "author_cn",
    "institution", "institution_extracted", "institution_norm",
    "journal_en", "journal_cn",
    "doi", "year", "publish_date",
    "abstract_en", "abstract_cn",
    "keywords_en", "keywords_cn",
    "cited_count",
    "source_db",
]

def clean_doi(doi):
    if pd.isna(doi):
        return ""
    return str(doi).strip().lower()

def clean_title(title):
    if pd.isna(title):
        return ""
    return re.sub(r'\s+', ' ', str(title).strip().lower())

def clean_year(year):
    if pd.isna(year):
        return ""
    return str(year).strip()[:4]

# 月份缩写映射
_MONTH_MAP = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}

def normalize_publish_date(val):
    """统一转换为 YYYY-MM 格式，无月份补 -00"""
    if pd.isna(val):
        return ""
    s = str(val).strip()
    if not s or s == "nan":
        return ""
    # 已是 YYYY-MM 或 YYYY-MM-DD
    m = re.match(r'^(\d{4})-(\d{2})', s)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    # 纯年份 YYYY 或 YYYY.0
    m = re.match(r'^(\d{4})(?:\.0+)?$', s)
    if m:
        return f"{m.group(1)}-00"
    # WOS格式: "DEC 12 2025.0" / "APR 22 2025" / "MAY 2007"
    parts = s.replace(".", "").split()
    year = ""
    month = "00"
    for p in parts:
        if re.match(r'^\d{4}$', p):
            year = p
        elif p.lower() in _MONTH_MAP:
            month = _MONTH_MAP[p.lower()]
    if year:
        return f"{year}-{month}"
    return s

# ── 读取知网 ──────────────────────────────────────────────
print("读取知网数据...")
cnki = pd.read_csv(CNKI_PATH, encoding='utf-8-sig', low_memory=False)
cnki_out = pd.DataFrame()
cnki_out["title_en"]             = cnki.get("title_en", "")
cnki_out["title_cn"]             = cnki.get("title", "")
cnki_out["title_cn_en"]          = cnki.get("title_cn_en", "")
cnki_out["author"]               = ""
cnki_out["author_cn"]            = cnki.get("author", "")
cnki_out["institution"]          = cnki.get("institution", "")
cnki_out["institution_extracted"]= cnki.get("institution_extracted", "")
cnki_out["institution_norm"]     = cnki.get("institution_norm", "")
cnki_out["journal_en"]           = cnki.get("journal_en", "")
cnki_out["journal_cn"]           = cnki.get("journal_cn", "")
cnki_out["doi"]                  = cnki.get("doi", "")
cnki_out["year"]                 = cnki.get("year", "")
cnki_out["publish_date"]         = cnki.get("publish_date", "").apply(normalize_publish_date)
cnki_out["abstract_en"]          = cnki.get("abstract_en", "")
cnki_out["abstract_cn"]          = cnki.get("abstract_cn", "")
cnki_out["keywords_en"]          = cnki.get("keywords_en", "")
cnki_out["keywords_cn"]          = cnki.get("keywords_cn", "")
cnki_out["cited_count"]          = cnki.get("cited_count", "")
cnki_out["source_db"]            = "CNKI"
print(f"  知网原始行数: {len(cnki_out)}")

# ── 读取WOS ───────────────────────────────────────────────
print("读取WOS数据...")
wos = pd.read_csv(WOS_PATH, encoding='utf-8-sig', low_memory=False)

# pd列 + py列拼接 publish_date
def wos_publish_date(row):
    pd_val = str(row.get("pd", "")).strip()
    py_val = str(row.get("py", "")).strip()
    if pd_val and pd_val != "nan":
        raw = f"{pd_val} {py_val}".strip()
    else:
        raw = py_val
    return normalize_publish_date(raw)

wos_out = pd.DataFrame()
wos_out["title_en"]             = wos.get("ti", "")
wos_out["title_cn"]             = ""
wos_out["title_cn_en"]          = wos.get("ti", "")
wos_out["author"]               = wos.apply(lambda r: r.get("af", "") if pd.notna(r.get("af", "")) else r.get("au", ""), axis=1)
wos_out["author_cn"]            = ""
wos_out["institution"]          = wos.get("c1", "")
wos_out["institution_extracted"]= ""
wos_out["institution_norm"]     = ""
wos_out["journal_en"]           = wos.get("so", "")
wos_out["journal_cn"]           = ""
wos_out["doi"]                  = wos.get("di", "")
wos_out["year"]                 = wos.get("py", "").astype(str)
wos_out["publish_date"]         = wos.apply(wos_publish_date, axis=1)
wos_out["abstract_en"]          = wos.get("ab", "")
wos_out["abstract_cn"]          = ""
wos_out["keywords_en"]          = wos.get("de", "")
wos_out["keywords_cn"]          = ""
wos_out["cited_count"]          = wos.get("tc", "")
wos_out["source_db"]            = "WOS"
print(f"  WOS原始行数: {len(wos_out)}")

# ── 读取CSCD ──────────────────────────────────────────────
print("读取CSCD数据...")
cscd = pd.read_csv(CSCD_PATH, encoding='utf-8-sig', low_memory=False)

def cscd_title_cn_en(row):
    ti = str(row.get("ti", "")).strip()
    z1 = str(row.get("z1", "")).strip()
    parts = [x for x in [ti, z1] if x and x != "nan"]
    return " | ".join(parts) if parts else ""

cscd_out = pd.DataFrame()
cscd_out["title_en"]             = cscd.get("ti", "")
cscd_out["title_cn"]             = cscd.get("z1", "")
cscd_out["title_cn_en"]          = cscd.apply(cscd_title_cn_en, axis=1)
cscd_out["author"]               = cscd.get("au", "")
cscd_out["author_cn"]            = cscd.get("z2", "")
cscd_out["institution"]          = cscd.apply(lambda r: r.get("c1", "") if pd.notna(r.get("c1", "")) else r.get("z6", ""), axis=1)
cscd_out["institution_extracted"]= ""
cscd_out["institution_norm"]     = ""
cscd_out["journal_en"]           = cscd.get("so", "")
cscd_out["journal_cn"]           = cscd.get("z3", "")
cscd_out["doi"]                  = cscd.get("di", "")
cscd_out["year"]                 = cscd.get("py", "").astype(str)
cscd_out["publish_date"]         = cscd.get("py", "").astype(str).apply(normalize_publish_date)
cscd_out["abstract_en"]          = cscd.get("ab", "")
cscd_out["abstract_cn"]          = cscd.get("z4", "")
cscd_out["keywords_en"]          = cscd.get("de", "")
cscd_out["keywords_cn"]          = cscd.get("z5", "")
cscd_out["cited_count"]          = cscd.get("z9", "")
cscd_out["source_db"]            = "CSCD"
print(f"  CSCD原始行数: {len(cscd_out)}")

# ── 合并 ──────────────────────────────────────────────────
print("合并三库...")
merged = pd.concat([cnki_out, wos_out, cscd_out], ignore_index=True)
print(f"  合并后总行数: {len(merged)}")

# ── 去重 ──────────────────────────────────────────────────
print("去重处理...")

# 标准化doi和title_en用于比较
merged["_doi_key"]   = merged["doi"].apply(clean_doi)
merged["_title_key"] = merged["title_en"].apply(clean_title)
merged["_year_key"]  = merged["year"].apply(clean_year)

# 优先级：CNKI > WOS > CSCD（keep first，先排序）
source_order = {"CNKI": 0, "WOS": 1, "CSCD": 2}
merged["_src_order"] = merged["source_db"].map(source_order)
merged = merged.sort_values("_src_order").reset_index(drop=True)

# 有doi的记录按doi去重
has_doi = merged["_doi_key"] != ""
no_doi  = merged["_doi_key"] == ""

dedup_doi   = merged[has_doi].drop_duplicates(subset=["_doi_key"], keep="first")
dedup_nodoi = merged[no_doi].copy()

# 无doi的按 title_en + year 去重（title_en非空才参与）
has_title = dedup_nodoi["_title_key"] != ""
no_title  = dedup_nodoi["_title_key"] == ""

dedup_title   = dedup_nodoi[has_title].drop_duplicates(subset=["_title_key", "_year_key"], keep="first")
dedup_notitle = dedup_nodoi[no_title]  # 无doi无title，保留全部

result = pd.concat([dedup_doi, dedup_title, dedup_notitle], ignore_index=True)

# 删除辅助列
result = result.drop(columns=["_doi_key", "_title_key", "_year_key", "_src_order"])

print(f"  去重后行数: {len(result)}")

# ── 统计来源分布 ──────────────────────────────────────────
print("\n来源分布（去重后）：")
print(result["source_db"].value_counts().to_string())

# ── 输出 ──────────────────────────────────────────────────
result.to_csv(OUT_PATH, index=False, encoding='utf-8-sig')
print(f"\n已输出: {OUT_PATH}")
