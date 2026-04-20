"""
机构抽取脚本
输入：D:\毕业论文\merged_clean_dedup.csv
输出：D:\毕业论文\merged_clean_dedup_inst.csv（新增/填充 institution_extracted 字段）

WOS格式：[作者名] 机构, 城市, 国家; [作者名] 机构, 城市, 国家
CSCD格式：作者名, 机构, 城市, 省, 邮编, 国家.; 作者名, 机构, ...
CNKI：institution_extracted 已有值，直接保留
"""

import re
import pandas as pd

IN_PATH  = r"D:\毕业论文\merged_clean_dedup.csv"
OUT_PATH = r"D:\毕业论文\merged_clean_dedup_inst.csv"


def extract_wos(raw: str) -> str:
    """从WOS institution字段抽取机构名列表，返回分号分隔字符串"""
    if not raw or pd.isna(raw):
        return ""
    s = str(raw)
    # 按 "; [" 分割每个作者-机构块（避免 [...] 内部的 ; 干扰）
    blocks = re.split(r';\s*(?=\[)', s)
    institutions = []
    for block in blocks:
        # 去掉开头的 [作者名...] 部分（含内部分号）
        block = re.sub(r'^\[.*?\]\s*', '', block).strip()
        if not block:
            continue
        # 机构名是第一个逗号前的部分
        inst = block.split(",")[0].strip()
        if inst and inst not in institutions:
            institutions.append(inst)
    return "; ".join(institutions)


def extract_cscd(raw: str) -> str:
    """从CSCD institution字段抽取机构名列表，返回分号分隔字符串"""
    if not raw or pd.isna(raw):
        return ""
    # 按 ". " 或 ".; " 分割每个作者-机构块（机构名内部可能含;）
    blocks = [b.strip() for b in re.split(r'\.\s*;\s*|\.\s*$', str(raw)) if b.strip()]
    institutions = []
    for block in blocks:
        # 第一个逗号前是作者名，之后到下一个逗号前是机构名
        parts = [p.strip() for p in block.split(",")]
        if len(parts) < 2:
            continue
        # parts[0] 是作者名，parts[1] 是机构名（可能含;拼接的子机构）
        inst = parts[1].strip()
        if inst and inst not in institutions:
            institutions.append(inst)
    return "; ".join(institutions)


def main():
    print("读取数据...")
    df = pd.read_csv(IN_PATH, encoding='utf-8-sig', low_memory=False)
    print(f"  总行数: {len(df)}")

    # CNKI 已有 institution_extracted，直接保留
    # WOS / CSCD 重新抽取
    mask_wos  = df['source_db'] == 'WOS'
    mask_cscd = df['source_db'] == 'CSCD'

    print("抽取WOS机构...")
    df.loc[mask_wos, 'institution_extracted'] = df.loc[mask_wos, 'institution'].apply(extract_wos)

    print("抽取CSCD机构...")
    df.loc[mask_cscd, 'institution_extracted'] = df.loc[mask_cscd, 'institution'].apply(extract_cscd)

    # 统计
    for db in ['CNKI', 'WOS', 'CSCD']:
        sub = df[df['source_db'] == db]
        has = sub['institution_extracted'].notna() & (sub['institution_extracted'].str.strip() != '')
        print(f"  {db}: {len(sub)}条，institution_extracted非空{has.sum()}条")

    print(f"输出到 {OUT_PATH} ...")
    df.to_csv(OUT_PATH, index=False, encoding='utf-8-sig')
    print("完成")


if __name__ == "__main__":
    main()
