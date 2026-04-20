from __future__ import annotations

import os
import sys
from pathlib import Path


BASE_DIR = Path("D:/毕业论文")
VENDOR_DIR = BASE_DIR / ".vendor"
MPLCONFIGDIR = BASE_DIR / ".mplconfig"
OUTPUT_DIR = BASE_DIR / "figure_materials_2011_2025"

os.environ.setdefault("MPLCONFIGDIR", str(MPLCONFIGDIR.resolve()))
MPLCONFIGDIR.mkdir(parents=True, exist_ok=True)
sys.path.insert(0, str(VENDOR_DIR.resolve()))

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib import font_manager


def configure_matplotlib_fonts() -> None:
    font_files = [
        Path("C:/Windows/Fonts/msyh.ttc"),
        Path("C:/Windows/Fonts/msyhbd.ttc"),
        Path("C:/Windows/Fonts/msyhl.ttc"),
        Path("C:/Windows/Fonts/simhei.ttf"),
        Path("C:/Windows/Fonts/simsun.ttc"),
        Path("C:/Windows/Fonts/Deng.ttf"),
    ]
    for font_file in font_files:
        if font_file.exists():
            try:
                font_manager.fontManager.addfont(str(font_file))
            except RuntimeError:
                continue

    available_fonts = {font.name for font in font_manager.fontManager.ttflist}
    preferred_fonts = [
        name
        for name in ["Microsoft YaHei", "SimHei", "SimSun", "DengXian"]
        if name in available_fonts
    ]
    if not preferred_fonts:
        preferred_fonts = ["sans-serif"]

    sns.set_theme(
        style="whitegrid",
        rc={
            "font.family": "sans-serif",
            "font.sans-serif": preferred_fonts,
            "axes.unicode_minus": False,
            "svg.fonttype": "none",
        },
    )
    plt.rcParams["font.family"] = "sans-serif"
    plt.rcParams["font.sans-serif"] = preferred_fonts
    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams["svg.fonttype"] = "none"


configure_matplotlib_fonts()

INPUT_ENCODING = "gb18030"

TOP20_FILE = BASE_DIR / "institution_topsis_top20_2011_2025.csv"
SCORE_FILE = BASE_DIR / "institution_topsis_score_top100_2011_2025.csv"
WEIGHT_FILE = BASE_DIR / "institution_weight_scheme_top100_2011_2025.csv"

TYPICAL_INSTITUTIONS = [
    "中国科学院",
    "中国石油大学（北京）",
    "清华大学",
    "新加坡国立大学",
    "西南石油大学",
]

RADAR_FIELDS = [
    "去重论文总数",
    "近五年发文占比",
    "H指数",
    "高被引论文占比",
    "国际合作论文占比",
    "合作国家/地区数",
]

HEATMAP_FIELDS = [
    "去重论文总数",
    "近五年发文占比",
    "H指数",
    "高被引论文占比",
    "合作论文占比",
    "国际合作论文占比",
    "合作国家/地区数",
]

FIGURE_CAPTIONS = [
    {
        "figure_no": "图4-1",
        "file_stem": "figure_4_1_top20_topsis_bar",
        "title": "核心机构TOPSIS综合得分前20排名",
        "description": "展示2011-2025年核心机构综合影响力前20名及其TOPSIS综合得分。",
    },
    {
        "figure_no": "图4-2",
        "file_stem": "figure_4_2_top10_dimension_compare",
        "title": "综合前10机构三维子得分对比",
        "description": "比较综合排名前10机构在科研产出、学术影响、合作与国际化三个维度上的表现差异。",
    },
    {
        "figure_no": "图4-3",
        "file_stem": "figure_4_3_typical_institution_radar",
        "title": "典型机构多指标雷达图",
        "description": "基于归一化后的关键指标展示典型机构的优势结构差异。",
    },
    {
        "figure_no": "图4-4",
        "file_stem": "figure_4_4_top20_indicator_heatmap",
        "title": "综合前20机构七项入模指标热力图",
        "description": "展示综合前20机构在7项入模指标上的相对强弱分布。",
    },
    {
        "figure_no": "图4-5",
        "file_stem": "figure_4_5_weight_scheme_bar",
        "title": "组合权重分布图",
        "description": "展示7项入模指标的组合权重结构。",
    },
]


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def save_figure(fig: plt.Figure, stem: str) -> None:
    png_path = OUTPUT_DIR / f"{stem}.png"
    svg_path = OUTPUT_DIR / f"{stem}.svg"
    fig.savefig(png_path, dpi=300, bbox_inches="tight")
    fig.savefig(svg_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def normalize_series(series: pd.Series) -> pd.Series:
    min_value = series.min()
    max_value = series.max()
    if pd.isna(min_value) or pd.isna(max_value) or float(max_value) == float(min_value):
        return pd.Series([1.0] * len(series), index=series.index)
    return (series - min_value) / (max_value - min_value)


def build_top20_bar(top20_df: pd.DataFrame) -> pd.DataFrame:
    df = top20_df.copy().sort_values("TOPSIS综合得分", ascending=True)
    fig, ax = plt.subplots(figsize=(10, 8))
    colors = sns.color_palette("Blues", n_colors=len(df))
    ax.barh(df["标准化机构名称"], df["TOPSIS综合得分"], color=colors)
    ax.set_title("2011-2025年核心机构TOPSIS综合得分前20排名", fontsize=15, pad=12)
    ax.set_xlabel("TOPSIS综合得分")
    ax.set_ylabel("标准化机构名称")
    ax.grid(axis="x", linestyle="--", alpha=0.35)
    for i, value in enumerate(df["TOPSIS综合得分"]):
        ax.text(value + 0.005, i, f"{value:.3f}", va="center", fontsize=9)
    save_figure(fig, "figure_4_1_top20_topsis_bar")
    return df[["TOPSIS综合排名", "标准化机构名称", "TOPSIS综合得分", "主属国家/地区"]]


def build_top10_dimension_compare(top20_df: pd.DataFrame) -> pd.DataFrame:
    df = top20_df.nsmallest(10, "TOPSIS综合排名").copy()
    df = df.sort_values("TOPSIS综合排名", ascending=False)
    fig, ax = plt.subplots(figsize=(11, 8))
    y = np.arange(len(df))
    height = 0.22
    ax.barh(y - height, df["科研产出子得分"], height=height, label="科研产出", color="#3C6E71")
    ax.barh(y, df["学术影响子得分"], height=height, label="学术影响", color="#D9AE61")
    ax.barh(y + height, df["合作与国际化子得分"], height=height, label="合作与国际化", color="#C45B5B")
    ax.set_yticks(y)
    ax.set_yticklabels(df["标准化机构名称"])
    ax.set_xlabel("子得分")
    ax.set_title("综合前10机构三维子得分对比", fontsize=15, pad=12)
    ax.legend(loc="lower right")
    ax.grid(axis="x", linestyle="--", alpha=0.35)
    save_figure(fig, "figure_4_2_top10_dimension_compare")
    return df[
        [
            "TOPSIS综合排名",
            "标准化机构名称",
            "科研产出子得分",
            "学术影响子得分",
            "合作与国际化子得分",
            "主导优势维度",
        ]
    ]


def build_typical_radar(score_df: pd.DataFrame) -> pd.DataFrame:
    df = score_df[score_df["标准化机构名称"].isin(TYPICAL_INSTITUTIONS)].copy()
    norm_df = df[["标准化机构名称"] + RADAR_FIELDS].copy()
    for field in RADAR_FIELDS:
        norm_df[field] = normalize_series(score_df[field]).loc[df.index].values

    labels = RADAR_FIELDS
    angles = np.linspace(0, 2 * np.pi, len(labels), endpoint=False).tolist()
    angles += angles[:1]

    fig = plt.figure(figsize=(9, 9))
    ax = plt.subplot(111, polar=True)
    palette = sns.color_palette("tab10", n_colors=len(norm_df))

    for color, (_, row) in zip(palette, norm_df.iterrows()):
        values = [float(row[label]) for label in labels]
        values += values[:1]
        ax.plot(angles, values, linewidth=2, label=row["标准化机构名称"], color=color)
        ax.fill(angles, values, alpha=0.08, color=color)

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(labels, fontsize=10)
    ax.set_yticklabels([])
    ax.set_title("典型机构多指标雷达图（归一化）", fontsize=15, pad=20)
    ax.legend(loc="upper right", bbox_to_anchor=(1.28, 1.10), frameon=False)
    save_figure(fig, "figure_4_3_typical_institution_radar")
    return norm_df


def build_indicator_heatmap(top20_df: pd.DataFrame, score_df: pd.DataFrame) -> pd.DataFrame:
    norm_source = score_df[["标准化机构名称"] + HEATMAP_FIELDS].copy()
    for field in HEATMAP_FIELDS:
        norm_source[field] = normalize_series(norm_source[field])

    top20_names = top20_df.sort_values("TOPSIS综合排名")["标准化机构名称"].tolist()
    heat_df = norm_source[norm_source["标准化机构名称"].isin(top20_names)].copy()
    heat_df["排序"] = heat_df["标准化机构名称"].map({name: i for i, name in enumerate(top20_names)})
    heat_df = heat_df.sort_values("排序").drop(columns=["排序"])
    heat_df = heat_df.set_index("标准化机构名称")

    fig, ax = plt.subplots(figsize=(10, 10))
    sns.heatmap(
        heat_df,
        cmap="YlGnBu",
        linewidths=0.5,
        linecolor="white",
        cbar_kws={"label": "归一化值"},
        ax=ax,
    )
    ax.set_title("综合前20机构七项入模指标热力图", fontsize=15, pad=12)
    ax.set_xlabel("入模指标")
    ax.set_ylabel("标准化机构名称")
    save_figure(fig, "figure_4_4_top20_indicator_heatmap")
    return heat_df.reset_index()


def build_weight_bar(weight_df: pd.DataFrame) -> pd.DataFrame:
    df = weight_df.copy().sort_values("组合权重", ascending=True)
    fig, ax = plt.subplots(figsize=(9, 5.5))
    ax.barh(df["指标名称"], df["组合权重"], color="#7A9E9F")
    ax.set_title("入模指标组合权重分布", fontsize=15, pad=12)
    ax.set_xlabel("组合权重")
    ax.set_ylabel("指标名称")
    ax.grid(axis="x", linestyle="--", alpha=0.35)
    for i, value in enumerate(df["组合权重"]):
        ax.text(value + 0.003, i, f"{value:.3f}", va="center", fontsize=9)
    save_figure(fig, "figure_4_5_weight_scheme_bar")
    return df[["指标名称", "指标维度", "组合权重"]]


def write_manifest() -> None:
    manifest = pd.DataFrame(FIGURE_CAPTIONS)
    manifest.to_csv(OUTPUT_DIR / "figure_manifest_2011_2025.csv", index=False, encoding=INPUT_ENCODING)


def main() -> None:
    ensure_output_dir()

    top20_df = pd.read_csv(TOP20_FILE, encoding=INPUT_ENCODING)
    score_df = pd.read_csv(SCORE_FILE, encoding=INPUT_ENCODING)
    weight_df = pd.read_csv(WEIGHT_FILE, encoding=INPUT_ENCODING)

    numeric_cols = [
        "TOPSIS综合排名",
        "TOPSIS综合得分",
        "科研产出子得分",
        "学术影响子得分",
        "合作与国际化子得分",
        "去重论文总数",
        "H指数",
        "高被引论文占比",
        "国际合作论文占比",
        "近五年发文占比",
        "合作论文占比",
        "合作国家/地区数",
    ]
    for col in numeric_cols:
        if col in top20_df.columns:
            top20_df[col] = pd.to_numeric(top20_df[col])
        if col in score_df.columns:
            score_df[col] = pd.to_numeric(score_df[col])
    if "组合权重" in weight_df.columns:
        weight_df["组合权重"] = pd.to_numeric(weight_df["组合权重"])

    data_top20 = build_top20_bar(top20_df)
    data_top10 = build_top10_dimension_compare(top20_df)
    data_radar = build_typical_radar(score_df)
    data_heatmap = build_indicator_heatmap(top20_df, score_df)
    data_weight = build_weight_bar(weight_df)

    data_top20.to_csv(OUTPUT_DIR / "figure_data_4_1_top20_topsis_bar.csv", index=False, encoding=INPUT_ENCODING)
    data_top10.to_csv(OUTPUT_DIR / "figure_data_4_2_top10_dimension_compare.csv", index=False, encoding=INPUT_ENCODING)
    data_radar.to_csv(OUTPUT_DIR / "figure_data_4_3_typical_institution_radar.csv", index=False, encoding=INPUT_ENCODING)
    data_heatmap.to_csv(OUTPUT_DIR / "figure_data_4_4_top20_indicator_heatmap.csv", index=False, encoding=INPUT_ENCODING)
    data_weight.to_csv(OUTPUT_DIR / "figure_data_4_5_weight_scheme_bar.csv", index=False, encoding=INPUT_ENCODING)

    write_manifest()
    print(f"output_dir={OUTPUT_DIR}")


if __name__ == "__main__":
    main()
