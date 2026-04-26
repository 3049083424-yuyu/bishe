from __future__ import annotations

import argparse
import csv
import math
import os
import random
import re
import time
import unicodedata
from collections import Counter
from pathlib import Path

import jieba
import matplotlib
import numpy as np
from scipy.optimize import linear_sum_assignment
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer, ENGLISH_STOP_WORDS

matplotlib.use("Agg")
import matplotlib.pyplot as plt


BASE_DIR = Path(r"D:/毕业论文")
DEFAULT_INPUT_PATH = BASE_DIR / "merged_clean_doi_required_2011_2025.csv"
DEFAULT_OUTPUT_DIR = BASE_DIR / "topic_evolution_doi_required_2011_2025"
DEFAULT_DATASET_TAG = "doi_required"
DEFAULT_DATASET_LABEL = "DOI主键版本"

INPUT_ENCODING = "utf-8-sig"
OUTPUT_ENCODING = "gb18030"
SUMMARY_ENCODING = "gb18030"
RANDOM_STATE = 42
LDA_N_JOBS = int(os.environ.get("TOPIC_LDA_N_JOBS", "1"))

PERIODS = (
    ("2011-2015", 2011, 2015),
    ("2016-2020", 2016, 2020),
    ("2021-2025", 2021, 2025),
)

CANDIDATE_TOPIC_COUNTS = (6, 7, 8, 9, 10, 11, 12)
MODEL_SELECTION_SAMPLE_SIZE = 20000
TOP_WORD_COUNT = 12
HIGH_FREQ_TERM_COUNT = 100
MIN_TOKEN_COUNT = 8
VECTORIZER_MAX_FEATURES = 5000
VECTORIZER_MIN_DF = 20
VECTORIZER_MAX_DF = 0.45
TOPIC_LINK_MIN_SIMILARITY = 0.20

RE_MULTI_SPACE = re.compile(r"\s+")
RE_CJK = re.compile(r"[\u4e00-\u9fff]")
RE_EN_TOKEN = re.compile(r"[a-z][a-z0-9_]{1,}")
RE_NUMBER_ONLY = re.compile(r"^\d+(?:\.\d+)?$")

OUTPUT_FILE_TEMPLATES = {
    "preprocess_stats": "topic_preprocess_stats_{dataset_tag}_2011_2025.csv",
    "model_selection": "topic_model_selection_{dataset_tag}_2011_2025.csv",
    "keyword_distribution": "topic_keyword_distribution_{dataset_tag}_2011_2025.csv",
    "topic_strength": "topic_strength_{dataset_tag}_2011_2025.csv",
    "high_frequency_terms": "topic_high_frequency_terms_{dataset_tag}_2011_2025.csv",
    "topic_assignment": "topic_document_assignment_{dataset_tag}_2011_2025.csv",
    "similarity_links": "topic_similarity_links_{dataset_tag}_2011_2025.csv",
    "evolution_paths": "topic_evolution_paths_{dataset_tag}_2011_2025.csv",
    "intensity_curve": "topic_intensity_evolution_curve_{dataset_tag}_2011_2025.png",
    "summary": "topic_evolution_summary_{dataset_tag}_2011_2025.txt",
}

ENGLISH_PHRASES = {
    "oil and gas exploration": "oil_gas_exploration",
    "petroleum exploration": "petroleum_exploration",
    "exploration and development": "exploration_development",
    "enhanced oil recovery": "enhanced_oil_recovery",
    "shale gas": "shale_gas",
    "shale oil": "shale_oil",
    "tight gas": "tight_gas",
    "tight oil": "tight_oil",
    "hydraulic fracturing": "hydraulic_fracturing",
    "fracture network": "fracture_network",
    "carbon capture and storage": "carbon_capture_storage",
    "co2 flooding": "co2_flooding",
    "machine learning": "machine_learning",
    "deep learning": "deep_learning",
    "numerical simulation": "numerical_simulation",
    "reservoir engineering": "reservoir_engineering",
    "reservoir evaluation": "reservoir_evaluation",
    "reservoir simulation": "reservoir_simulation",
    "reservoir characterization": "reservoir_characterization",
    "drilling fluid": "drilling_fluid",
    "directional drilling": "directional_drilling",
    "horizontal well": "horizontal_well",
    "well completion": "well_completion",
    "well logging": "well_logging",
    "production logging": "production_logging",
    "logging while drilling": "logging_while_drilling",
    "formation evaluation": "formation_evaluation",
    "pore structure": "pore_structure",
    "relative permeability": "relative_permeability",
    "seismic inversion": "seismic_inversion",
    "seismic interpretation": "seismic_interpretation",
    "basin modeling": "basin_modeling",
    "source rock": "source_rock",
    "chemical flooding": "chemical_flooding",
    "polymer flooding": "polymer_flooding",
    "water flooding": "water_flooding",
    "gas flooding": "gas_flooding",
    "fluid catalytic cracking": "fluid_catalytic_cracking",
    "catalytic cracking": "catalytic_cracking",
    "hydrotreating": "hydrotreating",
    "hydrodesulfurization": "hydrodesulfurization",
    "crude distillation": "crude_distillation",
    "electrical resistivity": "electrical_resistivity",
    "wastewater treatment": "wastewater_treatment",
    "quantum dot": "quantum_dot",
    "quantum dots": "quantum_dot",
    "carbon dot": "carbon_dot",
    "carbon dots": "carbon_dot",
}

TERM_CANONICAL_MAP = {
    "oil_gas_exploration": "油气勘探",
    "petroleum_exploration": "石油勘探",
    "exploration_development": "勘探开发",
    "enhanced_oil_recovery": "提高采收率",
    "shale_gas": "页岩气",
    "shale_oil": "页岩油",
    "tight_gas": "致密气",
    "tight_oil": "致密油",
    "hydraulic_fracturing": "水力压裂",
    "fracture_network": "裂缝网络",
    "carbon_capture_storage": "碳捕集与封存",
    "co2_flooding": "二氧化碳驱",
    "machine_learning": "机器学习",
    "deep_learning": "深度学习",
    "numerical_simulation": "数值模拟",
    "reservoir": "储层",
    "reservoir_engineering": "油藏工程",
    "reservoir_evaluation": "储层评价",
    "reservoir_simulation": "储层模拟",
    "reservoir_characterization": "储层表征",
    "drilling": "钻井",
    "drilling_fluid": "钻井液",
    "directional_drilling": "定向钻井",
    "horizontal_well": "水平井",
    "well_completion": "完井",
    "well_logging": "测井",
    "production_logging": "生产测井",
    "logging_while_drilling": "随钻测井",
    "formation": "地层",
    "formation_evaluation": "地层评价",
    "pore_structure": "孔隙结构",
    "permeability": "渗透率",
    "relative_permeability": "相对渗透率",
    "seismic": "地震",
    "seismic_inversion": "地震反演",
    "seismic_interpretation": "地震解释",
    "basin": "盆地",
    "basin_modeling": "盆地模拟",
    "source_rock": "烃源岩",
    "chemical_flooding": "化学驱",
    "polymer_flooding": "聚合物驱",
    "water_flooding": "水驱",
    "gas_flooding": "气驱",
    "fracture": "裂缝",
    "fracturing": "水力压裂",
    "shale": "页岩",
    "pore": "孔隙",
    "distribution": "分布",
    "recovery": "采收率",
    "injection": "注入",
    "carbon": "碳",
    "product": "产品",
    "products": "产品",
    "frequency": "频率",
    "stress": "应力",
    "rock": "岩石",
    "rocks": "岩石",
    "prediction": "预测",
    "exploration": "勘探",
    "combustion": "燃烧",
    "hydrogen": "氢",
    "yield": "收率",
    "pyrolysis": "热解",
    "activity": "活性",
    "wave": "波",
    "horizontal": "水平",
    "sample": "样品",
    "samples": "样品",
    "coal": "煤",
    "concentration": "浓度",
    "efficiency": "效率",
    "heat": "热",
    "oilfield": "油田",
    "characteristic": "特征",
    "characteristics": "特征",
    "sand": "砂体",
    "sulfur": "硫",
    "reactor": "反应器",
    "evaluation": "评价",
    "optimization": "优化",
    "developed": "开发",
    "facies": "相",
    "channel": "河道",
    "flooding": "驱替",
    "gel": "凝胶",
    "foam": "泡沫",
    "geological": "地质",
    "diesel": "柴油",
    "gasoline": "汽油",
    "ni": "镍",
    "fe": "铁",
    "metal": "金属",
    "co2": "二氧化碳",
    "methane": "甲烷",
    "hydrate": "水合物",
    "polymer": "聚合物",
    "catalyst": "催化剂",
    "catalytic": "催化",
    "sandstone": "砂岩",
    "delta": "三角洲",
    "fault": "断层",
    "tectonic": "构造",
    "structural": "构造",
    "structure": "构造",
    "sedimentary": "沉积",
    "adsorption": "吸附",
    "viscosity": "黏度",
    "surfactant": "表面活性剂",
    "inversion": "反演",
    "refinery": "炼油",
    "fluid_catalytic_cracking": "流化催化裂化",
    "catalytic_cracking": "催化裂化",
    "hydrotreating": "加氢处理",
    "hydrodesulfurization": "加氢脱硫",
    "crude_distillation": "原油蒸馏",
    "porosity": "孔隙度",
    "storage": "储存",
    "ch4": "甲烷",
    "experimental": "实验",
    "experiments": "实验",
    "design": "设计",
    "sequence": "层序",
    "accumulation": "成藏",
    "steam": "蒸汽",
    "saturation": "饱和度",
    "zone": "区带",
    "deformation": "变形",
    "migration": "运移",
    "velocity": "速度",
    "carbonate": "碳酸盐岩",
    "materials": "材料",
    "composite": "复合材料",
    "conversion": "转化",
    "soot": "烟炱",
    "network": "网络",
    "flame": "火焰",
    "displacement": "驱替",
    "evolution": "演化",
    "oxidation": "氧化",
    "liquid": "液相",
    "asphaltene": "沥青质",
    "wellbore": "井筒",
    "stability": "稳定性",
    "co_2": "二氧化碳",
    "dioxide": "二氧化碳",
    "simulation": "模拟",
    "equation": "方程",
    "agent": "剂",
    "logging": "测井",
    "processing": "处理",
    "amplitude": "振幅",
    "noise": "噪声",
    "imaging": "成像",
    "interpretation": "解释",
    "tight": "致密",
    "zn": "锌",
    "separation": "分离",
    "strain": "应变",
    "signals": "信号",
    "maturity": "成熟度",
    "brine": "盐水",
    "exchange": "交换",
    "composition": "组成",
    "ash": "灰分",
    "solvent": "溶剂",
    "limestone": "石灰岩",
    "deposition": "沉积",
    "bentonite": "膨润土",
    "rheological": "流变",
    "biomass": "生物质",
    "dissociation": "分解",
    "sediments": "沉积物",
    "kinetics": "动力学",
    "wettability": "润湿性",
    "salinity": "盐度",
    "molecular": "分子",
    "mechanism": "机理",
    "mechanisms": "机理",
    "behavior": "行为",
    "chemical": "化学",
    "sites": "位点",
    "oxygen": "氧",
    "density": "密度",
    "shear": "剪切",
    "slip": "滑移",
    "casing": "套管",
    "hole": "井眼",
    "mud": "泥浆",
    "drill": "钻进",
    "bit": "钻头",
    "cement": "水泥",
    "corrosion": "腐蚀",
    "micro": "微观",
    "throat": "喉道",
    "core": "岩心",
    "slurry": "浆体",
    "generation": "生成",
    "oils": "油品",
    "productivity": "产能",
    "porous": "多孔",
    "numerical": "数值",
    "synthesized": "合成",
    "nanoparticles": "纳米颗粒",
    "electron": "电子",
    "quartz": "石英",
    "minerals": "矿物",
    "established": "建立",
    "reactions": "反应",
    "light": "轻质",
    "compounds": "化合物",
    "coke": "焦炭",
    "loss": "漏失",
    "fluids": "流体",
    "ves": "黏弹表面活性剂",
    "strength": "强度",
    "resistance": "阻力",
    "plugging": "封堵",
    "wind": "风能",
    "power": "电力",
    "management": "管理",
    "resources": "资源",
    "future": "未来",
    "equipment": "设备",
    "cost": "成本",
    "ratio": "比值",
    "sag": "凹陷",
    "thermal": "热采",
    "layer": "层系",
    "block": "区块",
    "depth": "埋深",
    "thickness": "厚度",
    "member": "层段",
    "matter": "有机质",
    "heavy": "稠油",
    "rich": "富集",
    "particle": "颗粒",
    "particles": "颗粒",
    "vertical": "垂向",
    "scale": "尺度",
    "volume": "体积",
    "conventional": "常规",
    "middle": "中部",
    "upper": "上部",
    "early": "早期",
    "technique": "工艺",
    "gradient": "梯度",
    "resistivity": "电阻率",
    "electrical_resistivity": "电阻率",
    "angle": "角度",
    "information": "信息",
    "reserves": "储量",
    "clay": "黏土",
    "air": "空气",
    "gasification": "气化",
    "membrane": "膜",
    "selectivity": "选择性",
    "kerogen": "干酪根",
    "diffusion": "扩散",
    "transport": "传输",
    "dolomite": "白云岩",
    "degradation": "降解",
    "aromatic": "芳香族",
    "hydrogel": "水凝胶",
    "mineral": "矿物",
    "dissolution": "溶解",
    "growth": "生长",
    "eor": "提高采收率",
    "accuracy": "准确率",
    "electrolyte": "电解质",
    "ion": "离子",
    "electrochemical": "电化学",
    "wastewater_treatment": "废水处理",
    "removal": "去除",
    "wastewater": "废水",
    "quantum_dot": "量子点",
    "carbon_dot": "碳点",
    "modeling": "模拟",
    "algorithm": "算法",
    "ammonia": "氨",
    "nh3": "氨",
    "nox": "氮氧化物",
    "spectroscopy": "光谱",
    "functional": "官能团",
    "groups": "官能团",
    "emissions": "排放",
    "wax": "蜡",
    "nucleation": "成核",
    "damage": "损害",
    "waste": "废弃物",
    "lignin": "木质素",
    "consumption": "消耗",
    "fractured": "裂缝",
    "hydraulic": "水力",
    "diagenetic": "成岩",
    "unit": "装置",
    "fcc": "催化裂化",
    "interfacial": "界面",
    "strategy": "策略",
    "cell": "电池",
    "toc": "总有机碳",
    "ca": "钙",
    "char": "炭",
    "li": "锂",
    "cu": "铜",
    "pt": "铂",
    "mg": "镁",
    "ph": "pH值",
    "oh": "羟基",
    "页岩气": "页岩气",
    "页岩油": "页岩油",
    "油气勘探": "油气勘探",
    "石油勘探": "石油勘探",
    "勘探开发": "勘探开发",
    "提高采收率": "提高采收率",
    "致密气": "致密气",
    "致密油": "致密油",
    "水力压裂": "水力压裂",
    "压裂": "水力压裂",
    "裂缝": "裂缝",
    "裂缝网络": "裂缝网络",
    "碳捕集与封存": "碳捕集与封存",
    "二氧化碳驱": "二氧化碳驱",
    "机器学习": "机器学习",
    "深度学习": "深度学习",
    "数值模拟": "数值模拟",
    "储层": "储层",
    "储层评价": "储层评价",
    "储层模拟": "储层模拟",
    "储层表征": "储层表征",
    "油藏工程": "油藏工程",
    "钻井": "钻井",
    "钻井液": "钻井液",
    "定向钻井": "定向钻井",
    "水平井": "水平井",
    "完井": "完井",
    "测井": "测井",
    "生产测井": "生产测井",
    "随钻测井": "随钻测井",
    "地层": "地层",
    "地层评价": "地层评价",
    "孔隙结构": "孔隙结构",
    "渗透率": "渗透率",
    "相对渗透率": "相对渗透率",
    "地震": "地震",
    "地震反演": "地震反演",
    "地震解释": "地震解释",
    "盆地": "盆地",
    "盆地模拟": "盆地模拟",
    "烃源岩": "烃源岩",
    "化学驱": "化学驱",
    "聚合物驱": "聚合物驱",
    "水驱": "水驱",
    "气驱": "气驱",
    "页岩": "页岩",
    "孔隙": "孔隙",
    "分布": "分布",
    "采收率": "采收率",
    "注入": "注入",
    "碳": "碳",
    "产品": "产品",
    "频率": "频率",
    "应力": "应力",
    "岩石": "岩石",
    "预测": "预测",
    "勘探": "勘探",
    "燃烧": "燃烧",
    "氢": "氢",
    "收率": "收率",
    "热解": "热解",
    "活性": "活性",
    "波": "波",
    "水平": "水平",
    "样品": "样品",
    "煤": "煤",
    "浓度": "浓度",
    "效率": "效率",
    "热": "热",
    "油田": "油田",
    "特征": "特征",
    "砂体": "砂体",
    "硫": "硫",
    "反应器": "反应器",
    "评价": "评价",
    "优化": "优化",
    "相": "相",
    "河道": "河道",
    "驱替": "驱替",
    "凝胶": "凝胶",
    "泡沫": "泡沫",
    "地质": "地质",
    "柴油": "柴油",
    "汽油": "汽油",
    "镍": "镍",
    "铁": "铁",
    "金属": "金属",
    "二氧化碳": "二氧化碳",
    "甲烷": "甲烷",
    "孔隙度": "孔隙度",
    "储存": "储存",
    "实验": "实验",
    "设计": "设计",
    "层序": "层序",
    "成藏": "成藏",
    "蒸汽": "蒸汽",
    "饱和度": "饱和度",
    "区带": "区带",
    "变形": "变形",
    "运移": "运移",
    "速度": "速度",
    "碳酸盐岩": "碳酸盐岩",
    "材料": "材料",
    "复合材料": "复合材料",
    "转化": "转化",
    "烟炱": "烟炱",
    "网络": "网络",
    "火焰": "火焰",
    "演化": "演化",
    "氧化": "氧化",
    "液相": "液相",
    "沥青质": "沥青质",
    "井筒": "井筒",
    "稳定性": "稳定性",
    "方程": "方程",
    "剂": "剂",
    "锌": "锌",
    "分离": "分离",
    "应变": "应变",
    "信号": "信号",
    "成熟度": "成熟度",
    "盐水": "盐水",
    "交换": "交换",
    "组成": "组成",
    "灰分": "灰分",
    "溶剂": "溶剂",
    "石灰岩": "石灰岩",
    "膨润土": "膨润土",
    "流变": "流变",
    "生物质": "生物质",
    "分解": "分解",
    "沉积物": "沉积物",
    "动力学": "动力学",
    "润湿性": "润湿性",
    "盐度": "盐度",
    "分子": "分子",
    "机理": "机理",
    "行为": "行为",
    "化学": "化学",
    "位点": "位点",
    "氧": "氧",
    "密度": "密度",
    "剪切": "剪切",
    "滑移": "滑移",
    "套管": "套管",
    "井眼": "井眼",
    "泥浆": "泥浆",
    "钻进": "钻进",
    "钻头": "钻头",
    "水泥": "水泥",
    "腐蚀": "腐蚀",
    "微观": "微观",
    "喉道": "喉道",
    "岩心": "岩心",
    "浆体": "浆体",
    "生成": "生成",
    "油品": "油品",
    "产能": "产能",
    "多孔": "多孔",
    "数值": "数值",
    "合成": "合成",
    "纳米颗粒": "纳米颗粒",
    "电子": "电子",
    "石英": "石英",
    "矿物": "矿物",
    "建立": "建立",
    "反应": "反应",
    "轻质": "轻质",
    "化合物": "化合物",
    "焦炭": "焦炭",
    "漏失": "漏失",
    "流体": "流体",
    "黏弹表面活性剂": "黏弹表面活性剂",
    "强度": "强度",
    "阻力": "阻力",
    "封堵": "封堵",
    "风能": "风能",
    "电力": "电力",
    "管理": "管理",
    "资源": "资源",
    "未来": "未来",
    "设备": "设备",
    "成本": "成本",
    "水合物": "水合物",
    "聚合物": "聚合物",
    "催化剂": "催化剂",
    "催化": "催化",
    "砂岩": "砂岩",
    "三角洲": "三角洲",
    "断层": "断层",
    "构造": "构造",
    "沉积": "沉积",
    "吸附": "吸附",
    "黏度": "黏度",
    "表面活性剂": "表面活性剂",
    "反演": "反演",
    "炼油": "炼油",
    "流化催化裂化": "流化催化裂化",
    "催化裂化": "催化裂化",
    "加氢处理": "加氢处理",
    "加氢脱硫": "加氢脱硫",
    "原油蒸馏": "原油蒸馏",
}

PRESENTATION_TERM_ALIASES = {
    "ratio": "比值",
    "sag": "凹陷",
    "thermal": "热采",
    "layer": "层系",
    "block": "区块",
    "depth": "埋深",
    "thickness": "厚度",
    "member": "层段",
    "matter": "有机质",
    "heavy": "稠油",
    "rich": "富集",
    "particle": "颗粒",
    "particles": "颗粒",
    "vertical": "垂向",
    "scale": "尺度",
    "volume": "体积",
    "conventional": "常规",
    "middle": "中部",
    "upper": "上部",
    "early": "早期",
    "technique": "工艺",
}

PRESENTATION_TERM_STOPWORDS = {
    "ratio",
    "studied",
    "increase",
    "increases",
    "increasing",
    "improve",
    "better",
    "various",
    "presence",
    "addition",
    "key",
    "number",
    "average",
    "small",
    "multi",
    "significantly",
    "比值",
    "尺度",
    "体积",
    "常规",
    "中部",
    "上部",
    "早期",
    "平均",
    "数量",
    "层系",
    "特征",
    "分布",
    "岩石",
    "工艺",
    "实验",
    "机理",
    "地质",
}

ZH_CUSTOM_WORDS = {
    "油气勘探",
    "石油勘探",
    "勘探开发",
    "提高采收率",
    "页岩气",
    "页岩油",
    "致密气",
    "致密油",
    "水力压裂",
    "裂缝网络",
    "碳捕集与封存",
    "二氧化碳驱",
    "机器学习",
    "深度学习",
    "数值模拟",
    "油藏工程",
    "储层评价",
    "储层模拟",
    "储层表征",
    "钻井液",
    "定向钻井",
    "水平井",
    "生产测井",
    "随钻测井",
    "地层评价",
    "孔隙结构",
    "相对渗透率",
    "地震反演",
    "地震解释",
    "盆地模拟",
    "烃源岩",
    "化学驱",
    "聚合物驱",
    "流化催化裂化",
    "催化裂化",
    "加氢处理",
    "加氢脱硫",
    "原油蒸馏",
}

PETROLEUM_ANCHORS = {
    "油气勘探",
    "石油勘探",
    "勘探开发",
    "提高采收率",
    "页岩气",
    "页岩油",
    "致密气",
    "致密油",
    "水力压裂",
    "裂缝",
    "裂缝网络",
    "储层",
    "储层评价",
    "储层模拟",
    "储层表征",
    "油藏工程",
    "钻井",
    "钻井液",
    "测井",
    "生产测井",
    "随钻测井",
    "地层",
    "地层评价",
    "孔隙结构",
    "渗透率",
    "地震",
    "地震反演",
    "地震解释",
    "盆地",
    "盆地模拟",
    "烃源岩",
    "化学驱",
    "聚合物驱",
    "水驱",
    "气驱",
    "炼油",
    "流化催化裂化",
    "催化裂化",
    "加氢处理",
    "加氢脱硫",
    "原油蒸馏",
}

DOMAIN_GENERIC_STOPWORDS = {
    "gas",
    "oil",
    "petroleum",
    "crude",
    "hydrocarbon",
    "energy",
    "fuel",
    "natural",
    "production",
    "development",
    "high",
    "low",
    "temperature",
    "water",
    "pressure",
    "area",
    "flow",
    "time",
    "content",
    "phase",
    "surface",
    "reaction",
    "source",
    "organic",
    "well",
    "wells",
    "fluid",
    "acid",
    "rate",
    "capacity",
    "technology",
    "control",
    "operation",
    "china",
    "late",
    "potential",
    "compared",
    "effective",
    "obtained",
    "increase",
    "increased",
    "size",
    "important",
    "stage",
    "large",
    "complex",
    "main",
    "including",
    "solution",
    "significant",
    "order",
    "good",
    "factors",
    "respectively",
    "reduction",
    "analyzed",
    "test",
    "systems",
    "formed",
    "quality",
    "provide",
    "provides",
    "influence",
    "similar",
    "range",
    "applied",
    "observed",
    "according",
    "situ",
    "impact",
    "physical",
    "single",
    "basis",
    "present",
    "total",
    "石油",
    "油气",
    "天然气",
    "原油",
    "能源",
    "燃料",
    "生产",
    "开发",
}

EXTRA_EN_STOPWORDS = {
    "study",
    "studies",
    "method",
    "methods",
    "result",
    "results",
    "analysis",
    "paper",
    "research",
    "based",
    "using",
    "used",
    "show",
    "shows",
    "shown",
    "proposed",
    "new",
    "different",
    "effect",
    "effects",
    "performance",
    "application",
    "applications",
    "approach",
    "properties",
    "model",
    "models",
    "work",
    "process",
    "data",
    "field",
    "author",
    "authors",
    "value",
    "values",
    "change",
    "changes",
    "condition",
    "conditions",
    "parameter",
    "parameters",
    "rights",
    "elsevier",
    "degrees",
    "lower",
    "higher",
    "reserved",
    "showed",
    "investigated",
    "mainly",
    "types",
    "type",
    "characterized",
    "favorable",
    "deep",
    "developed",
    "experimental",
    "experiments",
    "technical",
    "technologies",
    "modified",
    "excellent",
    "mechanical",
    "active",
    "species",
    "controlled",
    "prepared",
    "prepare",
    "preparation",
    "addition",
    "additions",
    "improve",
    "improved",
    "improves",
    "increasing",
    "increases",
    "various",
    "key",
    "presence",
    "small",
    "use",
    "average",
    "factor",
    "novel",
    "promising",
    "resulting",
    "revealed",
    "furthermore",
    "period",
    "number",
    "multi",
    "better",
    "enhanced",
    "effectively",
    "efficient",
    "highly",
    "solid",
    "understanding",
    "transfer",
    "processes",
    "mass",
    "treatment",
    "self",
    "center",
    "dot",
    "wt",
    "nc",
}

EXTRA_ZH_STOPWORDS = {
    "研究",
    "分析",
    "结果",
    "方法",
    "采用",
    "提出",
    "进行",
    "表明",
    "认为",
    "本文",
    "文中",
    "发现",
    "得到",
    "基于",
    "通过",
    "可以",
    "对于",
    "以及",
    "技术",
    "参数",
    "变化",
    "文献",
    "作者",
    "显示",
    "工作",
    "问题",
    "条件",
    "结构",
    "主要",
    "系统",
    "发展",
    "过程",
    "重要",
    "相关",
}

COMMON_ZH_STOPWORDS = {
    "的",
    "了",
    "和",
    "是",
    "在",
    "对",
    "与",
    "及",
    "并",
    "为",
    "由",
    "等",
    "中",
    "其",
    "该",
    "各",
    "可",
    "更",
    "从",
    "到",
    "后",
}

EN_STOPWORDS = ENGLISH_STOP_WORDS | EXTRA_EN_STOPWORDS
TOPIC_STOPWORDS = EN_STOPWORDS | DOMAIN_GENERIC_STOPWORDS | EXTRA_ZH_STOPWORDS | COMMON_ZH_STOPWORDS


def compact_text(value: object) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    text = text.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    text = text.replace("\u3000", " ").replace("\xa0", " ")
    text = RE_MULTI_SPACE.sub(" ", text)
    return text.strip()


def build_output_paths(output_dir: Path, dataset_tag: str) -> dict[str, Path]:
    return {
        key: output_dir / template.format(dataset_tag=dataset_tag)
        for key, template in OUTPUT_FILE_TEMPLATES.items()
    }


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding=OUTPUT_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_text(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding=SUMMARY_ENCODING)


def log_progress(message: str) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def period_label_from_year(year: int) -> str:
    for label, start_year, end_year in PERIODS:
        if start_year <= year <= end_year:
            return label
    return ""


def choose_abstract(row: dict[str, str]) -> tuple[str, str]:
    abstract_en = compact_text(row.get("abstract_en", ""))
    abstract_cn = compact_text(row.get("abstract_cn", ""))
    if abstract_en and len(abstract_en) >= 30:
        return abstract_en, "en"
    if abstract_cn:
        return abstract_cn, "zh"
    if abstract_en:
        return abstract_en, "en"
    return "", ""


def build_document_signature(row: dict[str, str]) -> str:
    parts = [
        compact_text(row.get("source_db", "")).lower(),
        compact_text(row.get("doi", "")).lower(),
        compact_text(row.get("year", "")),
        compact_text(row.get("title_en", "")).lower(),
        compact_text(row.get("title_cn", "")).lower(),
        compact_text(row.get("author", "")).lower(),
        compact_text(row.get("journal_en", "")).lower(),
        compact_text(row.get("journal_cn", "")).lower(),
    ]
    return "||".join(parts)


def replace_phrases(text: str, replacements: dict[str, str]) -> str:
    lowered = f" {compact_text(text).lower()} "
    for raw_phrase in sorted(replacements, key=len, reverse=True):
        lowered = lowered.replace(f" {raw_phrase} ", f" {replacements[raw_phrase]} ")
    return lowered


def canonicalize_topic_term(token: str) -> str:
    normalized = compact_text(token).lower().replace("-", "_").replace("/", "_")
    normalized = normalized.strip("_")
    if not normalized:
        return ""
    if normalized in TERM_CANONICAL_MAP:
        return TERM_CANONICAL_MAP[normalized]
    if normalized.endswith("ies") and normalized[:-3] + "y" in TERM_CANONICAL_MAP:
        return TERM_CANONICAL_MAP[normalized[:-3] + "y"]
    if normalized.endswith("s") and normalized[:-1] in TERM_CANONICAL_MAP:
        return TERM_CANONICAL_MAP[normalized[:-1]]
    return normalized


def clean_presentation_topic_term(token: str) -> str:
    canonical = canonicalize_topic_term(token)
    if not canonical:
        return ""
    alias = PRESENTATION_TERM_ALIASES.get(canonical, canonical)
    if alias in TOPIC_STOPWORDS or alias in PRESENTATION_TERM_STOPWORDS:
        return ""
    return alias


def valid_token(token: str) -> bool:
    if not token or token in TOPIC_STOPWORDS:
        return False
    if len(token) < 2 or RE_NUMBER_ONLY.fullmatch(token):
        return False
    return True


def tokenize_english(text: str) -> list[str]:
    prepared = replace_phrases(text, ENGLISH_PHRASES)
    prepared = prepared.replace("-", " ").replace("/", " ")
    tokens: list[str] = []
    for raw_token in RE_EN_TOKEN.findall(prepared):
        canonical = canonicalize_topic_term(raw_token)
        if valid_token(canonical):
            tokens.append(canonical)
    return tokens


def tokenize_chinese(text: str) -> list[str]:
    prepared = compact_text(text)
    tokens: list[str] = []
    for raw_token in jieba.lcut(prepared, cut_all=False):
        raw_token = compact_text(raw_token)
        if not raw_token:
            continue
        if RE_EN_TOKEN.fullmatch(raw_token.lower()):
            continue
        canonical = canonicalize_topic_term(raw_token)
        if valid_token(canonical):
            tokens.append(canonical)
    return tokens + tokenize_english(prepared)


def preprocess_tokens(text: str, language: str) -> list[str]:
    if not text:
        return []
    if language == "zh" or RE_CJK.search(text):
        return tokenize_chinese(text)
    return tokenize_english(text)


def is_petroleum_relevant(row: dict[str, str], abstract_text: str) -> bool:
    combined_text = " ".join(
        [
            compact_text(row.get("title_en", "")),
            compact_text(row.get("title_cn", "")),
            compact_text(row.get("title_cn_en", "")),
            compact_text(row.get("keywords_en", "")),
            compact_text(row.get("keywords_cn", "")),
            compact_text(abstract_text),
        ]
    )
    if RE_CJK.search(combined_text):
        tokens = set(tokenize_chinese(combined_text))
    else:
        tokens = set(tokenize_english(combined_text))
    return bool(tokens & PETROLEUM_ANCHORS)


def build_vectorizer() -> CountVectorizer:
    return CountVectorizer(
        lowercase=False,
        token_pattern=r"(?u)\b[\w\u4e00-\u9fff]{2,}\b",
        min_df=VECTORIZER_MIN_DF,
        max_df=VECTORIZER_MAX_DF,
        max_features=VECTORIZER_MAX_FEATURES,
    )


def rank_with_ties(values: list[float], reverse: bool) -> list[int]:
    ordered = sorted(enumerate(values), key=lambda item: item[1], reverse=reverse)
    ranks = [0 for _ in values]
    current_rank = 1
    previous_value: float | None = None
    for position, (index, value) in enumerate(ordered, start=1):
        if previous_value is None or not math.isclose(value, previous_value, rel_tol=1e-12, abs_tol=1e-12):
            current_rank = position
            previous_value = value
        ranks[index] = current_rank
    return ranks


def compute_topic_coherence(model: LatentDirichletAllocation, matrix, feature_names: list[str], top_n: int = 10) -> float:
    if matrix.shape[0] == 0 or matrix.shape[1] == 0:
        return 0.0
    binary = (matrix > 0).astype(np.int8).tocsc()
    doc_count = float(binary.shape[0])
    doc_freq = np.asarray(binary.sum(axis=0)).ravel()
    topic_scores: list[float] = []
    for component in model.components_:
        top_indices = np.argsort(component)[::-1][:top_n]
        pair_scores: list[float] = []
        for i in range(len(top_indices)):
            for j in range(i + 1, len(top_indices)):
                left = top_indices[i]
                right = top_indices[j]
                left_count = float(doc_freq[left])
                right_count = float(doc_freq[right])
                if left_count <= 0 or right_count <= 0:
                    continue
                joint_count = float(binary[:, left].multiply(binary[:, right]).sum())
                if joint_count <= 0:
                    pair_scores.append(-1.0)
                    continue
                p_i = left_count / doc_count
                p_j = right_count / doc_count
                p_ij = joint_count / doc_count
                numerator = math.log((p_ij + 1e-12) / (p_i * p_j + 1e-12))
                denominator = -math.log(p_ij + 1e-12)
                pair_scores.append(numerator / denominator if denominator else 0.0)
        topic_scores.append(float(np.mean(pair_scores)) if pair_scores else 0.0)
    return float(np.mean(topic_scores)) if topic_scores else 0.0


def sample_documents(documents: list[str], size: int) -> list[str]:
    if len(documents) <= size:
        return documents
    rng = random.Random(RANDOM_STATE)
    indices = list(range(len(documents)))
    rng.shuffle(indices)
    sampled_indices = sorted(indices[:size])
    return [documents[index] for index in sampled_indices]


def evaluate_topic_counts(period_label: str, documents: list[str]) -> tuple[list[dict[str, object]], int]:
    log_progress(f"{period_label}：开始主题数评估，文档数={len(documents)}，采样上限={MODEL_SELECTION_SAMPLE_SIZE}")
    sampled_documents = sample_documents(documents, MODEL_SELECTION_SAMPLE_SIZE)
    vectorizer = build_vectorizer()
    matrix = vectorizer.fit_transform(sampled_documents)
    feature_names = vectorizer.get_feature_names_out().tolist()
    log_progress(f"{period_label}：主题数评估向量化完成，样本文档数={len(sampled_documents)}，词汇表规模={len(feature_names)}")
    rows: list[dict[str, object]] = []
    for topic_count in CANDIDATE_TOPIC_COUNTS:
        log_progress(f"{period_label}：开始拟合候选主题数 k={topic_count}")
        model = LatentDirichletAllocation(
            n_components=topic_count,
            max_iter=12,
            learning_method="online",
            learning_offset=50.0,
            batch_size=2048,
            random_state=RANDOM_STATE,
            n_jobs=LDA_N_JOBS,
        )
        model.fit(matrix)
        perplexity = round(float(model.perplexity(matrix)), 6)
        coherence = round(compute_topic_coherence(model, matrix, feature_names), 6)
        log_progress(f"{period_label}：完成候选主题数 k={topic_count}，困惑度={perplexity}，一致性={coherence}")
        rows.append(
            {
                "阶段": period_label,
                "候选主题数": topic_count,
                "样本文献数": len(sampled_documents),
                "词汇表规模": int(matrix.shape[1]),
                "困惑度": perplexity,
                "一致性得分": coherence,
            }
        )
    perplexity_ranks = rank_with_ties([float(row["困惑度"]) for row in rows], reverse=False)
    coherence_ranks = rank_with_ties([float(row["一致性得分"]) for row in rows], reverse=True)
    for index, row in enumerate(rows):
        row["困惑度排名"] = perplexity_ranks[index]
        row["一致性排名"] = coherence_ranks[index]
        row["综合排序值"] = perplexity_ranks[index] + coherence_ranks[index]
    rows.sort(key=lambda row: (int(row["综合排序值"]), int(row["一致性排名"]), float(row["困惑度"])))
    selected_topic_count = int(rows[0]["候选主题数"])
    for row in rows:
        row["是否选中"] = 1 if int(row["候选主题数"]) == selected_topic_count else 0
    rows.sort(key=lambda row: int(row["候选主题数"]))
    log_progress(f"{period_label}：主题数评估完成，选定 k={selected_topic_count}")
    return rows, selected_topic_count


def topic_label_from_terms(terms: list[str]) -> str:
    filtered_terms = [term for term in terms if term not in TOPIC_STOPWORDS]
    chinese_first = [term for term in filtered_terms if not re.search(r"[A-Za-z]", term)]
    if chinese_first:
        filtered_terms = chinese_first
    return " / ".join((filtered_terms or terms)[:4])


def normalize_topic_distribution(weights: np.ndarray, feature_names: list[str]) -> dict[str, float]:
    total = float(weights.sum())
    if total <= 0:
        return {}
    return {
        feature_names[index]: float(weight / total)
        for index, weight in enumerate(weights)
        if weight > 0
    }


def cosine_similarity_dict(left: dict[str, float], right: dict[str, float]) -> float:
    if not left or not right:
        return 0.0
    dot = sum(value * right.get(term, 0.0) for term, value in left.items())
    left_norm = math.sqrt(sum(value * value for value in left.values()))
    right_norm = math.sqrt(sum(value * value for value in right.values()))
    if left_norm <= 0 or right_norm <= 0:
        return 0.0
    return dot / (left_norm * right_norm)


def fit_period_model(period_label: str, documents: list[str], topic_count: int) -> dict[str, object]:
    log_progress(f"{period_label}：开始最终模型拟合，文档数={len(documents)}，主题数={topic_count}")
    vectorizer = build_vectorizer()
    matrix = vectorizer.fit_transform(documents)
    feature_names = vectorizer.get_feature_names_out().tolist()
    log_progress(f"{period_label}：最终模型向量化完成，矩阵形状={matrix.shape[0]}x{matrix.shape[1]}")
    model = LatentDirichletAllocation(
        n_components=topic_count,
        max_iter=18,
        learning_method="online",
        learning_offset=50.0,
        batch_size=4096,
        random_state=RANDOM_STATE,
        n_jobs=LDA_N_JOBS,
    )
    doc_topic = model.fit_transform(matrix)
    log_progress(f"{period_label}：最终模型拟合完成")
    dominant_topics = np.argmax(doc_topic, axis=1)
    dominant_probabilities = np.max(doc_topic, axis=1)
    dominant_counts = Counter(int(topic_index) for topic_index in dominant_topics.tolist())
    topic_keyword_rows: list[dict[str, object]] = []
    topic_strength_rows: list[dict[str, object]] = []
    topic_vectors: dict[int, dict[str, float]] = {}
    for topic_index, component in enumerate(model.components_):
        topic_vectors[topic_index] = normalize_topic_distribution(component, feature_names)
        top_indices = np.argsort(component)[::-1][:TOP_WORD_COUNT]
        top_terms = [feature_names[index] for index in top_indices]
        topic_label = topic_label_from_terms(top_terms)
        document_count = int(dominant_counts.get(topic_index, 0))
        intensity = document_count / len(documents) if documents else 0.0
        avg_probability = float(np.mean(doc_topic[:, topic_index])) if len(documents) else 0.0
        topic_strength_rows.append(
            {
                "阶段": period_label,
                "主题编号": f"T{topic_index + 1:02d}",
                "主题标签": topic_label,
                "主题文献数": document_count,
                "阶段文献总数": len(documents),
                "主题强度": round(intensity, 6),
                "平均主题概率": round(avg_probability, 6),
                "代表关键词": " | ".join(top_terms),
            }
        )
        total_weight = float(component.sum()) or 1.0
        for rank, feature_index in enumerate(top_indices, start=1):
            topic_keyword_rows.append(
                {
                    "阶段": period_label,
                    "主题编号": f"T{topic_index + 1:02d}",
                    "主题标签": topic_label,
                    "主题强度": round(intensity, 6),
                    "平均主题概率": round(avg_probability, 6),
                    "关键词排名": rank,
                    "关键词": feature_names[feature_index],
                    "词项概率": round(float(component[feature_index] / total_weight), 6),
                }
            )
    term_frequency = np.asarray(matrix.sum(axis=0)).ravel()
    document_frequency = np.asarray((matrix > 0).sum(axis=0)).ravel()
    high_freq_order = np.argsort(term_frequency)[::-1][:HIGH_FREQ_TERM_COUNT]
    high_freq_rows = [
        {
            "阶段": period_label,
            "词项": feature_names[index],
            "总词频": int(term_frequency[index]),
            "文献频次": int(document_frequency[index]),
        }
        for index in high_freq_order
    ]
    return {
        "matrix_shape": matrix.shape,
        "dominant_topics": dominant_topics,
        "dominant_probabilities": dominant_probabilities,
        "topic_keyword_rows": topic_keyword_rows,
        "topic_strength_rows": topic_strength_rows,
        "high_freq_rows": high_freq_rows,
        "topic_vectors": topic_vectors,
    }


def match_topics_between_periods(
    left_period: str,
    right_period: str,
    left_vectors: dict[int, dict[str, float]],
    right_vectors: dict[int, dict[str, float]],
) -> tuple[dict[int, int], list[dict[str, object]]]:
    left_topics = sorted(left_vectors)
    right_topics = sorted(right_vectors)
    if not left_topics or not right_topics:
        return {}, []
    similarity_matrix = np.zeros((len(left_topics), len(right_topics)), dtype=float)
    for left_index, left_topic in enumerate(left_topics):
        for right_index, right_topic in enumerate(right_topics):
            similarity_matrix[left_index, right_index] = cosine_similarity_dict(
                left_vectors[left_topic],
                right_vectors[right_topic],
            )
    row_index, col_index = linear_sum_assignment(1.0 - similarity_matrix)
    match_map: dict[int, int] = {}
    rows: list[dict[str, object]] = []
    for left_index, right_index in zip(row_index.tolist(), col_index.tolist()):
        left_topic = left_topics[left_index]
        right_topic = right_topics[right_index]
        similarity = float(similarity_matrix[left_index, right_index])
        if similarity >= TOPIC_LINK_MIN_SIMILARITY:
            match_map[left_topic] = right_topic
        rows.append(
            {
                "左阶段": left_period,
                "左主题编号": f"T{left_topic + 1:02d}",
                "右阶段": right_period,
                "右主题编号": f"T{right_topic + 1:02d}",
                "主题相似度": round(similarity, 6),
                "纳入演化路径": 1 if similarity >= TOPIC_LINK_MIN_SIMILARITY else 0,
            }
        )
    rows.sort(key=lambda row: (row["左主题编号"], row["右主题编号"]))
    return match_map, rows


def build_evolution_paths(
    topic_strength_by_period: dict[str, list[dict[str, object]]],
    match_12: dict[int, int],
    match_23: dict[int, int],
) -> list[dict[str, object]]:
    first_period, second_period, third_period = [label for label, _, _ in PERIODS]
    strength_lookup = {
        period_label: {str(row["主题编号"]): row for row in rows}
        for period_label, rows in topic_strength_by_period.items()
    }
    matched_second_topics = set(match_12.values())
    matched_third_topics = set(match_23.values())
    paths: list[dict[str, object]] = []
    path_counter = 1

    def build_row(path_id: int, topic_1: int | None, topic_2: int | None, topic_3: int | None) -> dict[str, object]:
        key_1 = f"T{topic_1 + 1:02d}" if topic_1 is not None else ""
        key_2 = f"T{topic_2 + 1:02d}" if topic_2 is not None else ""
        key_3 = f"T{topic_3 + 1:02d}" if topic_3 is not None else ""
        row_1 = strength_lookup[first_period].get(key_1, {})
        row_2 = strength_lookup[second_period].get(key_2, {})
        row_3 = strength_lookup[third_period].get(key_3, {})
        return {
            "演化路径编号": f"P{path_id:02d}",
            f"{first_period}主题编号": key_1,
            f"{first_period}主题标签": row_1.get("主题标签", ""),
            f"{first_period}主题强度": row_1.get("主题强度", ""),
            f"{second_period}主题编号": key_2,
            f"{second_period}主题标签": row_2.get("主题标签", ""),
            f"{second_period}主题强度": row_2.get("主题强度", ""),
            f"{third_period}主题编号": key_3,
            f"{third_period}主题标签": row_3.get("主题标签", ""),
            f"{third_period}主题强度": row_3.get("主题强度", ""),
        }

    for topic_1 in sorted(int(key[1:]) - 1 for key in strength_lookup[first_period]):
        topic_2 = match_12.get(topic_1)
        topic_3 = match_23.get(topic_2) if topic_2 is not None else None
        paths.append(build_row(path_counter, topic_1, topic_2, topic_3))
        path_counter += 1
    for topic_2 in sorted(int(key[1:]) - 1 for key in strength_lookup[second_period]):
        if topic_2 in matched_second_topics:
            continue
        topic_3 = match_23.get(topic_2)
        paths.append(build_row(path_counter, None, topic_2, topic_3))
        path_counter += 1
    for topic_3 in sorted(int(key[1:]) - 1 for key in strength_lookup[third_period]):
        if topic_3 in matched_third_topics:
            continue
        paths.append(build_row(path_counter, None, None, topic_3))
        path_counter += 1
    return paths


def plot_topic_intensity(paths: list[dict[str, object]], out_path: Path, dataset_label: str) -> None:
    period_labels = [label for label, _, _ in PERIODS]
    plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False
    fig, ax = plt.subplots(figsize=(13, 7))
    cmap = plt.get_cmap("tab20")
    for index, path in enumerate(paths):
        values = []
        labels: list[str] = []
        for period_label in period_labels:
            value = path.get(f"{period_label}主题强度", "")
            values.append(float(value) if str(value) not in {"", "nan"} else np.nan)
            topic_label = str(path.get(f"{period_label}主题标签", "")).strip()
            if topic_label and not labels:
                labels.append(topic_label)
        label = f"{path['演化路径编号']} {labels[0] if labels else '新增主题'}"
        ax.plot(period_labels, values, marker="o", linewidth=2.0, color=cmap(index % 20), label=label, alpha=0.9)
    ax.set_title(f"石油领域研究主题强度时序演变曲线（{dataset_label}）")
    ax.set_xlabel("时间阶段")
    ax.set_ylabel("主题强度（主题文献占比）")
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.4)
    ax.legend(loc="center left", bbox_to_anchor=(1.02, 0.5), frameon=False, fontsize=9)
    fig.tight_layout()
    fig.savefig(out_path, dpi=220, bbox_inches="tight")
    plt.close(fig)


def write_summary(
    out_path: Path,
    dataset_label: str,
    preprocess_rows: list[dict[str, object]],
    selection_rows: list[dict[str, object]],
    topic_strength_rows: list[dict[str, object]],
    path_rows: list[dict[str, object]],
) -> None:
    selected_map = {
        str(row["阶段"]): int(row["候选主题数"])
        for row in selection_rows
        if int(row["是否选中"]) == 1
    }
    strongest_by_period: dict[str, dict[str, object]] = {}
    for row in topic_strength_rows:
        period_label = str(row["阶段"])
        current = strongest_by_period.get(period_label)
        if current is None or float(row["主题强度"]) > float(current["主题强度"]):
            strongest_by_period[period_label] = row
    lines = [
        f"主题演化分析结果说明（{dataset_label}，2011-2025）",
        "",
        "1. 按 2011-2015、2016-2020、2021-2025 三个阶段分别训练 LDA 主题模型。",
        "2. 文本预处理包括摘要选择、分词、去停用词、术语统一映射和词袋建模。",
        "3. 领域过滤依据题名、关键词与摘要中的石油领域术语命中进行。",
        "4. 主题数通过困惑度与一致性得分联合选择。",
        f"5. 主题演化路径基于相邻阶段主题词分布的余弦相似度一对一匹配，仅保留相似度不低于 {TOPIC_LINK_MIN_SIMILARITY:.2f} 的连接。",
        "",
        "各阶段建模文献量：",
    ]
    for row in preprocess_rows:
        lines.append(
            f"- {row['阶段']}：原始文献 {row['阶段文献数']} 篇，有摘要 {row['有摘要文献数']} 篇，通过领域过滤 {row['通过领域过滤文献数']} 篇，进入建模 {row['进入建模文献数']} 篇。"
        )
    lines.append("")
    lines.append("各阶段选定主题数：")
    for period_label, _, _ in PERIODS:
        lines.append(f"- {period_label}：K = {selected_map.get(period_label, 0)}")
    lines.append("")
    lines.append("各阶段主题强度最高的主题：")
    for period_label, _, _ in PERIODS:
        row = strongest_by_period.get(period_label, {})
        lines.append(f"- {period_label}：{row.get('主题编号', '')} {row.get('主题标签', '')}，主题强度 {row.get('主题强度', '')}")
    lines.append("")
    lines.append(f"共生成 {len(path_rows)} 条主题演化路径。")
    write_text(out_path, lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(DEFAULT_INPUT_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--dataset-tag", default=DEFAULT_DATASET_TAG)
    parser.add_argument("--dataset-label", default=DEFAULT_DATASET_LABEL)
    return parser.parse_args()


def main() -> None:
    os.environ.setdefault("JOBLIB_TEMP_FOLDER", str(Path("E:/joblib_tmp")))
    for word in sorted(ZH_CUSTOM_WORDS):
        jieba.add_word(word)
    args = parse_args()
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    dataset_tag = str(args.dataset_tag).strip() or DEFAULT_DATASET_TAG
    dataset_label = str(args.dataset_label).strip() or DEFAULT_DATASET_LABEL
    output_dir.mkdir(parents=True, exist_ok=True)
    output_paths = build_output_paths(output_dir, dataset_tag)
    log_progress(f"主题演化主链启动：input={input_path}，output_dir={output_dir}，dataset_tag={dataset_tag}")

    period_documents: dict[str, list[str]] = {label: [] for label, _, _ in PERIODS}
    period_source_rows: dict[str, list[dict[str, object]]] = {label: [] for label, _, _ in PERIODS}
    preprocess_stats: dict[str, dict[str, object]] = {
        label: {
            "阶段": label,
            "年份范围": label,
            "阶段文献数": 0,
            "有摘要文献数": 0,
            "英文摘要文献数": 0,
            "中文摘要文献数": 0,
            "通过领域过滤文献数": 0,
            "进入建模文献数": 0,
            "平均有效词数": 0.0,
            "词汇表规模": 0,
        }
        for label, _, _ in PERIODS
    }
    token_counter: Counter[str] = Counter()
    scanned_rows = 0

    with input_path.open("r", encoding=INPUT_ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            scanned_rows += 1
            if scanned_rows % 50000 == 0:
                log_progress(f"预处理扫描进度：已读取 {scanned_rows} 篇文献")
            year_text = compact_text(row.get("year", ""))
            if not year_text.isdigit():
                continue
            period_label = period_label_from_year(int(year_text))
            if not period_label:
                continue
            preprocess_stats[period_label]["阶段文献数"] = int(preprocess_stats[period_label]["阶段文献数"]) + 1
            abstract_text, language = choose_abstract(row)
            if not abstract_text:
                continue
            preprocess_stats[period_label]["有摘要文献数"] = int(preprocess_stats[period_label]["有摘要文献数"]) + 1
            abstract_field = "英文摘要文献数" if language == "en" else "中文摘要文献数"
            preprocess_stats[period_label][abstract_field] = int(preprocess_stats[period_label][abstract_field]) + 1
            if not is_petroleum_relevant(row, abstract_text):
                continue
            preprocess_stats[period_label]["通过领域过滤文献数"] = int(preprocess_stats[period_label]["通过领域过滤文献数"]) + 1
            tokens = preprocess_tokens(abstract_text, language)
            if len(tokens) < MIN_TOKEN_COUNT:
                continue
            token_counter[period_label] += len(tokens)
            preprocess_stats[period_label]["进入建模文献数"] = int(preprocess_stats[period_label]["进入建模文献数"]) + 1
            period_documents[period_label].append(" ".join(tokens))
            period_source_rows[period_label].append(
                {
                    "文献签名": build_document_signature(row),
                    "年份": year_text,
                    "来源库": compact_text(row.get("source_db_primary", "")) or compact_text(row.get("source_db", "")),
                    "DOI": compact_text(row.get("doi", "")),
                    "标题": compact_text(row.get("title_cn", "")) or compact_text(row.get("title_en", "")),
                    "第一作者": compact_text(row.get("author_cn", "")) or compact_text(row.get("author", "")),
                }
            )

    for period_label, _, _ in PERIODS:
        log_progress(
            f"{period_label}：预处理完成，阶段文献数={preprocess_stats[period_label]['阶段文献数']}，"
            f"有摘要={preprocess_stats[period_label]['有摘要文献数']}，"
            f"通过领域过滤={preprocess_stats[period_label]['通过领域过滤文献数']}，"
            f"进入建模={preprocess_stats[period_label]['进入建模文献数']}"
        )

    preprocess_rows = [preprocess_stats[label] for label, _, _ in PERIODS]
    selection_rows: list[dict[str, object]] = []
    fitted_results: dict[str, dict[str, object]] = {}
    all_topic_keyword_rows: list[dict[str, object]] = []
    all_topic_strength_rows: list[dict[str, object]] = []
    all_high_freq_rows: list[dict[str, object]] = []
    all_topic_assignment_rows: list[dict[str, object]] = []
    selected_topic_counts: dict[str, int] = {}

    for period_label, _, _ in PERIODS:
        doc_count = int(preprocess_stats[period_label]["进入建模文献数"])
        preprocess_stats[period_label]["平均有效词数"] = round(token_counter[period_label] / doc_count if doc_count else 0.0, 4)
        documents = period_documents[period_label]
        if not documents:
            raise ValueError(f"{period_label} 无可建模文献。")
        selection_part_rows, selected_topic_count = evaluate_topic_counts(period_label, documents)
        selection_rows.extend(selection_part_rows)
        selected_topic_counts[period_label] = selected_topic_count
        result = fit_period_model(period_label, documents, selected_topic_count)
        log_progress(f"{period_label}：最终模型结果整理完成，主题数={selected_topic_count}")
        fitted_results[period_label] = result
        preprocess_stats[period_label]["词汇表规模"] = int(result["matrix_shape"][1])
        all_topic_keyword_rows.extend(result["topic_keyword_rows"])
        all_topic_strength_rows.extend(result["topic_strength_rows"])
        all_high_freq_rows.extend(result["high_freq_rows"])
        topic_label_map = {str(row["主题编号"]): str(row["主题标签"]) for row in result["topic_strength_rows"]}
        for source_row, topic_index, topic_probability in zip(
            period_source_rows[period_label],
            result["dominant_topics"].tolist(),
            result["dominant_probabilities"].tolist(),
        ):
            topic_id = f"T{int(topic_index) + 1:02d}"
            all_topic_assignment_rows.append(
                {
                    "阶段": period_label,
                    "文献签名": source_row["文献签名"],
                    "年份": source_row["年份"],
                    "来源库": source_row["来源库"],
                    "DOI": source_row["DOI"],
                    "标题": source_row["标题"],
                    "第一作者": source_row["第一作者"],
                    "主导主题编号": topic_id,
                    "主导主题标签": topic_label_map.get(topic_id, ""),
                    "主导主题概率": round(float(topic_probability), 6),
                }
            )

    left_period, middle_period, right_period = [label for label, _, _ in PERIODS]
    log_progress("开始计算相邻阶段主题相似度与演化路径")
    match_12, similarity_rows_12 = match_topics_between_periods(
        left_period,
        middle_period,
        fitted_results[left_period]["topic_vectors"],
        fitted_results[middle_period]["topic_vectors"],
    )
    match_23, similarity_rows_23 = match_topics_between_periods(
        middle_period,
        right_period,
        fitted_results[middle_period]["topic_vectors"],
        fitted_results[right_period]["topic_vectors"],
    )
    topic_strength_by_period = {
        period_label: fitted_results[period_label]["topic_strength_rows"]
        for period_label, _, _ in PERIODS
    }
    path_rows = build_evolution_paths(topic_strength_by_period, match_12, match_23)
    log_progress("主题演化路径计算完成，开始写出结果文件")

    write_csv(output_paths["preprocess_stats"], ["阶段", "年份范围", "阶段文献数", "有摘要文献数", "英文摘要文献数", "中文摘要文献数", "通过领域过滤文献数", "进入建模文献数", "平均有效词数", "词汇表规模"], preprocess_rows)
    write_csv(output_paths["model_selection"], ["阶段", "候选主题数", "样本文献数", "词汇表规模", "困惑度", "一致性得分", "困惑度排名", "一致性排名", "综合排序值", "是否选中"], selection_rows)
    write_csv(output_paths["keyword_distribution"], ["阶段", "主题编号", "主题标签", "主题强度", "平均主题概率", "关键词排名", "关键词", "词项概率"], all_topic_keyword_rows)
    write_csv(output_paths["topic_strength"], ["阶段", "主题编号", "主题标签", "主题文献数", "阶段文献总数", "主题强度", "平均主题概率", "代表关键词"], all_topic_strength_rows)
    write_csv(output_paths["high_frequency_terms"], ["阶段", "词项", "总词频", "文献频次"], all_high_freq_rows)
    write_csv(output_paths["topic_assignment"], ["阶段", "文献签名", "年份", "来源库", "DOI", "标题", "第一作者", "主导主题编号", "主导主题标签", "主导主题概率"], all_topic_assignment_rows)
    write_csv(output_paths["similarity_links"], ["左阶段", "左主题编号", "右阶段", "右主题编号", "主题相似度", "纳入演化路径"], similarity_rows_12 + similarity_rows_23)
    write_csv(output_paths["evolution_paths"], ["演化路径编号", "2011-2015主题编号", "2011-2015主题标签", "2011-2015主题强度", "2016-2020主题编号", "2016-2020主题标签", "2016-2020主题强度", "2021-2025主题编号", "2021-2025主题标签", "2021-2025主题强度"], path_rows)
    plot_topic_intensity(path_rows, output_paths["intensity_curve"], dataset_label)
    write_summary(output_paths["summary"], dataset_label, preprocess_rows, selection_rows, all_topic_strength_rows, path_rows)
    log_progress("主题演化主链写出完成")

    print(f"output_dir={output_dir}")
    for period_label, _, _ in PERIODS:
        print(f"{period_label}\tdocs={len(period_documents[period_label])}\tselected_k={selected_topic_counts[period_label]}")


__all__ = [
    "INPUT_ENCODING",
    "OUTPUT_ENCODING",
    "PERIODS",
    "build_document_signature",
    "build_output_paths",
    "canonicalize_topic_term",
    "clean_presentation_topic_term",
    "compact_text",
    "main",
    "period_label_from_year",
]


if __name__ == "__main__":
    main()
