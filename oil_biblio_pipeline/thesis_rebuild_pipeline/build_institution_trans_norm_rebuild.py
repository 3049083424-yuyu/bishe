from __future__ import annotations

import argparse
import csv
import importlib.util
import math
import re
import sys
import types
import unicodedata
from collections import Counter
from pathlib import Path


PIPELINE_ROOT = Path(__file__).resolve().parent.parent
TRANSLATE_SCRIPT = PIPELINE_ROOT / "translate_normalize_institution_table.py"
REPAIR_SCRIPT = PIPELINE_ROOT / "repair_dual_key_institution_trans_norm.py"

REBUILD_ROOT = Path(r"D:\graduate\thesis_rebuild")
DEFAULT_INPUT_PATH = REBUILD_ROOT / "institution_eval" / "institution_name_table_dual_key_2011_2025.csv"
DEFAULT_OUTPUT_PATH = REBUILD_ROOT / "institution_eval" / "institution_name_table_dual_key_trans_norm_2011_2025.csv"
DEFAULT_REVIEW_PATH = REBUILD_ROOT / "qa" / "institution_trans_norm_review_dual_key_2011_2025.csv"
DEFAULT_NOTE_PATH = REBUILD_ROOT / "institution_eval" / "institution_trans_norm_method_note_2011_2025.txt"
DEFAULT_MANUAL_OVERRIDE_PATH = REBUILD_ROOT / "qa" / "institution_manual_override_rules.csv"
DEFAULT_WIKIDATA_LOOKUP_PATH = REBUILD_ROOT / "qa" / "institution_wikidata_lookup_rules.csv"
DEFAULT_CORPUS_PATH = REBUILD_ROOT / "corpus" / "merged_clean_dual_key_dedup_2011_2025.csv"

INPUT_ENCODING = "utf-8-sig"
OUTPUT_ENCODING = "gb18030"

MANUAL_OVERRIDE_ENCODING_CANDIDATES = ("utf-8-sig", "utf-8", "gb18030")

EXTRA_EXACT_PAIRS: dict[str, tuple[str, str]] = {
    "Chinese Acad Sci": ("中国科学院", "中国科学院"),
    "Univ Chinese Acad Sci": ("中国科学院大学", "中国科学院大学"),
    "China Univ Petr": ("中国石油大学", "中国石油大学（北京）"),
    "China Univ Petr East China": ("中国石油大学（华东）", "中国石油大学（华东）"),
    "China University of Petroleum": ("中国石油大学", "中国石油大学（北京）"),
    "China University of Petroleum(Beijing)": ("中国石油大学（北京）", "中国石油大学（北京）"),
    "China University of Petroleum (Beijing)": ("中国石油大学（北京）", "中国石油大学（北京）"),
    "China University of Petroleum(East China)": ("中国石油大学（华东）", "中国石油大学（华东）"),
    "China University of Petroleum (East China)": ("中国石油大学（华东）", "中国石油大学（华东）"),
    "China Univ Geosci": ("中国地质大学", "中国地质大学"),
    "China Univ Geosci Beijing": ("中国地质大学（北京）", "中国地质大学（北京）"),
    "China Univ Min & Technol": ("中国矿业大学", "中国矿业大学"),
    "China Univ Min & Technol Beijing": ("中国矿业大学（北京）", "中国矿业大学（北京）"),
    "Southwest Petr Univ": ("西南石油大学", "西南石油大学"),
    "Southwest Petroleum University": ("西南石油大学", "西南石油大学"),
    "SINOPEC": ("中国石油化工股份有限公司", "中国石油化工股份有限公司"),
    "PetroChina": ("中国石油天然气股份有限公司", "中国石油天然气股份有限公司"),
    "CNPC": ("中国石油天然气集团有限公司", "中国石油天然气集团有限公司"),
    "CNOOC Research Institute": ("中海油研究总院", "中海油研究总院"),
    "中海油研究总院": ("中海油研究总院", "中海油研究总院"),
    "中国石油勘探开发研究院": ("中国石油勘探开发研究院", "中国石油勘探开发研究院"),
    "中国石化石油化工科学研究院": ("中国石化石油化工科学研究院", "中国石化石油化工科学研究院"),
    "中国石油大学": ("中国石油大学", "中国石油大学（北京）"),
    "中国石油大学石油工程学院": ("中国石油大学石油工程学院", "中国石油大学（北京）"),
    "中国石油大学油气资源与探测国家重点实验室": ("中国石油大学油气资源与探测国家重点实验室", "中国石油大学（北京）"),
    "中国石油大学地球科学与技术学院": ("中国石油大学地球科学与技术学院", "中国石油大学（北京）"),
    "Research Institute of Petroleum Exploration and Development": ("中国石油勘探开发研究院", "中国石油勘探开发研究院"),
    "Research Institute of Petroleum Exploration & Development": ("中国石油勘探开发研究院", "中国石油勘探开发研究院"),
    "PetroChina Research Institute of Petroleum Exploration & Development": ("中国石油勘探开发研究院", "中国石油勘探开发研究院"),
    "PetroChina Res Inst Petr Explorat & Dev": ("中国石油勘探开发研究院", "中国石油勘探开发研究院"),
    "Research Institute of Exploration and Development": ("中国石油勘探开发研究院", "中国石油勘探开发研究院"),
    "Research Institute of Petroleum Processing": ("中国石油化工股份有限公司石油化工科学研究院", "中国石油化工股份有限公司石油化工科学研究院"),
    "Minist Educ": ("中国教育部", "中国教育部"),
    "MIT": ("麻省理工学院", "麻省理工学院"),
    "UCL": ("伦敦大学学院", "伦敦大学学院"),
    "Sichuan Univ": ("四川大学", "四川大学"),
    "Russian Acad Sci": ("俄罗斯科学院", "俄罗斯科学院"),
    "Nanjing Tech Univ": ("南京工业大学", "南京工业大学"),
    "Cent South Univ": ("中南大学", "中南大学"),
    "Cent S Univ": ("中南大学", "中南大学"),
    "North China Elect Power Univ": ("华北电力大学", "华北电力大学"),
    "Peking Univ": ("北京大学", "北京大学"),
    "Tech Univ Denmark": ("丹麦技术大学", "丹麦技术大学"),
    "Hong Kong Polytech Univ": ("香港理工大学", "香港理工大学"),
    "Nanyang Technol Univ": ("南洋理工大学", "南洋理工大学"),
    "Delft Univ Technol": ("代尔夫特理工大学", "代尔夫特理工大学"),
    "China University of Geosciences(Beijing)": ("中国地质大学（北京）", "中国地质大学（北京）"),
    "China University of Geosciences (Beijing)": ("中国地质大学（北京）", "中国地质大学（北京）"),
    "CNRS": ("法国国家科学研究中心", "法国国家科学研究中心"),
    "CNR": ("意大利国家研究委员会", "意大利国家研究委员会"),
    "CSIC": ("西班牙国家研究委员会", "西班牙国家研究委员会"),
    "Texas A&M Univ": ("得克萨斯农工大学", "得克萨斯农工大学"),
    "Zhejiang Sci Tech Univ": ("浙江理工大学", "浙江理工大学"),
    "Northeastern University": ("东北大学（美国）", "东北大学（美国）"),
    "Tohoku University": ("东北大学（日本）", "东北大学（日本）"),
    "Northwestern Univ": ("西北大学（美国）", "西北大学（美国）"),
    "Northwestern University": ("西北大学（美国）", "西北大学（美国）"),
    "Newcastle Univ": ("纽卡斯尔大学（英国）", "纽卡斯尔大学（英国）"),
    "Univ Newcastle": ("纽卡斯尔大学（澳大利亚）", "纽卡斯尔大学（澳大利亚）"),
    "Queens Univ": ("女王大学（加拿大）", "女王大学（加拿大）"),
    "Queens Univ Belfast": ("贝尔法斯特女王大学", "贝尔法斯特女王大学"),
    "Natl Inst Technol": ("美国国家标准与技术研究院", "美国国家标准与技术研究院"),
    "Inst Chem Technol": ("化学技术学院", "化学技术学院"),
    "Univ Stavanger": ("斯塔万格大学", "斯塔万格大学"),
    "Abo Akad Univ": ("奥博学术大学", "奥博学术大学"),
    "Lawrence Berkeley Natl Lab": ("劳伦斯伯克利国家实验室", "劳伦斯伯克利国家实验室"),
    "Ernest Orlando Lawrence Berkeley Natl Lab": ("劳伦斯伯克利国家实验室", "劳伦斯伯克利国家实验室"),
    "KTH Royal Inst Technol": ("瑞典皇家理工学院", "瑞典皇家理工学院"),
    "Univ Tennessee": ("田纳西大学", "田纳西大学"),
    "Cardiff Univ": ("卡迪夫大学", "卡迪夫大学"),
    "Univ Zaragoza": ("萨拉戈萨大学", "萨拉戈萨大学"),
    "Univ Fed Rio de Janeiro": ("里约热内卢联邦大学", "里约热内卢联邦大学"),
    "Univ Hong Kong": ("香港大学", "香港大学"),
    "Ohio State Univ": ("俄亥俄州立大学", "俄亥俄州立大学"),
    "Missouri Univ Sci & Technol": ("密苏里科技大学", "密苏里科技大学"),
    "Anhui Univ Technol": ("安徽工业大学", "安徽工业大学"),
    "Univ Naples Federico II": ("那不勒斯费德里科二世大学", "那不勒斯费德里科二世大学"),
    "Korea Inst Energy Res": ("韩国能源研究院", "韩国能源研究院"),
    "Univ Sheffield": ("谢菲尔德大学", "谢菲尔德大学"),
    "China Agr Univ": ("中国农业大学", "中国农业大学"),
    "Tiangong Univ": ("天津工业大学", "天津工业大学"),
    "Jiangsu Univ Sci & Technol": ("江苏科技大学", "江苏科技大学"),
    "Univ Delaware": ("特拉华大学", "特拉华大学"),
    "Univ Oxford": ("牛津大学", "牛津大学"),
    "Shanghai Univ Elect Power": ("上海电力大学", "上海电力大学"),
    "Univ Aberdeen": ("阿伯丁大学", "阿伯丁大学"),
    "Univ Politecn Valencia": ("瓦伦西亚理工大学", "瓦伦西亚理工大学"),
    "Washington State Univ": ("华盛顿州立大学", "华盛顿州立大学"),
    "Inst Mexicano Petr": ("墨西哥石油研究所", "墨西哥石油研究所"),
    "Yanshan Univ": ("燕山大学", "燕山大学"),
    "Indian Inst Technol Madras": ("印度理工学院马德拉斯分校", "印度理工学院马德拉斯分校"),
    "McGill Univ": ("麦吉尔大学", "麦吉尔大学"),
    "Univ Lisbon": ("里斯本大学", "里斯本大学"),
    "Collaborat Innovat Ctr Chem Sci & Engn Tianjin": ("天津化学科学与工程协同创新中心", "天津化学科学与工程协同创新中心"),
    "Univ Nacl Autonoma Mexico": ("墨西哥国立自治大学", "墨西哥国立自治大学"),
    "Indian Inst Technol Guwahati": ("印度理工学院古瓦哈提分校", "印度理工学院古瓦哈提分校"),
    "Iran Univ Sci & Technol": ("伊朗科学技术大学", "伊朗科学技术大学"),
    "Univ Castilla La Mancha": ("卡斯蒂利亚-拉曼查大学", "卡斯蒂利亚-拉曼查大学"),
    "Univ Edinburgh": ("爱丁堡大学", "爱丁堡大学"),
    "Rice Univ": ("莱斯大学", "莱斯大学"),
    "Arizona State Univ": ("亚利桑那州立大学", "亚利桑那州立大学"),
    "Argonne Natl Lab": ("阿贡国家实验室", "阿贡国家实验室"),
    "Korea Inst Sci & Technol": ("韩国科学技术研究院", "韩国科学技术研究院"),
    "Univ Twente": ("特文特大学", "特文特大学"),
    "Kyung Hee Univ": ("庆熙大学", "庆熙大学"),
    "Univ Sains Malaysia": ("马来西亚理科大学", "马来西亚理科大学"),
    "Yeungnam Univ": ("岭南大学（韩国）", "岭南大学（韩国）"),
    "Pusan Natl Univ": ("釜山国立大学", "釜山国立大学"),
    "Kazan Fed Univ": ("喀山联邦大学", "喀山联邦大学"),
    "South China Normal Univ": ("华南师范大学", "华南师范大学"),
    "Cranfield Univ": ("克兰菲尔德大学", "克兰菲尔德大学"),
    "Univ Maryland": ("马里兰大学", "马里兰大学"),
    "RMIT Univ": ("皇家墨尔本理工大学", "皇家墨尔本理工大学"),
    "Wuhan Text Univ": ("武汉纺织大学", "武汉纺织大学"),
    "Shandong Univ Technol": ("山东理工大学", "山东理工大学"),
    "South China Agr Univ": ("华南农业大学", "华南农业大学"),
    "Chonnam Natl Univ": ("全南国立大学", "全南国立大学"),
    "Cornell Univ": ("康奈尔大学", "康奈尔大学"),
    "Northwest A&F Univ": ("西北农林科技大学", "西北农林科技大学"),
    "Univ Groningen": ("格罗宁根大学", "格罗宁根大学"),
    "Norwegian Univ Sci & Technol NTNU": ("挪威科技大学", "挪威科技大学"),
    "Univ Western Ontario": ("西安大略大学", "西安大略大学"),
    "Pacific Northwest Natl Lab": ("太平洋西北国家实验室", "太平洋西北国家实验室"),
    "Southwest Univ": ("西南大学", "西南大学"),
    "Changsha Univ Sci & Technol": ("长沙理工大学", "长沙理工大学"),
    "Univ Seville": ("塞维利亚大学", "塞维利亚大学"),
    "PetroChina Changqing Oilfield Co": ("中国石油长庆油田公司", "中国石油长庆油田公司"),
    "Louisiana State Univ": ("路易斯安那州立大学", "路易斯安那州立大学"),
    "Hubei Univ": ("湖北大学", "湖北大学"),
    "Fujian Normal Univ": ("福建师范大学", "福建师范大学"),
    "Univ Tulsa": ("塔尔萨大学", "塔尔萨大学"),
    "China Natl Petr Corp": ("中国石油天然气集团有限公司", "中国石油天然气集团有限公司"),
    "Qingdao Natl Lab Marine Sci & Technol": ("青岛海洋科学与技术试点国家实验室", "青岛海洋科学与技术试点国家实验室"),
    "Univ Utrecht": ("乌得勒支大学", "乌得勒支大学"),
    "North Univ China": ("中北大学", "中北大学"),
    "Princeton Univ": ("普林斯顿大学", "普林斯顿大学"),
    "King Abdullah Univ Sci & Technol KAUST": ("阿卜杜拉国王科技大学", "阿卜杜拉国王科技大学"),
    "Southwest Univ Sci & Technol": ("西南科技大学", "西南科技大学"),
    "Tech Univ Berlin": ("柏林工业大学", "柏林工业大学"),
    "Zhejiang Normal Univ": ("浙江师范大学", "浙江师范大学"),
    "Aarhus Univ": ("奥胡斯大学", "奥胡斯大学"),
    "Anna Univ": ("安娜大学", "安娜大学"),
    "China Jiliang Univ": ("中国计量大学", "中国计量大学"),
    "Univ KwaZulu Natal": ("夸祖鲁-纳塔尔大学", "夸祖鲁-纳塔尔大学"),
    "Univ Saskatchewan": ("萨斯喀彻温大学", "萨斯喀彻温大学"),
    "Univ Padua": ("帕多瓦大学", "帕多瓦大学"),
    "Hiroshima Univ": ("广岛大学", "广岛大学"),
    "Anhui Univ": ("安徽大学", "安徽大学"),
    "Sandia Natl Labs": ("桑迪亚国家实验室", "桑迪亚国家实验室"),
    "Huaqiao Univ": ("华侨大学", "华侨大学"),
    "Inha Univ": ("仁荷大学", "仁荷大学"),
    "Minist Nat Resources": ("中国自然资源部", "中国自然资源部"),
    "Shanxi Univ": ("山西大学", "山西大学"),
    "Aristotle Univ Thessaloniki": ("塞萨洛尼基亚里士多德大学", "塞萨洛尼基亚里士多德大学"),
    "Chinese Univ Hong Kong": ("香港中文大学", "香港中文大学"),
    "Natl Taiwan Univ Sci & Technol": ("国立台湾科技大学", "国立台湾科技大学"),
    "Indian Inst Technol Roorkee": ("印度理工学院鲁尔基分校", "印度理工学院鲁尔基分校"),
    "Isfahan Univ Technol": ("伊斯法罕理工大学", "伊斯法罕理工大学"),
    "Malardalen Univ": ("马拉达伦大学", "马拉达伦大学"),
    "Univ Basque Country": ("巴斯克大学", "巴斯克大学"),
    "Polish Acad Sci": ("波兰科学院", "波兰科学院"),
    "Michigan State Univ": ("密歇根州立大学", "密歇根州立大学"),
    "Nagoya Univ": ("名古屋大学", "名古屋大学"),
    "Queensland Univ Technol": ("昆士兰科技大学", "昆士兰科技大学"),
    "Ufa State Petr Technol Univ": ("乌法国立石油技术大学", "乌法国立石油技术大学"),
    "Beijing Jiaotong Univ": ("北京交通大学", "北京交通大学"),
    "Univ Bologna": ("博洛尼亚大学", "博洛尼亚大学"),
    "Institute of Geology and Geophysics": ("地质与地球物理研究所", "地质与地球物理研究所"),
    "Tech Univ Darmstadt": ("达姆施塔特工业大学", "达姆施塔特工业大学"),
    "Univ Kansas": ("堪萨斯大学", "堪萨斯大学"),
    "China Acad Engn Phys": ("中国工程物理研究院", "中国工程物理研究院"),
    "Univ Barcelona": ("巴塞罗那大学", "巴塞罗那大学"),
    "Shaanxi Normal Univ": ("陕西师范大学", "陕西师范大学"),
    "Univ Calif Davis": ("加州大学戴维斯分校", "加州大学戴维斯分校"),
    "Univ Kentucky": ("肯塔基大学", "肯塔基大学"),
    "McMaster Univ": ("麦克马斯特大学", "麦克马斯特大学"),
    "Univ Surrey": ("萨里大学", "萨里大学"),
    "Yantai Univ": ("烟台大学", "烟台大学"),
    "Sinopec Research Institute of Petroleum Engineering": ("中石化石油工程技术研究院", "中石化石油工程技术研究院"),
    "SINOPEC Research Institute of Petroleum Engineering": ("中石化石油工程技术研究院", "中石化石油工程技术研究院"),
    "Sinopec Research Institute of Petroleum Engineering Co": ("中石化石油工程技术研究院有限公司", "中石化石油工程技术研究院有限公司"),
    "SINOPEC Research Institute of Petroleum Engineering Co": ("中石化石油工程技术研究院有限公司", "中石化石油工程技术研究院有限公司"),
    "Sinopec Research Institute of Petroleum Engineering Technology": ("中石化石油工程技术研究院有限公司", "中石化石油工程技术研究院有限公司"),
    "SINOPEC Research Institute of Petroleum Engineering Technology": ("中石化石油工程技术研究院有限公司", "中石化石油工程技术研究院有限公司"),
    "中国石油大学石油工程教育部重点实验室": ("石油工程教育部重点实验室", "石油工程教育部重点实验室"),
    "MOE Key Laboratory of Petroleum Engineering": ("石油工程教育部重点实验室", "石油工程教育部重点实验室"),
    "中国石化石油勘探开发研究院": ("中国石化石油勘探开发研究院", "中国石油化工股份有限公司"),
    "中国石油化工股份有限公司石油勘探开发研究院": ("中国石化石油勘探开发研究院", "中国石油化工股份有限公司"),
    "Research Institute of Petroleum Exploration and Production": ("中国石化石油勘探开发研究院", "中国石油化工股份有限公司"),
    "Sinopec Research Institute of Petroleum Exploration and Production": ("中国石化石油勘探开发研究院", "中国石油化工股份有限公司"),
    "SINOPEC Research Institute of Petroleum Exploration and Production": ("中国石化石油勘探开发研究院", "中国石油化工股份有限公司"),
    "Wuxi Research Institute of Petroleum Geology": ("无锡石油地质研究所", "中国石油化工股份有限公司"),
    "中国石油勘探开发研究院": ("中国石油勘探开发研究院", "中国石油天然气股份有限公司"),
    "Exploration and Development Research Institute": ("中国石油勘探开发研究院", "中国石油天然气股份有限公司"),
    "Research Institute of Petroleum Exploration and Development": ("中国石油勘探开发研究院", "中国石油天然气股份有限公司"),
    "Research Institute of Petroleum Exploration & Development": ("中国石油勘探开发研究院", "中国石油天然气股份有限公司"),
    "PetroChina Research Institute of Petroleum Exploration and Development": ("中国石油勘探开发研究院", "中国石油天然气股份有限公司"),
    "PetroChina Research Institute of Petroleum Exploration & Development": ("中国石油勘探开发研究院", "中国石油天然气股份有限公司"),
    "PetroChina Res Inst Petr Explorat & Dev": ("中国石油勘探开发研究院", "中国石油天然气股份有限公司"),
    "CNPC Research Institute of Petroleum Exploration and Development": ("中国石油勘探开发研究院", "中国石油天然气股份有限公司"),
    "Sinopec Research Institute of Petroleum Exploration and Development": ("中国石化石油勘探开发研究院", "中国石油化工股份有限公司"),
    "SINOPEC Research Institute of Petroleum Exploration and Development": ("中国石化石油勘探开发研究院", "中国石油化工股份有限公司"),
    "Res Inst Petr Explorat & Dev": ("中国石油勘探开发研究院", "中国石油天然气股份有限公司"),
    "PetroChina Southwest Oil & Gas Field Co": ("中国石油西南油气田公司", "中国石油西南油气田公司"),
    "Shengli Oilfield Company": ("中国石化胜利油田分公司", "中国石化胜利油田分公司"),
    "Chungnam Natl Univ": ("忠南国立大学", "忠南国立大学"),
    "North Carolina State Univ": ("北卡罗来纳州立大学", "北卡罗来纳州立大学"),
    "Univ Bath": ("巴斯大学", "巴斯大学"),
    "Chinese Res Inst Environm Sci": ("中国环境科学研究院", "中国环境科学研究院"),
    "Chinese Acad Agr Sci": ("中国农业科学院", "中国农业科学院"),
    "Liaocheng Univ": ("聊城大学", "聊城大学"),
    "Univ Ottawa": ("渥太华大学", "渥太华大学"),
    "Egyptian Petr Res Inst": ("埃及石油研究院", "埃及石油研究院"),
    "Texas Tech Univ": ("得克萨斯理工大学", "得克萨斯理工大学"),
    "Univ Cincinnati": ("辛辛那提大学", "辛辛那提大学"),
    "Univ Perugia": ("佩鲁贾大学", "佩鲁贾大学"),
    "Lanzhou Univ Technol": ("兰州理工大学", "兰州理工大学"),
    "Univ Calif Los Angeles": ("加州大学洛杉矶分校", "加州大学洛杉矶分校"),
    "Natl Tsing Hua Univ": ("国立清华大学", "国立清华大学"),
    "Bhabha Atom Res Ctr": ("巴巴原子研究中心", "巴巴原子研究中心"),
    "West Virginia Univ": ("西弗吉尼亚大学", "西弗吉尼亚大学"),
    "Nat Resources Canada": ("加拿大自然资源部", "加拿大自然资源部"),
    "Graz Univ Technol": ("格拉茨工业大学", "格拉茨工业大学"),
    "Edith Cowan Univ": ("伊迪斯科文大学", "伊迪斯科文大学"),
    "Cairo Univ": ("开罗大学", "开罗大学"),
    "Univ Stuttgart": ("斯图加特大学", "斯图加特大学"),
    "Auburn Univ": ("奥本大学", "奥本大学"),
    "Nanchang Hangkong Univ": ("南昌航空大学", "南昌航空大学"),
    "Qatar Univ": ("卡塔尔大学", "卡塔尔大学"),
    "Univ Wollongong": ("卧龙岗大学", "卧龙岗大学"),
    "Columbia Univ": ("哥伦比亚大学", "哥伦比亚大学"),
    "Univ Nacl Colombia": ("哥伦比亚国立大学", "哥伦比亚国立大学"),
    "Dalian Polytech Univ": ("大连工业大学", "大连工业大学"),
    "Heilongjiang Univ": ("黑龙江大学", "黑龙江大学"),
    "Northeast Forestry Univ": ("东北林业大学", "东北林业大学"),
    "Dalian Maritime Univ": ("大连海事大学", "大连海事大学"),
    "Northeast Normal Univ": ("东北师范大学", "东北师范大学"),
    "Haihe Lab Sustainable Chem Transformat": ("海河实验室", "海河实验室"),
    "Novosibirsk State Univ": ("新西伯利亚国立大学", "新西伯利亚国立大学"),
    "Sejong Univ": ("世宗大学", "世宗大学"),
    "Xian Univ Technol": ("西安工业大学", "西安工业大学"),
    "Dongguan Univ Technol": ("东莞理工学院", "东莞理工学院"),
    "Khalifa Univ Sci & Technol": ("哈利法科技大学", "哈利法科技大学"),
    "Umea Univ": ("于默奥大学", "于默奥大学"),
    "Univ Belgrade": ("贝尔格莱德大学", "贝尔格莱德大学"),
    "Univ Liverpool": ("利物浦大学", "利物浦大学"),
    "Huazhong Agr Univ": ("华中农业大学", "华中农业大学"),
    "Deakin Univ": ("迪肯大学", "迪肯大学"),
    "Univ Putra Malaysia": ("马来西亚博特拉大学", "马来西亚博特拉大学"),
    "Indian Inst Technol Kharagpur": ("印度理工学院卡拉格普尔分校", "印度理工学院卡拉格普尔分校"),
    "Tech Univ Dresden": ("德累斯顿工业大学", "德累斯顿工业大学"),
    "Max Planck Inst Dynam Complex Tech Syst": ("马克斯普朗克复杂技术系统动力学研究所", "马克斯普朗克复杂技术系统动力学研究所"),
    "Petrobras SA": ("巴西国家石油公司", "巴西国家石油公司"),
    "Ruhr Univ Bochum": ("鲁尔大学波鸿", "鲁尔大学波鸿"),
    "Wenzhou Univ": ("温州大学", "温州大学"),
    "Univ Massachusetts": ("马萨诸塞大学", "马萨诸塞大学"),
    "Sultan Qaboos Univ": ("苏丹卡布斯大学", "苏丹卡布斯大学"),
    "Tianjin Univ Technol": ("天津理工大学", "天津理工大学"),
    "Khalifa Univ": ("哈利法大学", "哈利法大学"),
    "Rutgers State Univ": ("罗格斯大学", "罗格斯大学"),
    "Jiangxi Normal Univ": ("江西师范大学", "江西师范大学"),
    "Univ Oslo": ("奥斯陆大学", "奥斯陆大学"),
    "Guilin Univ Technol": ("桂林理工大学", "桂林理工大学"),
    "Northeast Elect Power Univ": ("东北电力大学", "东北电力大学"),
    "Univ Pisa": ("比萨大学", "比萨大学"),
    "Univ Laval": ("拉瓦尔大学", "拉瓦尔大学"),
    "Chinese Acad Forestry": ("中国林业科学研究院", "中国林业科学研究院"),
    "Univ Florida": ("佛罗里达大学", "佛罗里达大学"),
    "Indian Inst Sci": ("印度科学研究院", "印度科学研究院"),
    "Indian Sch Mines": ("印度矿业学院", "印度矿业学院"),
    "Univ Basque Country UPV EHU": ("巴斯克大学", "巴斯克大学"),
    "Hebei Univ": ("河北大学", "河北大学"),
    "Ferdowsi Univ Mashhad": ("菲尔多西大学", "菲尔多西大学"),
    "Liaoning Shihua University": ("辽宁石油化工大学", "辽宁石油化工大学"),
    "Univ Lyon": ("里昂大学", "里昂大学"),
    "Univ Colorado": ("科罗拉多大学", "科罗拉多大学"),
    "Yancheng Inst Technol": ("盐城工学院", "盐城工学院"),
    "Qingdao Univ Technol": ("青岛理工大学", "青岛理工大学"),
    "Fujian Agr & Forestry Univ": ("福建农林大学", "福建农林大学"),
    "Curtin Univ Technol": ("科廷大学", "科廷大学"),
    "Univ Connecticut": ("康涅狄格大学", "康涅狄格大学"),
    "CSIRO Energy": ("澳大利亚联邦科学与工业研究组织能源部", "澳大利亚联邦科学与工业研究组织"),
    "Univ Cordoba": ("科尔多瓦大学", "科尔多瓦大学"),
    "China Univ Petr Beijing Karamay": ("中国石油大学（北京）克拉玛依校区", "中国石油大学（北京）"),
    "Univ Salerno": ("萨莱诺大学", "萨莱诺大学"),
    "Chung Ang Univ": ("中央大学（韩国）", "中央大学（韩国）"),
    "Mem Univ Newfoundland": ("纽芬兰纪念大学", "纽芬兰纪念大学"),
    "Univ Coimbra": ("科英布拉大学", "科英布拉大学"),
    "Univ Bergen": ("卑尔根大学", "卑尔根大学"),
    "Osaka Univ": ("大阪大学", "大阪大学"),
    "Nanjing Univ Informat Sci & Technol": ("南京信息工程大学", "南京信息工程大学"),
    "Univ Tabriz": ("大不里士大学", "大不里士大学"),
    "Chevron Energy Technol Co": ("雪佛龙能源技术公司", "雪佛龙能源技术公司"),
    "Inst Politecn Nacl": ("墨西哥国立理工学院", "墨西哥国立理工学院"),
    "Otto von Guericke Univ": ("奥托冯格里克大学马格德堡", "奥托冯格里克大学马格德堡"),
    "Univ Napoli Federico II": ("那不勒斯费德里科二世大学", "那不勒斯费德里科二世大学"),
    "Jiangxi Univ Sci & Technol": ("江西理工大学", "江西理工大学"),
    "Tallinn Univ Technol": ("塔林理工大学", "塔林理工大学"),
    "Univ Fed Santa Catarina": ("圣卡塔琳娜联邦大学", "圣卡塔琳娜联邦大学"),
    "Univ Fed Rio Grande do Sul": ("南里奥格兰德联邦大学", "南里奥格兰德联邦大学"),
    "Pohang Univ Sci & Technol POSTECH": ("浦项科技大学", "浦项科技大学"),
    "Tokyo Univ Sci": ("东京理科大学", "东京理科大学"),
    "Univ Durham": ("杜伦大学", "杜伦大学"),
    "Chengdu Univ": ("成都大学", "成都大学"),
    "Univ Politecn Cataluna": ("加泰罗尼亚理工大学", "加泰罗尼亚理工大学"),
    "Jeonbuk Natl Univ": ("全北国立大学", "全北国立大学"),
    "Istanbul Tech Univ": ("伊斯坦布尔工业大学", "伊斯坦布尔工业大学"),
    "Univ Calabria": ("卡拉布里亚大学", "卡拉布里亚大学"),
    "Univ Alicante": ("阿利坎特大学", "阿利坎特大学"),
    "Univ Alabama": ("阿拉巴马大学", "阿拉巴马大学"),
    "Univ Auckland": ("奥克兰大学", "奥克兰大学"),
    "Florida State Univ": ("佛罗里达州立大学", "佛罗里达州立大学"),
    "Hebei Univ Sci & Technol": ("河北科技大学", "河北科技大学"),
    "Dow Chem Co USA": ("美国陶氏化学公司", "美国陶氏化学公司"),
    "Univ New S Wales": ("新南威尔士大学", "新南威尔士大学"),
    "AGH Univ Sci & Technol": ("AGH科技大学", "AGH科技大学"),
    "Korea Res Inst Chem Technol": ("韩国化学技术研究院", "韩国化学技术研究院"),
    "Univ Calif Riverside": ("加州大学河滨分校", "加州大学河滨分校"),
    "Harbin Inst Technol Shenzhen": ("哈尔滨工业大学（深圳）", "哈尔滨工业大学"),
    "China University of Geosciences": ("中国地质大学", "中国地质大学"),
    "Univ Sci & Technol": ("科学技术大学", "科学技术大学"),
    "Guangdong Prov Key Lab New & Renewable Energy Res": ("广东省新能源与可再生能源研究重点实验室", "广东省新能源与可再生能源研究重点实验室"),
    "AGH Univ Sci & Technol": ("克拉科夫矿业冶金大学", "克拉科夫矿业冶金大学"),
    "Natl Taipei Univ Technol": ("台北科技大学", "台北科技大学"),
    "Qingyuan Innovat Lab": ("清源创新实验室", "清源创新实验室"),
    "Pukyong Natl Univ": ("釜庆国立大学", "釜庆国立大学"),
    "Indian Inst Technol Kanpur": ("印度理工学院坎普尔分校", "印度理工学院坎普尔分校"),
    "Hangzhou Dianzi Univ": ("杭州电子科技大学", "杭州电子科技大学"),
    "Uppsala Univ": ("乌普萨拉大学", "乌普萨拉大学"),
    "Cent South Univ Forestry & Technol": ("中南林业科技大学", "中南林业科技大学"),
    "SRM Inst Sci & Technol": ("SRM理工学院", "SRM理工学院"),
    "Natl Chung Hsing Univ": ("国立中兴大学", "国立中兴大学"),
    "Univ Strathclyde": ("斯特拉斯克莱德大学", "斯特拉斯克莱德大学"),
    "Chinese Acad Geol Sci": ("中国地质科学院", "中国地质科学院"),
    "Inst Zhejiang Univ Quzhou": ("浙江大学衢州研究院", "浙江大学"),
    "Univ Complutense Madrid": ("马德里康普顿斯大学", "马德里康普顿斯大学"),
    "Hangzhou Normal Univ": ("杭州师范大学", "杭州师范大学"),
    "Univ Witwatersrand": ("威特沃特斯兰德大学", "威特沃特斯兰德大学"),
    "Lomonosov Moscow State Univ": ("莫斯科国立大学", "莫斯科国立大学"),
    "Univ Paris Saclay": ("巴黎萨克雷大学", "巴黎萨克雷大学"),
    "Northwest Normal Univ": ("西北师范大学", "西北师范大学"),
    "Univ Georgia": ("佐治亚大学", "佐治亚大学"),
    "Univ Pittsburgh": ("匹兹堡大学", "匹兹堡大学"),
    "PetroChina Tarim Oilfield Company": ("中国石油塔里木油田公司", "中国石油塔里木油田公司"),
    "Virginia Tech": ("弗吉尼亚理工大学", "弗吉尼亚理工大学"),
    "Univ Macau": ("澳门大学", "澳门大学"),
    "Univ Warwick": ("华威大学", "华威大学"),
    "Univ Montpellier": ("蒙彼利埃大学", "蒙彼利埃大学"),
    "Univ New Brunswick": ("新不伦瑞克大学", "新不伦瑞克大学"),
    "Chung Yuan Christian Univ": ("中原大学", "中原大学"),
    "Inner Mongolia Univ": ("内蒙古大学", "内蒙古大学"),
    "Univ South China": ("南华大学", "南华大学"),
    "Yale Univ": ("耶鲁大学", "耶鲁大学"),
    "Tianjin Polytech Univ": ("天津工业大学", "天津工业大学"),
    "Univ Granada": ("格拉纳达大学", "格拉纳达大学"),
    "Sahand Univ Technol": ("萨汉德理工大学", "萨汉德理工大学"),
    "Technion Israel Inst Technol": ("以色列理工学院", "以色列理工学院"),
    "Konkuk Univ": ("建国大学（韩国）", "建国大学（韩国）"),
    "Silesian Tech Univ": ("西里西亚理工大学", "西里西亚理工大学"),
    "Northeast Agr Univ": ("东北农业大学", "东北农业大学"),
    "Univ Politecn Madrid": ("马德里理工大学", "马德里理工大学"),
    "Shaoxing Univ": ("绍兴文理学院", "绍兴文理学院"),
    "Wenzhou Med Univ": ("温州医科大学", "温州医科大学"),
    "East China Univ Technol": ("东华理工大学", "东华理工大学"),
    "Univ Pretoria": ("比勒陀利亚大学", "比勒陀利亚大学"),
    "Univ Zagreb": ("萨格勒布大学", "萨格勒布大学"),
    "Univ Fed Minas Gerais": ("米纳斯吉拉斯联邦大学", "米纳斯吉拉斯联邦大学"),
    "Univ Copenhagen": ("哥本哈根大学", "哥本哈根大学"),
    "Chonbuk Natl Univ": ("全北国立大学", "全北国立大学"),
    "Univ York": ("约克大学", "约克大学"),
    "Liaoning Univ": ("辽宁大学", "辽宁大学"),
    "Renmin Univ China": ("中国人民大学", "中国人民大学"),
    "Homi Bhabha Natl Inst": ("霍米巴巴国立研究院", "霍米巴巴国立研究院"),
    "Univ Pau & Pays Adour": ("波城与阿杜尔地区大学", "波城与阿杜尔地区大学"),
    "Res Inst Petr Ind": ("石油工业研究院", "石油工业研究院"),
    "Zhengzhou Univ Light Ind": ("郑州轻工业大学", "郑州轻工业大学"),
    "Inner Mongolia Univ Technol": ("内蒙古工业大学", "内蒙古工业大学"),
    "Univ Lille": ("里尔大学", "里尔大学"),
    "Univ Antwerp": ("安特卫普大学", "安特卫普大学"),
    "Univ Coll Dublin": ("都柏林大学学院", "都柏林大学学院"),
    "PetroChina Tarim Oilfield Co": ("中国石油塔里木油田公司", "中国石油塔里木油田公司"),
    "Univ Patras": ("帕特雷大学", "帕特雷大学"),
    "Shanghai Univ Engn Sci": ("上海工程技术大学", "上海工程技术大学"),
    "PetroChina Xinjiang Oilfield Co": ("中国石油新疆油田公司", "中国石油新疆油田公司"),
    "China Univ Geosci Wuhan": ("中国地质大学（武汉）", "中国地质大学（武汉）"),
    "North China Univ Sci & Technol": ("华北理工大学", "华北理工大学"),
    "Univ Seoul": ("首尔市立大学", "首尔市立大学"),
    "Univ Valladolid": ("巴利亚多利德大学", "巴利亚多利德大学"),
    "Swinburne Univ Technol": ("斯威本科技大学", "斯威本科技大学"),
    "Oklahoma State Univ": ("俄克拉荷马州立大学", "俄克拉荷马州立大学"),
    "CNOOC China Ltd": ("中海石油（中国）有限公司", "中海石油（中国）有限公司"),
    "CNPC Engn Technol R&D Co Ltd": ("中国石油集团工程技术研究有限公司", "中国石油集团工程技术研究有限公司"),
    "Univ Autonoma Madrid": ("马德里自治大学", "马德里自治大学"),
    "Korea Adv Inst Sci & Technol KAIST": ("韩国科学技术院", "韩国科学技术院"),
    "Southern Med Univ": ("南方医科大学", "南方医科大学"),
    "Duy Tan Univ": ("维新大学", "维新大学"),
    "Australian Natl Univ": ("澳大利亚国立大学", "澳大利亚国立大学"),
    "Schlumberger Doll Res Ctr": ("斯伦贝谢多尔研究中心", "斯伦贝谢多尔研究中心"),
    "Univ Sci & Technol Liaoning": ("辽宁科技大学", "辽宁科技大学"),
}

EXTRA_CANONICAL_TRANSLATIONS: dict[str, str] = {
    "chinese academy sciences": "中国科学院",
    "university chinese academy sciences": "中国科学院大学",
    "tsinghua university": "清华大学",
    "tianjin university": "天津大学",
    "zhejiang university": "浙江大学",
    "xi an jiao tong university": "西安交通大学",
    "xian jiao tong university": "西安交通大学",
    "shanghai jiao tong university": "上海交通大学",
    "huazhong university science technology": "华中科技大学",
    "beijing university chemical technology": "北京化工大学",
    "dalian university technology": "大连理工大学",
    "east china university science technology": "华东理工大学",
    "e china university science technology": "华东理工大学",
    "harbin institute technology": "哈尔滨工业大学",
    "university science technology china": "中国科学技术大学",
    "southeast university": "东南大学",
    "chongqing university": "重庆大学",
    "south china university technology": "华南理工大学",
    "s china university technology": "华南理工大学",
    "tongji university": "同济大学",
    "taiyuan university technology": "太原理工大学",
    "shandong university": "山东大学",
    "jiangsu university": "江苏大学",
    "national university singapore": "新加坡国立大学",
    "nanjing tech university": "南京工业大学",
    "hunan university": "湖南大学",
    "beijing institute technology": "北京理工大学",
    "jilin university": "吉林大学",
    "central south university": "中南大学",
    "zhengzhou university": "郑州大学",
    "north china electric power university": "华北电力大学",
    "nanjing university": "南京大学",
    "hong kong polytechnic university": "香港理工大学",
    "university science technology beijing": "北京科技大学",
    "sun yat sen university": "中山大学",
    "guangdong university technology": "广东工业大学",
    "zhejiang university technology": "浙江工业大学",
    "nanjing forestry university": "南京林业大学",
    "qingdao university science technology": "青岛科技大学",
    "nanjing university science technology": "南京理工大学",
    "chengdu university technology": "成都理工大学",
    "shandong university science technology": "山东科技大学",
    "xiamen university": "厦门大学",
    "penn state university": "宾夕法尼亚州立大学",
    "fuzhou university": "福州大学",
    "nankai university": "南开大学",
    "guangxi university": "广西大学",
    "kunming university science technology": "昆明理工大学",
    "imperial college london": "帝国理工学院",
    "wuhan university": "武汉大学",
    "wuhan university technology": "武汉理工大学",
    "northeastern university": "东北大学（美国）",
    "shenzhen university": "深圳大学",
    "georgia institute technology": "佐治亚理工学院",
    "king fahd university petroleum minerals": "法赫德国王石油与矿业大学",
    "city university hong kong": "香港城市大学",
    "monash university": "蒙纳士大学",
    "jiangnan university": "江南大学",
    "northeast petroleum university": "东北石油大学",
    "hebei university technology": "河北工业大学",
    "soochow university": "苏州大学",
    "beijing university technology": "北京工业大学",
    "yangtze university": "长江大学",
    "northwestern polytechnical university": "西北工业大学",
    "fudan university": "复旦大学",
    "king saud university": "沙特国王大学",
    "changzhou university": "常州大学",
    "henan polytechnic university": "河南理工大学",
    "qingdao university": "青岛大学",
    "shanghai university": "上海大学",
    "ningxia university": "宁夏大学",
    "hefei university technology": "合肥工业大学",
    "northwest university": "西北大学",
    "yangzhou university": "扬州大学",
    "seoul national university": "首尔大学",
    "norwegian university science technology": "挪威科技大学",
    "lanzhou university": "兰州大学",
    "beihang university": "北京航空航天大学",
    "jinan university": "暨南大学",
    "university jinan": "济南大学",
    "southern university science technology": "南方科技大学",
    "beijing normal university": "北京师范大学",
    "xinjiang university": "新疆大学",
    "ocean university china": "中国海洋大学",
    "nanchang university": "南昌大学",
    "nanjing normal university": "南京师范大学",
    "guizhou university": "贵州大学",
    "shaanxi university science technology": "陕西科技大学",
    "xian shiyou university": "西安石油大学",
    "xian university science technology": "西安科技大学",
    "qilu university technology": "齐鲁工业大学",
    "heriot watt university": "赫瑞瓦特大学",
    "lund university": "隆德大学",
    "king abdulaziz university": "阿卜杜勒阿齐兹国王大学",
    "sichuan university": "四川大学",
    "russian academy science": "俄罗斯科学院",
    "texas a m university": "得克萨斯农工大学",
    "university alberta": "阿尔伯塔大学",
    "nanjing technology university": "南京工业大学",
    "university texas austin": "得克萨斯大学奥斯汀分校",
    "indian institute technology": "印度理工学院",
    "university calgary": "卡尔加里大学",
    "center south university": "中南大学",
    "north china elect power university": "华北电力大学",
    "peking university": "北京大学",
    "technology university denmark": "丹麦技术大学",
    "hong kong polytech university": "香港理工大学",
    "nanyang technology university": "南洋理工大学",
    "delft university technology": "代尔夫特理工大学",
    "islamic azad university": "伊斯兰阿扎德大学",
    "university queensland": "昆士兰大学",
    "korea university": "高丽大学",
    "rhein westfal th aachen": "亚琛工业大学",
    "university tehran": "德黑兰大学",
    "curtin university": "科廷大学",
    "donghua university": "东华大学",
    "eindhoven university technology": "埃因霍温理工大学",
    "university ghent": "根特大学",
    "northwestern polytech university": "西北工业大学",
    "university manchester": "曼彻斯特大学",
    "hanyang university": "汉阳大学",
    "yonsei university": "延世大学",
    "oak ridge national lab": "橡树岭国家实验室",
    "university leeds": "利兹大学",
    "henan polytech university": "河南理工大学",
    "katholieke university leuven": "鲁汶大学",
    "national institute adv ind science technology": "日本产业技术综合研究所",
    "university nottingham": "诺丁汉大学",
    "university birmingham": "伯明翰大学",
    "politecn milan": "米兰理工大学",
    "purdue university": "普渡大学",
    "university porto": "波尔图大学",
    "chalmers university technology": "查尔姆斯理工大学",
    "university adelaide": "阿德莱德大学",
    "amirkabir university technology": "阿米尔卡比尔理工大学",
    "sharif university technology": "谢里夫理工大学",
    "national renewable energy lab": "美国国家可再生能源实验室",
    "university cambridge": "剑桥大学",
    "university waterloo": "滑铁卢大学",
    "university tokyo": "东京大学",
    "beijing forestry university": "北京林业大学",
    "university elect science technology china": "电子科技大学",
    "university new south wales": "新南威尔士大学",
    "xiangtan university": "湘潭大学",
    "university shanghai science technology": "上海理工大学",
    "university sao paulo": "圣保罗大学",
    "stanford university": "斯坦福大学",
    "national taiwan university": "台湾大学",
    "hong kong university science technology": "香港科技大学",
    "tohoku university": "东北大学（日本）",
    "northwestern university": "西北大学（美国）",
    "aalto university": "阿尔托大学",
    "university illinois": "伊利诺伊大学",
    "national cheng kung university": "成功大学",
    "sungkyunkwan university": "成均馆大学",
    "university toulouse": "图卢兹大学",
    "colorado school mines": "科罗拉多矿业学院",
    "iowa state university": "爱荷华州立大学",
    "university regina": "里贾纳大学",
    "petroleum university technology": "石油技术大学",
    "university wisconsin": "威斯康星大学",
    "university oklahoma": "俄克拉荷马大学",
    "karlsruhe institute technology": "卡尔斯鲁厄理工学院",
    "kyushu university": "九州大学",
    "kyoto university": "京都大学",
    "chulalongkorn university": "朱拉隆功大学",
    "hokkaido university": "北海道大学",
    "university houston": "休斯敦大学",
    "wuhan institute technology": "武汉工程大学",
    "korea adv institute science technology": "韩国科学技术院",
    "university western australia": "西澳大学",
    "tarbiat modares university": "塔比阿特莫达雷斯大学",
    "ningbo university": "宁波大学",
    "shiraz university": "设拉子大学",
    "academy science innovat research acsir": "印度科学与创新研究院",
    "university malaya": "马来亚大学",
    "zhejiang science technology university": "浙江理工大学",
    "xian university architecture technology": "西安建筑科技大学",
    "university lorraine": "洛林大学",
    "nanjing university aeronaut astronaut": "南京航空航天大学",
    "east china normal university": "华东师范大学",
    "southwest jiaotong university": "西南交通大学",
    "university michigan": "密歇根大学",
    "university melbourne": "墨尔本大学",
    "university sydney": "悉尼大学",
    "shihezi university": "石河子大学",
    "university fed rio de janeiro": "里约热内卢联邦大学",
    "indian institute technology delhi": "印度理工学院德里分校",
    "changan university": "长安大学",
    "carnegie mellon university": "卡内基梅隆大学",
    "university teknol malaysia": "马来西亚工艺大学",
    "university london imperial college science technology medical": "帝国理工学院",
    "hohai university": "河海大学",
    "king abdullah university science technology": "阿卜杜拉国王科技大学",
    "aalborg university": "奥尔堡大学",
    "tokyo institute technology": "东京工业大学",
    "university utah": "犹他大学",
    "university teknol petronas": "马来西亚国油科技大学",
    "guangzhou university": "广州大学",
    "university aveiro": "阿威罗大学",
    "moscow mv lomonosov state university": "莫斯科国立大学",
    "university technology sydney": "悉尼科技大学",
    "lulea university technology": "吕勒奥理工大学",
    "swiss fed institute technology": "瑞士联邦理工学院",
    "university british columbia": "英属哥伦比亚大学",
    "university estadual campinas": "坎皮纳斯州立大学",
    "university calif berkeley": "加州大学伯克利分校",
    "university toronto": "多伦多大学",
    "university wyoming": "怀俄明大学",
    "university newcastle": "纽卡斯尔大学",
    "university minnesota": "明尼苏达大学",
    "technology university munich": "慕尼黑工业大学",
    "shanghai institute pollut control ecol secur": "上海污染控制与生态安全研究院",
}

SUPPLEMENTAL_EXACT_PAIRS: dict[str, tuple[str, str]] = {
    "IN": ("未识别机构", "未识别机构"),
    "SRM Inst Sci & Technol": ("埃斯艾尔艾姆理工学院", "埃斯艾尔艾姆理工学院"),
    "Univ Engn & Technol": ("工程技术大学", "工程技术大学"),
    "SINTEF Energy Res": ("挪威工业技术研究院能源研究所", "挪威工业技术研究院"),
    "Institute of Sedimentary Geology": ("沉积地质研究所", "沉积地质研究所"),
    "Zhejiang Univ Sci & Technol": ("浙江科技大学", "浙江科技大学"),
    "Univ Canterbury": ("坎特伯雷大学", "坎特伯雷大学"),
    "Concordia Univ": ("康考迪亚大学", "康考迪亚大学"),
    "Univ Notre Dame": ("圣母大学", "圣母大学"),
    "Univ Punjab": ("旁遮普大学", "旁遮普大学"),
    "Univ Sci & Technol UST": ("科学技术大学", "科学技术大学"),
    "Pacific NW Natl Lab": ("太平洋西北国家实验室", "太平洋西北国家实验室"),
    "Texas A&M Univ Qatar": ("得克萨斯农工大学卡塔尔分校", "得克萨斯农工大学"),
    "China Med Univ": ("中国医科大学", "中国医科大学"),
    "Xian Modern Chem Res Inst": ("西安近代化学研究所", "西安近代化学研究所"),
    "Boreskov Inst Catalysis": ("博列斯科夫催化研究所", "博列斯科夫催化研究所"),
    "Ain Shams Univ": ("艾因夏姆斯大学", "艾因夏姆斯大学"),
    "Kwangwoon Univ": ("光云大学", "光云大学"),
    "Seoul Natl Univ Sci & Technol": ("首尔国立科学技术大学", "首尔国立科学技术大学"),
    "Quaid I Azam Univ": ("卡伊德阿扎姆大学", "卡伊德阿扎姆大学"),
    "Wayne State Univ": ("韦恩州立大学", "韦恩州立大学"),
    "St Petersburg State Univ": ("圣彼得堡国立大学", "圣彼得堡国立大学"),
    "Harbin Normal Univ": ("哈尔滨师范大学", "哈尔滨师范大学"),
    "South Cent Univ Nationalities": ("中南民族大学", "中南民族大学"),
    "Guilin Univ Elect Technol": ("桂林电子科技大学", "桂林电子科技大学"),
    "Normandie Univ": ("诺曼底大学", "诺曼底大学"),
    "Univ South Carolina": ("南卡罗来纳大学", "南卡罗来纳大学"),
    "Sathyabama Inst Sci & Technol": ("萨蒂亚巴马科学技术学院", "萨蒂亚巴马科学技术学院"),
    "Univ Missouri": ("密苏里大学", "密苏里大学"),
    "N Carolina State Univ": ("北卡罗来纳州立大学", "北卡罗来纳州立大学"),
    "Univ Tsukuba": ("筑波大学", "筑波大学"),
    "Pohang Univ Sci & Technol": ("浦项科技大学", "浦项科技大学"),
    "Wuhan Polytech Univ": ("武汉轻工大学", "武汉轻工大学"),
    "Univ Claude Bernard Lyon 1": ("克洛德·贝尔纳里昂第一大学", "克洛德·贝尔纳里昂第一大学"),
    "Univ Milan": ("米兰大学", "米兰大学"),
    "Tanta Univ": ("坦塔大学", "坦塔大学"),
    "Univ Florence": ("佛罗伦萨大学", "佛罗伦萨大学"),
    "Univ Calif San Diego": ("加州大学圣迭戈分校", "加州大学圣迭戈分校"),
    "Samara State Tech Univ": ("萨马拉国立技术大学", "萨马拉国立技术大学"),
    "Univ Teknol MARA": ("玛拉工艺大学", "玛拉工艺大学"),
    "Shandong Agr Univ": ("山东农业大学", "山东农业大学"),
    "(Beijing)China University of Petroleum": ("中国石油大学（北京）", "中国石油大学（北京）"),
    "Lawrence Livermore Natl Lab": ("劳伦斯利弗莫尔国家实验室", "劳伦斯利弗莫尔国家实验室"),
    "Univ Roma La Sapienza": ("罗马第一大学", "罗马第一大学"),
    "Korea Inst Machinery & Mat": ("韩国机械材料研究院", "韩国机械材料研究院"),
    "Dongguk Univ": ("东国大学", "东国大学"),
    "Natl Yunlin Univ Sci & Technol": ("国立云林科技大学", "国立云林科技大学"),
    "Budapest Univ Technol & Econ": ("布达佩斯技术与经济大学", "布达佩斯技术与经济大学"),
    "Univ Lancaster": ("兰卡斯特大学", "兰卡斯特大学"),
    "Selcuk Univ": ("塞尔丘克大学", "塞尔丘克大学"),
    "Johns Hopkins Univ": ("约翰斯·霍普金斯大学", "约翰斯·霍普金斯大学"),
    "W Virginia Univ": ("西弗吉尼亚大学", "西弗吉尼亚大学"),
    "Univ Fed Bahia": ("巴伊亚联邦大学", "巴伊亚联邦大学"),
    "Chang Gung Univ": ("长庚大学", "长庚大学"),
    "Natl Univ Def Technol": ("国防科技大学", "国防科技大学"),
    "Univ Loughborough": ("拉夫堡大学", "拉夫堡大学"),
    "Univ Buenos Aires": ("布宜诺斯艾利斯大学", "布宜诺斯艾利斯大学"),
    "Royal Inst Technol KTH": ("瑞典皇家理工学院", "瑞典皇家理工学院"),
    "Shenyang Aerosp Univ": ("沈阳航空航天大学", "沈阳航空航天大学"),
    "Indian Inst Technol Hyderabad": ("印度理工学院海得拉巴分校", "印度理工学院海得拉巴分校"),
    "Erciyes Univ": ("埃尔吉耶斯大学", "埃尔吉耶斯大学"),
    "Ewha Womans Univ": ("梨花女子大学", "梨花女子大学"),
    "Cent China Normal Univ": ("华中师范大学", "华中师范大学"),
    "Wroclaw Univ Sci & Technol": ("弗罗茨瓦夫科技大学", "弗罗茨瓦夫科技大学"),
    "King Mongkuts Univ Technol Thonburi": ("吞武里先皇科技大学", "吞武里先皇科技大学"),
    "Natl Sun Yat Sen Univ": ("国立中山大学", "国立中山大学"),
    "Assiut Univ": ("艾斯尤特大学", "艾斯尤特大学"),
    "Tech Univ Denmark DTU": ("丹麦技术大学", "丹麦技术大学"),
    "Indian Inst Petr": ("印度石油研究所", "印度石油研究所"),
    "Univ Fed Parana": ("巴拉那联邦大学", "巴拉那联邦大学"),
    "Razi Univ": ("拉齐大学", "拉齐大学"),
    "Univ Petr & Energy Studies": ("石油与能源研究大学", "石油与能源研究大学"),
    "Anhui Normal Univ": ("安徽师范大学", "安徽师范大学"),
    "Taif Univ": ("塔伊夫大学", "塔伊夫大学"),
    "COMSATS Univ Islamabad": ("科姆萨茨大学伊斯兰堡校区", "科姆萨茨大学"),
    "KN Toosi Univ Technol": ("哈贾·纳西尔丁·图西理工大学", "哈贾·纳西尔丁·图西理工大学"),
    "Univ Arizona": ("亚利桑那大学", "亚利桑那大学"),
    "Geoscience Research Institute": ("地球科学研究所", "地球科学研究所"),
    "Worcester Polytech Inst": ("伍斯特理工学院", "伍斯特理工学院"),
    "Univ Bordeaux": ("波尔多大学", "波尔多大学"),
    "Gyeongsang Natl Univ": ("庆尚国立大学", "庆尚国立大学"),
    "Chongqing Jiaotong Univ": ("重庆交通大学", "重庆交通大学"),
    "Keio Univ": ("庆应义塾大学", "庆应义塾大学"),
    "Univ Arkansas": ("阿肯色大学", "阿肯色大学"),
    "Yuan Ze Univ": ("元智大学", "元智大学"),
    "Univ Idaho": ("爱达荷大学", "爱达荷大学"),
    "Shahrood Univ Technol": ("沙赫鲁德理工大学", "沙赫鲁德理工大学"),
    "Hong Kong Baptist Univ": ("香港浸会大学", "香港浸会大学"),
    "North China Univ Water Resources & Elect Power": ("华北水利水电大学", "华北水利水电大学"),
    "CAS Key Lab Renewable Energy": ("中国科学院可再生能源重点实验室", "中国科学院"),
}

HIGH_FREQ_WELL_TRANSLATED_PAIRS: dict[str, tuple[str, str]] = {
    "Consejo Nacl Invest Cient & Tecn": ("阿根廷国家科学技术研究委员会", "阿根廷国家科学技术研究委员会"),
    "Inst Carboquim ICB CSIC": ("西班牙国家科研委员会炭化学研究所", "西班牙国家科研委员会炭化学研究所"),
    "Univ Leoben": ("莱奥本矿业大学", "莱奥本矿业大学"),
    "Univ Concepcion": ("康塞普西翁大学", "康塞普西翁大学"),
    "Univ Cagliari": ("卡利亚里大学", "卡利亚里大学"),
    "Middle E Tech Univ": ("中东科技大学", "中东科技大学"),
    "Middle East Tech Univ": ("中东科技大学", "中东科技大学"),
    "Kocaeli Univ": ("科贾埃利大学", "科贾埃利大学"),
    "GFZ German Res Ctr Geosci": ("德国地球科学研究中心", "德国地球科学研究中心"),
    "Univ Wisconsin Madison": ("威斯康星大学麦迪逊分校", "威斯康星大学麦迪逊分校"),
    "Univ Extremadura": ("埃斯特雷马杜拉大学", "埃斯特雷马杜拉大学"),
    "Collaborat Innovat Ctr Chem Sci & Engn": ("化学科学与工程协同创新中心", "化学科学与工程协同创新中心"),
    "Univ Fed Fluminense": ("弗鲁米嫩塞联邦大学", "弗鲁米嫩塞联邦大学"),
    "Bulgarian Acad Sci": ("保加利亚科学院", "保加利亚科学院"),
    "Univ Salamanca": ("萨拉曼卡大学", "萨拉曼卡大学"),
    "Tecnol Monterrey": ("蒙特雷科技大学", "蒙特雷科技大学"),
    "Taiyuan Normal Univ": ("太原师范学院", "太原师范学院"),
    "Karadeniz Tech Univ": ("卡拉德尼兹技术大学", "卡拉德尼兹技术大学"),
    "Univ Lleida": ("莱里达大学", "莱里达大学"),
    "Univ Cantabria": ("坎塔布里亚大学", "坎塔布里亚大学"),
    "Guangzhou Med Univ": ("广州医科大学", "广州医科大学"),
    "Zagazig Univ": ("扎加齐格大学", "扎加齐格大学"),
    "Univ Nottingham Malaysia": ("诺丁汉大学马来西亚校区", "诺丁汉大学"),
    "Lanzhou Jiaotong Univ": ("兰州交通大学", "兰州交通大学"),
    "Annamalai Univ": ("安纳马莱大学", "安纳马莱大学"),
    "Ming Chi Univ Technol": ("明志科技大学", "明志科技大学"),
    "CSIR Indian Inst Petr": ("印度石油研究所", "印度石油研究所"),
    "Natl Inst Clean & Low Carbon Energy": ("国家低碳清洁能源研究所", "国家低碳清洁能源研究所"),
    "Adam Mickiewicz Univ": ("波兹南密茨凯维奇大学", "波兹南密茨凯维奇大学"),
    "Univ Geneva": ("日内瓦大学", "日内瓦大学"),
    "Fushun Research Institute of Petroleum and Petrochemicals": ("抚顺石油化工研究院", "中国石油天然气股份有限公司"),
    "Univ Malaysia Terengganu": ("马来西亚登嘉楼大学", "马来西亚登嘉楼大学"),
    "Univ Fed Uberlandia": ("乌贝兰迪亚联邦大学", "乌贝兰迪亚联邦大学"),
    "Skolkovo Inst Sci & Technol": ("斯科尔科沃科学技术学院", "斯科尔科沃科学技术学院"),
    "Inst Teknol Bandung": ("万隆理工学院", "万隆理工学院"),
    "Friedrich Alexander Univ Erlangen Nurnberg": ("埃朗根-纽伦堡弗里德里希-亚历山大大学", "埃朗根-纽伦堡弗里德里希-亚历山大大学"),
    "Zhongyuan Univ Technol": ("中原工学院", "中原工学院"),
    "United Arab Emirates Univ": ("阿联酋大学", "阿联酋大学"),
    "Shell Int Explorat & Prod Inc": ("壳牌国际勘探与生产公司", "壳牌国际勘探与生产公司"),
    "Obafemi Awolowo Univ": ("奥巴费米·阿沃洛沃大学", "奥巴费米·阿沃洛沃大学"),
    "Univ Carlos III Madrid": ("马德里卡洛斯三世大学", "马德里卡洛斯三世大学"),
    "Univ Antioquia": ("安蒂奥基亚大学", "安蒂奥基亚大学"),
    "Tezpur Univ": ("提斯浦尔大学", "提斯浦尔大学"),
    "Pontificia Univ Catolica Chile": ("智利宗座天主教大学", "智利宗座天主教大学"),
    "Leibniz Univ Hannover": ("汉诺威莱布尼茨大学", "汉诺威莱布尼茨大学"),
    "Inner Mongolia Univ Sci & Technol": ("内蒙古科技大学", "内蒙古科技大学"),
    "Guangzhou Institute of Geochemistry": ("中国科学院广州地球化学研究所", "中国科学院"),
    "Univ Hawaii Manoa": ("夏威夷大学马诺阿分校", "夏威夷大学马诺阿分校"),
    "Univ Fed Santa Maria": ("圣玛丽亚联邦大学", "圣玛丽亚联邦大学"),
    "Univ Eastern Finland": ("东芬兰大学", "东芬兰大学"),
    "Hamad Bin Khalifa Univ": ("哈马德·本·哈利法大学", "哈马德·本·哈利法大学"),
    "Univ Pannonia": ("潘诺尼亚大学", "潘诺尼亚大学"),
    "Univ Manitoba": ("曼尼托巴大学", "曼尼托巴大学"),
    "Univ Louisiana Lafayette": ("路易斯安那大学拉法叶分校", "路易斯安那大学拉法叶分校"),
    "Stellenbosch Univ": ("斯泰伦博斯大学", "斯泰伦博斯大学"),
    "Qingdao Inst Marine Geol": ("青岛海洋地质研究所", "青岛海洋地质研究所"),
    "Persian Gulf Univ": ("波斯湾大学", "波斯湾大学"),
    "Lebanese Amer Univ": ("黎巴嫩美国大学", "黎巴嫩美国大学"),
    "Catholic Univ Louvain": ("鲁汶天主教大学", "鲁汶天主教大学"),
    "Univ Vienna": ("维也纳大学", "维也纳大学"),
    "Tianfu Yongxing Lab": ("天府永兴实验室", "天府永兴实验室"),
    "Tech Univ Carolo Wilhelmina Braunschweig": ("卡洛-威廉明娜不伦瑞克工业大学", "卡洛-威廉明娜不伦瑞克工业大学"),
    "Johnson Matthey Technol Ctr": ("庄信万丰技术中心", "庄信万丰技术中心"),
    "Univ Valencia": ("瓦伦西亚大学", "瓦伦西亚大学"),
    "Univ Nevada": ("内华达大学", "内华达大学"),
    "Sapienza Univ Rome": ("罗马第一大学", "罗马第一大学"),
    "Rajiv Gandhi Inst Petr Technol": ("拉吉夫·甘地石油技术学院", "拉吉夫·甘地石油技术学院"),
    "Northeastern Univ Qinhuangdao": ("东北大学秦皇岛分校", "东北大学"),
    "Linkoping Univ": ("林雪平大学", "林雪平大学"),
    "Khon Kaen Univ": ("孔敬大学", "孔敬大学"),
    "ExxonMobil Upstream Res Co": ("埃克森美孚上游研究公司", "埃克森美孚公司"),
    "Univ Victoria": ("维多利亚大学", "维多利亚大学"),
    "Collaborat Innovat Ctr Elect Vehicles Beijing": ("北京电动车辆协同创新中心", "北京电动车辆协同创新中心"),
    "Yamagata Univ": ("山形大学", "山形大学"),
    "Univ Fed Pernambuco": ("伯南布哥联邦大学", "伯南布哥联邦大学"),
    "Korea Inst Geosci & Mineral Resources KIGAM": ("韩国地质资源研究院", "韩国地质资源研究院"),
    "Joint BioEnergy Inst": ("联合生物能源研究所", "联合生物能源研究所"),
    "Chinese Acad Med Sci & Peking Union Med Coll": ("中国医学科学院北京协和医学院", "中国医学科学院北京协和医学院"),
    "Univ Guilan": ("吉兰大学", "吉兰大学"),
    "Aligarh Muslim Univ": ("阿里格尔穆斯林大学", "阿里格尔穆斯林大学"),
    "Indiana Univ": ("印第安纳大学", "印第安纳大学"),
    "Indian Inst Chem Technol": ("印度化学技术研究所", "印度化学技术研究所"),
    "Ecole Polytech Fed Lausanne EPFL": ("洛桑联邦理工学院", "洛桑联邦理工学院"),
    "Chandigarh Univ": ("昌迪加尔大学", "昌迪加尔大学"),
    "Yangtze Normal Univ": ("长江师范学院", "长江师范学院"),
    "Virginia Commonwealth Univ": ("弗吉尼亚联邦大学", "弗吉尼亚联邦大学"),
    "Kyonggi Univ": ("京畿大学", "京畿大学"),
    "Babol Noshirvani Univ Technol": ("巴博勒努希拉瓦尼理工大学", "巴博勒努希拉瓦尼理工大学"),
    "Wageningen Univ & Res": ("瓦赫宁根大学及研究中心", "瓦赫宁根大学及研究中心"),
    "Univ Kaiserslautern": ("凯泽斯劳滕大学", "凯泽斯劳滕大学"),
    "Trinity Coll Dublin": ("都柏林圣三一学院", "都柏林圣三一学院"),
    "Natl Engn Res Ctr Distillat Technol": ("蒸馏技术国家工程研究中心", "蒸馏技术国家工程研究中心"),
    "Kanazawa Univ": ("金泽大学", "金泽大学"),
    "EnergyVille": ("EnergyVille能源研究中心", "EnergyVille能源研究中心"),
    "CSIR Indian Inst Chem Technol": ("CSIR-印度化学技术研究所", "印度化学技术研究所"),
    "Tel Aviv Univ": ("特拉维夫大学", "特拉维夫大学"),
    "Chiang Mai Univ": ("清迈大学", "清迈大学"),
    "Tech Univ Bergakad Freiberg": ("弗赖贝格工业大学", "弗赖贝格工业大学"),
    "Monash Univ Malaysia": ("蒙纳士大学马来西亚校区", "蒙纳士大学"),
    "Huaiyin Normal Univ": ("淮阴师范学院", "淮阴师范学院"),
    "Huaibei Normal Univ": ("淮北师范大学", "淮北师范大学"),
    "Feng Chia Univ": ("逢甲大学", "逢甲大学"),
    "Coventry Univ": ("考文垂大学", "考文垂大学"),
    "VTT Tech Res Ctr Finland Ltd": ("芬兰国家技术研究中心", "芬兰国家技术研究中心"),
    "Univ Publ Navarra": ("纳瓦拉公立大学", "纳瓦拉公立大学"),
    "Dankook Univ": ("檀国大学", "檀国大学"),
    "Chongqing Univ Arts & Sci": ("重庆文理学院", "重庆文理学院"),
    "Canakkale Onsekiz Mart Univ": ("恰纳卡莱十八马特大学", "恰纳卡莱十八马特大学"),
    "CUNY City Coll": ("纽约市立大学城市学院", "纽约市立大学城市学院"),
    "Univ Cologne": ("科隆大学", "科隆大学"),
    "Univ Cape Town": ("开普敦大学", "开普敦大学"),
}

HIGH_FREQ_SECOND_WAVE_PAIRS: dict[str, tuple[str, str]] = {
    "Northwestern Polytech Univ": ("西北工业大学", "西北工业大学"),
    "Natl Inst Adv Ind Sci & Technol": ("日本产业技术综合研究所", "日本产业技术综合研究所"),
    "Swiss Fed Inst Technol": ("瑞士联邦理工学院", "瑞士联邦理工学院"),
    "Politecn Torino": ("都灵理工大学", "都灵理工大学"),
    "Korea Adv Inst Sci & Technol": ("韩国科学技术院", "韩国科学技术院"),
    "Acad Sci & Innovat Res AcSIR": ("印度科学与创新研究院", "印度科学与创新研究院"),
    "Polytech Montreal": ("蒙特利尔理工学院", "蒙特利尔理工学院"),
    "Minist Educ China": ("中国教育部", "中国教育部"),
    "Taizhou Univ": ("泰州学院", "泰州学院"),
    "Lakehead Univ": ("湖首大学", "湖首大学"),
    "Natl Iranian Oil Co": ("伊朗国家石油公司", "伊朗国家石油公司"),
    "Univ Baghdad": ("巴格达大学", "巴格达大学"),
    "Jimei Univ": ("集美大学", "集美大学"),
    "Al Azhar Univ": ("爱资哈尔大学", "爱资哈尔大学"),
    "Univ Guanajuato": ("瓜纳华托大学", "瓜纳华托大学"),
    "Jeju Natl Univ": ("济州大学", "济州大学"),
    "Acad Sinica": ("中央研究院", "中央研究院"),
    "Kongju Natl Univ": ("公州大学", "公州大学"),
    "Drexel Univ": ("德雷塞尔大学", "德雷塞尔大学"),
    "Korea Univ Sci & Technol": ("韩国科学技术大学", "韩国科学技术大学"),
    "Shantou Univ": ("汕头大学", "汕头大学"),
    "Kuwait Univ": ("科威特大学", "科威特大学"),
    "Univ Libre Bruxelles": ("布鲁塞尔自由大学", "布鲁塞尔自由大学"),
    "Mansoura Univ": ("曼苏拉大学", "曼苏拉大学"),
    "Univ Nebraska": ("内布拉斯加大学", "内布拉斯加大学"),
    "Mahidol Univ": ("玛希隆大学", "玛希隆大学"),
    "Ural Fed Univ": ("乌拉尔联邦大学", "乌拉尔联邦大学"),
    "Mem Univ": ("纽芬兰纪念大学", "纽芬兰纪念大学"),
    "Univ Akron": ("阿克伦大学", "阿克伦大学"),
    "Kuwait Inst Sci Res": ("科威特科学研究所", "科威特科学研究所"),
    "Northumbria Univ": ("诺森比亚大学", "诺森比亚大学"),
    "Gachon Univ": ("嘉泉大学", "嘉泉大学"),
    "Brookhaven Natl Lab": ("布鲁克海文国家实验室", "布鲁克海文国家实验室"),
    "North West Univ": ("西北大学", "西北大学"),
    "Ankara Univ": ("安卡拉大学", "安卡拉大学"),
    "SINTEF Mat & Chem": ("挪威工业技术研究院材料与化学研究所", "挪威工业技术研究院"),
    "Ryerson Univ": ("瑞尔森大学", "瑞尔森大学"),
    "PSL Res Univ": ("巴黎文理研究大学", "巴黎文理研究大学"),
    "Benha Univ": ("本哈大学", "本哈大学"),
    "Utah State Univ": ("犹他州立大学", "犹他州立大学"),
    "Univ Coll Cork": ("科克大学学院", "科克大学学院"),
    "Ind Technol Res Inst": ("工业技术研究院", "工业技术研究院"),
    "Kazan Volga Reg Fed Univ": ("喀山联邦大学", "喀山联邦大学"),
    "Shinshu Univ": ("信州大学", "信州大学"),
    "Duke Univ": ("杜克大学", "杜克大学"),
    "Univ Hamburg": ("汉堡大学", "汉堡大学"),
    "Marie Curie Sklodowska Univ": ("玛丽·居里-斯克沃多夫斯卡大学", "玛丽·居里-斯克沃多夫斯卡大学"),
    "Univ Girona": ("赫罗纳大学", "赫罗纳大学"),
    "Minjiang Univ": ("闽江学院", "闽江学院"),
    "Univ Bristol": ("布里斯托大学", "布里斯托大学"),
    "Lamar Univ": ("拉马尔大学", "拉马尔大学"),
    "Clarkson Univ": ("克拉克森大学", "克拉克森大学"),
    "Natl Chin Yi Univ Technol": ("国立勤益科技大学", "国立勤益科技大学"),
    "Tomsk Polytech Univ": ("托木斯克理工大学", "托木斯克理工大学"),
    "New Mexico State Univ": ("新墨西哥州立大学", "新墨西哥州立大学"),
    "Myongji Univ": ("明知大学", "明知大学"),
    "Univ Kiel": ("基尔大学", "基尔大学"),
    "Czech Acad Sci": ("捷克科学院", "捷克科学院"),
    "Hanbat Natl Univ": ("韩巴特国立大学", "韩巴特国立大学"),
    "Brno Univ Technol": ("布尔诺理工大学", "布尔诺理工大学"),
    "Tech Univ Dortmund": ("多特蒙德工业大学", "多特蒙德工业大学"),
    "Cukurova Univ": ("丘库罗瓦大学", "丘库罗瓦大学"),
    "Univ Bayreuth": ("拜罗伊特大学", "拜罗伊特大学"),
    "Univ Almeria": ("阿尔梅里亚大学", "阿尔梅里亚大学"),
    "Yokohama Natl Univ": ("横滨国立大学", "横滨国立大学"),
    "Azerbaijan State Oil & Ind Univ": ("阿塞拜疆国立石油与工业大学", "阿塞拜疆国立石油与工业大学"),
    "King Fahd Univ Petr & Minerals KFUPM": ("法赫德国王石油与矿业大学", "法赫德国王石油与矿业大学"),
    "Natl Univ Sci & Technol NUST": ("巴基斯坦国立科技大学", "巴基斯坦国立科技大学"),
    "Helwan Univ": ("赫勒万大学", "赫勒万大学"),
    "Lodz Univ Technol": ("罗兹工业大学", "罗兹工业大学"),
    "China Elect Power Res Inst": ("中国电力科学研究院", "中国电力科学研究院"),
    "Univ Delhi": ("德里大学", "德里大学"),
    "Westlake Univ": ("西湖大学", "西湖大学"),
    "Univ Chile": ("智利大学", "智利大学"),
    "Shandong Jianzhu Univ": ("山东建筑大学", "山东建筑大学"),
    "Natl Synchrotron Radiat Res Ctr": ("国家同步辐射研究中心", "国家同步辐射研究中心"),
    "Yulin Univ": ("榆林学院", "榆林学院"),
}

HIGH_FREQ_THIRD_WAVE_PAIRS: dict[str, tuple[str, str]] = {
    "Ecole Polytech Fed Lausanne": ("洛桑联邦理工学院", "洛桑联邦理工学院"),
    "KLE Technol Univ": ("卡纳塔克林加亚特教育理工大学", "卡纳塔克林加亚特教育理工大学"),
    "BGP Inc": ("中国石油集团东方地球物理勘探有限责任公司", "中国石油天然气集团有限公司"),
    "Saveetha Univ": ("萨维塔医科与技术科学学院", "萨维塔医科与技术科学学院"),
    "Univ Paris 06": ("巴黎第六大学", "巴黎第六大学"),
    "New Mexico Inst Min & Technol": ("新墨西哥矿业与技术学院", "新墨西哥矿业与技术学院"),
    "State Key Lab Shale Oil & Gas Enrichment Mech & E": ("页岩油气富集机理与有效开发国家重点实验室", "页岩油气富集机理与有效开发国家重点实验室"),
    "State Key Lab Shale Oil & Gas Enrichment Mech & Ef": ("页岩油气富集机理与有效开发国家重点实验室", "页岩油气富集机理与有效开发国家重点实验室"),
    "School of Petroleum Engineering in China University of Petroleum(Qingdao)": ("中国石油大学（华东）石油工程学院", "中国石油大学（华东）"),
    "School of Petroleum Engineering in China University of Petroleum (Qingdao)": ("中国石油大学（华东）石油工程学院", "中国石油大学（华东）"),
    "School of Geosciences in China University of Petroleum(Qingdao)": ("中国石油大学（华东）地球科学学院", "中国石油大学（华东）"),
    "School of Geosciences in China University of Petroleum (Qingdao)": ("中国石油大学（华东）地球科学学院", "中国石油大学（华东）"),
    "Xian Thermal Power Res Inst Co Ltd": ("西安热工研究院有限公司", "西安热工研究院有限公司"),
    "Res Inst Petr Ind RIPI": ("伊朗石油工业研究所", "伊朗石油工业研究所"),
    "Sri Sivasubramaniya Nadar Coll Engn": ("斯里希瓦苏布拉马尼亚纳达尔工程学院", "斯里希瓦苏布拉马尼亚纳达尔工程学院"),
    "PUT": ("伊朗石油工业大学", "伊朗石油工业大学"),
    "VSB Tech Univ Ostrava": ("俄斯特拉发技术大学", "俄斯特拉发技术大学"),
    "Case Western Reserve Univ": ("凯斯西储大学", "凯斯西储大学"),
    "China West Normal Univ": ("西华师范大学", "西华师范大学"),
    "Nanjing Univ Chinese Med": ("南京中医药大学", "南京中医药大学"),
    "Korea Basic Sci Inst": ("韩国基础科学研究院", "韩国基础科学研究院"),
    "UFZ Helmholtz Ctr Environm Res": ("亥姆霍兹环境研究中心", "亥姆霍兹环境研究中心"),
    "China University of Petroleum (Huadong)": ("中国石油大学（华东）", "中国石油大学（华东）"),
    "China Univ Petr Huadong": ("中国石油大学（华东）", "中国石油大学（华东）"),
    "EnergyVille": ("能源谷研究中心", "能源谷研究中心"),
    "CSIR Natl Chem Lab": ("印度国家化学实验室", "印度国家化学实验室"),
    "Chengdu Univ Informat Technol": ("成都信息工程大学", "成都信息工程大学"),
    "Huazhong Univ Sci & Technol HUST": ("华中科技大学", "华中科技大学"),
    "Korea Inst Energy Res KIER": ("韩国能源研究院", "韩国能源研究院"),
    "Pilot Natl Lab Marine Sci & Technol Qingdao": ("青岛海洋科学与技术试点国家实验室", "青岛海洋科学与技术试点国家实验室"),
    "Birla Inst Technol & Sci": ("比尔拉理工与科学学院", "比尔拉理工与科学学院"),
    "Natl Ctr Nanosci & Technol": ("国家纳米科学与技术中心", "国家纳米科学与技术中心"),
    "Shanxi Zheda Inst Adv Mat & Chem Engn": ("山西浙大先进材料与化工研究院", "山西浙大先进材料与化工研究院"),
    "Shandong First Med Univ & Shandong Acad Med Sci": ("山东第一医科大学（山东省医学科学院）", "山东第一医科大学"),
    "Univ Nat Resources & Life Sci": ("维也纳自然资源与生命科学大学", "维也纳自然资源与生命科学大学"),
    "Natl Inst Mat Sci NIMS": ("日本国立材料科学研究所", "日本国立材料科学研究所"),
    "Rey Juan Carlos Univ": ("胡安卡洛斯国王大学", "胡安卡洛斯国王大学"),
    "Guangdong Technion Israel Inst Technol": ("广东以色列理工学院", "广东以色列理工学院"),
    "COMSATS Inst Informat Technol": ("科姆萨茨大学", "科姆萨茨大学"),
    "Ctr Res & Technol Hellas": ("希腊研究与技术中心", "希腊研究与技术中心"),
    "Flemish Inst Technol Res VITO": ("弗拉芒技术研究院", "弗拉芒技术研究院"),
    "Southern Marine Sci & Engn Guangdong Lab Guangzhou": ("南方海洋科学与工程广东实验室（广州）", "南方海洋科学与工程广东实验室"),
    "VIT Univ": ("韦洛尔理工学院", "韦洛尔理工学院"),
    "China Coal Res Inst": ("煤炭科学研究总院", "煤炭科学研究总院"),
    "Indian Assoc Cultivat Sci": ("印度科学培育协会", "印度科学培育协会"),
    "Indian Inst Technol ISM": ("印度理工学院丹巴德校区", "印度理工学院"),
    "Natl Acad Sci Azerbaijan": ("阿塞拜疆国家科学院", "阿塞拜疆国家科学院"),
    "Petr Univ Technol PUT": ("伊朗石油工业大学", "伊朗石油工业大学"),
    "Univ Tenaga Nas": ("马来西亚国家能源大学", "马来西亚国家能源大学"),
    "Harbin Inst Technol Weihai": ("哈尔滨工业大学（威海）", "哈尔滨工业大学"),
    "Ho Chi Minh City Univ Transport": ("胡志明市交通大学", "胡志明市交通大学"),
    "Malaysian Palm Oil Board": ("马来西亚棕榈油局", "马来西亚棕榈油局"),
    "Qingdao Marine Sci & Technol Ctr": ("青岛海洋科学与技术中心", "青岛海洋科学与技术中心"),
    "Ist Italiano Tecnol": ("意大利技术研究院", "意大利技术研究院"),
    "Korea Atom Energy Res Inst": ("韩国原子能研究院", "韩国原子能研究院"),
    "Natl Yang Ming Chiao Tung Univ": ("国立阳明交通大学", "国立阳明交通大学"),
    "Peoples Friendship Univ Russia": ("俄罗斯人民友谊大学", "俄罗斯人民友谊大学"),
    "So Cross Univ": ("南十字星大学", "南十字星大学"),
    "Univ Politehn Bucuresti": ("布加勒斯特理工大学", "布加勒斯特理工大学"),
    "Hefei Comprehens Natl Sci Ctr": ("合肥综合性国家科学中心", "合肥综合性国家科学中心"),
    "Hubei Univ Automot Technol": ("湖北汽车工业学院", "湖北汽车工业学院"),
    "Korea Inst Ind Technol KITECH": ("韩国产业技术研究院", "韩国产业技术研究院"),
    "Korea Univ Sci & Technol UST": ("韩国科学技术大学", "韩国科学技术大学"),
    "Pontificia Univ Catolica Rio de Janeiro": ("里约热内卢天主教大学", "里约热内卢天主教大学"),
    "Reliance Ind Ltd": ("信实工业有限公司", "信实工业有限公司"),
    "SUNY Coll Environm Sci & Forestry": ("纽约州立大学环境科学与林业学院", "纽约州立大学环境科学与林业学院"),
    "UCSI Univ": ("思特雅大学", "思特雅大学"),
    "CAS Ctr Excellence Deep Earth Sci": ("中国科学院深地科学卓越创新中心", "中国科学院"),
    "China Acad Safety Sci & Technol": ("中国安全生产科学研究院", "中国安全生产科学研究院"),
    "South Cent Minzu Univ": ("中南民族大学", "中南民族大学"),
    "Shanghai Inst Space Power Sources": ("上海空间电源研究所", "上海空间电源研究所"),
    "US Forest Serv": ("美国林务局", "美国林务局"),
    "Univ Texas Dallas": ("得克萨斯大学达拉斯分校", "得克萨斯大学达拉斯分校"),
    "Weichai Power Co Ltd": ("潍柴动力股份有限公司", "潍柴动力股份有限公司"),
    "Xiamen Univ Technol": ("厦门理工学院", "厦门理工学院"),
    "Natl Inst Technol": ("美国国家标准与技术研究院", "美国国家标准与技术研究院"),
    "Natl Inst Technol Karnataka": ("卡纳塔克国立理工学院", "卡纳塔克国立理工学院"),
    "Natl Inst Technol Silchar": ("锡尔查尔国立理工学院", "锡尔查尔国立理工学院"),
    "Politecn Milan": ("米兰理工大学", "米兰理工大学"),
    "Shanghai Inst Pollut Control & Ecol Secur": ("上海污染控制与生态安全研究院", "上海污染控制与生态安全研究院"),
    "Nanjing Med Univ": ("南京医科大学", "南京医科大学"),
    "Nanjing Inst Technol": ("南京工程学院", "南京工程学院"),
    "Harbin Univ Sci & Technol": ("哈尔滨理工大学", "哈尔滨理工大学"),
    "Northwest Branch": ("西北分院", "西北分院"),
    "Univ Michoacana": ("米却肯大学", "米却肯大学"),
    "Univ Virginia": ("弗吉尼亚大学", "弗吉尼亚大学"),
    "LUT Univ": ("拉彭兰塔-拉赫蒂理工大学", "拉彭兰塔-拉赫蒂理工大学"),
    "Natl Inst Mat Sci": ("日本国立材料科学研究所", "日本国立材料科学研究所"),
    "Engineering Technology Research Institute": ("工程技术研究所", "工程技术研究所"),
    "Fujian Univ Technol": ("福建理工大学", "福建理工大学"),
    "Minist Ecol & Environm": ("生态环境部", "生态环境部"),
    "Educ Univ Hong Kong": ("香港教育大学", "香港教育大学"),
    "Xian Technol Univ": ("西安工业大学", "西安工业大学"),
    "Univ Hohenheim": ("霍恩海姆大学", "霍恩海姆大学"),
    "Univ Turin": ("都灵大学", "都灵大学"),
    "Univ Helsinki": ("赫尔辛基大学", "赫尔辛基大学"),
    "Univ Sherbrooke": ("舍布鲁克大学", "舍布鲁克大学"),
    "Univ Technol Baghdad": ("巴格达理工大学", "巴格达理工大学"),
    "Univ Fed Goias": ("戈亚斯联邦大学", "戈亚斯联邦大学"),
    "Univ Maribor": ("马里博尔大学", "马里博尔大学"),
    "Shenzhen Technol Univ": ("深圳技术大学", "深圳技术大学"),
    "Univ Maine": ("缅因大学", "缅因大学"),
    "Univ Rennes": ("雷恩大学", "雷恩大学"),
    "Univ Nacl Litoral": ("国立滨海大学", "国立滨海大学"),
    "Capital Med Univ": ("首都医科大学", "首都医科大学"),
    "Ford Motor Co": ("福特汽车公司", "福特汽车公司"),
    "Nguyen Tat Thanh Univ": ("阮必成大学", "阮必成大学"),
    "Jilin Jianzhu Univ": ("吉林建筑大学", "吉林建筑大学"),
    "King Mongkuts Univ Technol North Bangkok": ("北曼谷先皇科技大学", "北曼谷先皇科技大学"),
    "Prince Songkla Univ": ("宋卡王子大学", "宋卡王子大学"),
    "CSIRO Earth Sci & Resource Engn": ("澳大利亚联邦科学与工业研究组织地球科学与资源工程部", "澳大利亚联邦科学与工业研究组织"),
    "Shiraz Univ Technol": ("设拉子理工大学", "设拉子理工大学"),
    "Tshwane Univ Technol": ("茨瓦内理工大学", "茨瓦内理工大学"),
    "Jain Univ": ("杰恩大学", "杰恩大学"),
    "Bohai Univ": ("渤海大学", "渤海大学"),
    "Sunway Univ": ("双威大学", "双威大学"),
    "Karabuk Univ": ("卡拉比克大学", "卡拉比克大学"),
    "Koc Univ": ("科奇大学", "科奇大学"),
    "Univ South Africa": ("南非大学", "南非大学"),
    "Univ Windsor": ("温莎大学", "温莎大学"),
    "Univ Bradford": ("布拉德福德大学", "布拉德福德大学"),
    "Univ Tartu": ("塔尔图大学", "塔尔图大学"),
    "Hanoi Univ Sci & Technol": ("河内科技大学", "河内科技大学"),
    "Xuzhou Med Univ": ("徐州医科大学", "徐州医科大学"),
    "Incheon Natl Univ": ("仁川国立大学", "仁川国立大学"),
    "Xuzhou Univ Technol": ("徐州工程学院", "徐州工程学院"),
    "Gwangju Inst Sci & Technol": ("光州科学技术院", "光州科学技术院"),
    "Gwangju Inst Sci & Technol GIST": ("光州科学技术院", "光州科学技术院"),
    "NingboTech Univ": ("浙大宁波理工学院", "浙大宁波理工学院"),
    "Tibet Univ": ("西藏大学", "西藏大学"),
    "Univ S Australia": ("南澳大学", "南澳大学"),
    "Changshu Inst Technol": ("常熟理工学院", "常熟理工学院"),
    "Univ Nacl Sur": ("南方国立大学", "南方国立大学"),
    "Univ Parma": ("帕尔马大学", "帕尔马大学"),
    "Univ Politecn Marche": ("马尔凯理工大学", "马尔凯理工大学"),
    "Bu Ali Sina Univ": ("布阿里西纳大学", "布阿里西纳大学"),
    "Univ Autonoma San Luis Potosi": ("圣路易斯波托西自治大学", "圣路易斯波托西自治大学"),
    "Univ Aquila": ("拉奎拉大学", "拉奎拉大学"),
    "Univ Huelva": ("韦尔瓦大学", "韦尔瓦大学"),
    "Univ North Texas": ("北得克萨斯大学", "北得克萨斯大学"),
    "Univ Cent Florida": ("中佛罗里达大学", "中佛罗里达大学"),
    "Univ Southern Denmark": ("南丹麦大学", "南丹麦大学"),
    "Univ East Anglia": ("东安格利亚大学", "东安格利亚大学"),
    "Univ Messina": ("墨西拿大学", "墨西拿大学"),
    "Univ Miami": ("迈阿密大学", "迈阿密大学"),
    "Univ Sannio": ("萨尼奥大学", "萨尼奥大学"),
    "Univ Novi Sad": ("诺维萨德大学", "诺维萨德大学"),
    "Univ S Florida": ("南佛罗里达大学", "南佛罗里达大学"),
    "Amer Univ Beirut": ("贝鲁特美国大学", "贝鲁特美国大学"),
    "Indian Inst Technol Ropar": ("印度理工学院鲁帕尔分校", "印度理工学院"),
    "Istanbul Univ": ("伊斯坦布尔大学", "伊斯坦布尔大学"),
    "Shaanxi Yanchang Petr Grp Co Ltd": ("陕西延长石油（集团）有限责任公司", "陕西延长石油（集团）有限责任公司"),
    "Kinki Univ": ("近畿大学", "近畿大学"),
    "Univ Autonoma Nuevo Leon": ("新莱昂自治大学", "新莱昂自治大学"),
    "Univ Calcutta": ("加尔各答大学", "加尔各答大学"),
    "Univ Haute Alsace": ("上阿尔萨斯大学", "上阿尔萨斯大学"),
    "Univ Rennes 1": ("雷恩第一大学", "雷恩第一大学"),
    "Univ Los Andes": ("安第斯大学", "安第斯大学"),
    "Eastern Inst Technol": ("东部理工学院", "东部理工学院"),
    "Basque Fdn Sci": ("巴斯克科学基金会", "巴斯克科学基金会"),
    "Exploration & Production Research Institute": ("勘探与生产研究院", "勘探与生产研究院"),
    "Research Institute": ("研究所", "研究所"),
    "Petrochemical Research Institute": ("石油化工研究所", "石油化工研究所"),
    "Natl Chem Lab": ("国家化学实验室", "国家化学实验室"),
    "Natl Inst Chem": ("国家化学研究所", "国家化学研究所"),
    "Natl Cent Univ": ("国立中央大学", "国立中央大学"),
    "Univ Chem & Technol": ("化工与技术大学", "化工与技术大学"),
    "Hangzhou Yanqu Informat Technol Co Ltd": ("杭州研趣信息技术有限公司", "杭州研趣信息技术有限公司"),
    "Thapar Inst Engn & Technol": ("塔帕尔工程技术学院", "塔帕尔工程技术学院"),
    "Friedrich Alexander Univ Erlangen Nurnberg FAU": ("埃尔朗根-纽伦堡弗里德里希-亚历山大大学", "埃尔朗根-纽伦堡弗里德里希-亚历山大大学"),
    "Univ So Calif": ("南加州大学", "南加州大学"),
    "Univ South Australia": ("南澳大学", "南澳大学"),
    "Univ Toyama": ("富山大学", "富山大学"),
    "Wroclaw Univ Technol": ("弗罗茨瓦夫理工大学", "弗罗茨瓦夫理工大学"),
    "Bahauddin Zakariya Univ": ("巴哈丁扎卡里亚大学", "巴哈丁扎卡里亚大学"),
    "Inst Tecnol Aguascalientes": ("阿瓜斯卡连特斯技术学院", "阿瓜斯卡连特斯技术学院"),
    "PSL Univ": ("巴黎文理研究大学", "巴黎文理研究大学"),
    "Pandit Deendayal Energy Univ": ("潘迪特迪恩达亚尔能源大学", "潘迪特迪恩达亚尔能源大学"),
    "St Petersburg Min Univ": ("圣彼得堡矿业大学", "圣彼得堡矿业大学"),
    "Tech Univ Crete": ("克里特理工大学", "克里特理工大学"),
    "Univ Bourgogne Franche Comte": ("勃艮第-弗朗什-孔泰大学", "勃艮第-弗朗什-孔泰大学"),
    "Univ Mons": ("蒙斯大学", "蒙斯大学"),
    "Univ Nantes": ("南特大学", "南特大学"),
    "Univ St Andrews": ("圣安德鲁斯大学", "圣安德鲁斯大学"),
    "Univ Stellenbosch": ("斯泰伦博斯大学", "斯泰伦博斯大学"),
    "Swansea Univ": ("斯旺西大学", "斯旺西大学"),
    "Natl Res Univ": ("国立研究型大学", "国立研究型大学"),
    "Natl Univ Sci & Technol": ("国立科学技术大学", "国立科学技术大学"),
    "Institute of Geophysics": ("地球物理研究所", "地球物理研究所"),
    "College of Chemistry": ("化学学院", "化学学院"),
    "DWA Energy Ltd": ("德瓦能源有限公司", "德瓦能源有限公司"),
    "Univ Roma Tor Vergata": ("罗马第二大学", "罗马第二大学"),
    "Anhui Med Univ": ("安徽医科大学", "安徽医科大学"),
    "Delhi Technol Univ": ("德里理工大学", "德里理工大学"),
}

HIGH_FREQ_FOURTH_WAVE_PAIRS: dict[str, tuple[str, str]] = {
    "中国石油大学CNPC物探重点实验室": ("中国石油大学中国石油天然气集团物探重点实验室", "中国石油大学（北京）"),
    "CSIR Indian Inst Chem Technol": ("印度化学技术研究所", "印度化学技术研究所"),
    "Jiangsu Univ Technol": ("江苏理工学院", "江苏理工学院"),
    "Natl Chiao Tung Univ": ("国立交通大学", "国立交通大学"),
    "Royal Holloway Univ London": ("伦敦大学皇家霍洛威学院", "伦敦大学皇家霍洛威学院"),
    "Univ Basilicata": ("巴斯利卡塔大学", "巴斯利卡塔大学"),
    "Univ Jaume 1": ("海梅一世大学", "海梅一世大学"),
    "Yili Normal Univ": ("伊犁师范大学", "伊犁师范大学"),
    "Qiqihar Univ": ("齐齐哈尔大学", "齐齐哈尔大学"),
    "Shahid Chamran Univ Ahvaz": ("阿瓦士沙希德查姆兰大学", "阿瓦士沙希德查姆兰大学"),
    "Univ Dayton": ("代顿大学", "代顿大学"),
    "Univ Jordan": ("约旦大学", "约旦大学"),
    "Xiamen Univ Malaysia": ("厦门大学马来西亚分校", "厦门大学"),
    "Alma Mater Studiorum Univ Bologna": ("博洛尼亚大学", "博洛尼亚大学"),
    "Dongguk Univ Seoul": ("东国大学首尔校区", "东国大学"),
    "Hungarian Acad Sci": ("匈牙利科学院", "匈牙利科学院"),
    "Jilin Agr Univ": ("吉林农业大学", "吉林农业大学"),
    "Norwegian Univ Life Sci": ("挪威生命科学大学", "挪威生命科学大学"),
    "Silpakorn Univ": ("泰国艺术大学", "泰国艺术大学"),
    "Southwest Forestry Univ": ("西南林业大学", "西南林业大学"),
    "Umm Al Qura Univ": ("乌姆阿尔库拉大学", "乌姆阿尔库拉大学"),
    "Univ Vaasa": ("瓦萨大学", "瓦萨大学"),
    "Van Lang Univ": ("文郎大学", "文郎大学"),
    "Boreskov Inst Catalysis SB RAS": ("俄罗斯科学院西伯利亚分院博列斯科夫催化研究所", "博列斯科夫催化研究所"),
    "Chang'an University": ("长安大学", "长安大学"),
    "Dokuz Eylul Univ": ("九月九日大学", "九月九日大学"),
    "Hebrew Univ Jerusalem": ("耶路撒冷希伯来大学", "耶路撒冷希伯来大学"),
    "Iran Univ Sci & Technol IUST": ("伊朗科学技术大学", "伊朗科学技术大学"),
    "Minist Ind & Informat Technol": ("工业和信息化部", "工业和信息化部"),
    "Quanzhou Normal Univ": ("泉州师范学院", "泉州师范学院"),
    "Univ Ferrara": ("费拉拉大学", "费拉拉大学"),
    "Univ Nacl La Plata": ("拉普拉塔国立大学", "拉普拉塔国立大学"),
    "Batman Univ": ("巴特曼大学", "巴特曼大学"),
    "Bogazici Univ": ("博阿济奇大学", "博阿济奇大学"),
    "De La Salle Univ": ("德拉萨大学", "德拉萨大学"),
    "Guangxi Univ Sci & Technol": ("广西科技大学", "广西科技大学"),
    "Kharazmi Univ": ("哈拉兹米大学", "哈拉兹米大学"),
    "King Fand Univ Petr & Minerals": ("法赫德国王石油与矿业大学", "法赫德国王石油与矿业大学"),
    "Laval Univ": ("拉瓦尔大学", "拉瓦尔大学"),
    "Queensland Univ Technol QUT": ("昆士兰科技大学", "昆士兰科技大学"),
    "Soongsil Univ": ("崇实大学", "崇实大学"),
    "Suez Univ": ("苏伊士大学", "苏伊士大学"),
    "Sunchon Natl Univ": ("国立顺天大学", "国立顺天大学"),
    "UAE Univ": ("阿联酋大学", "阿联酋大学"),
    "Univ Fed Vicosa": ("维索萨联邦大学", "维索萨联邦大学"),
    "Univ Magdeburg": ("马格德堡大学", "马格德堡大学"),
    "Univ Tecn Federico Santa Maria": ("费德里科圣玛丽亚技术大学", "费德里科圣玛丽亚技术大学"),
    "Aramco Serv Co": ("沙特阿美服务公司", "沙特阿美石油公司"),
    "Beijing Institute of Petrochemical Technology": ("北京石油化工学院", "北京石油化工学院"),
    "Charles Univ Prague": ("查理大学", "查理大学"),
    "Chinese Acad Med Sci": ("中国医学科学院", "中国医学科学院"),
    "Imperial Coll": ("帝国理工学院", "帝国理工学院"),
    "Inst Politecn Braganca": ("布拉干萨理工学院", "布拉干萨理工学院"),
    "Jozef Stefan Inst": ("约瑟夫·斯特凡研究所", "约瑟夫·斯特凡研究所"),
    "King Mongkuts Inst Technol Ladkrabang": ("拉卡邦先皇理工学院", "拉卡邦先皇理工学院"),
    "Korea Natl Univ Transportat": ("韩国国立交通大学", "韩国国立交通大学"),
    "Kyushu Inst Technol": ("九州工业大学", "九州工业大学"),
    "N China Elect Power Univ": ("华北电力大学", "华北电力大学"),
    "Natl Engn Res Ctr Flue Gas Desulfurizat": ("烟气脱硫国家工程研究中心", "烟气脱硫国家工程研究中心"),
    "PetroChina Hangzhou Res Inst Geol": ("中国石油杭州地质研究院", "中国石油天然气股份有限公司"),
    "Seikei Univ": ("成蹊大学", "成蹊大学"),
    "Soran Univ": ("索兰大学", "索兰大学"),
    "State Key Lab Petr Resources & Prospecting": ("石油资源与探测国家重点实验室", "石油资源与探测国家重点实验室"),
    "Univ Antioquia UdeA": ("安蒂奥基亚大学", "安蒂奥基亚大学"),
    "Univ Brighton": ("布莱顿大学", "布莱顿大学"),
    "Univ Fed Paraiba": ("帕拉伊巴联邦大学", "帕拉伊巴联邦大学"),
    "Univ Leicester": ("莱斯特大学", "莱斯特大学"),
    "Univ Potsdam": ("波茨坦大学", "波茨坦大学"),
    "Univ Technol Iraq": ("伊拉克技术大学", "伊拉克技术大学"),
    "Univ Turku": ("图尔库大学", "图尔库大学"),
    "Zhongkai Univ Agr & Engn": ("仲恺农业工程学院", "仲恺农业工程学院"),
    "CNOOC Res Inst Ltd": ("中海油研究总院有限责任公司", "中国海洋石油有限公司"),
    "Chubu Univ": ("中部大学", "中部大学"),
    "Nagaoka Univ Technol": ("长冈技术科学大学", "长冈技术科学大学"),
    "Shaanxi Univ Technol": ("陕西理工大学", "陕西理工大学"),
    "Suez Canal Univ": ("苏伊士运河大学", "苏伊士运河大学"),
    "Univ Gottingen": ("哥廷根大学", "哥廷根大学"),
    "Univ Lahore": ("拉合尔大学", "拉合尔大学"),
    "Univ Pavia": ("帕维亚大学", "帕维亚大学"),
    "Univ Warsaw": ("华沙大学", "华沙大学"),
    "CSIR Cent Salt & Marine Chem Res Inst": ("印度中央盐与海洋化学研究所", "印度中央盐与海洋化学研究所"),
    "Eskisehir Osmangazi Univ": ("埃斯基谢希尔奥斯曼加齐大学", "埃斯基谢希尔奥斯曼加齐大学"),
    "Heze Univ": ("菏泽学院", "菏泽学院"),
    "Hunan Univ Technol": ("湖南工业大学", "湖南工业大学"),
    "Izmir Inst Technol": ("伊兹密尔理工学院", "伊兹密尔理工学院"),
    "NED Univ Engn & Technol": ("内德工程与技术大学", "内德工程与技术大学"),
    "Nanyang Normal Univ": ("南阳师范学院", "南阳师范学院"),
    "Natl Acad Sci Belarus": ("白俄罗斯国家科学院", "白俄罗斯国家科学院"),
    "Petru Poni Inst Macromol Chem": ("彼得鲁·波尼高分子化学研究所", "彼得鲁·波尼高分子化学研究所"),
    "Tufts Univ": ("塔夫茨大学", "塔夫茨大学"),
    "Univ Catania": ("卡塔尼亚大学", "卡塔尼亚大学"),
    "Univ Freiburg": ("弗赖堡大学", "弗赖堡大学"),
    "Univ Leipzig": ("莱比锡大学", "莱比锡大学"),
    "Univ Modena & Reggio Emilia": ("摩德纳与雷焦艾米利亚大学", "摩德纳与雷焦艾米利亚大学"),
    "Univ Sci & Technol China USTC": ("中国科学技术大学", "中国科学技术大学"),
    "Univ Sistan & Baluchestan": ("锡斯坦-俾路支斯坦大学", "锡斯坦-俾路支斯坦大学"),
    "Univ Tecn Lisboa": ("里斯本技术大学", "里斯本技术大学"),
    "Univ Ulster": ("阿尔斯特大学", "阿尔斯特大学"),
    "Univ Zanjan": ("赞詹大学", "赞詹大学"),
    "Woods Hole Oceanog Inst": ("伍兹霍尔海洋研究所", "伍兹霍尔海洋研究所"),
    "Xijing Univ": ("西京学院", "西京学院"),
    "Zhengzhou Inst Emerging Ind Technol": ("郑州新兴产业技术研究院", "郑州新兴产业技术研究院"),
    "Amer Univ Cairo": ("开罗美国大学", "开罗美国大学"),
    "GEOMAR Helmholtz Ctr Ocean Res Kiel": ("基尔亥姆霍兹海洋研究中心", "基尔亥姆霍兹海洋研究中心"),
    "GNS Sci": ("新西兰地质与核科学研究所", "新西兰地质与核科学研究所"),
    "Sinopec Lubricant Co": ("中国石化润滑油有限公司", "中国石油化工股份有限公司"),
    "Sinopec Lubricant Co Ltd": ("中国石化润滑油有限公司", "中国石油化工股份有限公司"),
}

COUNTRY_NAME_ALIASES = {
    "Turkiye": "Turkey",
    "Türkiye": "Turkey",
    "Korea, South": "South Korea",
    "Korea, Republic of": "South Korea",
    "USA": "United States",
    "U.S.A.": "United States",
    "UK": "United Kingdom",
}

COUNTRY_DOMINANT_EXACT_PAIRS: dict[str, dict[str, tuple[str, str]]] = {
    "Natl Res Ctr": {
        "Egypt": ("埃及国家研究中心", "埃及国家研究中心"),
    },
}

COUNTRY_DISAMBIGUATION_TOKENS = {
    "univ",
    "university",
    "college",
    "school",
    "academy",
    "acad",
    "inst",
    "institute",
    "dept",
    "department",
    "faculty",
    "lab",
    "laboratory",
    "ctr",
    "center",
    "centre",
    "minist",
    "ministry",
    "commission",
    "commiss",
    "council",
    "technol",
    "technology",
}

COUNTRY_DISAMBIGUATION_SKIP_NAMES = {
    "astrazeneca",
    "baker hughes",
    "bp",
    "chevron",
    "conoco phillips",
    "eastman chem co",
    "european commiss",
    "ge global res",
    "halliburton",
    "schlumberger",
    "shell",
    "shell global solut",
    "shell int explorat & prod",
    "slb",
    "totalenergies",
    "unilever r&d",
}

SUPPLEMENTAL_EN_REPL: dict[str, str] = {
    "fed": "federal",
    "pharmaceut": "pharmaceutical",
    "environm": "environmental",
    "innovat": "innovation",
    "proc": "process",
    "syst": "systems",
    "serv": "service",
    "appl": "applied",
    "tecnol": "technology",
    "aerosp": "aerospace",
    "geophys": "geophysics",
    "hlth": "health",
    "educ": "education",
    "elect": "electronic",
    "econ": "economics",
    "minist": "ministry",
    "polytech": "polytechnic",
    "politecn": "polytechnic",
    "adv": "advanced",
    "autonoma": "autonomous",
    "vocat": "vocational",
    "mech": "mechanical",
    "pharm": "pharmaceutical",
    "nacl": "national",
    "informat": "information",
    "automot": "automotive",
    "ecol": "ecology",
    "nanosci": "nanoscience",
    "comprehens": "comprehensive",
    "cultivat": "cultivation",
    "fdn": "foundation",
    "atom": "atomic",
    "amer": "american",
    "resour": "resource",
    "tradit": "traditional",
}

SUPPLEMENTAL_WORD_TRANSLATIONS: dict[str, str] = {
    "federal": "联邦",
    "normal": "师范",
    "polytechnic": "理工",
    "pharmaceutical": "药科",
    "electronic": "电子",
    "economics": "经济",
    "education": "教育",
    "innovation": "创新",
    "environmental": "环境",
    "process": "过程",
    "systems": "系统",
    "applied": "应用",
    "advanced": "先进",
    "aerospace": "航空航天",
    "health": "健康",
    "mechanical": "机械",
    "autonomous": "自治",
    "vocational": "职业",
    "renewable": "可再生",
    "electric": "电",
    "power": "电力",
    "information": "信息",
    "informatics": "信息",
    "automotive": "汽车",
    "maritime": "海事",
    "ocean": "海洋",
    "thermal": "热工",
    "ecology": "生态",
    "nanoscience": "纳米科学",
    "comprehensive": "综合",
    "cultivation": "培育",
    "foundation": "基金会",
    "atomic": "原子能",
    "board": "局",
    "palm": "棕榈",
    "transport": "交通",
    "forestry": "林业",
    "forest": "林业",
    "space": "航天",
    "sources": "电源",
    "life": "生命",
    "deep": "深地",
    "excellence": "卓越",
    "royal": "皇家",
    "american": "美国",
    "southern": "南方",
    "northern": "北方",
    "western": "西部",
    "eastern": "东部",
    "industrial": "工业",
    "traditional": "传统",
}

SUPPLEMENTAL_PROPER_NOUN_TRANSLATIONS: dict[str, str] = {
    "china": "中国",
    "korea": "韩国",
    "japan": "日本",
    "iran": "伊朗",
    "vietnam": "越南",
    "malaysia": "马来西亚",
    "azerbaijan": "阿塞拜疆",
    "stockholm": "斯德哥尔摩",
    "lausanne": "洛桑",
    "virginia": "弗吉尼亚",
    "maine": "缅因",
    "rennes": "雷恩",
    "jianghan": "江汉",
    "chengjian": "城建",
    "huzhou": "湖州",
    "zhanjiang": "湛江",
    "alexandria": "亚历山大",
    "hohenheim": "霍恩海姆",
    "turin": "都灵",
    "ostrava": "俄斯特拉发",
    "ege": "爱琴",
    "incheon": "仁川",
    "sakarya": "萨卡里亚",
    "helsinki": "赫尔辛基",
    "sherbrooke": "舍布鲁克",
    "goias": "戈亚斯",
    "maribor": "马里博尔",
    "firat": "幼发拉底",
    "gwangju": "光州",
    "syracuse": "锡拉丘兹",
    "parma": "帕尔马",
    "bradford": "布拉德福德",
    "tartu": "塔尔图",
    "hanoi": "河内",
    "urmia": "乌尔米耶",
    "messina": "墨西拿",
    "sannio": "萨尼奥",
    "carleton": "卡尔顿",
    "gebze": "盖布泽",
    "kaohsiung": "高雄",
    "calcutta": "加尔各答",
    "toyama": "富山",
    "alagappa": "阿拉加帕",
    "ataturk": "阿塔图尔克",
    "delhi": "德里",
    "basilicata": "巴西利卡塔",
    "jaume": "海梅",
    "linnaeus": "林奈",
    "okayama": "冈山",
    "thammasat": "法政",
    "dayton": "代顿",
    "akdeniz": "阿克德尼兹",
    "shivaji": "希瓦吉",
    "vaasa": "瓦萨",
    "yamaguchi": "山口",
    "dokuz": "九月九日",
    "ferrara": "费拉拉",
    "brighton": "布莱顿",
    "potsdam": "波茨坦",
    "leicester": "莱斯特",
    "warsaw": "华沙",
    "pavia": "帕维亚",
    "gottingen": "哥廷根",
    "lahore": "拉合尔",
    "mons": "蒙斯",
    "nantes": "南特",
    "stellenbosch": "斯泰伦博斯",
    "brown": "布朗",
    "brunel": "布鲁内尔",
    "menoufia": "米努菲耶",
    "miami": "迈阿密",
    "shizuoka": "静冈",
    "panjab": "旁遮普",
    "silchar": "锡尔查尔",
    "karnataka": "卡纳塔克",
    "aquila": "拉奎拉",
    "huelva": "韦尔瓦",
    "nanjingtech": "南京工业",
    "ningbotech": "浙大宁波理工",
    "novisad": "诺维萨德",
    "taiz": "塔伊兹",
    "shiraz": "设拉子",
    "thapar": "塔帕尔",
    "paris": "巴黎",
    "litoral": "滨海",
    "huadong": "华东",
    "jiaotong": "交通",
    "xuzhou": "徐州",
    "quzhou": "衢州",
    "changshu": "常熟",
    "hefei": "合肥",
    "linyi": "临沂",
    "minzu": "民族",
    "jianzhu": "建筑",
    "qingdao": "青岛",
    "bohai": "渤海",
    "michoacana": "米却肯",
    "sunway": "双威",
    "windsor": "温莎",
    "stock": "斯托克",
    "holm": "霍尔姆",
    "zhejiang": "浙江",
    "shandong": "山东",
    "jiangsu": "江苏",
    "guangdong": "广东",
    "sichuan": "四川",
    "hubei": "湖北",
    "shaanxi": "陕西",
    "guangxi": "广西",
    "anhui": "安徽",
    "shanxi": "山西",
    "fujian": "福建",
    "guizhou": "贵州",
    "qinghai": "青海",
    "jiangxi": "江西",
    "liaoning": "辽宁",
    "xinjiang": "新疆",
    "hebei": "河北",
    "guilin": "桂林",
    "shenyang": "沈阳",
    "changqing": "长庆",
    "liaohe": "辽河",
    "tarim": "塔里木",
    "canterbury": "坎特伯雷",
    "qatar": "卡塔尔",
    "punjab": "旁遮普",
    "normandie": "诺曼底",
    "loughborough": "拉夫堡",
    "budapest": "布达佩斯",
    "lancaster": "兰卡斯特",
    "selcuk": "塞尔丘克",
    "samara": "萨马拉",
    "bahia": "巴伊亚",
    "islamabad": "伊斯兰堡",
    "arizona": "亚利桑那",
    "bordeaux": "波尔多",
    "idaho": "爱达荷",
    "arkansas": "阿肯色",
    "florence": "佛罗伦萨",
    "milan": "米兰",
    "tanta": "坦塔",
    "virginia": "弗吉尼亚",
    "carolina": "卡罗来纳",
    "tsukuba": "筑波",
    "samaria": "撒马利亚",
    "seoul": "首尔",
    "pohang": "浦项",
    "dongguk": "东国",
    "assiut": "艾斯尤特",
    "taif": "塔伊夫",
    "buenos": "布宜诺斯",
    "aires": "艾利斯",
    "hong": "香",
    "kong": "港",
    "wroclaw": "弗罗茨瓦夫",
    "erciyes": "埃尔吉耶斯",
    "gyeongsang": "庆尚",
    "kwangwoon": "光云",
    "concordia": "康考迪亚",
    "canada": "加拿大",
    "mexico": "墨西哥",
    "texas": "得克萨斯",
    "islamabad": "伊斯兰堡",
    "cnpc": "中国石油天然气集团",
    "cnooc": "中国海洋石油",
    "cpe": "中国石油工程建设",
    "cnodc": "中国石油天然气勘探开发公司",
    "bp": "英国石油",
    "cae": "计算机辅助工程",
    "eor": "提高采收率",
    "hse": "健康安全环境",
    "qhse": "质量健康安全环境",
    "icu": "重症监护病房",
    "picu": "儿童重症监护病房",
    "pacu": "麻醉恢复室",
    "mri": "磁共振室",
    "ct": "计算机断层扫描室",
    "lng": "液化天然气",
    "cng": "压缩天然气",
    "pvc": "聚氯乙烯",
    "abs": "丙烯腈丁二烯苯乙烯",
    "pta": "精对苯二甲酸",
    "mtp": "甲醇制丙烯",
    "sagd": "蒸汽辅助重力泄油",
    "epc": "设计采购施工",
    "fioc": "艾弗艾欧西",
    "argg": "艾尔吉吉",
    "cact": "西艾艾西提",
    "og": "油气",
    "gnt": "吉恩提",
    "mi": "艾姆艾",
    "kbr": "凯洛格布朗鲁特",
    "lanxess": "朗盛",
    "effectech": "艾费克泰克",
    "turner": "特纳",
    "fairbank": "费尔班克",
    "hotwell": "霍特韦尔",
    "beckbury": "贝克伯里",
    "cgg": "西吉吉",
    "veritas": "维里塔斯",
    "heriot": "赫瑞",
    "watt": "瓦特",
    "knowledge": "知识",
    "knowlege": "知识",
    "reservoir": "储库",
    "esmu": "伊艾斯艾姆优",
    "pk": "皮开",
    "b": "乙",
}

NAME_PHRASE_TRANSLATIONS: dict[str, str] = {
    "hong kong": "香港",
    "new mexico": "新墨西哥",
    "north carolina": "北卡罗来纳",
    "south carolina": "南卡罗来纳",
    "west virginia": "西弗吉尼亚",
    "new south wales": "新南威尔士",
    "buenos aires": "布宜诺斯艾利斯",
    "rio janeiro": "里约热内卢",
    "johns hopkins": "约翰斯·霍普金斯",
    "claude bernard": "克洛德·贝尔纳",
    "texas a m": "得克萨斯农工",
    "north china": "华北",
    "south china": "华南",
    "east china": "华东",
    "central south": "中南",
    "cent china": "华中",
    "south cent": "中南",
    "north texas": "北得克萨斯",
    "south florida": "南佛罗里达",
    "central florida": "中佛罗里达",
    "east anglia": "东安格利亚",
    "los andes": "安第斯",
    "north bangkok": "北曼谷",
    "south australia": "南澳",
    "new leon": "新莱昂",
    "san luis potosi": "圣路易斯波托西",
    "royal holloway": "皇家霍洛威",
    "ho chi minh city": "胡志明市",
    "de la salle": "德拉萨",
    "st petersburg": "圣彼得堡",
    "james cook": "詹姆斯库克",
    "rey juan carlos": "胡安卡洛斯国王",
    "case western reserve": "凯斯西储",
    "north minzu": "北方民族",
    "south africa": "南非",
    "west lake": "西湖",
}

SUPPLEMENTAL_ACRONYM_TRANSLATIONS: dict[str, str] = {
    "fau": "弗奥大学",
    "gist": "光州科学技术院",
    "kier": "韩国能源研究院",
    "nims": "日本国立材料科学研究所",
    "simats": "萨维塔医科与技术科学学院",
    "put": "伊朗石油工业大学",
    "vit": "韦洛尔理工学院",
    "lut": "拉彭兰塔-拉赫蒂理工大学",
    "ufz": "亥姆霍兹环境研究中心",
    "ripi": "伊朗石油工业研究所",
    "ucsi": "思特雅大学",
}

LETTER_TRANSLITERATION = {
    "a": "艾",
    "b": "比",
    "c": "西",
    "d": "迪",
    "e": "伊",
    "f": "艾弗",
    "g": "吉",
    "h": "艾奇",
    "i": "艾",
    "j": "杰",
    "k": "开",
    "l": "艾勒",
    "m": "艾姆",
    "n": "恩",
    "o": "欧",
    "p": "皮",
    "q": "吉吾",
    "r": "艾尔",
    "s": "艾斯",
    "t": "提",
    "u": "优",
    "v": "维",
    "w": "达布留",
    "x": "艾克斯",
    "y": "吾艾",
    "z": "齐",
}

GENERIC_NORM_BLACKLIST = {
    "大学",
    "学院",
    "研究院",
    "研究所",
    "国家实验室",
    "技术大学",
    "石油大学",
}

METHOD_LABELS = {
    "manual_exact": "人工精确匹配",
    "manual_review_backfill": "人工复核回灌",
    "manual_generated": "人工规则生成",
    "manual_canonical": "人工规范名匹配",
    "auto_word_level": "自动分词翻译",
    "rule_stable_cn": "规则稳定译名",
    "forced_cn_auto": "低置信自动补译",
    "raw_fallback": "原文回退保留",
    "ambiguous_abbrev": "缩写释义待判定",
}

REASON_LABELS = {
    "empty_trans_or_norm": "译名或标准名为空",
    "norm_not_cjk": "标准名仍含英文或非中文主名",
    "high_freq_auto_result": "高频自动结果需人工复核",
    "top_freq_needs_review": "核心高频机构需人工复核",
    "generic_enterprise_alias": "企业别名或内部研发单元需复核",
    "ambiguous_abbrev": "缩写信息不足，需结合原始地址或国家字段复核",
    "same_name_country_disambiguation": "同名机构已按原英文名区分国家，建议抽样复核",
    "pending_disambiguation_label": "标准名含待区分标记，需继续人工判定",
    "manual_backfill_pending_review": "人工回灌条目仍存在歧义，需保留在复核队列",
}

AMBIGUOUS_REVIEW_NAMES = {
    "Inst Chem Technol",
    "Queens Univ",
    "China University of Geosciences",
    "Univ Sci & Technol",
}

GENERIC_CJK_REVIEW_NAMES = {
    "国家研究中心",
    "石油研究所",
    "地球物理研究研究所",
    "埃克奥勒埃理工",
}

DIRECT_PARENT_ENTERPRISE_ALIASES = {
    "sinopec",
    "petrochina",
    "cnpc",
    "cnooc ltd",
    "china natl offshore oil corp",
    "china national offshore oil corporation",
}

SPECIAL_LAB_PATTERNS: list[tuple[re.Pattern[str], tuple[str, str]]] = [
    (
        re.compile(r"(中国石油大学)?石油工程教育部重点实验室|MOE Key Laboratory of Petroleum Engineering", re.I),
        ("石油工程教育部重点实验室", "石油工程教育部重点实验室"),
    ),
    (
        re.compile(r"(中国石油大学)?重质油国家重点实验室|State Key Laboratory of Heavy Oil Processing", re.I),
        ("重质油国家重点实验室", "重质油国家重点实验室"),
    ),
]

SINOPEC_ENTERPRISE_PATTERNS: list[tuple[re.Pattern[str], tuple[str, str]]] = [
    (
        re.compile(
            r"(中国石化石油勘探开发研究院|中国石油化工股份有限公司石油勘探开发研究院|"
            r"SINOPEC Research Institute of Petroleum Exploration and Production|"
            r"Sinopec Research Institute of Petroleum Exploration and Production|"
            r"SINOPEC Research Institute of Petroleum Exploration and Development|"
            r"Sinopec Research Institute of Petroleum Exploration and Development|"
            r"Research Institute of Petroleum Exploration and Development of SINOPEC|"
            r"Research Institute of Petroleum Exploration and Production)",
            re.I,
        ),
        ("中国石化石油勘探开发研究院", "中国石油化工股份有限公司"),
    ),
    (
        re.compile(
            r"(Wuxi Research Institute of Petroleum Geology|无锡石油地质研究所)",
            re.I,
        ),
        ("无锡石油地质研究所", "中国石油化工股份有限公司"),
    ),
]

PETROCHINA_ENTERPRISE_PATTERNS: list[tuple[re.Pattern[str], tuple[str, str]]] = [
    (
        re.compile(
            r"(中国石油勘探开发研究院|"
            r"PetroChina Research Institute of Petroleum Exploration and Development|"
            r"PetroChina Research Institute of Petroleum Exploration & Development|"
            r"CNPC Research Institute of Petroleum Exploration and Development|"
            r"Research Institute of Petroleum Exploration and Development|"
            r"Research Institute of Petroleum Exploration & Development)",
            re.I,
        ),
        ("中国石油勘探开发研究院", "中国石油天然气股份有限公司"),
    ),
]

CNOOC_ENTERPRISE_PATTERNS: list[tuple[re.Pattern[str], tuple[str, str]]] = [
    (
        re.compile(r"(China Natl Offshore Oil Corp|China National Offshore Oil Corporation)", re.I),
        ("中国海洋石油集团有限公司", "中国海洋石油集团有限公司"),
    ),
    (
        re.compile(r"(CNOOC Ltd|CNOOC Limited)", re.I),
        ("中国海洋石油有限公司", "中国海洋石油有限公司"),
    ),
    (
        re.compile(r"(CNOOC Res Inst Co Ltd|CNOOC Research Institute)", re.I),
        ("中海油研究总院有限责任公司", "中国海洋石油有限公司"),
    ),
]


def install_pandas_stub() -> None:
    fake = types.ModuleType("pandas")

    def isna(value: object) -> bool:
        try:
            return math.isnan(value)  # type: ignore[arg-type]
        except Exception:
            return value is None

    def read_excel(*_args, **_kwargs):
        raise RuntimeError("read_excel is unavailable in the rebuild wrapper")

    fake.isna = isna  # type: ignore[attr-defined]
    fake.read_excel = read_excel  # type: ignore[attr-defined]
    sys.modules["pandas"] = fake


def load_module(path: Path, module_name: str, stub_pandas: bool = False):
    if stub_pandas:
        install_pandas_stub()
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read_csv_guess(path: Path) -> list[dict[str, str]]:
    last_error: Exception | None = None
    for encoding in MANUAL_OVERRIDE_ENCODING_CANDIDATES:
        try:
            with path.open("r", encoding=encoding, newline="") as f:
                return list(csv.DictReader(f))
        except Exception as exc:  # pragma: no cover - best effort reader
            last_error = exc
    raise RuntimeError(f"Unable to read CSV with known encodings: {path}") from last_error


def load_manual_override_pairs(tn, path: Path) -> dict[str, tuple[str, str]]:
    if not path.exists():
        return {}

    exact_pairs: dict[str, tuple[str, str]] = {}
    for row in read_csv_guess(path):
        raw_name = tn.clean_name(
            row.get("机构原始名称")
            or row.get("institution_name")
            or row.get("raw_institution")
            or ""
        )
        trans = tn.clean_name(
            row.get("机构译名")
            or row.get("institution_trans")
            or row.get("translated_institution")
            or ""
        )
        norm = tn.clean_name(
            row.get("机构标准名")
            or row.get("institution_norm")
            or row.get("normalized_institution")
            or ""
        )
        if raw_name and trans and norm:
            exact_pairs[raw_name] = (
                tn.beautify_cn_punct(trans),
                tn.beautify_cn_punct(norm),
            )
    return exact_pairs


def strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(text or ""))
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def extend_translation_resources(tn) -> None:
    for attr, extra in (
        ("EN_REPL", SUPPLEMENTAL_EN_REPL),
        ("WORD_TRANSLATIONS", SUPPLEMENTAL_WORD_TRANSLATIONS),
        ("PROPER_NOUN_TRANSLATIONS", SUPPLEMENTAL_PROPER_NOUN_TRANSLATIONS),
    ):
        base = getattr(tn, attr, None)
        if isinstance(base, dict):
            base.update(extra)
        else:
            setattr(tn, attr, dict(extra))


TRANSLIT_CHUNK_MAP: dict[str, str] = {
    "sch": "施",
    "sh": "什",
    "ch": "奇",
    "ph": "夫",
    "th": "斯",
    "kh": "赫",
    "gh": "格",
    "ck": "克",
    "qu": "库",
    "ng": "恩",
    "ai": "艾",
    "ay": "艾",
    "ei": "伊",
    "ey": "伊",
    "oi": "奥伊",
    "oy": "奥伊",
    "au": "奥",
    "ou": "欧",
    "oo": "乌",
    "ee": "伊",
    "ea": "伊",
    "ie": "耶",
    "ia": "亚",
    "io": "约",
    "oa": "奥",
    "ua": "瓦",
    "ue": "韦",
    "ui": "维",
    "st": "斯特",
    "sp": "斯普",
    "sk": "斯克",
    "tr": "特拉",
    "dr": "德拉",
    "br": "布拉",
    "pr": "普拉",
    "gr": "格拉",
    "fr": "弗拉",
    "cr": "克拉",
    "kr": "克拉",
    "cl": "克勒",
    "pl": "普勒",
    "gl": "格勒",
    "sl": "斯勒",
    "sw": "斯瓦",
    "wr": "罗",
    "wh": "瓦",
    "kn": "纳",
    "ps": "斯",
    "a": "阿",
    "b": "布",
    "c": "克",
    "d": "德",
    "e": "埃",
    "f": "弗",
    "g": "格",
    "h": "赫",
    "i": "伊",
    "j": "杰",
    "k": "克",
    "l": "勒",
    "m": "姆",
    "n": "恩",
    "o": "奥",
    "p": "普",
    "q": "库",
    "r": "尔",
    "s": "斯",
    "t": "特",
    "u": "乌",
    "v": "维",
    "w": "瓦",
    "x": "克斯",
    "y": "伊",
    "z": "兹",
}

TRANSLIT_EXACT_MAP: dict[str, str] = {
    "stockholm": "斯德哥尔摩",
    "lausanne": "洛桑",
    "hohenheim": "霍恩海姆",
    "sherbrooke": "舍布鲁克",
    "syracuse": "锡拉丘兹",
    "palacky": "帕拉茨基",
    "tampere": "坦佩雷",
    "marmara": "马尔马拉",
    "kumamoto": "熊本",
    "carleton": "卡尔顿",
    "gebze": "盖布泽",
    "linnaeus": "林奈",
    "menoufia": "米努菲耶",
    "thammasat": "法政",
    "brunel": "布鲁内尔",
    "brighton": "布莱顿",
    "leicester": "莱斯特",
    "potsdam": "波茨坦",
    "warsaw": "华沙",
    "pavia": "帕维亚",
    "gottingen": "哥廷根",
    "yamaguchi": "山口",
    "vaasa": "瓦萨",
    "taiz": "塔伊兹",
    "thapar": "塔帕尔",
    "shiraz": "设拉子",
    "kaohsiung": "高雄",
    "royalholloway": "皇家霍洛威",
    "jamescook": "詹姆斯库克",
    "reyjuancarlos": "胡安卡洛斯国王",
    "bualisina": "布阿里西纳",
    "novisad": "诺维萨德",
    "houston": "休斯敦",
    "huzhou": "湖州",
    "xuzhou": "徐州",
    "linyi": "临沂",
    "quzhou": "衢州",
    "jianghan": "江汉",
    "ningbotech": "浙大宁波理工",
    "jilinjianzhu": "吉林建筑",
    "xinyang": "信阳",
}


def transliterate_ascii_token(token: str) -> str:
    value = strip_accents(token or "").lower()
    value = re.sub(r"[^a-z0-9]", "", value)
    if not value:
        return ""
    if value in SUPPLEMENTAL_ACRONYM_TRANSLATIONS:
        return SUPPLEMENTAL_ACRONYM_TRANSLATIONS[value]
    if value in TRANSLIT_EXACT_MAP:
        return TRANSLIT_EXACT_MAP[value]

    result: list[str] = []
    i = 0
    while i < len(value):
        if value[i].isdigit():
            result.append(value[i])
            i += 1
            continue
        matched = False
        for span in (3, 2, 1):
            chunk = value[i : i + span]
            if chunk in TRANSLIT_CHUNK_MAP:
                result.append(TRANSLIT_CHUNK_MAP[chunk])
                i += span
                matched = True
                break
        if matched:
            continue
        result.append(value[i].upper())
        i += 1

    text = "".join(result)
    text = re.sub(r"(.)\1{2,}", r"\1", text)
    text = text.replace("埃伊", "伊").replace("伊伊", "伊").replace("奥伊", "欧伊")
    return text


def translate_ascii_token_detailed(token: str, tn) -> tuple[str, str]:
    raw = strip_accents(token).strip()
    if not raw:
        return "", "empty"
    key = raw.lower().strip(".")
    key = getattr(tn, "EN_REPL", {}).get(key, key)
    if key in SUPPLEMENTAL_ACRONYM_TRANSLATIONS:
        return SUPPLEMENTAL_ACRONYM_TRANSLATIONS[key], "acronym"
    if key in getattr(tn, "PROPER_NOUN_TRANSLATIONS", {}):
        return tn.PROPER_NOUN_TRANSLATIONS[key], "proper"
    if key in getattr(tn, "WORD_TRANSLATIONS", {}):
        return tn.WORD_TRANSLATIONS[key], "word"
    if key.isdigit():
        return key, "digit"
    if strip_accents(raw).lower().strip(".") in TRANSLIT_EXACT_MAP:
        return TRANSLIT_EXACT_MAP[strip_accents(raw).lower().strip(".")], "translit_exact"
    translit = transliterate_ascii_token(raw)
    if translit and not has_ascii_letter(translit):
        return translit, "translit"
    title = raw.replace("_", " ").strip()
    if title.islower():
        title = title.title()
    return title, "fallback"


def translate_ascii_token(token: str, tn) -> str:
    return translate_ascii_token_detailed(token, tn)[0]


def translate_name_tokens(tokens: list[str], tn) -> str:
    translated: list[str] = []
    i = 0
    while i < len(tokens):
        matched = False
        for span in range(min(4, len(tokens) - i), 1, -1):
            phrase = " ".join(tokens[i : i + span])
            if phrase in NAME_PHRASE_TRANSLATIONS:
                translated.append(NAME_PHRASE_TRANSLATIONS[phrase])
                i += span
                matched = True
                break
        if matched:
            continue
        translated.append(translate_ascii_token(tokens[i], tn))
        i += 1
    return "".join(part for part in translated if part)


LOW_CONFIDENCE_CN_RESULTS = {
    "大学",
    "学院",
    "研究所",
    "研究院",
    "中心",
    "联邦大学",
    "国家大学",
    "科学院",
    "国家研究中心",
    "石油研究所",
    "化学学院",
}

LOW_CONFIDENCE_CN_PREFIXES = (
    "大学",
    "学院",
    "研究所",
    "研究院",
)

LOW_CONFIDENCE_MARKERS = (
    "研究研究所",
    "研究研究院",
    "大学大学",
    "学院学院",
    "大学技术",
    "研究所技术",
    "国家研究中心",
)

STABLE_CN_SUFFIXES = (
    "大学",
    "学院",
    "研究所",
    "研究院",
    "实验室",
    "公司",
    "有限公司",
    "集团",
    "中心",
    "研究中心",
    "科学院",
    "医院",
    "学会",
    "委员会",
    "部",
    "局",
)


def analyze_name_token_sources(name: str, tn) -> dict[str, int]:
    canonical = tn.canonical_en_key(name)
    tokens = [part for part in canonical.split() if part]
    stats = {
        "token_total": 0,
        "mapped_total": 0,
        "phrase_total": 0,
        "specific_total": 0,
        "translit_total": 0,
        "fallback_total": 0,
    }
    i = 0
    while i < len(tokens):
        matched = False
        for span in range(min(4, len(tokens) - i), 1, -1):
            phrase = " ".join(tokens[i : i + span])
            if phrase in NAME_PHRASE_TRANSLATIONS:
                stats["token_total"] += span
                stats["mapped_total"] += span
                stats["phrase_total"] += span
                stats["specific_total"] += span
                i += span
                matched = True
                break
        if matched:
            continue
        _, source = translate_ascii_token_detailed(tokens[i], tn)
        stats["token_total"] += 1
        if source in {"acronym", "proper", "word", "digit", "translit_exact"}:
            stats["mapped_total"] += 1
            if source in {"acronym", "proper", "translit_exact"}:
                stats["specific_total"] += 1
        elif source == "translit":
            stats["translit_total"] += 1
        else:
            stats["fallback_total"] += 1
        i += 1
    return stats


def should_mark_rule_stable(name: str, trans: str, norm: str, tn) -> bool:
    clean_trans = tn.clean_name(trans)
    clean_norm = tn.clean_name(norm)
    if not clean_trans or not clean_norm:
        return False
    if has_ascii_letter(clean_trans) or has_ascii_letter(clean_norm):
        return False
    if clean_norm in GENERIC_NORM_BLACKLIST or clean_norm in GENERIC_CJK_REVIEW_NAMES or clean_norm in LOW_CONFIDENCE_CN_RESULTS:
        return False
    if any(marker in clean_trans for marker in LOW_CONFIDENCE_MARKERS):
        return False
    if any(marker in clean_norm for marker in LOW_CONFIDENCE_MARKERS):
        return False
    if clean_trans.startswith(LOW_CONFIDENCE_CN_PREFIXES) and clean_trans not in {clean_norm, "南澳大学"}:
        return False
    if clean_norm.startswith(LOW_CONFIDENCE_CN_PREFIXES) and clean_norm not in {clean_trans, "南澳大学"}:
        return False
    if clean_trans != clean_norm:
        return False
    if not clean_trans.endswith(STABLE_CN_SUFFIXES):
        return False
    if not clean_norm.endswith(STABLE_CN_SUFFIXES):
        return False

    stats = analyze_name_token_sources(name, tn)
    token_total = stats["token_total"]
    specific_total = stats["specific_total"]
    translit_total = stats["translit_total"]
    fallback_total = stats["fallback_total"]
    if token_total == 0:
        return False
    if specific_total == 0:
        return False
    if fallback_total > 0:
        return False
    return translit_total == 0


def structured_translate_tokens(tokens: list[str], tn) -> str:
    if not tokens:
        return ""

    def name(parts: list[str]) -> str:
        return translate_name_tokens(parts, tn)

    if len(tokens) >= 4 and tokens[-4:] == ["electronic", "science", "technology", "university"]:
        return name(tokens[:-4]) + "电子科技大学"
    if len(tokens) >= 3 and tokens[-3:] == ["science", "technology", "university"]:
        return name(tokens[:-3]) + "科技大学"
    if len(tokens) >= 2 and tokens[-2:] == ["state", "university"]:
        return name(tokens[:-2]) + "州立大学"
    if len(tokens) >= 2 and tokens[-2:] == ["national", "university"]:
        return name(tokens[:-2]) + "国立大学"
    if len(tokens) >= 2 and tokens[-2:] == ["federal", "university"]:
        return name(tokens[:-2]) + "联邦大学"
    if len(tokens) >= 2 and tokens[-2:] == ["normal", "university"]:
        return name(tokens[:-2]) + "师范大学"
    if len(tokens) >= 2 and tokens[-2:] == ["medical", "university"]:
        return name(tokens[:-2]) + "医科大学"
    if len(tokens) >= 2 and tokens[-2:] == ["agricultural", "university"]:
        return name(tokens[:-2]) + "农业大学"
    if len(tokens) >= 2 and tokens[-2:] == ["petroleum", "university"]:
        return name(tokens[:-2]) + "石油大学"
    if len(tokens) >= 2 and tokens[-2:] == ["polytechnic", "university"]:
        return name(tokens[:-2]) + "理工大学"
    if len(tokens) >= 2 and tokens[-2:] == ["technology", "university"]:
        return name(tokens[:-2]) + "理工大学"
    if len(tokens) >= 2 and tokens[-2:] == ["university", "technology"]:
        return name(tokens[:-2]) + "理工大学"
    if len(tokens) >= 2 and tokens[-2:] == ["institute", "technology"]:
        return name(tokens[:-2]) + "理工学院"
    if len(tokens) >= 2 and tokens[-2:] == ["technology", "institute"]:
        return name(tokens[:-2]) + "理工学院"
    if len(tokens) >= 3 and tokens[-3:] == ["institute", "science", "technology"]:
        return name(tokens[:-3]) + "科技学院"
    if len(tokens) >= 3 and tokens[-3:] == ["university", "science", "technology"]:
        return name(tokens[:-3]) + "科技大学"
    if len(tokens) >= 4 and tokens[:4] == ["university", "electronic", "science", "technology"]:
        return name(tokens[4:]) + "电子科技大学"
    if len(tokens) >= 3 and tokens[:3] == ["university", "science", "technology"]:
        return name(tokens[3:]) + "科技大学"
    if len(tokens) >= 3 and tokens[:3] == ["university", "engineering", "technology"]:
        return name(tokens[3:]) + "工程技术大学"
    if len(tokens) >= 2 and tokens[:2] == ["university", "technology"]:
        return name(tokens[2:]) + "理工大学"
    if len(tokens) >= 2 and tokens[:2] == ["university", "federal"]:
        return name(tokens[2:]) + "联邦大学"
    if len(tokens) >= 2 and tokens[:2] == ["university", "national"]:
        return name(tokens[2:]) + "国立大学"
    if len(tokens) >= 2 and tokens[:2] == ["university", "autonomous"]:
        return name(tokens[2:]) + "自治大学"
    if len(tokens) >= 2 and tokens[:2] == ["university", "polytechnic"]:
        return name(tokens[2:]) + "理工大学"
    if len(tokens) >= 1 and tokens[0] == "university":
        return name(tokens[1:]) + "大学"
    if len(tokens) >= 1 and tokens[-1] == "university":
        return name(tokens[:-1]) + "大学"
    if len(tokens) >= 2 and tokens[-2:] == ["research", "institute"]:
        return name(tokens[:-2]) + "研究院"
    if len(tokens) >= 1 and tokens[-1] == "institute":
        if any(token in tokens for token in ("technology", "polytechnic", "science", "engineering")):
            core = [token for token in tokens[:-1] if token not in {"technology", "polytechnic", "science", "engineering"}]
            return name(core) + "理工学院"
        return name(tokens[:-1]) + "研究所"
    if len(tokens) >= 2 and tokens[-2:] in (["national", "laboratory"], ["national", "lab"]):
        return name(tokens[:-2]) + "国家实验室"
    if len(tokens) >= 1 and tokens[-1] in {"laboratory", "lab"}:
        return name(tokens[:-1]) + "实验室"
    if len(tokens) >= 2 and tokens[-2:] in (["academy", "sciences"], ["academy", "science"]):
        return name(tokens[:-2]) + "科学院"
    if len(tokens) >= 2 and tokens[-2:] == ["oilfield", "company"]:
        return name(tokens[:-2]) + "油田公司"
    if len(tokens) >= 1 and tokens[-1] == "company":
        return name(tokens[:-1]) + "公司"
    if len(tokens) >= 1 and tokens[-1] == "group":
        return name(tokens[:-1]) + "集团"
    if len(tokens) >= 1 and tokens[-1] == "council":
        return name(tokens[:-1]) + "委员会"
    return name(tokens)


def force_translate_ascii_runs(text: str, tn) -> str:
    value = str(text or "")

    def repl(match: re.Match[str]) -> str:
        token = match.group(0)
        canonical = tn.canonical_en_key(token)
        tokens = [part for part in canonical.split() if part]
        if not tokens:
            return token
        if len(tokens) == 1:
            return translate_ascii_token(tokens[0], tn)
        return structured_translate_tokens(tokens, tn)

    value = re.sub(r"[A-Za-z][A-Za-z0-9&'./\-]*", repl, value)
    value = value.replace("&", "与").replace("/", "、")
    value = value.replace("，，", "，")
    value = re.sub(r"\s+", "", value)
    return tn.beautify_cn_punct(tn.clean_name(value))


def force_pair_to_chinese(
    name: str,
    trans: str,
    norm: str,
    method: str,
    exact_norm_map: dict[str, str],
    canonical_map: dict[str, str],
    university_names: list[str],
    tn,
) -> tuple[str, str, str]:
    if not name or tn.has_cjk(name):
        return trans, norm, method

    canonical = tn.canonical_en_key(name)
    tokens = [part for part in canonical.split() if part]
    forced_trans = trans
    forced_norm = norm

    if (not forced_trans or has_ascii_letter(forced_trans)) and canonical in canonical_map:
        forced_trans = canonical_map[canonical]
    if not forced_trans or has_ascii_letter(forced_trans):
        forced_trans = structured_translate_tokens(tokens, tn)
    if not forced_trans or has_ascii_letter(forced_trans):
        forced_trans = force_translate_ascii_runs(forced_trans or name, tn)
    else:
        forced_trans = force_translate_ascii_runs(forced_trans, tn)

    if canonical in canonical_map:
        forced_norm = canonical_map[canonical]
    if not forced_norm or has_ascii_letter(forced_norm):
        forced_norm = tn.normalize_chinese_name(forced_trans, exact_norm_map, university_names)
    if not forced_norm or has_ascii_letter(forced_norm):
        forced_norm = force_translate_ascii_runs(forced_norm or forced_trans, tn)
    if not forced_norm:
        forced_norm = forced_trans

    forced_trans = tn.beautify_cn_punct(tn.clean_name(forced_trans))
    forced_norm = tn.beautify_cn_punct(tn.clean_name(forced_norm))
    next_method = method if method in {"manual_exact", "manual_review_backfill", "manual_generated", "manual_canonical", "ambiguous_abbrev"} else "forced_cn_auto"
    return forced_trans, forced_norm, next_method


def load_manual_override_rules(tn, path: Path) -> tuple[dict[str, tuple[str, str]], dict[str, tuple[str, str]]]:
    if not path.exists():
        return {}, {}

    exact_pairs: dict[str, tuple[str, str]] = {}
    review_pairs: dict[str, tuple[str, str]] = {}
    review_modes = {"复核回灌", "人工复核回灌", "review_backfill", "manual_review_backfill"}

    for row in read_csv_guess(path):
        raw_name = tn.clean_name(
            row.get("机构原始名称")
            or row.get("institution_name")
            or row.get("raw_institution")
            or ""
        )
        trans = tn.clean_name(
            row.get("机构译名")
            or row.get("institution_trans")
            or row.get("translated_institution")
            or ""
        )
        norm = tn.clean_name(
            row.get("机构标准名")
            or row.get("institution_norm")
            or row.get("normalized_institution")
            or ""
        )
        mode = tn.clean_name(
            row.get("映射方式")
            or row.get("mapping_mode")
            or row.get("override_mode")
            or ""
        )
        if not raw_name or not trans or not norm:
            continue
        pair = (tn.beautify_cn_punct(trans), tn.beautify_cn_punct(norm))
        if mode in review_modes:
            review_pairs[raw_name] = pair
        else:
            exact_pairs[raw_name] = pair

    return exact_pairs, review_pairs


def build_maps(tn, rp, manual_override_path: Path):
    exact_pairs: dict[str, tuple[str, str]] = {}

    for raw_name, pair in getattr(rp, "MANUAL_EXACT", {}).items():
        if not isinstance(pair, tuple) or len(pair) != 2:
            continue
        trans = tn.clean_name(pair[0])
        norm = tn.clean_name(pair[1])
        raw = tn.clean_name(raw_name)
        if raw and trans and norm:
            exact_pairs[raw] = (tn.beautify_cn_punct(trans), tn.beautify_cn_punct(norm))

    for raw_name, pair in EXTRA_EXACT_PAIRS.items():
        exact_pairs[tn.clean_name(raw_name)] = pair

    for raw_name, pair in SUPPLEMENTAL_EXACT_PAIRS.items():
        exact_pairs[tn.clean_name(raw_name)] = pair

    for raw_name, pair in HIGH_FREQ_WELL_TRANSLATED_PAIRS.items():
        exact_pairs[tn.clean_name(raw_name)] = pair

    for raw_name, pair in HIGH_FREQ_SECOND_WAVE_PAIRS.items():
        exact_pairs[tn.clean_name(raw_name)] = pair

    for raw_name, pair in HIGH_FREQ_THIRD_WAVE_PAIRS.items():
        exact_pairs[tn.clean_name(raw_name)] = pair

    for raw_name, pair in HIGH_FREQ_FOURTH_WAVE_PAIRS.items():
        exact_pairs[tn.clean_name(raw_name)] = pair

    manual_override_pairs, manual_review_pairs = load_manual_override_rules(tn, manual_override_path)
    for raw_name, pair in manual_override_pairs.items():
        exact_pairs[raw_name] = pair

    wikidata_lookup_pairs = load_manual_override_pairs(tn, DEFAULT_WIKIDATA_LOOKUP_PATH)
    for raw_name, pair in wikidata_lookup_pairs.items():
        exact_pairs[raw_name] = pair

    exact_norm_map: dict[str, str] = {}
    for raw_name, pair in exact_pairs.items():
        norm = tn.clean_name(pair[1])
        if norm:
            exact_norm_map[raw_name] = tn.beautify_cn_punct(norm)

    canonical_map = dict(getattr(tn, "MANUAL_CANONICAL_TRANSLATIONS", {}))
    canonical_map.update(EXTRA_CANONICAL_TRANSLATIONS)

    for raw_name, norm in exact_norm_map.items():
        if not tn.has_cjk(raw_name) and tn.has_cjk(norm):
            canonical = tn.canonical_en_key(raw_name)
            if canonical:
                canonical_map.setdefault(canonical, norm)

    university_names = sorted(
        {
            tn.clean_name(value)
            for value in list(exact_norm_map.values()) + list(canonical_map.values())
            if tn.has_cjk(tn.clean_name(value)) and "大学" in tn.clean_name(value)
        },
        key=len,
        reverse=True,
    )

    return exact_pairs, exact_norm_map, canonical_map, university_names, manual_review_pairs


def compact_country_hint_text(value: str) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    text = text.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    text = text.replace("\u3000", " ").replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_country_name(country: str) -> str:
    clean = compact_country_hint_text(country)
    return COUNTRY_NAME_ALIASES.get(clean, clean)


def load_country_hint_map(corpus_path: Path) -> dict[str, dict[str, object]]:
    if not corpus_path.exists():
        return {}

    counters: dict[str, Counter[str]] = {}
    with corpus_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pairs = str(row.get("institution_country_pairs") or "")
            for part in pairs.split(" || "):
                if "@@" not in part:
                    continue
                raw_name, raw_country = part.rsplit("@@", 1)
                name = compact_country_hint_text(raw_name)
                country = normalize_country_name(raw_country)
                if not name or not country:
                    continue
                counters.setdefault(name, Counter())[country] += 1

    country_hint_map: dict[str, dict[str, object]] = {}
    for name, counter in counters.items():
        total = sum(counter.values())
        if total <= 0:
            continue
        primary_country, primary_count = counter.most_common(1)[0]
        country_hint_map[name] = {
            "primary_country": primary_country,
            "primary_share": primary_count / total,
            "country_count": len(counter),
            "summary": " / ".join(f"{country}:{count}" for country, count in counter.most_common(3)),
        }
    return country_hint_map


def resolve_country_dominant_exact_pair(name: str, country_hint: dict[str, object] | None) -> tuple[str, str] | None:
    if not country_hint or name not in COUNTRY_DOMINANT_EXACT_PAIRS:
        return None

    primary_country = str(country_hint.get("primary_country") or "")
    primary_share = float(country_hint.get("primary_share") or 0.0)
    country_count = int(country_hint.get("country_count") or 0)
    if country_count == 1 or primary_share >= 0.9:
        return COUNTRY_DOMINANT_EXACT_PAIRS[name].get(primary_country)
    return None


def has_country_disambiguation_risk(country_hint: dict[str, object] | None) -> bool:
    if not country_hint:
        return False
    country_count = int(country_hint.get("country_count") or 0)
    primary_share = float(country_hint.get("primary_share") or 0.0)
    return country_count > 1 and primary_share < 0.9


def should_review_country_disambiguation_name(name: str) -> bool:
    canonical = re.sub(r"[^a-z0-9]+", " ", str(name or "").lower()).strip()
    if not canonical:
        return False
    if canonical in COUNTRY_DISAMBIGUATION_SKIP_NAMES:
        return False

    tokens = [token for token in canonical.split() if token]
    if not tokens:
        return False
    if len(tokens) == 1 and len(tokens[0]) <= 6:
        return True
    return any(token in COUNTRY_DISAMBIGUATION_TOKENS for token in tokens)


def has_ascii_letter(text: str) -> bool:
    return any("A" <= ch <= "Z" or "a" <= ch <= "z" for ch in text)


def should_rebuild_auto_translation(name: str, trans: str, norm: str, method: str, tn) -> bool:
    if not name or tn.has_cjk(name):
        return False
    if method in {"manual_exact", "manual_generated", "manual_canonical", "ambiguous_abbrev"}:
        return False

    clean_trans = tn.clean_name(trans)
    clean_norm = tn.clean_name(norm)
    generic_prefixes = ("大学", "学院", "研究院", "研究所", "公司", "集团", "国家", "中心")
    noisy_markers = (
        "研究研究所",
        "研究研究院",
        "大学大学",
        "学院学院",
        "大学技术",
        "研究所技术",
        "中心研究技术",
    )

    if has_ascii_letter(clean_trans) or has_ascii_letter(clean_norm):
        return True
    if any(marker in clean_trans for marker in noisy_markers):
        return True
    if clean_trans.startswith(generic_prefixes) and len(clean_trans) > 4:
        return True
    if clean_norm in GENERIC_NORM_BLACKLIST:
        return True
    if clean_norm.startswith(generic_prefixes) and len(clean_norm) > 4:
        return True

    return False


def should_fallback_from_auto(name: str, trans: str, norm: str, method: str, tn) -> bool:
    if method != "auto_word_level":
        return False

    clean_name = tn.clean_name(name)
    clean_norm = tn.clean_name(norm)

    if not clean_norm:
        return True
    if clean_norm in GENERIC_NORM_BLACKLIST:
        return True
    if has_ascii_letter(clean_norm):
        return True
    if (
        not tn.has_cjk(clean_name)
        and len(clean_norm) <= 4
        and any(token in clean_norm for token in ("大学", "学院", "研究院", "研究所", "实验室"))
    ):
        return True

    return False


def localize_method(method: str) -> str:
    return METHOD_LABELS.get(method, method)


def localize_reason(reason_text: str) -> str:
    parts = [part.strip() for part in str(reason_text or "").split("|") if part.strip()]
    localized = [REASON_LABELS.get(part, part) for part in parts]
    return "；".join(localized)


def normalize_parent_level(name: str, trans: str, norm: str, method: str, tn) -> tuple[str, str, str]:
    clean_name = tn.clean_name(name)

    if (
        clean_name.startswith("中国石油大学")
        and "华东" not in clean_name
        and "（华东）" not in clean_name
        and clean_name not in {"中国石油大学（北京）", "中国石油大学（华东）"}
        and "重点实验室" not in clean_name
    ):
        return trans or clean_name, "中国石油大学（北京）", "manual_generated"

    if (
        clean_name.startswith("中国科学院")
        and clean_name not in {"中国科学院", "中国科学院大学"}
        and any(token in clean_name for token in ("研究所", "研究院", "实验室", "中心", "分院"))
    ):
        return trans or clean_name, "中国科学院", "manual_generated"

    if (
        clean_name.startswith("中国石化")
        or clean_name.startswith("中国石油化工股份有限公司")
    ) and any(token in clean_name for token in ("研究院", "研究所", "分公司", "油田", "实验室")):
        return trans or clean_name, "中国石油化工股份有限公司", "manual_generated"

    return trans, norm, method


def normalize_special_cases(name: str, trans: str, norm: str, method: str, tn) -> tuple[str, str, str]:
    clean_name = tn.clean_name(name)
    lower_name = clean_name.lower()

    for pattern, pair in SPECIAL_LAB_PATTERNS:
        if pattern.search(clean_name):
            return pair[0], pair[1], "manual_exact"

    if clean_name == "Inst Chem Technol":
        return trans, norm, "ambiguous_abbrev"

    for pattern, pair in SINOPEC_ENTERPRISE_PATTERNS:
        if pattern.search(clean_name):
            if "wuxi research institute of petroleum geology" in lower_name or "无锡石油地质研究所" in clean_name:
                return "无锡石油地质研究所", pair[1], "manual_exact"
            return pair[0], pair[1], "manual_exact"

    for pattern, pair in PETROCHINA_ENTERPRISE_PATTERNS:
        if pattern.search(clean_name):
            if "langfang" in lower_name:
                return "中国石油勘探开发研究院廊坊分院", pair[1], "manual_generated"
            if "northwest" in lower_name:
                return "中国石油勘探开发研究院西北分院", pair[1], "manual_generated"
            return pair[0], pair[1], "manual_exact"

    for pattern, pair in CNOOC_ENTERPRISE_PATTERNS:
        if pattern.search(clean_name):
            if "tianjin branch" in lower_name:
                return "中国海洋石油有限公司天津分公司", "中国海洋石油有限公司", "manual_generated"
            return pair[0], pair[1], "manual_exact"

    return normalize_parent_level(name, trans, norm, method, tn)


def finalize_method(name: str, trans: str, norm: str, method: str, tn) -> str:
    clean_name = tn.clean_name(name)
    clean_trans = tn.clean_name(trans)
    clean_norm = tn.clean_name(norm)
    if (
        method == "raw_fallback"
        and clean_name
        and tn.has_cjk(clean_name)
        and clean_name == clean_trans == clean_norm
    ):
        return "rule_stable_cn"
    if method == "forced_cn_auto" and should_mark_rule_stable(name, trans, norm, tn):
        return "rule_stable_cn"
    return method


def classify_result(
    name: str,
    count: int,
    trans: str,
    norm: str,
    method: str,
    country_hint: dict[str, object] | None,
    tn,
) -> tuple[int, str]:
    reasons: list[str] = []
    clean = tn.clean_name(name)
    clean_norm = tn.clean_name(norm)
    clean_trans = tn.clean_name(trans)
    lower_clean = clean.lower()
    raw_is_stable_cjk = bool(clean and clean == clean_norm and tn.has_cjk(clean_norm))
    norm_has_cjk = tn.has_cjk(clean_norm)
    trans_has_cjk = tn.has_cjk(clean_trans)
    unresolved_non_cjk = bool(clean_norm and not norm_has_cjk)
    generic_cn_result = clean_norm in GENERIC_CJK_REVIEW_NAMES
    enterprise_like = any(
        token in lower_clean
        for token in (
            "research institute of exploration and development",
            "exploration and development research institute",
            "petrochina",
            "sinopec",
            "cnpc",
            "cnooc",
            "oilfield",
            "company",
            "corp",
        )
    )
    direct_parent_alias = lower_clean in DIRECT_PARENT_ENTERPRISE_ALIASES
    high_impact = count >= 100
    medium_impact = count >= 50
    token_source_stats = analyze_name_token_sources(name, tn) if method == "forced_cn_auto" and medium_impact else None

    if not trans or not norm:
        reasons.append("empty_trans_or_norm")
    if unresolved_non_cjk and (high_impact or (enterprise_like and count >= 50)):
        reasons.append("norm_not_cjk")
    if generic_cn_result and high_impact:
        reasons.append("high_freq_auto_result")
    if method == "auto_word_level" and high_impact and (generic_cn_result or not trans_has_cjk or not norm_has_cjk):
        reasons.append("high_freq_auto_result")
    if method == "raw_fallback" and high_impact and not raw_is_stable_cjk and unresolved_non_cjk:
        reasons.append("high_freq_auto_result")
    if (
        method == "forced_cn_auto"
        and medium_impact
        and token_source_stats
        and (token_source_stats["translit_total"] > 0 or token_source_stats["fallback_total"] > 0)
    ):
        reasons.append("high_freq_auto_result")
    if (
        method not in {"manual_exact", "manual_generated", "manual_canonical"}
        and high_impact
        and not raw_is_stable_cjk
        and (unresolved_non_cjk or generic_cn_result)
    ):
        reasons.append("top_freq_needs_review")
    if enterprise_like and not direct_parent_alias and not tn.has_cjk(clean_norm) and count >= 50:
        reasons.append("generic_enterprise_alias")
    if clean in AMBIGUOUS_REVIEW_NAMES and count >= 20:
        reasons.append("ambiguous_abbrev")
    if has_country_disambiguation_risk(country_hint) and should_review_country_disambiguation_name(clean) and count >= 20:
        reasons.append("same_name_country_disambiguation")
    if "待区分" in clean_norm:
        reasons.append("pending_disambiguation_label")
    if method == "manual_review_backfill" and ("待区分" in clean_norm or "待区分" in clean_trans):
        reasons.append("manual_backfill_pending_review")

    reasons = list(dict.fromkeys(reasons))
    return (1 if reasons else 0), " | ".join(reasons)


def write_note(path: Path) -> None:
    lines = [
        "机构译名与标准化规则说明（重建版）",
        "",
        "1. 以双主键正式语料抽取出的机构表为唯一输入主表。",
        "2. 优先复用已确认的人工精确映射与规范名映射，并补充当前高频机构的官方译名。",
        "3. 中文机构优先做别名清洗与母体机构归并；学院、系所、普通中心等子单元默认回收到一级机构。",
        "4. 企业内部研究院、分院和研究所优先回收到母公司或一级企业研发主体，避免子单元直接进入主题画像。",
        "5. 对教育部重点实验室等少量具有独立科研平台属性的机构，保留平台级标准名。",
        "6. 外文简称先结合正式语料中的 institution_country_pairs 提取主国家提示；当同一简称在多个国家同时出现且无法稳定单值判定时，进入复核队列而不强行归并。",
        "7. 中文复核队列只保留高频高影响、缩写歧义、国家歧义和企业别名高风险条目，不把低频长尾噪声机械地下放人工审核。",
        "8. 输出结果是可复跑、可审查的标准化主表，不把未证实缩写静默写成确定机构。",
        "9. 人工复核确认的高频机构覆盖规则单独保存在 institution_manual_override_rules.csv，并在重跑时优先应用。",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def build_table(
    input_path: Path,
    output_path: Path,
    review_path: Path,
    note_path: Path,
    manual_override_path: Path,
) -> None:
    tn = load_module(TRANSLATE_SCRIPT, "rebuild_translate_module", stub_pandas=True)
    rp = load_module(REPAIR_SCRIPT, "rebuild_repair_module")
    extend_translation_resources(tn)

    exact_pairs, exact_norm_map, canonical_map, university_names, manual_review_pairs = build_maps(tn, rp, manual_override_path)
    country_hint_map = load_country_hint_map(DEFAULT_CORPUS_PATH)

    output_rows: list[dict[str, object]] = []
    review_rows: list[dict[str, object]] = []
    method_counter: Counter[str] = Counter()

    with input_path.open("r", encoding=INPUT_ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        for extra in (
            "institution_trans",
            "institution_norm",
            "candidate_method",
            "candidate_review_flag",
            "candidate_review_reason",
        ):
            if extra not in fieldnames:
                fieldnames.append(extra)

        for row in reader:
            name = tn.clean_name(row.get("institution_name", ""))
            count = int(float(row.get("count") or 0))
            country_hint = country_hint_map.get(name)

            pair = exact_pairs.get(name)
            method = ""

            if pair:
                method = "manual_exact"
            else:
                pair = resolve_country_dominant_exact_pair(name, country_hint)
                if pair:
                    method = "manual_exact"

            if not pair:
                generated = rp.manual_generated(name)
                if generated and generated[0] and generated[1]:
                    pair = (
                        tn.beautify_cn_punct(tn.clean_name(generated[0])),
                        tn.beautify_cn_punct(tn.clean_name(generated[1])),
                    )
                    method = "manual_generated"

            if not pair:
                canonical = tn.canonical_en_key(name) if name and not tn.has_cjk(name) else ""
                trans, norm = tn.translate_and_normalize(name, exact_norm_map, canonical_map, university_names)
                pair = (trans, norm)
                if canonical and canonical in canonical_map:
                    method = "manual_canonical"
                elif trans == name and norm == name:
                    method = "raw_fallback"
                elif tn.has_cjk(norm):
                    method = "auto_word_level"
                else:
                    method = "raw_fallback"

            if not tn.has_cjk(name) and (
                should_fallback_from_auto(name, pair[0], pair[1], method, tn)
                or should_rebuild_auto_translation(name, pair[0], pair[1], method, tn)
                or has_ascii_letter(pair[0])
                or has_ascii_letter(pair[1])
            ):
                trans, norm, method = force_pair_to_chinese(
                    name,
                    pair[0],
                    pair[1],
                    method,
                    exact_norm_map,
                    canonical_map,
                    university_names,
                    tn,
                )
                pair = (trans, norm)

            if has_ascii_letter(pair[0]) or has_ascii_letter(pair[1]):
                trans = force_translate_ascii_runs(pair[0], tn)
                norm = force_translate_ascii_runs(pair[1], tn)
                if not norm or has_ascii_letter(norm):
                    norm = tn.normalize_chinese_name(norm or trans, exact_norm_map, university_names)
                pair = (tn.beautify_cn_punct(tn.clean_name(trans)), tn.beautify_cn_punct(tn.clean_name(norm)))
                if method in {"raw_fallback", "auto_word_level"}:
                    method = "forced_cn_auto"

            review_pair = manual_review_pairs.get(name)
            if review_pair:
                pair = review_pair
                method = "manual_review_backfill"

            trans, norm = pair
            trans, norm, method = normalize_special_cases(name, trans, norm, method, tn)
            method = finalize_method(name, trans, norm, method, tn)
            review_flag, review_reason = classify_result(name, count, trans, norm, method, country_hint, tn)

            out_row = dict(row)
            out_row["institution_trans"] = trans
            out_row["institution_norm"] = norm
            out_row["candidate_method"] = localize_method(method)
            out_row["candidate_review_flag"] = "是" if review_flag else "否"
            out_row["candidate_review_reason"] = localize_reason(review_reason)
            output_rows.append(out_row)

            method_counter[method] += 1
            if review_flag and name:
                review_rows.append(
                    {
                        "institution_name": name,
                        "count": count,
                        "cnki_count": row.get("cnki_count", ""),
                        "wos_count": row.get("wos_count", ""),
                        "cscd_count": row.get("cscd_count", ""),
                        "institution_trans": trans,
                        "institution_norm": norm,
                        "candidate_method": localize_method(method),
                        "candidate_review_reason": localize_reason(review_reason),
                    }
                )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding=OUTPUT_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(output_rows[0].keys()))
        writer.writeheader()
        writer.writerows(output_rows)

    review_rows.sort(key=lambda row: (-int(row["count"]), str(row["institution_name"])))
    with review_path.open("w", encoding=OUTPUT_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(review_rows[0].keys()) if review_rows else [
            "institution_name",
            "count",
            "cnki_count",
            "wos_count",
            "cscd_count",
            "institution_trans",
            "institution_norm",
            "candidate_method",
            "candidate_review_reason",
        ])
        writer.writeheader()
        if review_rows:
            writer.writerows(review_rows)

    write_note(note_path)

    print(f"rows={len(output_rows)}")
    print(f"review_rows={len(review_rows)}")
    print(f"output={output_path}")
    print(f"review_output={review_path}")
    print(f"note_output={note_path}")
    for method, count in method_counter.most_common():
        print(f"{method}={count}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(DEFAULT_INPUT_PATH))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--review-out", default=str(DEFAULT_REVIEW_PATH))
    parser.add_argument("--note-out", default=str(DEFAULT_NOTE_PATH))
    parser.add_argument("--manual-override", default=str(DEFAULT_MANUAL_OVERRIDE_PATH))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    build_table(
        Path(args.input),
        Path(args.output),
        Path(args.review_out),
        Path(args.note_out),
        Path(args.manual_override),
    )
