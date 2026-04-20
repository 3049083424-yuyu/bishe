from __future__ import annotations

import argparse
import re
import unicodedata
from typing import Dict, List, Optional, Tuple

import pandas as pd


INPUT_PATH = r"D:\毕业论文\institution_name_table_doi_required.csv"
MAP_XLSX_PATH = r"D:\毕业论文\norm_institution\institution_map_new.xlsx"
SEED_XLSX_PATH = r"D:\毕业论文\norm_institution\institution_count_new.xlsx"
OUTPUT_PATH = r"D:\毕业论文\institution_name_table_doi_required_trans_norm.csv"


RE_MULTI_SPACE = re.compile(r"\s+")
RE_DIGITS = re.compile(r"\b\d{5,6}\b")
RE_CJK = re.compile(r"[\u4e00-\u9fff]")
RE_SPLIT = re.compile(r"\s*[,，;；|]\s*")
RE_REMOVE_ADDR = re.compile(r"[!！].*$")
RE_BARE_EN = re.compile(r"^[A-Za-z0-9 .,&'()/\-]+$")

EN_STOPWORDS = {"of", "and", "the", "for", "in", "at", "on", "to", "de", "la"}
EN_SUBUNIT_WORDS = {
    "school",
    "college",
    "department",
    "faculty",
    "laboratory",
    "lab",
    "center",
    "centre",
    "key",
}

EN_PARENT_WORDS = {
    "university",
    "academy",
    "hospital",
    "company",
    "corporation",
    "institute",
    "bureau",
    "group",
    "college",
}

CN_SCHOOL_HINTS = (
    "学院",
    "系",
    "实验室",
    "重点实验室",
    "研究中心",
    "中心",
    "教研室",
    "研究所",
    "研究院",
    "学部",
)

CN_HOSPITAL_HINTS = (
    "内科",
    "外科",
    "ICU",
    "icu",
    "科",
    "室",
    "中心",
)

EN_REPL = {
    "univ": "university",
    "inst": "institute",
    "technol": "technology",
    "tech": "technology",
    "sci": "science",
    "acad": "academy",
    "petr": "petroleum",
    "geosci": "geoscience",
    "geosci.": "geoscience",
    "engn": "engineering",
    "natl": "national",
    "ctr": "center",
    "cent": "center",
    "coll": "college",
    "sch": "school",
    "dept": "department",
    "hosp": "hospital",
    "med": "medical",
    "agr": "agricultural",
    "agric": "agricultural",
    "phys": "physics",
    "chem": "chemical",
    "mat": "materials",
    "env": "environment",
    "res": "research",
    "prov": "provincial",
    "nat": "national",
}

WORD_TRANSLATIONS = {
    "university": "大学",
    "academy": "科学院",
    "college": "学院",
    "school": "学院",
    "faculty": "学院",
    "department": "系",
    "laboratory": "实验室",
    "lab": "实验室",
    "center": "中心",
    "centre": "中心",
    "institute": "研究所",
    "research": "研究",
    "company": "公司",
    "corporation": "公司",
    "corp": "公司",
    "group": "集团",
    "bureau": "局",
    "branch": "分院",
    "division": "分部",
    "hospital": "医院",
    "national": "国家",
    "provincial": "省",
    "state": "国家",
    "key": "重点",
    "petroleum": "石油",
    "oil": "石油",
    "gas": "天然气",
    "geology": "地质",
    "geological": "地质",
    "geophysics": "地球物理",
    "geophysical": "地球物理",
    "earth": "地球",
    "science": "科学",
    "sciences": "科学",
    "technology": "技术",
    "engineering": "工程",
    "chemical": "化工",
    "chemistry": "化学",
    "materials": "材料",
    "material": "材料",
    "resource": "资源",
    "resources": "资源",
    "energy": "能源",
    "safety": "安全",
    "mining": "矿业",
    "prospecting": "勘探",
    "exploration": "勘探",
    "development": "开发",
    "processing": "加工",
    "production": "生产",
    "medical": "医学",
    "agricultural": "农业",
    "physics": "物理",
    "environment": "环境",
    "environmental": "环境",
    "solid": "固体",
    "fuels": "燃料",
    "fuel": "燃料",
    "integration": "综合",
    "heavy": "重质",
    "reservoir": "储层",
    "reservoirs": "储层",
    "mine": "矿",
    "mineral": "矿物",
    "petrochemical": "石油化工",
    "consulting": "咨询",
    "development": "开发",
    "service": "服务",
    "services": "服务",
    "limited": "有限公司",
    "ltd": "有限公司",
    "inc": "公司",
    "co": "公司",
    "oy": "公司",
    "ab": "公司",
    "ag": "公司",
    "gmbh": "公司",
    "sa": "公司",
    "plc": "公司",
    "llc": "公司",
}

PHRASE_TRANSLATIONS = {
    "state key laboratory heavy oil processing": "重质油国家重点实验室",
    "state key laboratory petroleum resources prospecting": "石油资源与探测国家重点实验室",
    "state key laboratory petroleum resource prospecting": "石油资源与探测国家重点实验室",
    "state key laboratory oil gas reservoir geology exploitation": "油气藏地质及开发工程国家重点实验室",
    "laboratory integration geology geophysics": "地质与地球物理综合研究实验室",
    "research institute petroleum processing": "石油化工科学研究院",
    "research institute petroleum exploration development": "石油勘探开发研究院",
    "research institute petroleum exploration production": "石油勘探开发研究院",
    "exploration development research institute": "勘探开发研究院",
    "school petroleum engineering": "石油工程学院",
    "college petroleum engineering": "石油工程学院",
    "faculty petroleum engineering": "石油工程学院",
    "school geosciences": "地球科学学院",
    "college geosciences": "地球科学学院",
    "school chemistry chemical engineering": "化学化工学院",
    "college chemistry chemical engineering": "化学化工学院",
    "school chemical engineering": "化学工程学院",
    "college chemical engineering": "化学工程学院",
    "department chemical engineering": "化学工程系",
    "school energy resources": "能源学院",
    "college energy resources": "能源学院",
    "school resource safety engineering": "资源与安全工程学院",
    "school materials science engineering": "材料科学与工程学院",
    "college materials science engineering": "材料科学与工程学院",
    "department geology": "地质系",
    "department chemistry": "化学系",
    "school geoscience technology": "地球科学与技术学院",
    "school earth sciences engineering": "地球科学与工程学院",
    "school earth sciences resources": "地球科学与资源学院",
    "school earth sciences": "地球科学学院",
    "college earth sciences": "地球科学学院",
    "college geophysics": "地球物理学院",
    "school geophysics information technology": "地球物理与信息技术学院",
    "state key laboratory catalytic material reaction engineering": "石油化工催化材料与反应工程国家重点实验室",
}

MANUAL_CANONICAL_TRANSLATIONS = {
    "china university petroleum": "中国石油大学",
    "china university petroleum beijing": "中国石油大学（北京）",
    "china university petroleum qingdao": "中国石油大学（华东）",
    "china university petroleum east china": "中国石油大学（华东）",
    "china university petroleum dongying": "中国石油大学（华东）",
    "research institute petroleum processing": "中国石油化工股份有限公司石油化工科学研究院",
    "sinopec research institute petroleum processing": "中国石油化工股份有限公司石油化工科学研究院",
    "research institute petroleum exploration development": "中国石油勘探开发研究院",
    "petrochina research institute petroleum exploration development": "中国石油勘探开发研究院",
    "cnpc research institute petroleum exploration development": "中国石油勘探开发研究院",
    "exploration development research institute": "中国石油勘探开发研究院",
    "research institute petroleum exploration production": "中国石油化工股份有限公司石油勘探开发研究院",
    "sinopec research institute petroleum exploration production": "中国石油化工股份有限公司石油勘探开发研究院",
    "scottish crop research institute": "苏格兰作物研究所",
    "heriot watt university": "赫瑞瓦特大学",
    "turku university": "图尔库大学",
    "kyungpook national university": "庆北国立大学",
    "eotvos lorand university": "罗兰大学",
    "jinan university": "济南大学",
    "university jinan": "济南大学",
    "hainan university": "海南大学",
    "henan university": "河南大学",
    "henan normal university": "河南师范大学",
    "yunnan university": "云南大学",
    "henan university technology": "河南工业大学",
    "hunan university science technology": "湖南科技大学",
    "hunan normal university": "湖南师范大学",
    "hunan agricultural university": "湖南农业大学",
    "yunnan normal university": "云南师范大学",
    "yanan university": "延安大学",
    "hainan normal university": "海南师范大学",
    "poznan university technology": "波兹南理工大学",
    "hunan institute engineering": "湖南工程学院",
    "hunan institute science technology": "湖南理工学院",
    "henan academy science": "河南省科学院",
    "us doe": "美国能源部",
    "usda": "美国农业部",
    "usda ars": "美国农业部农业研究局",
    "ars": "美国农业部农业研究局",
    "csiro": "澳大利亚联邦科学与工业研究组织",
    "csir": "科学与工业研究委员会",
    "cnpc": "中国石油天然气集团有限公司",
    "eth": "苏黎世联邦理工学院",
    "ifp energies nouvelles": "法国石油与新能源研究院",
    "saudi aramco": "沙特阿美公司",
    "china geological survey": "中国地质调查局",
    "china geol survey": "中国地质调查局",
    "us geological survey": "美国地质调查局",
    "us geol survey": "美国地质调查局",
    "schlumberger": "斯伦贝谢",
    "caltech": "加州理工学院",
    "inra": "法国国家农业研究院",
    "ras": "俄罗斯科学院",
    "us epa": "美国环境保护署",
    "geological survey canada": "加拿大地质调查局",
    "geol survey canada": "加拿大地质调查局",
    "astar": "新加坡科技研究局",
    "british geological survey": "英国地质调查局",
    "british geol survey": "英国地质调查局",
    "socar": "阿塞拜疆国家石油公司",
    "nist": "美国国家标准与技术研究院",
    "tno": "荷兰应用科学研究组织",
    "basf se": "巴斯夫",
    "halliburton": "哈里伯顿",
    "politecn torino": "都灵理工大学",
    "ecole polytech fed lausanne": "洛桑联邦理工学院",
    "ecole polytechnique federale lausanne": "洛桑联邦理工学院",
    "suny buffalo": "纽约州立大学布法罗分校",
    "shell global solut int bv": "壳牌全球解决方案国际公司",
    "statoil asa": "挪威国家石油公司",
    "statoil": "挪威国家石油公司",
    "minist educ china": "中国教育部",
    "iit": "印度理工学院",
    "forschungszentrum julich": "于利希研究中心",
    "ecole polytech": "综合理工学院",
    "chalmers": "查尔姆斯理工大学",
    "polytech montreal": "蒙特利尔理工学院",
    "suny stony brook": "纽约州立大学石溪分校",
    "cea": "法国原子能与替代能源委员会",
    "baker hughes": "贝克休斯",
    "sintef ind": "挪威工业技术研究院",
    "sintef": "挪威科技工业研究基金会",
    "guangzhou marine geological survey": "广州海洋地质调查局",
    "guangzhou marine geol survey": "广州海洋地质调查局",
    "ciemat": "西班牙能源环境与技术研究中心",
    "tu wien": "维也纳工业大学",
    "usa": "美国",
    "aist": "日本产业技术综合研究所",
    "tecnol monterrey": "蒙特雷理工学院",
    "kaust": "阿卜杜拉国王科技大学",
    "nasa": "美国国家航空航天局",
    "cnooc": "中国海洋石油集团有限公司",
    "mines paristech": "巴黎高科矿业学院",
    "mines paris tech": "巴黎高科矿业学院",
    "equinor asa": "挪威国家石油公司",
    "enea": "意大利国家新技术能源与可持续经济发展署",
    "conoco phillips": "康菲石油公司",
    "kit": "卡尔斯鲁厄理工学院",
    "ifremer": "法国海洋开发研究院",
    "geol survey denmark greenland": "丹麦和格陵兰地质调查局",
    "geol survey denmark greenland geus": "丹麦和格陵兰地质调查局",
    "european commiss": "欧盟委员会",
    "china earthquake adm": "中国地震局",
    "empa": "瑞士联邦材料科学与技术实验室",
    "consejo nacl invest cient tecn": "阿根廷国家科学技术研究委员会",
    "tu bergakad freiberg": "弗赖贝格工业大学",
    "helmholtz zentrum dresden rossendorf": "亥姆霍兹德累斯顿-罗森多夫研究中心",
    "ripi": "伊朗石油工业研究院",
    "jamia millia islamia": "贾米亚米利亚伊斯兰大学",
    "csiro mfg": "澳大利亚联邦科学与工业研究组织制造部",
    "us forest serv": "美国林务局",
    "nyu": "纽约大学",
    "ntnu": "挪威科技大学",
    "postech": "浦项工科大学",
    "umist": "曼彻斯特科技大学",
    "ist italiano tecnol": "意大利技术研究院",
    "petro china": "中国石油天然气股份有限公司",
    "epri": "美国电力研究院",
    "tu dortmund": "多特蒙德工业大学",
    "ipn": "墨西哥国立理工学院",
    "agcy def dev": "韩国国防科学研究所",
    "incar csic": "西班牙国家煤炭研究所",
    "eni spa": "埃尼集团",
    "shell int explorat prod bv": "壳牌国际勘探与生产有限公司",
    "shell int explorat prod": "壳牌国际勘探与生产公司",
    "kfupm": "法赫德国王石油与矿业大学",
    "iit madras": "印度理工学院马德拉斯分校",
    "canmetenergy": "加拿大Canmet能源研究中心",
    "bur rech geol minieres": "法国地质与矿产调查局",
    "sb ras": "俄罗斯科学院西伯利亚分院",
    "shenzhen polytech": "深圳职业技术大学",
    "unlp": "拉普拉塔国立大学",
    "ensic": "南锡高等化学工业学院",
    "transneft": "俄罗斯石油管道运输公司",
    "ecole mines nantes": "南特高等矿业学院",
    "us fda": "美国食品药品监督管理局",
    "rti int": "RTI国际研究所",
    "cirad": "法国国际农业发展研究中心",
    "astrazeneca": "阿斯利康",
    "total": "道达尔能源",
    "unsw sydney": "新南威尔士大学",
    "shell global solut int": "壳牌全球解决方案国际公司",
    "usaf": "美国空军",
    "canadian light source": "加拿大光源",
    "vito": "佛兰德技术研究院",
    "norsk hydro as": "挪威海德鲁公司",
    "novozymes as": "诺维信公司",
    "iust": "伊朗科技大学",
    "ineti": "葡萄牙国家工程技术与创新研究所",
    "promes cnrs": "法国国家科研中心PROMES实验室",
    "agroparistech": "巴黎高科农学院",
    "minist emergency management": "中国应急管理部",
    "ist nazl geofis vulcanol": "意大利国家地球物理与火山学研究所",
    "kier": "韩国能源研究院",
    "kist": "韩国科学技术研究院",
    "geol survey norway": "挪威地质调查局",
    "amrita vishwa vidyapeetham": "阿姆里塔大学",
    "minist environm protect": "中国环境保护部",
    "nioc": "伊朗国家石油公司",
    "johnson matthey": "庄信万丰",
    "geoforschungszentrum potsdam": "德国地学研究中心波茨坦",
    "us army": "美国陆军",
    "eth zurich": "苏黎世联邦理工学院",
    "ecopetrol": "哥伦比亚国家石油公司",
    "ncsr demokritos": "希腊国家科学研究中心德谟克利特",
    "iit roorkee": "印度理工学院鲁尔基分校",
    "sait polytech": "南阿尔伯塔理工学院",
    "krict": "韩国化学技术研究院",
    "politecn bari": "巴里理工大学",
    "ecole polytech montreal": "蒙特利尔理工学院",
    "iit ism": "印度理工学院丹巴德分校",
    "ifp": "法国石油研究院",
    "geosci australia": "澳大利亚地球科学局",
    "shell": "壳牌公司",
    "aramco asia": "沙特阿美亚洲",
    "uned": "西班牙国立远程教育大学",
    "ensiacet": "图卢兹国立高等化学工程技术与艺术学院",
    "glaxosmithkline": "葛兰素史克",
    "lneg": "葡萄牙国家能源与地质实验室",
    "bits pilani": "比拉理工与科学学院皮拉尼分校",
    "govt india": "印度政府",
    "postech": "浦项科技大学",
    "cuny": "纽约市立大学",
    "unam": "墨西哥国立自治大学",
    "bp": "英国石油公司",
    "chevron": "雪佛龙公司",
    "slb": "斯伦贝谢",
    "equinor asa": "挪威国家石油公司",
    "totalenergies": "道达尔能源",
    "riken": "日本理化学研究所",
    "ecole polytech fed lausanne epfl": "洛桑联邦理工学院",
    "energyville": "EnergyVille能源研究中心",
    "forschungszentrum karlsruhe": "卡尔斯鲁厄研究中心",
    "netl support contractor": "美国国家能源技术实验室支持承包商",
    "shell global solut": "壳牌全球解决方案公司",
    "icb csic": "西班牙国家科研委员会碳化学研究所",
    "unist": "蔚山国立科学技术院",
    "aecom": "艾奕康",
    "tu kaiserslautern": "凯泽斯劳滕工业大学",
    "suny syracuse": "纽约州立大学雪城分校",
    "scion": "新西兰Scion研究所",
    "icrea": "加泰罗尼亚研究与高级研究院",
    "escpe lyon": "里昂高等化学物理电子学院",
    "usthb": "乌阿里·布迈丁科学技术大学",
    "tecnol nacl mexico": "墨西哥国家技术学院",
    "eth honggerberg": "苏黎世联邦理工学院Hönggerberg校区",
    "plataforma solar almeria ciemat": "阿尔梅里亚太阳能平台",
    "sonatrach": "阿尔及利亚国家石油公司",
    "iit delhi": "印度理工学院德里分校",
    "suny binghamton": "纽约州立大学宾汉姆顿分校",
    "inpl": "洛林国立理工学院",
    "upes": "印度石油与能源研究大学",
    "ecole mines albi carmaux": "阿尔比-卡尔莫高等矿业学院",
    "ecole mines": "高等矿业学院",
    "vtt": "芬兰国家技术研究中心",
    "nicpb": "爱沙尼亚国家化学物理与生物物理研究所",
    "wetsus": "Wetsus欧洲可持续水技术中心",
    "bg grp": "BG集团",
    "iit kharagpur": "印度理工学院卡拉格普尔分校",
    "tu berlin": "柏林工业大学",
    "unsw": "新南威尔士大学",
    "ist nazl oceanog geofis sperimentale ogs": "意大利国家海洋学与实验地球物理研究所",
    "equinor": "挪威国家石油公司",
    "ecole normale super": "高等师范学院",
    "unesp": "圣保罗州立大学",
    "commiss european communities": "欧洲共同体委员会",
    "ecole mines ales": "阿莱斯高等矿业学院",
    "eth zentrum": "苏黎世联邦理工学院中心校区",
    "csiro land water": "澳大利亚联邦科学与工业研究组织土地与水资源部",
    "minist ecol environm peoples republ china": "中国生态环境部",
    "usn": "东南挪威大学",
    "minist ecol environm": "生态与环境部",
    "shell int explorat prod bv": "壳牌国际勘探与生产有限公司",
    "shell int explorat prod": "壳牌国际勘探与生产公司",
    "nit": "印度国立技术学院",
    "minist def russian federat": "俄罗斯联邦国防部",
    "geosci australia": "澳大利亚地球科学局",
    "tu braunschweig": "不伦瑞克工业大学",
    "norsk hydro asa": "挪威海德鲁公司",
    "geol survey canada atlantic": "加拿大地质调查局大西洋分部",
    "sk innovat": "SK创新公司",
    "unsw australia": "新南威尔士大学",
    "vtt proc": "芬兰国家技术研究中心工艺研究部",
    "cenpes": "巴西国家石油公司研究中心",
    "bp amer": "BP美国公司",
    "dgist": "大邱庆北科学技术院",
    "certh": "希腊研究与技术中心",
    "edf r d": "法国电力研发中心",
    "ineris": "法国国家工业环境与风险研究院",
    "irstea": "法国农业与环境工程研究院",
    "aker bp asa": "Aker BP公司",
    "johnson matthey catalysts": "庄信万丰催化剂公司",
    "environm canada": "加拿大环境部",
    "technion": "以色列理工学院",
    "inesc tec": "葡萄牙系统与计算机工程技术研究所",
    "exxonmobil": "埃克森美孚",
    "beicip franlab": "Beicip-Franlab公司",
    "abo akad": "奥博学术大学",
    "gist": "光州科学技术院",
    "tata steel": "塔塔钢铁",
    "nit agartala": "阿加尔塔拉国立技术学院",
    "shell int e p": "壳牌国际勘探与生产公司",
    "koneru lakshmaiah educ fdn": "科内鲁·拉克希马亚教育基金会",
    "nstda": "泰国国家科技发展署",
    "geoscience australia": "澳大利亚地球科学局",
    "consejo nacl invest cient tecn conicet": "阿根廷国家科学技术研究委员会",
    "minist def russia": "俄罗斯国防部",
    "unilever r d": "联合利华研发中心",
    "zae bayern": "巴伐利亚应用能源研究中心",
    "cags": "中国地质科学院",
    "flsmidth as": "艾法史密斯公司",
    "shengli oilfield": "胜利油田",
    "ongc": "印度石油天然气公司",
    "lukoil neftohim burgas": "卢克石油布尔加斯炼化公司",
    "statoilhydro asa": "挪威国家石油海德鲁公司",
    "ups": "保罗·萨巴捷大学",
    "caep": "中国工程物理研究院",
    "cder": "美国食品药品监督管理局药品评价与研究中心",
}

MANUAL_NORM_PATTERNS: List[Tuple[re.Pattern[str], str]] = [
    (
        re.compile(r"(石油化工科学研究院|中国石化石油化工科学研究院|中国石油化工股份有限公司石油化工科学研究院|中国石化股份有限公司石油化工科学研究院|中国石油化工股份公司石油化工科学研究院|Research Institute of Petroleum Processing)", re.I),
        "中国石油化工股份有限公司石油化工科学研究院",
    ),
    (
        re.compile(r"(中国石化石油勘探开发研究院|中国石油化工股份有限公司石油勘探开发研究院|中国石化股份有限公司石油勘探开发研究院|中石化石油勘探开发研究院|SINOPEC Research Institute of Petroleum Exploration and Production|Sinopec Research Institute of Petroleum Exploration and Production|Research Institute of Petroleum Exploration and Production)", re.I),
        "中国石油化工股份有限公司石油勘探开发研究院",
    ),
    (
        re.compile(r"(中国石油勘探开发研究院|中国石油天然气股份有限公司石油勘探开发研究院|石油勘探开发研究院|Research Institute of Petroleum Exploration and Development|PetroChina Research Institute of Petroleum Exploration and Development|PetroChina Research Institute of Petroleum Exploration & Development|CNPC Research Institute of Petroleum Exploration and Development|Exploration and Development Research Institute)", re.I),
        "中国石油勘探开发研究院",
    ),
]

CHINESE_REPLACEMENTS = {
    "农科院": "农业科学院",
    "动科院": "动物科技学院",
    "植保所": "植物保护研究所",
    "家禽所": "家禽研究所",
    "土肥所": "土壤肥料研究所",
    "果树所": "果树研究所",
    "畜牧所": "畜牧兽医研究所",
    "农技推广总站": "农业技术推广总站",
    "农技推广站": "农业技术推广站",
    "植保总站": "植物保护总站",
}

BAD_NORMALIZED = {"中国科", "cnpc科", "荆州市中心"}

PROPER_NOUN_TRANSLATIONS = {
    "jinan": "济南",
    "henan": "河南",
    "hunan": "湖南",
    "hainan": "海南",
    "yunnan": "云南",
    "yanan": "延安",
    "poznan": "波兹南",
    "semnan": "塞姆南",
    "qingdao": "青岛",
    "beijing": "北京",
    "shanghai": "上海",
    "tianjin": "天津",
    "nanjing": "南京",
    "wuhan": "武汉",
    "chengdu": "成都",
    "dalian": "大连",
    "changzhou": "常州",
    "dongying": "东营",
    "suzhou": "苏州",
    "xian": "西安",
    "harbin": "哈尔滨",
}


def compact_text(text: object) -> str:
    if text is None or pd.isna(text):
        return ""
    value = unicodedata.normalize("NFKC", str(text or ""))
    value = value.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    value = RE_MULTI_SPACE.sub(" ", value)
    return value.strip().strip(",;，； ")


def clean_name(text: object) -> str:
    value = compact_text(text)
    value = RE_REMOVE_ADDR.sub("", value)
    value = RE_DIGITS.sub("", value)
    value = compact_text(value)
    return value


def has_cjk(text: str) -> bool:
    return bool(RE_CJK.search(text))


def canonical_en_key(text: str) -> str:
    value = compact_text(text).lower()
    value = value.replace("&", " and ")
    value = re.sub(r"[(){}\[\]/\\\-_.]", " ", value)
    tokens = [tok for tok in RE_MULTI_SPACE.sub(" ", value).split(" ") if tok]
    expanded: List[str] = []
    for token in tokens:
        token = EN_REPL.get(token, token)
        if token in EN_STOPWORDS:
            continue
        expanded.append(token)
    return " ".join(expanded).strip()


def should_use_mapped(raw: str, normalized: str) -> bool:
    if not normalized:
        return False
    if normalized in BAD_NORMALIZED:
        return False
    if has_cjk(raw) and not has_cjk(normalized) and RE_BARE_EN.match(normalized or ""):
        return False
    if len(normalized) <= 3 and len(raw) >= 5:
        return False
    return True


def build_maps() -> Tuple[Dict[str, str], Dict[str, str], List[str]]:
    map_df = pd.read_excel(MAP_XLSX_PATH, sheet_name="institution_map")
    seed_df = pd.read_excel(SEED_XLSX_PATH)

    exact_map: Dict[str, str] = {}
    for _, row in map_df.iterrows():
        raw = clean_name(row.get("raw", ""))
        normalized = clean_name(row.get("normalized", ""))
        if raw and normalized and should_use_mapped(raw, normalized):
            exact_map[raw] = normalized

    for _, row in seed_df.iterrows():
        raw = clean_name(row.get("institution", ""))
        normalized = clean_name(row.get("institution_normal", ""))
        if raw and normalized:
            exact_map[raw] = normalized

    canonical_map: Dict[str, str] = {}
    for key, value in MANUAL_CANONICAL_TRANSLATIONS.items():
        canonical_map[key] = value

    seed_nonempty = seed_df[seed_df["institution_normal"].notna()]
    for _, row in seed_nonempty.iterrows():
        raw = clean_name(row.get("institution", ""))
        normalized = clean_name(row.get("institution_normal", ""))
        if raw and normalized and not has_cjk(raw):
            canonical = canonical_en_key(raw)
            if canonical and not is_english_subunit(raw):
                canonical_map[canonical] = normalized

    university_names = set()
    university_pattern = re.compile(r"^[\u4e00-\u9fff]{2,20}大学(?:（[^）]+）|\([^)]*\))?$")
    for candidate in list(exact_map.keys()) + list(exact_map.values()) + list(canonical_map.values()):
        candidate = clean_name(candidate)
        prefix = candidate.rsplit("大学", 1)[0] if "大学" in candidate else candidate
        has_subunit_prefix = any(hint in prefix for hint in CN_SCHOOL_HINTS)
        if has_cjk(candidate) and university_pattern.fullmatch(candidate) and not has_subunit_prefix:
            university_names.add(candidate)

    return exact_map, canonical_map, sorted(university_names, key=len, reverse=True)


def translate_phrase_by_words(text: str) -> str:
    tokens = re.split(r"\s+", compact_text(text).replace(",", " "))
    translated: List[str] = []
    for token in tokens:
        if not token:
            continue
        key = token.lower().strip(".")
        key = EN_REPL.get(key, key)
        translated.append(PROPER_NOUN_TRANSLATIONS.get(key, WORD_TRANSLATIONS.get(key, token)))
    return "".join(translated).strip()


def translate_english_segment(text: str, canonical_map: Dict[str, str]) -> str:
    cleaned = clean_name(text)
    if not cleaned:
        return ""

    canonical = canonical_en_key(cleaned)
    if canonical in canonical_map:
        return canonical_map[canonical]

    for phrase_key, phrase_value in PHRASE_TRANSLATIONS.items():
        if canonical == phrase_key:
            return phrase_value

    return translate_phrase_by_words(cleaned)


def find_parent_translation(segments: List[str], canonical_map: Dict[str, str]) -> Optional[str]:
    for segment in reversed(segments):
        canonical = canonical_en_key(segment)
        if canonical in canonical_map:
            return canonical_map[canonical]
    for segment in reversed(segments):
        canonical = canonical_en_key(segment)
        if any(word in canonical.split() for word in EN_PARENT_WORDS):
            return translate_english_segment(segment, canonical_map)
    return None


def is_english_subunit(segment: str) -> bool:
    canonical = canonical_en_key(segment)
    tokens = set(canonical.split())
    return bool(tokens & EN_SUBUNIT_WORDS)


def translate_english_name(name: str, exact_map: Dict[str, str], canonical_map: Dict[str, str]) -> str:
    cleaned = clean_name(name)
    if cleaned in exact_map and has_cjk(exact_map[cleaned]) and not is_english_subunit(cleaned):
        return exact_map[cleaned]

    canonical = canonical_en_key(cleaned)
    if canonical in canonical_map:
        return canonical_map[canonical]

    segments = [seg for seg in RE_SPLIT.split(cleaned) if seg]
    if len(segments) >= 2 and is_english_subunit(segments[0]):
        parent = find_parent_translation(segments[1:], canonical_map)
        child = translate_english_segment(segments[0], canonical_map)
        if parent and child:
            return parent + child

    translated_segments = [translate_english_segment(seg, canonical_map) for seg in segments]
    translated_segments = [seg for seg in translated_segments if seg]
    if translated_segments:
        if len(translated_segments) >= 2 and translated_segments[-1].endswith(("大学", "科学院", "医院", "公司", "研究院", "研究所")):
            parent = translated_segments[-1]
            child = "".join(translated_segments[:-1])
            if child.endswith(("学院", "系", "实验室", "中心", "研究院", "研究所")):
                return parent + child
        return "，".join(translated_segments)
    return cleaned


def apply_chinese_aliases(text: str) -> str:
    value = clean_name(text)
    for old, new in CHINESE_REPLACEMENTS.items():
        value = value.replace(old, new)
    value = compact_text(value)
    return value


def normalize_chinese_name(name: str, exact_map: Dict[str, str], university_names: List[str]) -> str:
    cleaned = apply_chinese_aliases(name)

    for university_name in university_names:
        if university_name in cleaned:
            remainder = cleaned.replace(university_name, "", 1)
            if not remainder:
                return university_name
            if any(hint in remainder for hint in CN_SCHOOL_HINTS):
                return university_name

    if cleaned in exact_map and has_cjk(exact_map[cleaned]):
        cleaned = exact_map[cleaned]

    for pattern, target in MANUAL_NORM_PATTERNS:
        if pattern.search(cleaned):
            return target

    if "医院" in cleaned:
        hospital = re.match(r"^.*?医院", cleaned)
        if hospital:
            tail = cleaned[hospital.end():]
            if any(hint in tail for hint in CN_HOSPITAL_HINTS):
                return hospital.group(0)

    university = re.match(r"^.*?大学(?:（[^）]+）|\([^)]*\))?", cleaned)
    if university:
        parent = university.group(0)
        if cleaned == parent:
            return parent
        tail = cleaned[len(parent):]
        if any(hint in tail for hint in CN_SCHOOL_HINTS):
            return parent
        if re.fullmatch(r"[市县区省院部校园基地园区分校分院临床]+\S*", tail):
            return parent

    if "大学" in cleaned:
        parent = cleaned.split("大学", 1)[0] + "大学"
        tail = cleaned[len(parent):]
        if tail and any(hint in tail for hint in CN_SCHOOL_HINTS):
            return parent

    university_anywhere = re.search(r".*?大学(?:（[^）]+）|\([^)]*\))?", cleaned)
    if university_anywhere and any(hint in cleaned for hint in CN_SCHOOL_HINTS):
        return university_anywhere.group(0)

    return cleaned


def normalize_english_name(name: str, translated: str, exact_map: Dict[str, str], canonical_map: Dict[str, str]) -> str:
    cleaned = clean_name(name)

    for pattern, target in MANUAL_NORM_PATTERNS:
        if pattern.search(cleaned):
            return target

    if cleaned in exact_map and has_cjk(exact_map[cleaned]):
        return exact_map[cleaned]

    canonical = canonical_en_key(cleaned)
    if canonical in canonical_map:
        return canonical_map[canonical]

    segments = [seg for seg in RE_SPLIT.split(cleaned) if seg]
    if len(segments) >= 2 and is_english_subunit(segments[0]):
        parent = find_parent_translation(segments[1:], canonical_map)
        if parent:
            return parent

    if translated.endswith(("大学", "科学院", "医院", "公司", "研究院", "研究所")):
        return translated

    match = re.match(r"^(.*?大学)", translated)
    if match and translated != match.group(1):
        return match.group(1)

    return translated


def translate_and_normalize(name: str, exact_map: Dict[str, str], canonical_map: Dict[str, str], university_names: List[str]) -> Tuple[str, str]:
    cleaned = clean_name(name)
    if not cleaned:
        return "", ""

    if has_cjk(cleaned):
        translated = apply_chinese_aliases(cleaned)
        normalized = normalize_chinese_name(cleaned, exact_map, university_names)
        return translated, normalized

    translated = translate_english_name(cleaned, exact_map, canonical_map)
    normalized = normalize_english_name(cleaned, translated, exact_map, canonical_map)
    return beautify_cn_punct(translated), beautify_cn_punct(normalized)


def beautify_cn_punct(text: str) -> str:
    value = compact_text(text)
    if has_cjk(value):
        value = re.sub(r"\(([^()]*[\u4e00-\u9fff][^()]*)\)", r"（\1）", value)
    return value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=INPUT_PATH)
    parser.add_argument("--output", default=OUTPUT_PATH)
    args = parser.parse_args()

    exact_map, canonical_map, university_names = build_maps()
    df = pd.read_csv(args.input, encoding="utf-8-sig")

    results = df["institution_name"].fillna("").astype(str).apply(
        lambda value: translate_and_normalize(value, exact_map, canonical_map, university_names)
    )
    df["institution_trans"] = results.str[0]
    df["institution_norm"] = results.str[1]

    df.to_csv(args.output, index=False, encoding="utf-8-sig")

    print(f"rows={len(df)}")
    print(f"output={args.output}")
    print(f"institution_trans_nonempty={(df['institution_trans'].astype(str).str.strip() != '').sum()}")
    print(f"institution_norm_nonempty={(df['institution_norm'].astype(str).str.strip() != '').sum()}")
    print("\npreview:")
    print(df.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
