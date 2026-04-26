from __future__ import annotations

import collections
import csv
import copy
import datetime as dt
import io
import shutil
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET


TEMPLATE_PATH = Path(r"D:\graduate\oil_price_sentiment\ppt_model.pptx")
OUTPUT_PATH = Path(r"D:\graduate\oil_price_sentiment\oil_price_system_presentation_generated.pptx")
HISTORY_DIR = Path(r"D:\graduate\oil_price_sentiment\history")
MEDIA_DIR = Path(r"D:\graduate\oil_price_sentiment\report_media")
SENTIMENT_CSV_PATH = Path(r"D:\graduate\oil_price_sentiment\news_sentiment.csv")


NS = {
    "a": "http://schemas.openxmlformats.org/drawingml/2006/main",
    "c": "http://schemas.openxmlformats.org/drawingml/2006/chart",
    "p": "http://schemas.openxmlformats.org/presentationml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "rel": "http://schemas.openxmlformats.org/package/2006/relationships",
    "s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
}


ET.register_namespace("a", NS["a"])
ET.register_namespace("p", NS["p"])
ET.register_namespace("r", NS["r"])


def qname(prefix: str, tag: str) -> str:
    return f"{{{NS[prefix]}}}{tag}"


def current_date_text() -> str:
    today = dt.date.today()
    return f"{today.year}年{today.month:02d}月{today.day:02d}日"


DATE_TEXT = current_date_text()
FOOTER_MAIN = "OilPrice新闻分析与预测系统"
FOOTER_SUB = "课程项目汇报"


SLIDE_TEXT = {
    1: {
        1: "基于 OilPrice 新闻的\n国际油价预测系统汇报",
        2: DATE_TEXT,
    },
    2: {
        5: "目录",
        6: "01",
        7: "02",
        8: "03",
        9: "04",
        10: "项目概览",
        11: "研究背景与系统流程",
        12: "数据收集处理与分类",
        13: "采集 清洗 标注",
        14: "摘要提取与情感分析",
        15: "摘要生成 词典扩充 得分计算",
        16: "油价预测与系统演示",
        17: "综合指标 建模分析 前端联调",
    },
    3: {
        1: "01",
        2: "项目概览",
        3: "从能源新闻采集到情感指标构建，再到油价预测与系统演示，形成完整的分析闭环。",
    },
    4: {
        2: "研究背景",
        3: "国际油价不仅受供需和宏观因素影响，新闻文本中的市场预期与风险情绪同样具有解释力。",
        5: "研究目标",
        6: "将 OilPrice 新闻转化为可量化情感特征，并与 WTI 价格数据结合完成预测分析。",
        8: "数据基础",
        9: "以能源新闻与周度油价为核心样本，构建可追溯、可聚合、可建模的数据链路。",
        11: "系统输出",
        12: "最终生成摘要、主题标签、情感得分、综合指标、预测结果与图形化演示界面。",
        13: "项目围绕“新闻如何影响油价预期”展开，将文本处理、情感量化、时序建模和前端展示整合为一体。",
        14: "系统总览",
        15: "左侧为图形化系统界面，右侧为预测结果示意，体现项目的完整性与可展示性。",
        16: FOOTER_MAIN,
        17: FOOTER_SUB,
    },
    5: {
        1: FOOTER_MAIN,
        2: FOOTER_SUB,
        3: "数据采集",
        8: "文本预处理",
        9: "主题分类",
        10: "情感与预测",
        15: "采集 OilPrice 多栏目能源新闻，保留标题、日期、正文和链接等核心字段。",
        16: "完成清洗、分词、停用词过滤与词干提取，形成统一、可复用的文本输入。",
        17: "将新闻映射为多标签主题结果，为分主题情感聚合与特征构建提供基础。",
        18: "构建方向性情感指标，并与价格特征共同输入预测模型，输出油价结果。",
    },
    6: {
        2: "1",
        3: "2",
        4: "3",
        5: "4",
        6: "新闻采集",
        7: "文本处理",
        8: "情感建模",
        9: "预测输出",
        10: "抓取能源新闻并整理关键字段，形成原始新闻库。",
        11: "执行清洗、分词和词干提取，生成适用于不同模块的标准化文本。",
        12: "完成摘要、分类、情感得分与周度综合指标构建。",
        13: "生成 K+1 特征并输入 Attention-LSTM，输出预测曲线与评价指标。",
        14: FOOTER_MAIN,
        15: FOOTER_SUB,
    },
    7: {
        3: "数据收集与分类",
        4: "说明新闻来源、采集流程、清洗规则及多标签分类结果。",
        8: "摘要与情感",
        18: "展示 LSA 摘要生成、词典扩充与方向性情感得分计算。",
        12: "综合指标与预测",
        19: "说明周度聚合、K+1 特征、5 周滞后和 Attention-LSTM 预测。",
        16: "系统演示",
        20: "展示 Gooey 前端如何串联摘要、分类、情感、指标和预测模块。",
        21: FOOTER_MAIN,
        22: FOOTER_SUB,
    },
    8: {
        1: "02",
        2: "数据收集处理与新闻分类",
        3: "围绕油价相关新闻完成采集、清洗、分词和多标签分类，形成结构化新闻库。",
    },
    9: {
        2: "新闻来源",
        3: "1",
        5: "以 OilPrice.com 为核心来源，聚焦能源、替代能源和公司新闻等栏目。",
        7: "稳定性设计",
        8: "4",
        10: "设置异常处理、重试与断点续爬，提高采集效率和连续性。",
        12: "采集字段",
        13: "2",
        15: "保留 topic、subtitle、title、date、article、link 等关键字段。",
        17: "预处理流程",
        18: "5",
        20: "执行去冗余、补缺失、小写化、去符号、分词、停用词过滤与词干提取。",
        22: "采集方式",
        23: "3",
        25: "采用列表翻页加详情抓取的方式，完成多栏目新闻批量采集。",
        27: "输出价值",
        28: "6",
        30: "为摘要、分类、情感分析和预测建模提供统一、可追溯的输入基础。",
        31: FOOTER_MAIN,
        32: FOOTER_SUB,
    },
    10: {
        1: "样本规模",
        2: "共完成 37,665 篇新闻情感计算，并作为后续周度聚合与建模输入。",
        3: "栏目覆盖",
        4: "新闻来源覆盖 4 个一级栏目，其中 Energy 为主体来源。",
        5: "分类输出",
        6: "主题结果采用 0/1 结构化表示，便于筛选、统计和特征构造。",
        9: "3.77万",
        12: "4栏",
        15: "0/1",
        16: FOOTER_MAIN,
        17: FOOTER_SUB,
    },
    11: {
        1: "清洗、分词、停用词过滤和词干提取后，原始新闻被转化为可直接进入摘要、分类和情感模块的标准化语料。",
        2: "标准化语料输出",
        3: "右侧图展示 4 个一级栏目在看涨、中性、看跌三类下的数量分布，其中 Energy 栏目数量最高，是后续聚合与建模的主体来源。",
        4: "主题-情感分布",
        5: FOOTER_MAIN,
        6: FOOTER_SUB,
    },
    12: {
        1: "新闻采集结果",
        2: "展示多栏目新闻抓取后的结构化记录，保留标题、日期、正文和链接等关键信息。",
        3: "预处理结果",
        4: "清洗后文本格式统一，为摘要、分类和情感得分计算提供稳定输入。",
        5: "分类结果",
        6: "分类结果以 0/1 形式标注主题，便于后续统计、筛选与周度聚合。",
        8: "1",
        10: "3",
        12: "2",
        13: FOOTER_MAIN,
        14: FOOTER_SUB,
    },
    13: {
        1: "03",
        2: "新闻摘要与情感分析",
        3: "从长篇新闻中提炼核心信息，并将文本内容转化为面向油价方向的情感得分。",
    },
    14: {
        2: "输入新闻正文后，可直接执行自动摘要提取，压缩长文本信息。",
        4: "采用 LSA 摘要算法识别代表性句子，保留新闻核心内容。",
        6: "输出结果为简短摘要文本，便于课堂展示与快速浏览。",
        8: "该模块提升了大量新闻的可读性，也为后续分析提供信息压缩结果。",
        14: FOOTER_MAIN,
        15: FOOTER_SUB,
    },
    15: {
        1: "价格上涨类",
        2: "rally、surge、spike 等词反映油价上行或反弹，归入 bullish。",
        9: "价格下跌类",
        10: "drop、plunge、decline 等词反映价格走弱，归入 bearish。",
        14: "供应收紧类",
        15: "shortage、tightness、drawdown 等词对应供应偏紧或库存下降，归入 bullish。",
        24: "01",
        25: "02",
        26: "03",
        27: "04",
        28: "需求走弱类",
        29: "slowdown、recession、weakness 等词反映需求疲软与经济放缓，归入 bearish。",
        30: FOOTER_MAIN,
        31: FOOTER_SUB,
    },
    16: {
        2: "词典扩充原则",
        4: "扩充并不是简单加词，而是围绕油价上涨、下跌、供需变化和风险事件，重新定义词语的方向含义。",
        5: "方向性口径",
        8: "情感得分关注的是新闻对国际油价形成何种方向性影响，而不是一般意义上的语气正负。",
        9: "领域词补充",
        12: "重点引入价格波动、库存变化、减产政策、地缘冲突和需求放缓等领域高频表达。",
        13: "更贴合油价语境",
        15: FOOTER_MAIN,
        16: FOOTER_SUB,
    },
    17: {
        10: "证据统计",
        11: "分别统计标题和正文中的看涨词、看跌词命中量，标题给予更高权重。",
        12: "得分公式",
        13: "raw_score = bullish_mass - bearish_mass\nevidence_mass = bullish_mass + bearish_mass\nscore = raw_score / (evidence_mass + 3)",
        14: "结果分布",
        15: "共完成 37,665 篇新闻得分计算；看涨 16,920 篇，中性 9,586 篇，看跌 11,159 篇；平均得分 0.075885。",
        16: FOOTER_MAIN,
        17: FOOTER_SUB,
    },
    18: {
        1: "04",
        2: "综合指标、预测建模与系统演示",
        3: "将新闻情绪按周聚合并与 WTI 价格对齐，完成特征构造、预测分析和图形化展示。",
    },
    19: {
        1: "将单篇新闻情感得分转化为周度市场情绪指标，使文本信号能够直接进入油价时序建模。",
        2: "时间对齐",
        4: "相关性分析",
        6: "News_t",
        8: "领先性",
        10: "VA_t",
        17: "将新闻日期与 WTI 周均价统一到自然周，剔除空值与时区噪声。",
        18: "News_t 表示周内全部新闻情感得分的平均值，用于刻画市场净情绪。",
        19: "情绪指数与价格曲线呈现形态相似性，情感方差与价格波动存在正向关系。",
        20: "滞后与相关分析表明，情绪信号具有一定前瞻性，为后续预测提供依据。",
        21: FOOTER_MAIN,
        22: FOOTER_SUB,
    },
    20: {
        17: "K+1 特征库",
        18: "按主题聚合情绪均值，并加入 News_Total 与 VA_Total。",
        19: "价格增强项",
        20: "结合周均价 Price_t 与 Volatility_Trend 构造联合特征矩阵。",
        21: "最佳滞后期",
        22: "利用 VAR 与 AIC 确定情绪影响存在 5 周最佳滞后。",
        23: "Attention-LSTM",
        24: "LSTM 提取时序特征，Attention 聚焦关键时间步。",
        25: "滑动窗口",
        26: "将连续 5 周数据重构为三维张量，供模型训练和预测。",
        27: FOOTER_MAIN,
        28: FOOTER_SUB,
    },
    21: {
        2: "预测结果",
        4: "前端整合",
        6: "系统测试",
        7: "测试集方向准确率 DA = 57.14%，优于随机波动基准。",
        8: "平均绝对百分比误差 MAPE = 5.34%，预测误差保持在可控区间。",
        9: "系统将摘要、分类、情感、指标、建模和预测整合到统一图形界面中。",
        10: "用户可通过页面切换与文件选择完成模块调用，适合课堂演示。",
        11: "重点验证文件读取、字段匹配、模型加载、图表生成与结果保存流程。",
        12: "通过编码统一、日期清理和兼容处理，系统能够稳定完成全流程运行。",
        13: FOOTER_MAIN,
        14: FOOTER_SUB,
    },
    22: {
        2: "02",
        4: "04",
        6: "03",
        8: "01",
        11: "展示主题标签与单篇情感得分，说明文本如何转化为结构化特征。",
        12: "载入各模块输出结果，完成预测曲线与评价指标的最终展示。",
        13: "分类与情感演示",
        14: "预测结果演示",
        15: "先展示摘要提取结果，突出长文本信息压缩与快速浏览效果。",
        16: "生成 News_t、VA_t 等综合情感指标，并与价格数据完成周度对齐。",
        17: "摘要提取演示",
        18: "综合指标演示",
        19: FOOTER_MAIN,
        20: FOOTER_SUB,
    },
    23: {
        8: "完成从新闻采集到油价预测的全流程系统实现。",
        9: "项目成果",
        10: "将方向性情感词典、周度聚合与 Attention-LSTM 有机串联。",
        11: "方法特色",
        12: "为能源舆情监测与价格研判提供可视化、可复现的支持工具。",
        13: "应用价值",
        14: "后续可继续引入预训练语言模型与更多外生金融变量。",
        15: "改进方向",
        16: "本项目验证了 OilPrice 新闻情绪对国际油价分析具有实际解释力，也展示了文本分析与时序预测融合的完整工程路径。",
        17: "总结与展望",
        18: FOOTER_MAIN,
        19: FOOTER_SUB,
        20: "谢谢观看！",
        21: DATE_TEXT,
    },
}


SLIDE_IMAGE_REPLACEMENTS = {
    4: {
        "rId1": ("report_gui.png", MEDIA_DIR / "image18.png"),
        "rId2": ("report_prediction.png", MEDIA_DIR / "image17.png"),
    },
    12: {
        "rId1": ("report_crawler.png", MEDIA_DIR / "image2.png"),
        "rId2": ("report_classification.png", MEDIA_DIR / "image5.png"),
        "rId3": ("report_preprocess.png", MEDIA_DIR / "image3.png"),
    },
    14: {
        "rId1": ("report_summary.png", MEDIA_DIR / "image4.png"),
    },
}


CHART_TOPIC_ORDER = [
    ("Energy", "能源"),
    ("Alternative-Energy", "替代能源"),
    ("Company-News", "公司新闻"),
    ("Latest-Energy-News", "最新能源"),
]

CHART_SERIES = [
    ("bullish", "看涨", "D96C3F"),
    ("neutral", "中性", "C9C9C9"),
    ("bearish", "看跌", "5577D1"),
]


def iter_text_shapes(slide_root: ET.Element):
    idx = 0
    for shape in slide_root.findall(".//p:spTree/p:sp", NS):
        if shape.find("p:txBody", NS) is None:
            continue
        idx += 1
        yield idx, shape


def build_paragraph(template_p: ET.Element, text: str) -> ET.Element:
    new_p = ET.Element(qname("a", "p"))

    ppr = template_p.find(qname("a", "pPr"))
    if ppr is not None:
        new_p.append(copy.deepcopy(ppr))

    template_rpr = template_p.find(f".//{qname('a', 'rPr')}")
    run = ET.SubElement(new_p, qname("a", "r"))
    if template_rpr is not None:
        run.append(copy.deepcopy(template_rpr))
    else:
        ET.SubElement(run, qname("a", "rPr"))

    text_node = ET.SubElement(run, qname("a", "t"))
    text_node.text = text

    end_rpr = template_p.find(qname("a", "endParaRPr"))
    if end_rpr is not None:
        new_p.append(copy.deepcopy(end_rpr))

    return new_p


def set_shape_text(shape: ET.Element, text: str) -> None:
    tx_body = shape.find("p:txBody", NS)
    if tx_body is None:
        return

    existing_paragraphs = tx_body.findall("a:p", NS)
    if existing_paragraphs:
        template_p = existing_paragraphs[0]
    else:
        template_p = ET.Element(qname("a", "p"))
        ET.SubElement(template_p, qname("a", "endParaRPr"))

    for paragraph in list(tx_body.findall("a:p", NS)):
        tx_body.remove(paragraph)

    lines = text.split("\n") if text else [""]
    for line in lines:
        tx_body.append(build_paragraph(template_p, line))


def update_slide_text(slide_bytes: bytes, slide_number: int) -> bytes:
    replacements = SLIDE_TEXT.get(slide_number)
    if not replacements:
        return slide_bytes

    root = ET.fromstring(slide_bytes)
    shape_map = {idx: shape for idx, shape in iter_text_shapes(root)}
    for shape_idx, text in replacements.items():
        if shape_idx in shape_map:
            set_shape_text(shape_map[shape_idx], text)

    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def update_slide_relationships(rel_bytes: bytes, slide_number: int) -> bytes:
    replacements = SLIDE_IMAGE_REPLACEMENTS.get(slide_number)
    if not replacements:
        return rel_bytes

    root = ET.fromstring(rel_bytes)
    for rel in root.findall("rel:Relationship", NS):
        rid = rel.attrib.get("Id")
        if rid in replacements:
            media_name, _ = replacements[rid]
            rel.set("Target", f"../media/{media_name}")

    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def compute_topic_sentiment_chart() -> dict:
    counts: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    total_rows = 0

    if SENTIMENT_CSV_PATH.exists():
        with SENTIMENT_CSV_PATH.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                total_rows += 1
                topic = (row.get("topic") or "").strip()
                label = (row.get("sentiment_label") or "").strip().lower()
                if not topic or label not in {item[0] for item in CHART_SERIES}:
                    continue
                counts[topic][label] += 1

    categories = [display for _, display in CHART_TOPIC_ORDER]
    series = []
    for label_key, label_name, color in CHART_SERIES:
        values = [counts[source][label_key] for source, _ in CHART_TOPIC_ORDER]
        series.append({"key": label_key, "name": label_name, "color": color, "values": values})

    return {
        "title": "各主题新闻的情感分布",
        "categories": categories,
        "series": series,
        "total_rows": total_rows,
    }


def update_chart4(chart_bytes: bytes, chart_data: dict) -> bytes:
    chart_ns = {
        "c": "http://schemas.openxmlformats.org/drawingml/2006/chart",
        "a": "http://schemas.openxmlformats.org/drawingml/2006/main",
    }
    root = ET.fromstring(chart_bytes)

    title_node = root.find(".//c:title//a:t", chart_ns)
    if title_node is not None:
        title_node.text = chart_data["title"]

    for body in root.findall(".//c:catAx/c:txPr/a:bodyPr", chart_ns):
        body.set("rot", "0")
    for body in root.findall(".//c:valAx/c:txPr/a:bodyPr", chart_ns):
        body.set("rot", "0")

    category_points = chart_data["categories"]
    for ser_idx, ser in enumerate(root.findall(".//c:barChart/c:ser", chart_ns)):
        if ser_idx >= len(chart_data["series"]):
            continue
        config = chart_data["series"][ser_idx]

        tx_val = ser.find(".//c:tx//c:v", chart_ns)
        if tx_val is not None:
            tx_val.text = config["name"]

        fill = ser.find(".//c:spPr/a:solidFill/a:srgbClr", chart_ns)
        if fill is not None:
            fill.set("val", config["color"])

        cat_cache = ser.find(".//c:cat//c:strCache", chart_ns)
        if cat_cache is not None:
            pt_count = cat_cache.find("c:ptCount", chart_ns)
            if pt_count is not None:
                pt_count.set("val", str(len(category_points)))
            for pt in list(cat_cache.findall("c:pt", chart_ns)):
                cat_cache.remove(pt)
            for idx, label in enumerate(category_points):
                pt = ET.SubElement(cat_cache, qname("c", "pt"), {"idx": str(idx)})
                v = ET.SubElement(pt, qname("c", "v"))
                v.text = label

        val_cache = ser.find(".//c:val//c:numCache", chart_ns)
        if val_cache is not None:
            pt_count = val_cache.find("c:ptCount", chart_ns)
            if pt_count is not None:
                pt_count.set("val", str(len(config["values"])))
            for pt in list(val_cache.findall("c:pt", chart_ns)):
                val_cache.remove(pt)
            for idx, value in enumerate(config["values"]):
                pt = ET.SubElement(val_cache, qname("c", "pt"), {"idx": str(idx)})
                v = ET.SubElement(pt, qname("c", "v"))
                v.text = str(value)

    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def update_workbook4(workbook_bytes: bytes, chart_data: dict) -> bytes:
    ss_ns = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    strings = ["主题"] + [series["name"] for series in chart_data["series"]] + chart_data["categories"]

    with zipfile.ZipFile(io.BytesIO(workbook_bytes), "r") as src:
        workbook_entries = {name: src.read(name) for name in src.namelist()}

    shared_root = ET.fromstring(workbook_entries["xl/sharedStrings.xml"])
    shared_root.set("count", str(len(strings)))
    shared_root.set("uniqueCount", str(len(strings)))
    for child in list(shared_root):
        shared_root.remove(child)
    for value in strings:
        si = ET.SubElement(shared_root, qname("s", "si"))
        t = ET.SubElement(si, qname("s", "t"))
        t.text = value
    workbook_entries["xl/sharedStrings.xml"] = ET.tostring(shared_root, encoding="utf-8", xml_declaration=True)

    sheet_root = ET.fromstring(workbook_entries["xl/worksheets/sheet1.xml"])
    dimension = sheet_root.find("s:dimension", ss_ns)
    if dimension is not None:
        dimension.set("ref", "A1:D5")

    sheet_data = sheet_root.find("s:sheetData", ss_ns)
    if sheet_data is not None:
        for row in list(sheet_data):
            sheet_data.remove(row)

        header_row = ET.SubElement(sheet_data, qname("s", "row"), {"r": "1", "spans": "1:4"})
        for ref, idx in [("A1", 0), ("B1", 1), ("C1", 2), ("D1", 3)]:
            cell = ET.SubElement(header_row, qname("s", "c"), {"r": ref, "t": "s"})
            value = ET.SubElement(cell, qname("s", "v"))
            value.text = str(idx)

        for row_idx, category in enumerate(chart_data["categories"], start=2):
            row = ET.SubElement(sheet_data, qname("s", "row"), {"r": str(row_idx), "spans": "1:4"})
            cell_a = ET.SubElement(row, qname("s", "c"), {"r": f"A{row_idx}", "t": "s"})
            val_a = ET.SubElement(cell_a, qname("s", "v"))
            val_a.text = str(4 + (row_idx - 2))
            for col_letter, series_idx in zip(["B", "C", "D"], range(3), strict=False):
                cell = ET.SubElement(row, qname("s", "c"), {"r": f"{col_letter}{row_idx}"})
                value = ET.SubElement(cell, qname("s", "v"))
                value.text = str(chart_data["series"][series_idx]["values"][row_idx - 2])

    workbook_entries["xl/worksheets/sheet1.xml"] = ET.tostring(sheet_root, encoding="utf-8", xml_declaration=True)

    table_root = ET.fromstring(workbook_entries["xl/tables/table1.xml"])
    table_root.set("ref", "A1:D5")
    columns = table_root.find("s:tableColumns", ss_ns)
    if columns is not None:
        columns.set("count", "4")
        column_names = ["主题"] + [series["name"] for series in chart_data["series"]]
        existing = columns.findall("s:tableColumn", ss_ns)
        for column, name in zip(existing, column_names, strict=False):
            column.set("name", name)
    workbook_entries["xl/tables/table1.xml"] = ET.tostring(table_root, encoding="utf-8", xml_declaration=True)

    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as dst:
        for name, data in workbook_entries.items():
            dst.writestr(name, data)
    return output.getvalue()


def backup_existing_output() -> None:
    if not OUTPUT_PATH.exists():
        return

    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = HISTORY_DIR / f"{OUTPUT_PATH.stem}_{timestamp}{OUTPUT_PATH.suffix}"
    shutil.copy2(OUTPUT_PATH, backup_path)


def build_presentation() -> None:
    backup_existing_output()

    with zipfile.ZipFile(TEMPLATE_PATH, "r") as src:
        entries = {name: src.read(name) for name in src.namelist()}

    chart_data = compute_topic_sentiment_chart()

    for slide_number in SLIDE_TEXT:
        slide_name = f"ppt/slides/slide{slide_number}.xml"
        if slide_name in entries:
            entries[slide_name] = update_slide_text(entries[slide_name], slide_number)

    for slide_number in SLIDE_IMAGE_REPLACEMENTS:
        rel_name = f"ppt/slides/_rels/slide{slide_number}.xml.rels"
        if rel_name in entries:
            entries[rel_name] = update_slide_relationships(entries[rel_name], slide_number)

    for replacements in SLIDE_IMAGE_REPLACEMENTS.values():
        for media_name, source_path in replacements.values():
            entries[f"ppt/media/{media_name}"] = source_path.read_bytes()

    if "ppt/charts/chart4.xml" in entries:
        entries["ppt/charts/chart4.xml"] = update_chart4(entries["ppt/charts/chart4.xml"], chart_data)
    if "ppt/embeddings/Workbook4.xlsx" in entries:
        entries["ppt/embeddings/Workbook4.xlsx"] = update_workbook4(entries["ppt/embeddings/Workbook4.xlsx"], chart_data)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(OUTPUT_PATH, "w", compression=zipfile.ZIP_DEFLATED) as dst:
        for name, data in entries.items():
            dst.writestr(name, data)


if __name__ == "__main__":
    build_presentation()
    print(OUTPUT_PATH)
