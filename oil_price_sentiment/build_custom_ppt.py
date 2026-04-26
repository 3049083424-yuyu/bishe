from __future__ import annotations

import copy
import datetime as dt
import math
import shutil
import struct
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET


BASE_PPT_PATH = Path(r"D:\graduate\oil_price_sentiment\ppt_model.pptx")
OUTPUT_PPT_PATH = Path(r"D:\graduate\oil_price_sentiment\oil_price_system_presentation_custom_generated.pptx")
HISTORY_DIR = Path(r"D:\graduate\oil_price_sentiment\history")
MEDIA_DIR = Path(r"D:\graduate\oil_price_sentiment\report_media")

SLIDE_W = 12192000
SLIDE_H = 6858000

BG = "F7F2EA"
NAVY = "17324A"
ORANGE = "D98A3D"
TEXT = "233240"
MUTED = "5E6A77"
LINE = "DDD4C6"
WHITE = "FFFFFF"
PALE = "FFF8EF"
PALE_BLUE = "EEF4FB"
PALE_ORANGE = "FFF1E3"
RED = "B4534C"
GREEN = "5B7E68"

FONT_FACE = "Microsoft YaHei"

NS = {
    "a": "http://schemas.openxmlformats.org/drawingml/2006/main",
    "p": "http://schemas.openxmlformats.org/presentationml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "rel": "http://schemas.openxmlformats.org/package/2006/relationships",
    "app": "http://schemas.openxmlformats.org/officeDocument/2006/extended-properties",
    "vt": "http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes",
}

for prefix in ["a", "p", "r"]:
    ET.register_namespace(prefix, NS[prefix])


def qn(prefix: str, tag: str) -> str:
    return f"{{{NS[prefix]}}}{tag}"


def today_text() -> str:
    now = dt.date.today()
    return f"{now.year}年{now.month:02d}月{now.day:02d}日"


def safe_copy_existing_output() -> None:
    if not OUTPUT_PPT_PATH.exists():
        return
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = HISTORY_DIR / f"{OUTPUT_PPT_PATH.stem}_{timestamp}{OUTPUT_PPT_PATH.suffix}"
    shutil.copy2(OUTPUT_PPT_PATH, backup)


def image_size(path: Path) -> tuple[int, int]:
    data = path.read_bytes()
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        width, height = struct.unpack(">II", data[16:24])
        return width, height
    if data.startswith(b"\xff\xd8"):
        idx = 2
        while idx < len(data):
            while idx < len(data) and data[idx] != 0xFF:
                idx += 1
            while idx < len(data) and data[idx] == 0xFF:
                idx += 1
            if idx >= len(data):
                break
            marker = data[idx]
            idx += 1
            if marker in {0xD8, 0xD9}:
                continue
            if idx + 2 > len(data):
                break
            seg_len = struct.unpack(">H", data[idx:idx + 2])[0]
            if marker in {0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF}:
                if idx + 7 > len(data):
                    break
                height, width = struct.unpack(">HH", data[idx + 3:idx + 7])
                return width, height
            idx += seg_len
    raise ValueError(f"Unsupported image format: {path}")


def fit_box(img_w: int, img_h: int, box_w: int, box_h: int) -> tuple[int, int]:
    scale = min(box_w / img_w, box_h / img_h)
    return max(1, int(img_w * scale)), max(1, int(img_h * scale))


class SlideBuilder:
    def __init__(self, title: str | None = None, section: str | None = None, page: int | None = None, cover: bool = False):
        self.root = ET.Element(qn("p", "sld"))
        c_sld = ET.SubElement(self.root, qn("p", "cSld"))
        self.sp_tree = ET.SubElement(c_sld, qn("p", "spTree"))
        nv = ET.SubElement(self.sp_tree, qn("p", "nvGrpSpPr"))
        ET.SubElement(nv, qn("p", "cNvPr"), {"id": "1", "name": ""})
        ET.SubElement(nv, qn("p", "cNvGrpSpPr"))
        ET.SubElement(nv, qn("p", "nvPr"))
        grp = ET.SubElement(self.sp_tree, qn("p", "grpSpPr"))
        xfrm = ET.SubElement(grp, qn("a", "xfrm"))
        ET.SubElement(xfrm, qn("a", "off"), {"x": "0", "y": "0"})
        ET.SubElement(xfrm, qn("a", "ext"), {"cx": "0", "cy": "0"})
        ET.SubElement(xfrm, qn("a", "chOff"), {"x": "0", "y": "0"})
        ET.SubElement(xfrm, qn("a", "chExt"), {"cx": "0", "cy": "0"})
        self.shape_id = 2
        self.pic_id = 1000
        self.media: list[tuple[str, Path]] = []
        if cover:
            self.cover_chrome()
        else:
            self.standard_chrome(title or "", section or "", page or 1)

    def next_shape_id(self) -> str:
        value = str(self.shape_id)
        self.shape_id += 1
        return value

    def next_pic_id(self) -> str:
        value = str(self.pic_id)
        self.pic_id += 1
        return value

    def cover_chrome(self) -> None:
        self.add_rect(0, 0, SLIDE_W, SLIDE_H, fill=BG, line=BG)
        self.add_rect(0, 0, 7700000, SLIDE_H, fill=NAVY, line=NAVY)
        self.add_rect(0, 0, 180000, SLIDE_H, fill=ORANGE, line=ORANGE)
        self.add_rect(7700000, 0, SLIDE_W - 7700000, 900000, fill=PALE_ORANGE, line=PALE_ORANGE)

    def standard_chrome(self, title: str, section: str, page: int) -> None:
        self.add_rect(0, 0, SLIDE_W, SLIDE_H, fill=BG, line=BG)
        self.add_rect(0, 0, SLIDE_W, 820000, fill=NAVY, line=NAVY)
        self.add_rect(0, 0, 170000, 820000, fill=ORANGE, line=ORANGE)
        self.add_rect(520000, 930000, 2300000, 340000, fill=ORANGE, line=ORANGE, geom="roundRect")
        self.add_text(650000, 990000, 2050000, 220000, section, 1700, WHITE, bold=True)
        self.add_text(540000, 160000, 7800000, 360000, title, 2800, WHITE, bold=True)
        self.add_text(11220000, 210000, 540000, 220000, f"{page:02d}", 1500, WHITE, bold=True, align="ctr")
        self.add_rect(520000, 6270000, 11160000, 3000, fill=LINE, line=LINE)
        self.add_text(540000, 6350000, 3500000, 220000, "OilPrice 新闻分析与预测系统", 1200, MUTED)

    def add_rect(self, x: int, y: int, cx: int, cy: int, fill: str, line: str, geom: str = "rect", line_w: str = "12700") -> None:
        sp = ET.SubElement(self.sp_tree, qn("p", "sp"))
        nv = ET.SubElement(sp, qn("p", "nvSpPr"))
        ET.SubElement(nv, qn("p", "cNvPr"), {"id": self.next_shape_id(), "name": "Shape"})
        ET.SubElement(nv, qn("p", "cNvSpPr"))
        ET.SubElement(nv, qn("p", "nvPr"))
        sp_pr = ET.SubElement(sp, qn("p", "spPr"))
        xfrm = ET.SubElement(sp_pr, qn("a", "xfrm"))
        ET.SubElement(xfrm, qn("a", "off"), {"x": str(x), "y": str(y)})
        ET.SubElement(xfrm, qn("a", "ext"), {"cx": str(cx), "cy": str(cy)})
        prst = ET.SubElement(sp_pr, qn("a", "prstGeom"), {"prst": geom})
        ET.SubElement(prst, qn("a", "avLst"))
        solid = ET.SubElement(sp_pr, qn("a", "solidFill"))
        ET.SubElement(solid, qn("a", "srgbClr"), {"val": fill})
        ln = ET.SubElement(sp_pr, qn("a", "ln"), {"w": line_w})
        ln_fill = ET.SubElement(ln, qn("a", "solidFill"))
        ET.SubElement(ln_fill, qn("a", "srgbClr"), {"val": line})
        ET.SubElement(ln, qn("a", "round"))
        ET.SubElement(sp, qn("p", "txBody"))

    def add_text(
        self,
        x: int,
        y: int,
        cx: int,
        cy: int,
        text: str,
        size: int,
        color: str,
        bold: bool = False,
        align: str = "l",
        fill: str | None = None,
        line: str | None = None,
        geom: str = "rect",
        inset: tuple[int, int, int, int] = (91440, 45720, 91440, 45720),
        anchor: str = "t",
    ) -> None:
        sp = ET.SubElement(self.sp_tree, qn("p", "sp"))
        nv = ET.SubElement(sp, qn("p", "nvSpPr"))
        ET.SubElement(nv, qn("p", "cNvPr"), {"id": self.next_shape_id(), "name": "Text"})
        ET.SubElement(nv, qn("p", "cNvSpPr"), {"txBox": "1"})
        ET.SubElement(nv, qn("p", "nvPr"))
        sp_pr = ET.SubElement(sp, qn("p", "spPr"))
        xfrm = ET.SubElement(sp_pr, qn("a", "xfrm"))
        ET.SubElement(xfrm, qn("a", "off"), {"x": str(x), "y": str(y)})
        ET.SubElement(xfrm, qn("a", "ext"), {"cx": str(cx), "cy": str(cy)})
        prst = ET.SubElement(sp_pr, qn("a", "prstGeom"), {"prst": geom})
        ET.SubElement(prst, qn("a", "avLst"))
        if fill is None:
            ET.SubElement(sp_pr, qn("a", "noFill"))
        else:
            solid = ET.SubElement(sp_pr, qn("a", "solidFill"))
            ET.SubElement(solid, qn("a", "srgbClr"), {"val": fill})
        ln = ET.SubElement(sp_pr, qn("a", "ln"))
        if line is None:
            ET.SubElement(ln, qn("a", "noFill"))
        else:
            solid_line = ET.SubElement(ln, qn("a", "solidFill"))
            ET.SubElement(solid_line, qn("a", "srgbClr"), {"val": line})
        tx_body = ET.SubElement(sp, qn("p", "txBody"))
        ET.SubElement(
            tx_body,
            qn("a", "bodyPr"),
            {
                "wrap": "square",
                "lIns": str(inset[0]),
                "tIns": str(inset[1]),
                "rIns": str(inset[2]),
                "bIns": str(inset[3]),
                "anchor": anchor,
            },
        )
        ET.SubElement(tx_body, qn("a", "lstStyle"))
        for raw_line in text.split("\n"):
            p = ET.SubElement(tx_body, qn("a", "p"))
            ET.SubElement(p, qn("a", "pPr"), {"algn": align})
            run = ET.SubElement(p, qn("a", "r"))
            rpr = ET.SubElement(
                run,
                qn("a", "rPr"),
                {
                    "lang": "zh-CN",
                    "sz": str(size),
                    "b": "1" if bold else "0",
                    "dirty": "0",
                    "smtClean": "0",
                },
            )
            ET.SubElement(rpr, qn("a", "latin"), {"typeface": FONT_FACE})
            ET.SubElement(rpr, qn("a", "ea"), {"typeface": FONT_FACE})
            fill_node = ET.SubElement(rpr, qn("a", "solidFill"))
            ET.SubElement(fill_node, qn("a", "srgbClr"), {"val": color})
            t = ET.SubElement(run, qn("a", "t"))
            t.text = raw_line if raw_line else " "
            end_rpr = ET.SubElement(p, qn("a", "endParaRPr"), {"lang": "zh-CN", "sz": str(size)})
            end_fill = ET.SubElement(end_rpr, qn("a", "solidFill"))
            ET.SubElement(end_fill, qn("a", "srgbClr"), {"val": color})

    def add_image(self, path: Path, x: int, y: int, cx: int, cy: int) -> None:
        img_w, img_h = image_size(path)
        fit_w, fit_h = fit_box(img_w, img_h, cx, cy)
        draw_x = x + (cx - fit_w) // 2
        draw_y = y + (cy - fit_h) // 2
        rid = f"rId{len(self.media) + 2}"
        media_name = f"report_{path.stem}{path.suffix.lower()}"
        self.media.append((media_name, path))

        pic = ET.SubElement(self.sp_tree, qn("p", "pic"))
        nv = ET.SubElement(pic, qn("p", "nvPicPr"))
        ET.SubElement(nv, qn("p", "cNvPr"), {"id": self.next_pic_id(), "name": path.name})
        c_nv_pic_pr = ET.SubElement(nv, qn("p", "cNvPicPr"))
        ET.SubElement(c_nv_pic_pr, qn("a", "picLocks"), {"noChangeAspect": "1"})
        ET.SubElement(nv, qn("p", "nvPr"))
        blip_fill = ET.SubElement(pic, qn("p", "blipFill"))
        ET.SubElement(blip_fill, qn("a", "blip"), {qn("r", "embed"): rid})
        stretch = ET.SubElement(blip_fill, qn("a", "stretch"))
        ET.SubElement(stretch, qn("a", "fillRect"))
        sp_pr = ET.SubElement(pic, qn("p", "spPr"))
        xfrm = ET.SubElement(sp_pr, qn("a", "xfrm"))
        ET.SubElement(xfrm, qn("a", "off"), {"x": str(draw_x), "y": str(draw_y)})
        ET.SubElement(xfrm, qn("a", "ext"), {"cx": str(fit_w), "cy": str(fit_h)})
        prst = ET.SubElement(sp_pr, qn("a", "prstGeom"), {"prst": "rect"})
        ET.SubElement(prst, qn("a", "avLst"))

    def slide_xml(self) -> bytes:
        clr = ET.SubElement(self.root, qn("p", "clrMapOvr"))
        ET.SubElement(clr, qn("a", "masterClrMapping"))
        return ET.tostring(self.root, encoding="utf-8", xml_declaration=True)

    def rels_xml(self) -> bytes:
        rel_root = ET.Element("Relationships", {"xmlns": NS["rel"]})
        ET.SubElement(
            rel_root,
            "Relationship",
            {
                "Id": "rId1",
                "Type": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/slideLayout",
                "Target": "../slideLayouts/slideLayout1.xml",
            },
        )
        for idx, (media_name, _path) in enumerate(self.media, start=2):
            ET.SubElement(
                rel_root,
                "Relationship",
                {
                    "Id": f"rId{idx}",
                    "Type": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/image",
                    "Target": f"../media/{media_name}",
                },
            )
        return ET.tostring(rel_root, encoding="utf-8", xml_declaration=True)


def cover_slide() -> SlideBuilder:
    b = SlideBuilder(cover=True)
    b.add_text(820000, 910000, 6200000, 650000, "基于 OilPrice 新闻的\n国际油价预测系统汇报", 3000, WHITE, bold=True)
    b.add_rect(820000, 2120000, 4200000, 9000, fill=ORANGE, line=ORANGE)
    b.add_text(820000, 2320000, 5900000, 740000, "围绕数据收集、新闻摘要、\n情感指标构建与油价预测展开", 2000, "E8EEF5")
    b.add_text(820000, 3350000, 5300000, 460000, "课程项目小组汇报", 1700, "C8D5E1", bold=True)
    b.add_text(820000, 3850000, 5300000, 300000, today_text(), 1500, "C8D5E1")

    metric_y = 5200000
    for idx, (value, label) in enumerate([("37,665", "新闻样本"), ("57.14%", "方向准确率"), ("5.34%", "MAPE")]):
        x = 820000 + idx * 1880000
        b.add_rect(x, metric_y, 1650000, 830000, fill=WHITE, line=WHITE, geom="roundRect")
        b.add_text(x + 100000, metric_y + 85000, 1450000, 250000, value, 2100, NAVY, bold=True, align="ctr")
        b.add_text(x + 100000, metric_y + 390000, 1450000, 200000, label, 1300, MUTED, align="ctr")

    b.add_rect(7900000, 1120000, 3550000, 4200000, fill=WHITE, line=LINE, geom="roundRect")
    b.add_text(8150000, 1280000, 3000000, 260000, "系统界面预览", 1600, NAVY, bold=True)
    b.add_image(MEDIA_DIR / "image18.png", 8100000, 1620000, 3150000, 3400000)
    b.add_text(8150000, 5180000, 3000000, 500000, "图形化界面统一串联摘要、分类、情感、综合指标与预测模块。", 1300, MUTED)
    return b


def agenda_slide() -> SlideBuilder:
    b = SlideBuilder("汇报目录", "项目总览", 2)
    b.add_text(620000, 1400000, 6500000, 250000, "本次汇报按“过程 - 指标 - 结果 - 演示”展开，便于课堂展示和系统说明。", 1500, MUTED)
    cards = [
        ("01", "数据收集与分类", "新闻来源、采集过程、\n清洗分词与主题分类"),
        ("02", "摘要与情感分析", "LSA 摘要、词典扩充、\n情感得分计算与分布"),
        ("03", "综合指标与油价预测", "News_t / VA_t、K+1 特征、\nAttention-LSTM 预测"),
        ("04", "系统演示与总结", "前端联调、测试调试、\n项目成果与改进方向"),
    ]
    positions = [(720000, 1950000), (6400000, 1950000), (720000, 4200000), (6400000, 4200000)]
    for (num, title, body), (x, y) in zip(cards, positions, strict=False):
        b.add_rect(x, y, 4900000, 1700000, fill=WHITE, line=LINE, geom="roundRect")
        b.add_rect(x + 180000, y + 160000, 760000, 620000, fill=ORANGE, line=ORANGE, geom="roundRect")
        b.add_text(x + 220000, y + 230000, 680000, 220000, num, 1900, WHITE, bold=True, align="ctr")
        b.add_text(x + 1120000, y + 210000, 3400000, 320000, title, 2100, NAVY, bold=True)
        b.add_text(x + 1120000, y + 620000, 3400000, 700000, body, 1500, MUTED)
    return b


def background_goal_slide() -> SlideBuilder:
    b = SlideBuilder("研究背景与项目目标", "01 项目概览", 3)
    b.add_rect(620000, 1540000, 5200000, 4200000, fill=WHITE, line=LINE, geom="roundRect")
    b.add_rect(6550000, 1540000, 5000000, 4200000, fill=WHITE, line=LINE, geom="roundRect")
    b.add_text(900000, 1720000, 1700000, 250000, "研究背景", 1900, NAVY, bold=True)
    b.add_text(
        900000,
        2140000,
        4600000,
        3000000,
        "• 国际油价同时受供需、地缘政治、宏观预期和市场情绪影响\n"
        "• 新闻文本中包含了大量对未来价格的方向判断与风险提示\n"
        "• 仅依赖传统结构化指标，难以完整捕捉市场预期变化\n"
        "• 因此需要把新闻内容转化为可量化、可建模的情绪信号",
        1600,
        TEXT,
    )
    b.add_text(6880000, 1720000, 1900000, 250000, "项目目标", 1900, NAVY, bold=True)
    b.add_text(
        6880000,
        2140000,
        4200000,
        3000000,
        "• 采集 OilPrice 能源新闻并完成标准化处理\n"
        "• 对新闻正文进行摘要提取和多标签主题分类\n"
        "• 基于领域词典计算方向性情感得分\n"
        "• 构建综合情感指标并与 WTI 价格按周对齐\n"
        "• 使用 Attention-LSTM 进行油价预测分析",
        1600,
        TEXT,
    )
    b.add_rect(620000, 5940000, 10900000, 620000, fill=PALE_ORANGE, line=PALE_ORANGE, geom="roundRect")
    b.add_text(900000, 6070000, 10200000, 240000, "核心思路：把非结构化新闻转化为可解释的周度情绪特征，再与价格序列结合完成预测分析。", 1500, NAVY, bold=True, align="ctr")
    return b


def workflow_slide() -> SlideBuilder:
    b = SlideBuilder("系统整体流程", "01 项目概览", 4)
    b.add_text(620000, 1420000, 8300000, 250000, "系统按“采集 - 处理 - 分析 - 预测”的逻辑串联为完整闭环。", 1500, MUTED)
    steps = [
        ("1", "新闻采集", "获取 OilPrice\n能源新闻"),
        ("2", "文本预处理", "清洗、分词、\n词干提取"),
        ("3", "摘要提取", "压缩正文信息，\n保留关键句"),
        ("4", "主题分类", "输出多标签\n0/1 结果"),
        ("5", "情感得分", "匹配词典并\n计算方向得分"),
        ("6", "综合指标", "周度聚合\nNews_t / VA_t"),
        ("7", "油价预测", "构造 K+1 特征，\n输入模型"),
    ]
    positions = [
        (700000, 2100000),
        (3600000, 2100000),
        (6500000, 2100000),
        (9400000, 2100000),
        (2100000, 4150000),
        (5000000, 4150000),
        (7900000, 4150000),
    ]
    for (num, title, body), (x, y) in zip(steps, positions, strict=False):
        b.add_rect(x, y, 2100000, 1250000, fill=WHITE, line=LINE, geom="roundRect")
        b.add_rect(x + 130000, y + 120000, 420000, 420000, fill=ORANGE, line=ORANGE, geom="roundRect")
        b.add_text(x + 150000, y + 180000, 360000, 180000, num, 1700, WHITE, bold=True, align="ctr")
        b.add_text(x + 620000, y + 130000, 1280000, 250000, title, 1750, NAVY, bold=True)
        b.add_text(x + 620000, y + 440000, 1320000, 520000, body, 1400, MUTED)
    return b


def data_preprocess_slide() -> SlideBuilder:
    b = SlideBuilder("数据收集与文本预处理", "02 数据收集与分类", 5)
    chips = [("新闻样本", "37,665"), ("一级栏目", "4"), ("主题输出", "0/1")]
    for idx, (label, value) in enumerate(chips):
        x = 760000 + idx * 1800000
        b.add_rect(x, 1450000, 1500000, 760000, fill=PALE_BLUE, line=PALE_BLUE, geom="roundRect")
        b.add_text(x + 100000, 1530000, 1300000, 240000, value, 2000, NAVY, bold=True, align="ctr")
        b.add_text(x + 100000, 1860000, 1300000, 180000, label, 1250, MUTED, align="ctr")
    b.add_rect(700000, 2440000, 5100000, 3000000, fill=WHITE, line=LINE, geom="roundRect")
    b.add_rect(6380000, 2440000, 5100000, 3000000, fill=WHITE, line=LINE, geom="roundRect")
    b.add_text(960000, 2610000, 2200000, 240000, "新闻来源与采集字段", 1850, NAVY, bold=True)
    b.add_text(
        960000,
        3010000,
        4300000,
        2200000,
        "• 来源栏目包括 Energy、Alternative Energy、Company News 和 Latest Energy News\n"
        "• 采集字段覆盖 topic、subtitle、title、date、article、link\n"
        "• 通过列表页翻页和详情页抓取完成批量采集\n"
        "• 设置异常处理、重试与断点续爬保证稳定性",
        1550,
        TEXT,
    )
    b.add_text(6640000, 2610000, 1900000, 240000, "预处理步骤", 1850, NAVY, bold=True)
    b.add_text(
        6640000,
        3010000,
        4300000,
        2200000,
        "• 去除冗余列与缺失值，统一日期和编码格式\n"
        "• 文本小写化、去特殊符号、分词和停用词过滤\n"
        "• 执行词干提取，减少词形变化带来的噪声\n"
        "• 为摘要、分类和情感模块保留可复用的标准化结果",
        1550,
        TEXT,
    )
    b.add_rect(700000, 5660000, 10780000, 560000, fill=PALE_ORANGE, line=PALE_ORANGE, geom="roundRect")
    b.add_text(980000, 5780000, 10100000, 220000, "预处理的目标不是“把文本变短”，而是把新闻转成可以被不同模块稳定复用的规范输入。", 1450, NAVY, bold=True, align="ctr")
    return b


def dual_image_slide(
    title: str,
    section: str,
    page: int,
    left_title: str,
    left_caption: str,
    left_image: Path,
    right_title: str,
    right_caption: str,
    right_image: Path,
    bottom_note: str,
) -> SlideBuilder:
    b = SlideBuilder(title, section, page)
    b.add_rect(680000, 1540000, 5150000, 4200000, fill=WHITE, line=LINE, geom="roundRect")
    b.add_rect(6380000, 1540000, 5150000, 4200000, fill=WHITE, line=LINE, geom="roundRect")
    b.add_text(920000, 1710000, 2100000, 240000, left_title, 1800, NAVY, bold=True)
    b.add_text(6640000, 1710000, 2100000, 240000, right_title, 1800, NAVY, bold=True)
    b.add_image(left_image, 910000, 2100000, 4550000, 2900000)
    b.add_image(right_image, 6570000, 2100000, 4550000, 2900000)
    b.add_text(900000, 5130000, 4600000, 400000, left_caption, 1350, MUTED)
    b.add_text(6580000, 5130000, 4600000, 400000, right_caption, 1350, MUTED)
    b.add_rect(680000, 5940000, 10800000, 500000, fill=PALE_BLUE, line=PALE_BLUE, geom="roundRect")
    b.add_text(950000, 6040000, 10200000, 200000, bottom_note, 1400, NAVY, bold=True, align="ctr")
    return b


def summary_slide() -> SlideBuilder:
    b = SlideBuilder("新闻摘要提取过程与结果", "02 数据收集与分类", 7)
    b.add_rect(700000, 1540000, 3900000, 4200000, fill=WHITE, line=LINE, geom="roundRect")
    b.add_rect(4900000, 1540000, 6600000, 4200000, fill=WHITE, line=LINE, geom="roundRect")
    b.add_text(980000, 1730000, 1900000, 240000, "摘要模块思路", 1850, NAVY, bold=True)
    b.add_text(
        980000,
        2140000,
        3200000,
        2500000,
        "• 采用 LSA 摘要算法，从正文中识别代表性句子\n"
        "• 通过少量关键句压缩长篇新闻，提高快速浏览效率\n"
        "• 可设置摘要句数与输出位置，结果适合展示与核验\n"
        "• 摘要文本也可作为后续人工阅读和系统演示材料",
        1550,
        TEXT,
    )
    b.add_rect(980000, 4780000, 3000000, 620000, fill=PALE_ORANGE, line=PALE_ORANGE, geom="roundRect")
    b.add_text(1090000, 4890000, 2780000, 260000, "输出价值：在保留核心信息的同时显著降低阅读负担。", 1400, NAVY, bold=True, align="ctr")
    b.add_text(5160000, 1730000, 2100000, 240000, "运行结果展示", 1850, NAVY, bold=True)
    b.add_image(MEDIA_DIR / "image4.png", 5170000, 2120000, 6000000, 3180000)
    b.add_text(5160000, 5400000, 5900000, 300000, "界面中可以直接设置新闻正文列、摘要列和摘要句数，输出摘要结果文件。", 1350, MUTED)
    return b


def classification_slide() -> SlideBuilder:
    b = SlideBuilder("新闻分类模块过程与结果", "02 数据收集与分类", 8)
    b.add_rect(700000, 1540000, 3900000, 4200000, fill=WHITE, line=LINE, geom="roundRect")
    b.add_rect(4900000, 1540000, 6600000, 4200000, fill=WHITE, line=LINE, geom="roundRect")
    b.add_text(980000, 1730000, 2000000, 240000, "分类方法设计", 1850, NAVY, bold=True)
    b.add_text(
        980000,
        2140000,
        3200000,
        2400000,
        "• 使用 TF-IDF 表示新闻文本特征\n"
        "• 采用 OneVsRest 多标签分类框架\n"
        "• 输出主题标签的 0/1 结果，支持多主题并存\n"
        "• 分类结果可直接用于周度聚合与建模特征构造",
        1550,
        TEXT,
    )
    b.add_rect(980000, 4700000, 3000000, 700000, fill=PALE_BLUE, line=PALE_BLUE, geom="roundRect")
    b.add_text(1100000, 4820000, 2760000, 300000, "结构化输出让海量新闻能够被筛选、统计和透视分析。", 1400, NAVY, bold=True, align="ctr")
    b.add_text(5160000, 1730000, 2200000, 240000, "分类结果展示", 1850, NAVY, bold=True)
    b.add_image(MEDIA_DIR / "image5.png", 5200000, 2160000, 5900000, 2850000)
    b.add_text(5160000, 5300000, 5900000, 340000, "系统自动读取文本列并加载模型，输出每篇新闻对应的主题标签结果。", 1350, MUTED)
    return b


def sentiment_design_slide() -> SlideBuilder:
    b = SlideBuilder("情感定义与词典扩充", "03 情感指标与预测建模", 9)
    cards = [
        ("看涨方向词", "rally、surge、spike、rebound\nshortage、drawdown、sanctions", PALE_ORANGE),
        ("看跌方向词", "drop、plunge、decline、slump\noversupply、surplus、glut", PALE_BLUE),
        ("领域语义校正", "在油价研究中，某些“负面事件”\n可能意味着供应风险上升，因此会被映射为看涨信号。", WHITE),
        ("扩充原则", "结合高频语料、供需逻辑、库存变化、\n地缘风险和需求预期，重建方向性词典。", WHITE),
    ]
    positions = [(700000, 1680000), (6380000, 1680000), (700000, 3950000), (6380000, 3950000)]
    for (title, body, fill), (x, y) in zip(cards, positions, strict=False):
        b.add_rect(x, y, 4700000, 1800000, fill=fill, line=LINE, geom="roundRect")
        b.add_text(x + 220000, y + 180000, 2200000, 240000, title, 1800, NAVY, bold=True)
        b.add_text(x + 220000, y + 570000, 4250000, 900000, body, 1450, TEXT)
    return b


def sentiment_score_slide() -> SlideBuilder:
    b = SlideBuilder("情感得分计算与结果分布", "03 情感指标与预测建模", 10)
    totals = [("16,920", "看涨", PALE_ORANGE), ("9,586", "中性", WHITE), ("11,159", "看跌", PALE_BLUE)]
    for idx, (value, label, fill) in enumerate(totals):
        x = 720000 + idx * 3660000
        b.add_rect(x, 1520000, 3200000, 1050000, fill=fill, line=LINE, geom="roundRect")
        b.add_text(x + 180000, 1690000, 2840000, 280000, value, 2400, NAVY, bold=True, align="ctr")
        b.add_text(x + 180000, 2080000, 2840000, 220000, label, 1450, MUTED, align="ctr")
    b.add_rect(720000, 2880000, 4800000, 2300000, fill=WHITE, line=LINE, geom="roundRect")
    b.add_rect(6400000, 2880000, 5100000, 2300000, fill=WHITE, line=LINE, geom="roundRect")
    b.add_text(980000, 3070000, 2000000, 240000, "核心计算公式", 1850, NAVY, bold=True)
    b.add_text(
        980000,
        3500000,
        4100000,
        1200000,
        "raw_score = bullish_mass - bearish_mass\n"
        "evidence_mass = bullish_mass + bearish_mass\n"
        "sentiment_score = raw_score / (evidence_mass + 3)",
        1600,
        TEXT,
    )
    b.add_text(6660000, 3070000, 2200000, 240000, "计算逻辑说明", 1850, NAVY, bold=True)
    b.add_text(
        6660000,
        3500000,
        4200000,
        1300000,
        "• 分别统计标题与正文中的看涨/看跌词命中\n"
        "• 标题给予更高权重，突出新闻主旨\n"
        "• 使用平滑项减弱少量词命中的极端波动\n"
        "• 最终按得分划分为看涨、看跌和中性三类",
        1500,
        TEXT,
    )
    b.add_rect(720000, 5480000, 10800000, 620000, fill=PALE_ORANGE, line=PALE_ORANGE, geom="roundRect")
    b.add_text(1050000, 5610000, 10100000, 240000, "整体平均情感得分为 0.075885，说明样本总体上略偏看涨，但多空信息并存。", 1450, NAVY, bold=True, align="ctr")
    return b


def indicator_build_slide() -> SlideBuilder:
    b = SlideBuilder("综合情感指标构建", "03 情感指标与预测建模", 11)
    b.add_rect(700000, 1560000, 5100000, 3950000, fill=WHITE, line=LINE, geom="roundRect")
    b.add_rect(6380000, 1560000, 5100000, 3950000, fill=WHITE, line=LINE, geom="roundRect")
    b.add_text(980000, 1750000, 2300000, 240000, "指标定义", 1850, NAVY, bold=True)
    b.add_text(
        980000,
        2160000,
        4200000,
        2100000,
        "• News_t：同一自然周内全部新闻情感得分的平均值\n"
        "• VA_t：同一自然周内情感得分的方差，用于度量分歧度\n"
        "• 两者共同刻画市场在该周的方向和紧张程度\n"
        "• 相比单条新闻分值，更适合作为周度预测特征",
        1550,
        TEXT,
    )
    b.add_text(6660000, 1750000, 2300000, 240000, "周度对齐过程", 1850, NAVY, bold=True)
    b.add_text(
        6660000,
        2160000,
        4200000,
        2200000,
        "• 统一新闻日期与 WTI 价格日期格式，并清理时区信息\n"
        "• 将新闻情感结果按自然周聚合生成 News_t 和 VA_t\n"
        "• 将 WTI 价格按周计算均价 Price_t\n"
        "• 删除空值后合并为可直接建模的周度数据集",
        1550,
        TEXT,
    )
    b.add_rect(700000, 5750000, 10800000, 500000, fill=PALE_BLUE, line=PALE_BLUE, geom="roundRect")
    b.add_text(970000, 5860000, 10200000, 200000, "News_t 提供方向性信息，VA_t 提供分歧度信息，二者组合后更适合解释后续油价波动。", 1400, NAVY, bold=True, align="ctr")
    return b


def indicator_result_slide() -> SlideBuilder:
    return dual_image_slide(
        title="综合情感指标结果展示",
        section="03 情感指标与预测建模",
        page=12,
        left_title="News_t 与 WTI 走势",
        left_caption="情感均值与价格曲线在多个阶段呈现出形态相似性，说明新闻情绪具有一定解释力。",
        left_image=MEDIA_DIR / "image12.png",
        right_title="VA_t 与波动关系",
        right_caption="情感方差增大时，市场分歧上升，价格波动风险也会随之增强。",
        right_image=MEDIA_DIR / "image13.png",
        bottom_note="实证结果表明：情绪均值反映方向，情绪方差反映不确定性，两类信号对价格分析都很关键。",
    )


def feature_slide() -> SlideBuilder:
    b = SlideBuilder("建模数据生成与特征工程", "03 情感指标与预测建模", 13)
    for idx, (value, label) in enumerate([("K+1", "情绪特征"), ("5 周", "最佳滞后"), ("3D", "序列输入")]):
        x = 760000 + idx * 1820000
        b.add_rect(x, 1450000, 1520000, 760000, fill=PALE_BLUE, line=PALE_BLUE, geom="roundRect")
        b.add_text(x + 100000, 1530000, 1320000, 240000, value, 2000, NAVY, bold=True, align="ctr")
        b.add_text(x + 100000, 1860000, 1320000, 180000, label, 1250, MUTED, align="ctr")
    b.add_rect(700000, 2440000, 5100000, 3000000, fill=WHITE, line=LINE, geom="roundRect")
    b.add_rect(6380000, 2440000, 5100000, 3000000, fill=WHITE, line=LINE, geom="roundRect")
    b.add_text(960000, 2620000, 2200000, 240000, "特征库构建", 1850, NAVY, bold=True)
    b.add_text(
        960000,
        3040000,
        4300000,
        2200000,
        "• 按主题聚合 Energy、Alternative-Energy、Company-News 等周度情绪均值\n"
        "• 加入 News_Total 与 VA_Total 两个全局指标\n"
        "• 同时保留 Price_t 与 Volatility_Trend 作为辅助金融特征",
        1550,
        TEXT,
    )
    b.add_text(6640000, 2620000, 2200000, 240000, "滞后与序列重构", 1850, NAVY, bold=True)
    b.add_text(
        6640000,
        3040000,
        4300000,
        2200000,
        "• 利用 VAR 与 AIC 确定最佳滞后期为 5 周\n"
        "• 通过滑动窗口把二维特征库重组为三维张量\n"
        "• 模型输入形式为 (Samples, Time_steps, Features)",
        1550,
        TEXT,
    )
    b.add_rect(700000, 5670000, 10800000, 560000, fill=PALE_ORANGE, line=PALE_ORANGE, geom="roundRect")
    b.add_text(950000, 5790000, 10200000, 220000, "建模数据的关键不是“变量越多越好”，而是让情绪、价格和时间顺序保持一致。", 1450, NAVY, bold=True, align="ctr")
    return b


def model_slide() -> SlideBuilder:
    b = SlideBuilder("油价预测模型架构", "03 情感指标与预测建模", 14)
    b.add_rect(700000, 1540000, 5600000, 4200000, fill=WHITE, line=LINE, geom="roundRect")
    b.add_rect(6600000, 1540000, 4900000, 4200000, fill=WHITE, line=LINE, geom="roundRect")
    b.add_text(960000, 1730000, 2300000, 240000, "Attention-LSTM 拓扑", 1850, NAVY, bold=True)
    b.add_image(MEDIA_DIR / "image15.png", 920000, 2100000, 4800000, 3300000)
    b.add_text(6880000, 1730000, 2100000, 240000, "模型设计要点", 1850, NAVY, bold=True)
    b.add_text(
        6880000,
        2150000,
        4000000,
        2200000,
        "• LSTM 负责提取情绪与价格的时序依赖关系\n"
        "• Attention 为不同时间步分配不同权重\n"
        "• Dense 层将聚合后的上下文向量映射为预测价格\n"
        "• 训练阶段采用训练集、验证集、测试集三集划分",
        1550,
        TEXT,
    )
    b.add_rect(6880000, 4740000, 3800000, 700000, fill=PALE_BLUE, line=PALE_BLUE, geom="roundRect")
    b.add_text(7050000, 4870000, 3450000, 300000, "正则化策略：Dropout + BatchNorm，提高在非平稳市场上的泛化能力。", 1380, NAVY, bold=True, align="ctr")
    return b


def prediction_slide() -> SlideBuilder:
    b = SlideBuilder("超参数优化与预测结果分析", "03 情感指标与预测建模", 15)
    b.add_rect(680000, 1540000, 5150000, 3600000, fill=WHITE, line=LINE, geom="roundRect")
    b.add_rect(6380000, 1540000, 5150000, 3600000, fill=WHITE, line=LINE, geom="roundRect")
    b.add_text(920000, 1730000, 2100000, 240000, "自动寻优结果", 1800, NAVY, bold=True)
    b.add_text(6640000, 1730000, 2100000, 240000, "预测曲线对比", 1800, NAVY, bold=True)
    b.add_image(MEDIA_DIR / "image16.png", 920000, 2100000, 4550000, 2500000)
    b.add_image(MEDIA_DIR / "image17.png", 6580000, 2100000, 4550000, 2500000)
    b.add_text(930000, 4670000, 4500000, 350000, "采用 TimeSeriesSplit 进行滚动验证，最终选择 32 个隐藏单元和 0.3 的 dropout。", 1320, MUTED)
    b.add_text(6580000, 4670000, 4500000, 350000, "预测曲线能够较好跟随价格趋势变化，对关键拐点具有一定识别能力。", 1320, MUTED)
    metrics = [("DA", "57.14%", PALE_ORANGE), ("MAPE", "5.34%", PALE_BLUE), ("结论", "具备可用的方向判断能力", WHITE)]
    for idx, (label, value, fill) in enumerate(metrics):
        x = 900000 + idx * 3600000
        b.add_rect(x, 5550000, 3000000, 720000, fill=fill, line=LINE, geom="roundRect")
        b.add_text(x + 120000, 5660000, 800000, 200000, label, 1400, MUTED, bold=True)
        b.add_text(x + 850000, 5600000, 2000000, 250000, value, 1850, NAVY, bold=True)
    return b


def demo_slide() -> SlideBuilder:
    b = SlideBuilder("系统演示与功能模块", "04 系统演示与总结", 16)
    b.add_rect(700000, 1540000, 3800000, 4200000, fill=WHITE, line=LINE, geom="roundRect")
    b.add_rect(4700000, 1540000, 6800000, 4200000, fill=WHITE, line=LINE, geom="roundRect")
    b.add_text(970000, 1730000, 2200000, 240000, "前端集成模块", 1850, NAVY, bold=True)
    b.add_text(
        980000,
        2140000,
        3000000,
        2500000,
        "• 新闻摘要\n"
        "• 新闻分类\n"
        "• 新闻情感分析\n"
        "• 综合情感分析\n"
        "• 建模数据生成\n"
        "• 模型预测",
        1650,
        TEXT,
    )
    b.add_rect(980000, 4980000, 2700000, 620000, fill=PALE_ORANGE, line=PALE_ORANGE, geom="roundRect")
    b.add_text(1080000, 5110000, 2500000, 260000, "多页面界面让各模块既可独立运行，也能顺序联动。", 1400, NAVY, bold=True, align="ctr")
    b.add_text(5000000, 1730000, 2300000, 240000, "界面展示", 1850, NAVY, bold=True)
    b.add_image(MEDIA_DIR / "image18.png", 5000000, 2100000, 6200000, 3150000)
    b.add_rect(700000, 5940000, 10800000, 500000, fill=PALE_BLUE, line=PALE_BLUE, geom="roundRect")
    b.add_text(980000, 6040000, 10200000, 200000, "演示路径：选择输入文件 -> 运行模块 -> 生成结果文件/图表 -> 查看预测输出。", 1420, NAVY, bold=True, align="ctr")
    return b


def final_slide() -> SlideBuilder:
    b = SlideBuilder("测试结果、项目成果与改进方向", "04 系统演示与总结", 17)
    cards = [
        ("测试与调试", "• 重点解决编码、日期解析、输出路径和模型兼容问题\n• 保证各模块文件字段衔接和图表生成稳定性"),
        ("项目成果", "• 完成从新闻采集到情感指标再到油价预测的全流程系统\n• 提供可展示、可复现、可扩展的分析框架"),
        ("后续改进", "• 引入预训练语言模型提升语义理解能力\n• 纳入库存、美元指数、利率等外生变量\n• 优化前端展示与交互体验"),
    ]
    xs = [700000, 4080000, 7460000]
    for (title, body), x in zip(cards, xs, strict=False):
        b.add_rect(x, 1700000, 3150000, 3450000, fill=WHITE, line=LINE, geom="roundRect")
        b.add_text(x + 220000, 1900000, 2200000, 240000, title, 1800, NAVY, bold=True)
        b.add_text(x + 220000, 2320000, 2700000, 2200000, body, 1500, TEXT)
    b.add_rect(700000, 5500000, 10600000, 740000, fill=NAVY, line=NAVY, geom="roundRect")
    b.add_text(920000, 5640000, 10100000, 240000, "结论：OilPrice 新闻中的情绪信息可以被有效量化，并为国际油价分析与预测提供有价值的辅助信号。", 1480, WHITE, bold=True, align="ctr")
    return b


SLIDE_BUILDERS = [
    cover_slide,
    agenda_slide,
    background_goal_slide,
    workflow_slide,
    data_preprocess_slide,
    lambda: dual_image_slide(
        title="数据采集与预处理结果",
        section="02 数据收集与分类",
        page=6,
        left_title="新闻采集结果",
        left_caption="爬虫能够稳定抓取多栏目能源新闻，并保留标题、日期、正文和链接等关键字段。",
        left_image=MEDIA_DIR / "image2.png",
        right_title="文本预处理结果",
        right_caption="清洗、分词和词干提取后，文本变为可直接用于分类与情感分析的标准化语料。",
        right_image=MEDIA_DIR / "image3.png",
        bottom_note="这一阶段完成了从原始网页文本到规范语料的转换，是后续分析模块的输入基础。",
    ),
    summary_slide,
    classification_slide,
    sentiment_design_slide,
    sentiment_score_slide,
    indicator_build_slide,
    indicator_result_slide,
    feature_slide,
    model_slide,
    prediction_slide,
    demo_slide,
    final_slide,
]


def update_presentation(entries: dict[str, bytes], slide_count: int) -> None:
    pres = ET.fromstring(entries["ppt/presentation.xml"])
    sld_id_lst = pres.find(f".//{qn('p', 'sldIdLst')}")
    if sld_id_lst is not None:
        for child in list(sld_id_lst):
            sld_id_lst.remove(child)
        for idx in range(slide_count):
            ET.SubElement(
                sld_id_lst,
                qn("p", "sldId"),
                {
                    "id": str(256 + idx),
                    qn("r", "id"): f"rId{idx + 3}",
                },
            )
    entries["ppt/presentation.xml"] = ET.tostring(pres, encoding="utf-8", xml_declaration=True)

    app_root = ET.fromstring(entries["docProps/app.xml"])
    slides_node = app_root.find("app:Slides", {"app": NS["app"]})
    if slides_node is not None:
        slides_node.text = str(slide_count)
    heading_pairs = app_root.find(".//vt:vector", {"vt": NS["vt"]})
    if heading_pairs is not None:
        values = heading_pairs.findall("vt:variant/vt:i4", {"vt": NS["vt"]})
        if values:
            values[-1].text = str(slide_count)
    entries["docProps/app.xml"] = ET.tostring(app_root, encoding="utf-8", xml_declaration=True)


def build_custom_presentation() -> Path:
    safe_copy_existing_output()
    with zipfile.ZipFile(BASE_PPT_PATH, "r") as src:
        entries = {name: src.read(name) for name in src.namelist()}

    media_written: dict[Path, str] = {}

    for idx, factory in enumerate(SLIDE_BUILDERS, start=1):
        slide = factory()
        slide_path = f"ppt/slides/slide{idx}.xml"
        rel_path = f"ppt/slides/_rels/slide{idx}.xml.rels"
        entries[slide_path] = slide.slide_xml()
        entries[rel_path] = slide.rels_xml()
        for media_name, media_path in slide.media:
            if media_path not in media_written:
                media_written[media_path] = media_name
                entries[f"ppt/media/{media_name}"] = media_path.read_bytes()
            else:
                current_xml = entries[rel_path].decode("utf-8")
                entries[rel_path] = current_xml.replace(media_name, media_written[media_path]).encode("utf-8")

    update_presentation(entries, len(SLIDE_BUILDERS))

    OUTPUT_PPT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(OUTPUT_PPT_PATH, "w", compression=zipfile.ZIP_DEFLATED) as out:
        for name, data in entries.items():
            out.writestr(name, data)
    return OUTPUT_PPT_PATH


if __name__ == "__main__":
    print(build_custom_presentation())
