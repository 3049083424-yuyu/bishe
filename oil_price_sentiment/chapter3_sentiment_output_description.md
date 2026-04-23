# 第三章情感得分结果文件说明

## 1. 情感得分的定义口径

本研究第三章采用基于情感词典的词典匹配方法，对 OilPrice 新闻数据进行情感得分计算。需要说明的是，本文中的“情感”并非通用意义上的正面或负面情绪，而是面向国际油价预测任务定义的市场情绪方向：

- `bullish` 表示新闻内容更可能对应国际油价上行压力，即看涨情绪；
- `bearish` 表示新闻内容更可能对应国际油价下行压力，即看跌情绪；
- `neutral` 表示新闻中多空信息相对均衡，或价格方向不明显。

因此，第三章得到的情感得分本质上是“油价方向情绪得分”，可作为第四章周度新闻情绪指标构建的基础。

## 2. `news_sentiment.csv` 字段说明

[`news_sentiment.csv`](/D:/graduate/oil_price_sentiment/news_sentiment.csv) 为第三章的核心输出文件，用于保存每篇新闻的情感得分计算结果。字段含义如表所示。

| 字段名 | 字段解释 | 说明 |
|---|---|---|
| `news_id` | 新闻编号 | 按原始数据顺序自增生成的唯一编号。 |
| `topic` | 一级主题 | 原始数据中的主题字段，例如 `Energy`。 |
| `subtitle` | 二级栏目 | 原始数据中的子栏目字段，例如 `Oil-Prices`、`Crude-Oil`。 |
| `title` | 新闻标题 | 原始新闻标题文本。 |
| `date` | 发布时间 | 原始数据中的发布时间字符串。 |
| `token_count` | 正文分词数 | 原始 `tokens` 字段中正文分词后的词数。 |
| `title_token_count` | 标题分词数 | 对新闻标题重新提取英文词后得到的词数。 |
| `bullish_hits` | 看涨词命中次数 | 标题和正文中命中的看涨词总次数。 |
| `bearish_hits` | 看跌词命中次数 | 标题和正文中命中的看跌词总次数。 |
| `bullish_mass` | 看涨证据强度 | 看涨词的加权强度之和，考虑了标题权重、否定词和程度副词。 |
| `bearish_mass` | 看跌证据强度 | 看跌词的加权强度之和，考虑了标题权重、否定词和程度副词。 |
| `evidence_mass` | 总证据强度 | 看涨证据强度与看跌证据强度之和，即 `bullish_mass + bearish_mass`。 |
| `raw_score` | 原始情感得分 | 看涨强度减去看跌强度后的原始得分。 |
| `sentiment_score` | 归一化情感得分 | 对原始得分进行归一化后的结果，越接近 `1` 表示越看涨，越接近 `-1` 表示越看跌。 |
| `sentiment_label` | 情感标签 | 根据 `sentiment_score` 划分为 `bullish`、`neutral` 或 `bearish`。 |

## 3. 情感得分的解释说明

为避免单个情感词对新闻整体方向造成过度放大，本文在计算归一化情感得分时引入平滑项。最终得到的 `sentiment_score` 可理解为单篇新闻的油价方向情绪强弱指标，其解释规则如下：

- 当 `sentiment_score` 越接近 `1` 时，说明新闻所包含的看涨证据越强；
- 当 `sentiment_score` 越接近 `-1` 时，说明新闻所包含的看跌证据越强；
- 当 `sentiment_score` 接近 `0` 时，说明新闻中的多空信号相对平衡，或价格方向不明确。

在标签划分上，本文采用如下规则：

- 当 `sentiment_score >= 0.10` 时，记为 `bullish`；
- 当 `sentiment_score <= -0.10` 时，记为 `bearish`；
- 其余情况记为 `neutral`。

## 4. 第三章输出文件说明

第三章情感得分计算部分共输出以下结果文件。

| 文件名 | 文件含义 | 主要用途 |
|---|---|---|
| [`news_sentiment.csv`](/D:/graduate/oil_price_sentiment/news_sentiment.csv) | 每篇新闻的情感得分结果表 | 作为第三章核心输出，并为第四章按周聚合新闻情绪指标提供输入。 |
| [`expanded_sentiment_lexicon.csv`](/D:/graduate/oil_price_sentiment/expanded_sentiment_lexicon.csv) | 扩充后情感词典的明细表 | 记录扩充词项、情感极性、语料出现频次、类别及扩充依据。 |
| [`expanded_positive_words.txt`](/D:/graduate/oil_price_sentiment/expanded_positive_words.txt) | 扩充后的看涨词表 | 用于情感得分脚本读取和匹配。 |
| [`expanded_negative_words.txt`](/D:/graduate/oil_price_sentiment/expanded_negative_words.txt) | 扩充后的看跌词表 | 用于情感得分脚本读取和匹配。 |
| [`lexicon_expansion_summary.txt`](/D:/graduate/oil_price_sentiment/lexicon_expansion_summary.txt) | 情感词典扩充汇总说明 | 汇总基础词典规模、扩充词数量、重分类词数量和处理语料规模。 |
| [`sentiment_summary.txt`](/D:/graduate/oil_price_sentiment/sentiment_summary.txt) | 情感得分计算汇总说明 | 汇总新闻总量、平均情感得分、情感标签分布及高频命中词。 |

## 5. 第三章脚本文件说明

为实现上述结果，本文编写了以下脚本文件。

| 文件名 | 文件含义 | 功能说明 |
|---|---|---|
| [`expand_lexicon.py`](/D:/graduate/oil_biblio_pipeline/expand_lexicon.py) | 情感词典扩充脚本 | 基于已有正负词典，并结合油价新闻语料中的高频领域词，对词典进行扩充和极性调整。 |
| [`sentiment_analysis.py`](/D:/graduate/oil_biblio_pipeline/sentiment_analysis.py) | 情感得分计算脚本 | 基于扩充后的领域词典，对每篇新闻计算看涨、看跌及中性情绪得分。 |

## 6. 可直接用于论文的表述示例

可在正文中使用如下表述：

> 本文在完成新闻清洗与分词处理后，采用基于情感词典的匹配方法对 OilPrice 新闻进行情感得分计算。考虑到本文研究目标是国际油价预测，因此在情感定义上不采用一般意义上的正负面情绪，而是将情感方向界定为看涨、看跌和中性三类。其中，看涨情绪表示新闻内容更可能对应国际油价上行压力，看跌情绪表示新闻内容更可能对应国际油价下行压力，中性情绪表示方向不显著。最终，本文生成 `news_sentiment.csv` 文件，为后续周度情感指标构建提供基础数据支撑。

如需在附录中说明结果文件，可使用如下表述：

> 第三章情感得分计算部分共输出 1 个文章级情感得分结果表、1 个扩充词典明细表、2 个扩充后词典文本文件，以及 2 个过程汇总说明文件。其中，`news_sentiment.csv` 保存每篇新闻的情感得分及其标签，是后续周度聚合分析的直接输入。
