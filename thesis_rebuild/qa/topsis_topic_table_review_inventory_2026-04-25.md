# TOPSIS与主题分析表格审查清单

更新时间：2026-04-25

## 一、字段中文化核对结论

### 1. TOPSIS链条

- `thesis_rebuild\institution_eval` 主结果目录中，以下表仍是英文或英中混合表头，不适合作为论文终稿直接审查版：
  - `institution_core_top100_dual_key_2011_2025.csv`
  - `institution_impact_indicator_top100_dual_key_2011_2025.csv`
  - `institution_topsis_indicator_selection_top100_dual_key_2011_2025.csv`
  - `institution_topsis_score_top100_dual_key_2011_2025.csv`
  - `institution_weight_scheme_top100_dual_key_2011_2025.csv`
- `thesis_rebuild\delivery_zh\institution_eval` 目录中，以上表均已有中文交付版，适合本轮人工审查。
- `institution_dimension_top10_dual_key_2011_2025.csv`、`institution_topsis_top20_dual_key_2011_2025.csv`、`institution_type_review_top100_dual_key_2011_2025.csv` 主结果版本身已基本中文化，仅保留必要术语缩写，如 `TOPSIS`、`H指数`。

### 2. 主题分析链条

- `thesis_rebuild\topic_evolution_dual_key_2011_2025` 目录中的正式结果表整体已经中文化。
- 仅保留少量专业缩写或固定标识：
  - `DOI`
  - `TOPSIS`
- 这些缩写属于论文和数据处理中常见固定术语，一般不建议强行再译。

### 3. 本轮建议审查口径

- TOPSIS相关表：优先审查 `delivery_zh\institution_eval` 下的中文交付版。
- 主题分析相关表：直接审查 `topic_evolution_dual_key_2011_2025` 下的正式结果表。
- 如你确认最终论文附表和交付包都以中文字段为准，后续我们再决定是否要把 `institution_eval` 主结果目录里的英文表头也统一改成中文，并同步调整脚本。

## 二、TOPSIS相关表格

### 1. 核心机构Top100入选表

- 内容：按标准化机构出现频次筛出的核心机构Top100，是后续指标计算与TOPSIS评价的样本入口表。
- 审查版路径：`D:\graduate\thesis_rebuild\delivery_zh\institution_eval\institution_core_top100_dual_key_2011_2025_zh.csv`
- 主结果版路径：`D:\graduate\thesis_rebuild\institution_eval\institution_core_top100_dual_key_2011_2025.csv`
- 行数：100
- 审查版表头：`Top100入选排名`、`标准化机构名称`、`机构出现总次数`、`CNKI机构出现次数`、`WOS机构出现次数`、`CSCD机构出现次数`
- 主结果版状态：英文表头，尚未中文化

### 2. Top100机构综合指标总表

- 内容：Top100机构的科研产出、学术影响、合作与国际化相关基础指标与衍生指标总表，是TOPSIS评分的核心输入表。
- 审查版路径：`D:\graduate\thesis_rebuild\delivery_zh\institution_eval\institution_impact_indicator_top100_dual_key_2011_2025_zh.csv`
- 主结果版路径：`D:\graduate\thesis_rebuild\institution_eval\institution_impact_indicator_top100_dual_key_2011_2025.csv`
- 行数：100
- 审查版表头：`Top100入选排名`、`标准化机构名称`、`机构出现总次数`、`CNKI机构出现次数`、`WOS机构出现次数`、`CSCD机构出现次数`、`去重论文总数`、`CNKI去重论文数`、`WOS去重论文数`、`CSCD去重论文数`、`首次发文年份`、`最近发文年份`、`活跃年份数`、`年均发文量`、`2011-2015发文量`、`2016-2020发文量`、`2021-2025发文量`、`近五年发文占比`、`总被引频次`、`篇均被引频次`、`篇均被引中位数`、`单篇最高被引频次`、`H指数`、`有被引论文数`、`有被引论文占比`、`未被引论文数`、`未被引论文占比`、`高被引论文数`、`高被引论文占比`、`合作论文数`、`合作论文占比`、`合作机构数`、`单篇合作论文平均合作机构数`、`主属国家/地区`、`国际合作论文数`、`国际合作论文占比`、`国际合作机构数`、`合作国家/地区数`
- 主结果版状态：38个字段全部为英文表头

### 3. TOPSIS候选指标筛选表

- 内容：记录进入TOPSIS前的候选指标、所属维度、指标方向以及是否纳入的筛选说明。
- 审查版路径：`D:\graduate\thesis_rebuild\delivery_zh\institution_eval\institution_topsis_indicator_selection_top100_dual_key_2011_2025_zh.csv`
- 主结果版路径：`D:\graduate\thesis_rebuild\institution_eval\institution_topsis_indicator_selection_top100_dual_key_2011_2025.csv`
- 行数：15
- 审查版表头：`指标代码`、`指标名称`、`维度代码`、`指标维度`、`指标方向`、`是否纳入TOPSIS`、`筛选说明`
- 主结果版状态：英文表头，尚未中文化

### 4. TOPSIS组合权重表

- 内容：记录最终纳入TOPSIS的7项指标及其先验权重、熵权法权重、CRITIC法权重和组合权重。
- 审查版路径：`D:\graduate\thesis_rebuild\delivery_zh\institution_eval\institution_weight_scheme_top100_dual_key_2011_2025_zh.csv`
- 主结果版路径：`D:\graduate\thesis_rebuild\institution_eval\institution_weight_scheme_top100_dual_key_2011_2025.csv`
- 行数：7
- 审查版表头：`指标代码`、`指标名称`、`维度代码`、`指标维度`、`指标方向`、`先验权重`、`熵权法权重`、`CRITIC法权重`、`组合权重`
- 主结果版状态：英文表头，尚未中文化

### 5. Top100机构TOPSIS得分总表

- 内容：给出Top100机构的综合得分、综合排名、三大维度子得分及7项核心指标值。
- 审查版路径：`D:\graduate\thesis_rebuild\delivery_zh\institution_eval\institution_topsis_score_top100_dual_key_2011_2025_zh.csv`
- 主结果版路径：`D:\graduate\thesis_rebuild\institution_eval\institution_topsis_score_top100_dual_key_2011_2025.csv`
- 行数：100
- 审查版表头：`Top100入选排名`、`标准化机构名称`、`TOPSIS综合得分`、`TOPSIS综合排名`、`科研产出子得分`、`学术影响子得分`、`合作与国际化子得分`、`去重论文总数`、`近五年发文占比`、`H指数`、`高被引论文占比`、`合作论文占比`、`国际合作论文占比`、`合作国家/地区数`
- 主结果版状态：英文表头，尚未中文化

### 6. TOPSIS前20机构摘要表

- 内容：用于论文正文直接引用的Top20摘要表，展示综合排名、国家地区、子得分和若干代表性指标。
- 审查版路径：`D:\graduate\thesis_rebuild\delivery_zh\institution_eval\institution_topsis_top20_dual_key_2011_2025_zh.csv`
- 主结果版路径：`D:\graduate\thesis_rebuild\institution_eval\institution_topsis_top20_dual_key_2011_2025.csv`
- 行数：20
- 当前表头：`TOPSIS综合排名`、`标准化机构名称`、`主属国家/地区`、`TOPSIS综合得分`、`科研产出子得分`、`学术影响子得分`、`合作与国际化子得分`、`主导优势维度`、`去重论文总数`、`H指数`、`高被引论文占比`、`国际合作论文占比`
- 字段状态：已中文化，保留 `TOPSIS` 与 `H指数`

### 7. 综合与分维度Top10表

- 内容：按综合排名、科研产出维度、学术影响维度、合作与国际化维度分别提取Top10，用于分维度比较。
- 审查版路径：`D:\graduate\thesis_rebuild\delivery_zh\institution_eval\institution_dimension_top10_dual_key_2011_2025_zh.csv`
- 主结果版路径：`D:\graduate\thesis_rebuild\institution_eval\institution_dimension_top10_dual_key_2011_2025.csv`
- 行数：40
- 当前表头：`榜单类型`、`榜内排名`、`标准化机构名称`、`主属国家/地区`、`TOPSIS综合排名`、`得分指标`、`得分值`、`备注`
- 字段状态：已中文化，保留 `TOPSIS`

### 8. Top100机构类型与层级复核表

- 内容：给出Top100机构的机构类型、机构层级、分类依据及是否建议人工复核，是机构画像比较的基础表。
- 审查版路径：`D:\graduate\thesis_rebuild\delivery_zh\institution_eval\institution_type_review_top100_dual_key_2011_2025_zh.csv`
- 主结果版路径：`D:\graduate\thesis_rebuild\institution_eval\institution_type_review_top100_dual_key_2011_2025.csv`
- 行数：100
- 当前表头：`TOPSIS综合排名`、`标准化机构名称`、`TOPSIS综合得分`、`机构层级`、`机构类型`、`分类依据`、`是否建议人工复核`
- 字段状态：已中文化，保留 `TOPSIS`

## 三、主题分析相关表格

### 1. 三阶段预处理统计表

- 内容：记录三个阶段原始文献量、有摘要文献量、通过领域过滤文献量、进入建模文献量等预处理结果。
- 路径：`D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_preprocess_stats_dual_key_2011_2025.csv`
- 行数：3
- 当前表头：`阶段`、`年份范围`、`阶段文献数`、`有摘要文献数`、`英文摘要文献数`、`中文摘要文献数`、`通过领域过滤文献数`、`进入建模文献数`、`平均有效词数`、`词汇表规模`
- 字段状态：已中文化

### 2. 主题数选择结果表

- 内容：记录不同候选主题数下的困惑度、一致性得分和综合排序，用于说明为什么三个阶段最终都选取11个主题。
- 路径：`D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_model_selection_dual_key_2011_2025.csv`
- 行数：21
- 当前表头：`阶段`、`候选主题数`、`样本文献数`、`词汇表规模`、`困惑度`、`一致性得分`、`困惑度排名`、`一致性排名`、`综合排序值`、`是否选中`
- 字段状态：已中文化

### 3. 主题关键词分布表

- 内容：记录每个阶段、每个主题下的关键词概率分布，是主题命名和结果解释的重要依据。
- 路径：`D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_keyword_distribution_dual_key_2011_2025.csv`
- 行数：396
- 当前表头：`阶段`、`主题编号`、`主题标签`、`主题强度`、`平均主题概率`、`关键词排名`、`关键词`、`词项概率`
- 字段状态：已中文化

### 4. 主题强度表

- 内容：记录每个阶段各主题的主题文献数、阶段文献总数、主题强度和平均主题概率，用于识别阶段性热点。
- 路径：`D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_strength_dual_key_2011_2025.csv`
- 行数：33
- 当前表头：`阶段`、`主题编号`、`主题标签`、`主题文献数`、`阶段文献总数`、`主题强度`、`平均主题概率`、`代表关键词`
- 字段状态：已中文化

### 5. 主题相似度连接表

- 内容：记录相邻阶段主题之间的相似度及是否纳入演化路径，用于构造主题演化链。
- 路径：`D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_similarity_links_dual_key_2011_2025.csv`
- 行数：22
- 当前表头：`左阶段`、`左主题编号`、`右阶段`、`右主题编号`、`主题相似度`、`纳入演化路径`
- 字段状态：已中文化

### 6. 主题演化路径表

- 内容：汇总最终保留下来的主题演化路径，是论文演化路径分析的核心表。
- 路径：`D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_evolution_paths_dual_key_2011_2025.csv`
- 行数：14
- 当前表头：`演化路径编号`、`2011-2015主题编号`、`2011-2015主题标签`、`2011-2015主题强度`、`2016-2020主题编号`、`2016-2020主题标签`、`2016-2020主题强度`、`2021-2025主题编号`、`2021-2025主题标签`、`2021-2025主题强度`
- 字段状态：已中文化

### 7. 文献主导主题归属表

- 内容：将每篇建模文献分配到主导主题，是类型比较和层级比较的底层明细表。
- 路径：`D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_document_assignment_dual_key_2011_2025.csv`
- 行数：103627
- 当前表头：`阶段`、`文献签名`、`年份`、`来源库`、`DOI`、`标题`、`第一作者`、`主导主题编号`、`主导主题标签`、`主导主题概率`
- 字段状态：已中文化，保留 `DOI`

### 8. 不同机构类型主题分布表

- 内容：比较高校/科研院所、企业研发中心、政府机构在各阶段各主题上的文献占比分布。
- 路径：`D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_distribution_by_institution_type_dual_key_2011_2025.csv`
- 行数：87
- 当前表头：`阶段`、`机构类型`、`主题编号`、`主题标签`、`主题论文数`、`机构类型文献总数`、`主题占比`
- 字段状态：已中文化

### 9. 不同机构层级主题分布表

- 内容：比较头部引领型、中坚创新型、特色细分型机构在各阶段各主题上的文献占比分布。
- 路径：`D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_distribution_by_institution_level_dual_key_2011_2025.csv`
- 行数：99
- 当前表头：`阶段`、`机构层级`、`主题编号`、`主题标签`、`主题论文数`、`机构层级文献总数`、`主题占比`
- 字段状态：已中文化

### 10. 机构主题画像分类表

- 内容：将标准化机构与机构类型、关联文献数、是否Top100、TOPSIS排名、机构层级等信息关联起来，是机构画像的总表。
- 路径：`D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\institution_profile_classification_dual_key_2011_2025.csv`
- 行数：7882
- 当前表头：`标准化机构名称`、`机构类型`、`分类依据`、`关联文献数`、`涉及阶段数`、`是否Top100`、`TOPSIS综合排名`、`TOPSIS综合得分`、`机构层级`
- 字段状态：已中文化，保留 `Top100` 与 `TOPSIS`

### 11. 高频词统计表

- 内容：记录各阶段高频词及其总词频、文献频次，用于辅助主题解释和词项清洗检查。
- 路径：`D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_high_frequency_terms_dual_key_2011_2025.csv`
- 行数：300
- 当前表头：`阶段`、`词项`、`总词频`、`文献频次`
- 字段状态：已中文化

## 四、配套但非表格的结果文件

以下文件不是表格，但论文写作和结果核对时会一起用到：

- 主题演化摘要：`D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_evolution_summary_dual_key_2011_2025.txt`
- 机构画像摘要：`D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_institution_profile_summary_dual_key_2011_2025.txt`
- 主题强度演化图：`D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_intensity_evolution_curve_dual_key_2011_2025.png`
- 机构类型主题热力图：`D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_distribution_heatmap_by_institution_type_dual_key_2011_2025.png`
- 机构层级主题热力图：`D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_distribution_heatmap_by_institution_level_dual_key_2011_2025.png`

## 五、我建议你本轮优先审查的文件

### 1. TOPSIS终稿候选表

- `D:\graduate\thesis_rebuild\delivery_zh\institution_eval\institution_core_top100_dual_key_2011_2025_zh.csv`
- `D:\graduate\thesis_rebuild\delivery_zh\institution_eval\institution_impact_indicator_top100_dual_key_2011_2025_zh.csv`
- `D:\graduate\thesis_rebuild\delivery_zh\institution_eval\institution_topsis_indicator_selection_top100_dual_key_2011_2025_zh.csv`
- `D:\graduate\thesis_rebuild\delivery_zh\institution_eval\institution_weight_scheme_top100_dual_key_2011_2025_zh.csv`
- `D:\graduate\thesis_rebuild\delivery_zh\institution_eval\institution_topsis_score_top100_dual_key_2011_2025_zh.csv`
- `D:\graduate\thesis_rebuild\delivery_zh\institution_eval\institution_topsis_top20_dual_key_2011_2025_zh.csv`
- `D:\graduate\thesis_rebuild\delivery_zh\institution_eval\institution_dimension_top10_dual_key_2011_2025_zh.csv`
- `D:\graduate\thesis_rebuild\delivery_zh\institution_eval\institution_type_review_top100_dual_key_2011_2025_zh.csv`

### 2. 主题分析终稿候选表

- `D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_preprocess_stats_dual_key_2011_2025.csv`
- `D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_model_selection_dual_key_2011_2025.csv`
- `D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_keyword_distribution_dual_key_2011_2025.csv`
- `D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_strength_dual_key_2011_2025.csv`
- `D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_similarity_links_dual_key_2011_2025.csv`
- `D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_evolution_paths_dual_key_2011_2025.csv`
- `D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_document_assignment_dual_key_2011_2025.csv`
- `D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_distribution_by_institution_type_dual_key_2011_2025.csv`
- `D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_distribution_by_institution_level_dual_key_2011_2025.csv`
- `D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\institution_profile_classification_dual_key_2011_2025.csv`
- `D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025\topic_high_frequency_terms_dual_key_2011_2025.csv`

