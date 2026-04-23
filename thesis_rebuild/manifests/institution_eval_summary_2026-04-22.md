# 机构评价阶段总结（2026-04-22）

## 一、本轮已完成工作

1. 收紧了重建版机构名称翻译与标准化规则，核心包括两条：
   - 对 Top100 敏感的高频机构别名补充人工映射；
   - 对会把不同机构错误压缩成“大学”“技术大学”这类伪标准名的自动翻译结果，实行回退保护，不再直接用于聚合。
2. 重新生成了机构翻译与标准化结果表及复核队列。
3. 生成了机构标准名频次表、核心 Top100 机构表、影响力指标表、TOPSIS 指标筛选表、权重表、得分表、Top20 结果表、分维度 Top10 表、机构类型复核表，以及论文结果写作草稿。

## 二、关键文件

- 机构标准化重建脚本：
  - `D:\graduate\oil_biblio_pipeline\thesis_rebuild_pipeline\build_institution_trans_norm_rebuild.py`
- 机构类型复核重建脚本：
  - `D:\graduate\oil_biblio_pipeline\thesis_rebuild_pipeline\export_institution_type_review_rebuild.py`

- 机构翻译与标准化结果表：
  - `D:\graduate\thesis_rebuild\institution_eval\institution_name_table_dual_key_trans_norm_2011_2025.csv`
- 机构标准化复核队列：
  - `D:\graduate\thesis_rebuild\qa\institution_trans_norm_review_dual_key_2011_2025.csv`
- 机构标准名频次表：
  - `D:\graduate\thesis_rebuild\institution_eval\institution_name_table_dual_key_norm_freq_2011_2025.csv`
- 核心 Top100 机构表：
  - `D:\graduate\thesis_rebuild\institution_eval\institution_core_top100_dual_key_2011_2025.csv`
- 机构影响力指标表：
  - `D:\graduate\thesis_rebuild\institution_eval\institution_impact_indicator_top100_dual_key_2011_2025.csv`
- TOPSIS 指标筛选表：
  - `D:\graduate\thesis_rebuild\institution_eval\institution_topsis_indicator_selection_top100_dual_key_2011_2025.csv`
- TOPSIS 权重表：
  - `D:\graduate\thesis_rebuild\institution_eval\institution_weight_scheme_top100_dual_key_2011_2025.csv`
- TOPSIS 得分表：
  - `D:\graduate\thesis_rebuild\institution_eval\institution_topsis_score_top100_dual_key_2011_2025.csv`
- TOPSIS Top20 结果表：
  - `D:\graduate\thesis_rebuild\institution_eval\institution_topsis_top20_dual_key_2011_2025.csv`
- TOPSIS 分维度 Top10 结果表：
  - `D:\graduate\thesis_rebuild\institution_eval\institution_dimension_top10_dual_key_2011_2025.csv`
- 机构类型复核表：
  - `D:\graduate\thesis_rebuild\institution_eval\institution_type_review_top100_dual_key_2011_2025.csv`
- 机构评价结果写作草稿：
  - `D:\graduate\thesis_rebuild\manuscript\results_topsis_institutions_draft_2011_2025.md`

## 三、机构标准化口径说明

- 不再允许“大学”“技术大学”这类泛化伪标准名主导聚合结果。
- 对于 `中国石油大学` 这类父级机构名，如果原始来源无法可靠区分北京校区或华东校区，则保留父级名称，不强行并入某一校区。
- 这一处理比把模糊来源一律强制压到 `中国石油大学（北京）` 或 `中国石油大学（华东）` 更稳妥，也更适合在论文方法部分进行辩护。

## 四、当前统计结果

- 机构原始记录数：`93,101`
- 标准名频次表记录数：`87,320`
- 空标准名跳过数：`150`

## 五、Top100 结果核查

- 当前 Top100 候选列表中已不再出现“大学”“技术大学”这类伪标准名。
- 当前 Top100 前 100 名全部为中文标准化机构名称。
- 与历史 Top100 复核表相比：
  - 重合数：`89 / 100`
  - 仅现版出现：`11`
  - 仅历史版出现：`11`

仅现版出现的机构：
- `东北大学`
- `东华大学`
- `中国石油化工股份有限公司`
- `中国石油化工股份有限公司石油勘探开发研究院`
- `中国石油大学`
- `北京工业大学`
- `埃因霍温理工大学`
- `根特大学`
- `河南理工大学`
- `科廷大学`
- `西北工业大学`

仅历史版出现的机构：
- `上海大学`
- `东北大学（美国）`
- `中国石化胜利油田分公司`
- `中国石油化工集团有限公司`
- `中国石油天然气集团有限公司`
- `中国矿业大学（北京）`
- `中海石油（中国）有限公司`
- `挪威科技大学`
- `新南威尔士大学`
- `西安大学`
- `青岛大学`

解释：
- 这部分差异视为“语料口径与标准化口径变化”带来的结果差异，不视为本次重建失败。
- 当前不再追求与历史放宽版名单或条数完全一致。

## 六、TOPSIS 结果摘要

按 TOPSIS 综合得分排名的前 10 家机构为：

1. `中国科学院`
2. `中国石油大学（北京）`
3. `中国石油勘探开发研究院`
4. `西南石油大学`
5. `中国科学院大学`
6. `清华大学`
7. `中国石油大学（华东）`
8. `天津大学`
9. `浙江大学`
10. `新加坡国立大学`

当前 7 个入模指标的组合权重为：

- `distinct_paper_count`：`0.27953`
- `recent_paper_ratio_2021_2025`：`0.139125`
- `h_index`：`0.104893`
- `high_cited_paper_ratio`：`0.124701`
- `collaboration_paper_ratio`：`0.105386`
- `international_collaboration_paper_ratio`：`0.150946`
- `partner_country_region_count`：`0.095419`

## 七、机构类型复核结果

- `高校/科研院所`：`93`
- `企业研发中心`：`6`
- `政府机构`：`1`

当前已将 `中国教育部` 归为 `政府机构`，分类依据为“政府机构特例：教育部”，不再保留为待人工复核项。

## 八、当前结论

- 机构评价主链已经可以视为本轮重建中的正式结果。
- 当前最适合人工继续介入的环节，不是 Top100 主链，而是高频但仍未完全规范化的长尾机构映射，以及后续主题链中重新引入的英文机构子单元问题。
- 机构评价阶段的正式结果可直接服务于论文中的“核心机构识别”“影响力评价”“TOPSIS 排序结果”三部分写作。
