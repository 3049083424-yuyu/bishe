# 主题分析链运行总结（2026-04-22）

## 一、环境说明

- 主题分析链不要使用 `C:\Python314\python.exe`。
- 主题分析链应使用 `D:\anaconda3\python.exe`。
- 已确认该环境中可用的关键依赖包括：
  - `jieba`
  - `numpy`
  - `scipy`
  - `sklearn`
  - `matplotlib`

## 二、执行时的重要注意事项

1. `D:\graduate\oil_biblio_pipeline\topic_evolution_pipeline.py` 文件本身没有 `if __name__ == "__main__": main()` 入口。
   - 直接执行脚本文件时，会正常退出，但不会生成任何输出文件。
   - 正确做法是导入模块后显式调用 `main()`。
2. 该脚本内部将 LDA 的 `n_jobs` 固定写为 `-1`。
   - 在当前 Windows 沙箱环境下，这会触发 `PermissionError: [WinError 5] 拒绝访问`。
   - 已验证可行的解决方式是：
     - 使用 `D:\anaconda3\python.exe`；
     - 在运行时把 `LatentDirichletAllocation` 强制改成 `n_jobs=1`；
     - 同时将 `JOBLIB_TEMP_FOLDER` 指到 `D:\graduate\joblib_tmp`。

## 三、本轮实际采用的运行方式

### 1. 主题演化链

- 通过导入 `topic_evolution_pipeline` 模块执行；
- 运行前显式设置 `sys.argv` 到当前重建版输入输出路径；
- 在运行时将 `LatentDirichletAllocation(..., n_jobs=1)` 单进程化；
- 最终调用 `main()` 完成主题演化建模。

### 2. 机构主题画像链

- 使用脚本：
  - `D:\graduate\oil_biblio_pipeline\build_topic_institution_profile_analysis.py`
- 显式传入当前重建版路径；
- 将
  - `institution_type_review_top100_dual_key_2011_2025.csv`
  同时作为 `--topsis` 和 `--review` 输入，以保证机构类型与机构层级口径一致。

## 四、主题演化输出结果

输出目录：

- `D:\graduate\thesis_rebuild\topic_evolution_dual_key_2011_2025`

主要文件包括：

- `topic_preprocess_stats_dual_key_2011_2025.csv`
- `topic_model_selection_dual_key_2011_2025.csv`
- `topic_keyword_distribution_dual_key_2011_2025.csv`
- `topic_strength_dual_key_2011_2025.csv`
- `topic_document_assignment_dual_key_2011_2025.csv`
- `topic_similarity_links_dual_key_2011_2025.csv`
- `topic_evolution_paths_dual_key_2011_2025.csv`
- `topic_intensity_evolution_curve_dual_key_2011_2025.png`
- `topic_evolution_summary_dual_key_2011_2025.txt`

本轮运行结果：

- `2011-2015`：建模文献数 `43,625`，选定主题数 `12`
- `2016-2020`：建模文献数 `28,835`，选定主题数 `12`
- `2021-2025`：建模文献数 `31,167`，选定主题数 `10`
- 共生成主题演化路径 `13` 条

## 五、机构主题画像输出结果

主要文件包括：

- `institution_profile_classification_dual_key_2011_2025.csv`
- `topic_distribution_by_institution_type_dual_key_2011_2025.csv`
- `topic_distribution_by_institution_level_dual_key_2011_2025.csv`
- `topic_distribution_heatmap_by_institution_type_dual_key_2011_2025.png`
- `topic_distribution_heatmap_by_institution_level_dual_key_2011_2025.png`
- `topic_institution_profile_summary_dual_key_2011_2025.txt`

本轮运行结果：

- 机构画像记录数：`19,657`
- 机构类型主题分布记录数：`72`
- 机构层级主题分布记录数：`102`

## 六、当前发现的关键质量问题

虽然主题链已经可以正常运行，但当前“机构主题画像”结果还不能直接作为论文终稿证据，原因是其中仍然残留了大量英文机构名与英文子单元名称。

已观察到的现象：

- `institution_profile_classification_dual_key_2011_2025.csv` 中，名称里含英文字母的记录有 `12,327 / 19,657`
- 典型例子包括：
  - `(Beijing) China University of Geosciences`
  - `(Beijing) China University of Petroleum`
  - `(Beijing) MOE Key Laboratory of Petroleum Engineering in China University of Petroleum`

这说明：

- 主题链中的机构抽取逻辑，比 Top100 主链的标准化逻辑更宽；
- 它会把英文校区名、学院名、实验室名、部门名重新带回结果表；
- 因而当前“按机构类型分布的主题偏好”和“按机构层级分布的主题偏好”虽然已经可运行、可检查，但还不是论文级最终标准结果。

## 七、下一步建议修复方向

在把机构主题画像正式写入论文之前，建议继续收紧 `topic_institution_profile_pipeline.py`，重点处理三类问题：

1. 更优先使用当前重建版 `trans_norm` 标准化映射表，而不是在主题链里重新放宽识别。
2. 对高频英文校区名、学院名、实验室名和部门名，尽量在有充分依据时并入上级标准化机构。
3. 对实验室、学院、系所、部门等子单元进行更严格过滤，避免它们作为独立机构进入主题画像表。

完成上述修复后，应重新生成以下结果：

- `institution_profile_classification_dual_key_2011_2025.csv`
- `topic_distribution_by_institution_type_dual_key_2011_2025.csv`
- `topic_distribution_by_institution_level_dual_key_2011_2025.csv`

只有在这一步完成后，再写论文中的“机构类型与机构层级主题分布分析”才比较稳妥。

## 八、当前可直接使用与暂缓使用的部分

当前可直接使用：

- 主题演化主链结果
- 各阶段文献量统计
- 主题数选择结果
- 主题强度表
- 主题演化路径表
- 主题演化图件

当前应暂缓直接写入论文的部分：

- 机构画像分类总表
- 机构类型主题分布表
- 机构层级主题分布表

原因不是模型失败，而是机构名称口径尚未完全收紧到论文可辩护水平。
