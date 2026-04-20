# 石油领域文献数据：切割 & 机构名称标准化

这套脚本专门解决你现在的两件事：

- **大数据集切割**：把超大 CSV 按行数或按年份列切成多个小文件，便于后续清洗/筛选。
- **机构名称标准化**：从 `institution`（或 WOS/CSCD 风格 `c1`）字段抽取机构名，输出
  - 去重后的原始机构列表
  - 初步规范化后的机构列表
  - `raw -> normalized` 的映射表（可人工再校正）
  - 回写到明细数据（新增 `institution_norm` 等列）

## 1) 切割大 CSV

按行数切割（推荐先用这个，最稳）：

```bash
python -m oil_biblio_pipeline.split_csv --input "D:\毕业论文\DBdata数据_2025_11_14.csv" --outdir "D:\毕业论文\split\DBdata" --rows-per-file 300000
```

按年份列切割（你的不同数据源可能是 `year` 或 `py`）：

```bash
python -m oil_biblio_pipeline.split_csv --input "D:\毕业论文\CSCD数据_2025_11_14.csv" --outdir "D:\毕业论文\split\CSCD" --year-col year
```

## 2) 抽取 + 标准化机构名称

从切割后的文件（一个目录）批处理：

```bash
python -m oil_biblio_pipeline.normalize_institutions --input "D:\毕业论文\split\DBdata" --outdir "D:\毕业论文\norm\DBdata" --inst-col institution
```

如果你要从 WOS/CSCD 的 `c1` 抽机构（地址字段更复杂）：

```bash
python -m oil_biblio_pipeline.normalize_institutions --input "D:\毕业论文\split\WOS" --outdir "D:\毕业论文\norm\WOS" --c1-col c1
```

## 输出说明

- `institutions_raw.csv`: 去重后的原始机构字符串（含频次）
- `institutions_norm.csv`: 初步规范化后的机构字符串（含频次）
- `institution_map.csv`: `raw, normalized` 映射表（你可以手工改 normalized）
- `data_*.csv`: 回写后的明细文件（新增 `institution_extracted` / `institution_norm`）

## 3) 清洗 + 去重（复合主键）

根据你的要求：

- **复合主键**：`doi + title_cn_en + publish_date`
- **必须非空字段**：`title_cn_en`、`publish_date`、`institution_extracted`、`journal_cn_en`、`doi`

可以在机构标准化后的目录上再跑一遍清洗去重：

```bash
python -m oil_biblio_pipeline.clean_and_dedup \
  --input  "D:\毕业论文\norm_institution" \
  --output "D:\毕业论文\cnki_clean_dedup.csv"
```

输出的 `cnki_clean_dedup.csv` 就是：

- 保证关键字段不为空；
- 按 `doi + title_cn_en + publish_date` 去重后的高质量文献数据集。

