# Institution Normalization Status (2026-04-22)

## Completed in current workspace

- Formal dual-key corpus has been promoted from probe to official rebuild output:
  - `D:\graduate\thesis_rebuild\corpus\merged_clean_dual_key_dedup_2011_2025.csv`
- Institution extraction master table has been generated:
  - `D:\graduate\thesis_rebuild\institution_eval\institution_name_table_dual_key_2011_2025.csv`

## Current extraction result

- Unique extracted institution names: `93,101`
- Top high-frequency raw names include:
  - `Chinese Acad Sci` (`11,341`)
  - `China Univ Petr` (`6,038`)
  - `Univ Chinese Acad Sci` (`4,707`)
  - `Tsinghua Univ` (`4,100`)
  - `Tianjin Univ` (`3,829`)

## What is not completed yet

The formal translated-and-normalized institution table has not been rebuilt in the current workspace in this round.

Reason:

- The current workspace does not yet contain a reusable reviewed master mapping asset equivalent to:
  - `institution_name_table_*_trans_norm.csv`
  - `institution_map_new.xlsx`
  - `institution_count_new.xlsx`
- Because institution normalization is a high-impact step for Top100 ranking, collaboration analysis and institution-type classification, it should not be replaced with unreviewed ad hoc machine normalization.

## Methodological decision

Current status is therefore:

1. The raw institution extraction table is accepted as the starting master list.
2. The translated-and-normalized table will be rebuilt only after a reviewed mapping asset is located or a new reviewed normalization workflow is executed.
3. No provisional Top100 or TOPSIS output should be treated as formal until institution normalization is closed.

## Recommended next step

Rebuild or recover the reviewed institution translation/normalization mapping in the current workspace, then continue with:

- `institution_name_table_dual_key_trans_norm_2011_2025.csv`
- `institution_name_table_dual_key_norm_freq_2011_2025.csv`
- `institution_core_top100_dual_key_2011_2025.csv`
