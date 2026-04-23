# thesis_rebuild probe summary (2026-04-22)

## Inputs

- CNKI rebuild clean input: `D:\graduate\thesis_rebuild\corpus\cnki_clean_rebuild.csv`
- CNKI clean stats: `D:\graduate\thesis_rebuild\corpus\cnki_clean_rebuild_stats.csv`
- CNKI clean method note: `D:\graduate\thesis_rebuild\corpus\cnki_clean_rebuild_method_note.txt`
- CNKI unstable-DOI review: `D:\graduate\thesis_rebuild\qa\review_cnki_unstable_doi_rebuild.csv`
- WOS raw input: `D:\BaiduNetdiskDownload\期刊数据\DBdata数据_2025_11_14.csv`
- CSCD raw input: `D:\BaiduNetdiskDownload\期刊数据\CSCD数据_2025_11_14.csv`

## Outputs

- Merged probe corpus: `D:\graduate\thesis_rebuild\corpus\merged_clean_dual_key_probe_2011_2025.csv`
- Probe stats: `D:\graduate\thesis_rebuild\corpus\dedup_stats_dual_key_probe_2011_2025.csv`
- Probe method note: `D:\graduate\thesis_rebuild\corpus\dedup_method_note_dual_key_probe_2011_2025.txt`
- Weak-similarity review table: `D:\graduate\thesis_rebuild\qa\review_weak_similarity_probe_2011_2025.csv`
- DOI-conflict review table: `D:\graduate\thesis_rebuild\qa\review_doi_conflicts_probe_2011_2025.csv`

## Headline result

- Probe unique rows: `311,321`
- Versus historical dual-key baseline `310,322`: `+999`
- Versus previously confirmed relaxed main table `312,259`: `-938`
- Versus the earlier inherited-clean probe `311,309`: `+12`

## CNKI clean rebuild

- Input rows: `225,971`
- Rebuild clean rows kept: `185,187`
- Rows dropped for missing at least one required bibliographic dimension: `40,784`
- Missing counts:
  - title: `0`
  - year/publication time: `91`
  - journal: `91`
  - institution: `40,707`
- Rows with strict candidate standard DOI: `15,739`
- Rows gaining strict DOI only from `registered_doi`: `15,173`
- Unstable CNKI standard DOI keys: `2`
- Rows affected by unstable CNKI standard DOI keys: `7`
- Clean-stage automatic dedup removed rows: `0`

## Per-source probe stats

- CNKI: `original_rows=185,187`, `year_kept_rows=55,996`, `rows_with_candidate_standard_doi=9,835`, `rows_with_unstable_standard_doi=7`, `rows_with_standard_doi=9,828`, `rows_with_meta_key=54,713`, `primary_kept_groups=55,931`
- WOS: `original_rows=336,459`, `year_kept_rows=230,364`, `rows_with_candidate_standard_doi=230,363`, `rows_with_unstable_standard_doi=0`, `rows_with_standard_doi=230,363`, `rows_with_meta_key=230,351`, `primary_kept_groups=230,224`
- CSCD: `original_rows=42,336`, `year_kept_rows=33,547`, `rows_with_candidate_standard_doi=49`, `rows_with_unstable_standard_doi=0`, `rows_with_standard_doi=49`, `rows_with_meta_key=33,524`, `primary_kept_groups=25,166`

## Key findings

- The probe now uses a strict DOI pattern requiring a slash. This reduces CNKI 2011-2025 standard DOI candidates from the earlier loose count `9,880` to `9,835`.
- Two CNKI DOI keys are unstable within the 2011-2025 window and affect `7` rows. These keys are excluded from DOI-based auto-merging.
- Source-local raw DOI-like strings are no longer used as automatic merge keys.
- The rebuilt CNKI clean is methodologically cleaner, but for the target 2011-2025 window it only increases CNKI input rows from `55,994` to `55,996`. The rebuild changes the full-history clean artifact much more than it changes the analysis window.
- Current merged output match basis distribution:
  - `single`: `303,015`
  - `meta_key`: `8,266`
  - `standard_doi|meta_key`: `35`
  - `standard_doi`: `5`
- Current source group distribution:
  - `WOS`: `229,545`
  - `CNKI`: `48,671`
  - `CSCD`: `25,166`
  - `CNKI|CSCD`: `7,120`
  - `WOS|CSCD`: `679`
  - `CNKI|WOS|CSCD`: `140`
- No direct `CNKI|WOS` two-way groups were observed in this probe.

## Review tables

- Weak-similarity groups: `5,740` groups / `11,481` rows
- DOI-conflict groups: `62` groups / `126` rows
- CNKI unstable-DOI review rows: `7`

## Current interpretation

- The merge-and-dedup rules are now easier to defend in the thesis:
  - CNKI admission is based on core bibliographic completeness rather than DOI availability.
  - DOI recognition is strict rather than loose.
  - unstable same-source DOI keys are explicitly excluded from automatic merging.
  - raw source-local DOI-like strings are not treated as merge keys.
- The remaining `-938` gap relative to the old relaxed corpus is still not explained by the rebuilt merge rules alone.
- Old relaxed `CNKI|WOS` DOI-side rows present in rebuilt CNKI clean: `0 / 89`
- Old relaxed `CNKI|WOS|CSCD` DOI-side rows present in rebuilt CNKI clean: `0 / 221`

## Next checks

1. Trace where the old relaxed `CNKI|WOS` and `CNKI|WOS|CSCD` DOI-side records came from, because they are still absent from the rebuilt CNKI clean artifact.
2. Decide whether downstream institution/topic chains should now branch from this scientific rebuild corpus even if it is not row-for-row identical to the old relaxed corpus.
3. Draft the thesis method subsection directly from the new clean-note and dedup-note files so the written rules stay synchronized with the executable pipeline.
