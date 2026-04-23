# Probe review findings (2026-04-22)

## Compared files

- Old relaxed dual-key corpus: `D:\毕业论文\version_dual_key_dedup_2011_2025\merged_clean_dual_key_dedup_2011_2025.csv`
- New probe corpus: `D:\graduate\thesis_rebuild\corpus\merged_clean_dual_key_probe_2011_2025.csv`
- New CNKI clean artifact: `D:\graduate\thesis_rebuild\corpus\cnki_clean_rebuild.csv`
- Weak-similarity review table: `D:\graduate\thesis_rebuild\qa\review_weak_similarity_probe_2011_2025.csv`
- DOI-conflict review table: `D:\graduate\thesis_rebuild\qa\review_doi_conflicts_probe_2011_2025.csv`
- CNKI unstable-DOI review table: `D:\graduate\thesis_rebuild\qa\review_cnki_unstable_doi_rebuild.csv`

## Row-count check

- Old relaxed corpus row count: `312,259`
- New probe corpus row count: `311,321`
- Net difference: `-938`

## Structural differences between old relaxed corpus and current probe

### Source-primary distribution
- Old: `CNKI 55,914 / WOS 230,054 / CSCD 26,291`
- Probe: `CNKI 55,931 / WOS 230,224 / CSCD 25,166`

### Source-group distribution deltas
- `CNKI` only: `49,904 -> 48,671` (`-1,233`)
- `CNKI|CSCD`: `5,700 -> 7,120` (`+1,420`)
- `CNKI|WOS`: `89 -> 0` (`-89`)
- `CNKI|WOS|CSCD`: `221 -> 140` (`-81`)
- `WOS` only: `229,164 -> 229,545` (`+381`)
- `WOS|CSCD`: `890 -> 679` (`-211`)
- `CSCD` only: `26,291 -> 25,166` (`-1,125`)

### Match-basis distribution deltas
- `single`: `304,912 -> 303,015` (`-1,897`)
- `meta_key`: `7,226 -> 8,266` (`+1,040`)
- `post_audit_residual_merge`: `74 -> 0` (`-74`)
- `standard_doi`: `9 -> 5` (`-4`)
- `standard_doi|meta_key`: `38 -> 35` (`-3`)

## Interpretation of the remaining 938-row gap

### Main structural driver
The largest structural change is still additional `CNKI|CSCD` consolidation.

- New probe has `1,420` more `CNKI|CSCD` merged rows than the old relaxed corpus.
- All `7,120` current `CNKI|CSCD` rows were merged via `meta_key`.
- This means the current rebuild still derives most cross-library consolidation from the strict metadata key rather than from aggressive DOI swallowing.

### Current clean-stage finding
The rebuilt CNKI clean artifact is methodologically better, but it barely changes the target analysis window:

- Old inherited CNKI clean rows entering merge in 2011-2025: `55,994`
- New rebuilt CNKI clean rows entering merge in 2011-2025: `55,996`
- Net gain inside the analysis window: `+2`

Therefore the remaining `-938` gap cannot be attributed mainly to the CNKI admission rule.

### Input-drift limitation still present
The old relaxed cross-database rows are still not reproducible from the rebuilt CNKI clean artifact:

- Old `CNKI|WOS` rows: `89`
  - Found in rebuilt CNKI clean by strict DOI: `0`
  - Missing from rebuilt CNKI clean: `89`
- Old `CNKI|WOS|CSCD` rows: `221`
  - Found in rebuilt CNKI clean by strict DOI: `0`
  - Missing from rebuilt CNKI clean: `221`

This means the unresolved gap still points to upstream CNKI artifact drift or earlier pipeline behavior that is not present in the current raw/normalized input chain.

## Scientific safeguards now in place

- Strict DOI recognition now requires a slash and reduces CNKI 2011-2025 candidate standard DOI rows from the earlier loose count `9,880` to `9,835`.
- Two unstable CNKI DOI keys affect `7` rows in the current window and are excluded from DOI-based auto-merging.
- Source-local raw DOI-like strings are no longer used as automatic merge keys.
- Clean-stage automatic dedup remains `0`, which is easier to justify than forcing same-source collapse on weak identifiers.

## Review-table status

- Weak-similarity review: `5,740` groups / `11,481` rows
- DOI-conflict review: `62` groups / `126` rows
- CNKI unstable-DOI review: `2` keys / `7` rows

These outputs are now aligned with the written method:
- weak metadata matches are reviewed rather than force-merged
- conflicting DOI situations are reviewed rather than force-merged
- unstable same-source DOI keys are reviewed rather than force-merged

## Current judgment

### What is now methodologically solid
- CNKI admission no longer depends on DOI availability.
- DOI normalization is strict and explicit.
- unstable same-source DOI keys are excluded from automatic merging.
- raw source-local DOI-like identifiers are not treated as bibliographic identifiers.
- the executable rules are now simple enough to write cleanly in the thesis.

### What is still unresolved
- The rebuilt corpus is still not row-for-row identical to the old relaxed corpus.
- The remaining difference is still driven by non-reproducible CNKI-side rows in the historical relaxed artifact, not by an obvious current merge bug.

## Recommended next action

1. Freeze the current probe as the thesis-method baseline unless exact historical replication is a hard requirement.
2. If exact replication is still required, audit the provenance of the missing `89 + 221` old CNKI-linked DOI rows outside the current raw-to-normalized-to-clean chain.
3. Start drafting the thesis method subsection from the current clean and merge notes instead of waiting for perfect historical row-count parity.
