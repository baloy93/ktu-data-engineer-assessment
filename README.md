# KTU Data Engineer Assessment — Multi-Source Training Data Pipeline

**Azure Databricks · PySpark · Delta Lake · Unity Catalog · Medallion Architecture**

---

## Overview

The Knowledge Translation Unit (KTU) operates training programmes across multiple districts in the Western Cape. Training attendance is captured through three separate mechanisms — a manual capturing tool, community health worker (CHW) completion registers, and a system-generated online school export — each with different structures, naming conventions, and data quality characteristics.

This pipeline ingests all six source files, conforms them through a Medallion architecture on Azure Databricks, applies a documented reportable rule and tiered participant matching, and produces two reconciled reporting outputs covering Q4 2025.

**Headline results — Q4 2025:**

| Metric | Value |
|---|---|
| Enrolments | 4,832 |
| Completions | 3,850 |
| Not completed | 982 |
| Unique participants | 3,259 |

All 17 validation checks and all 5 reconciliation checks pass.

---

## Business and Data Problem

The assessment brief requires a reproducible reporting pipeline that a senior reviewer can independently verify. The source data reflects real operational characteristics:

- Training attendance is captured manually by multiple operators, producing spelling variants, formatting inconsistencies, and incomplete records
- Three structurally incompatible source formats must be unified into a single reporting view
- Personal identifiers have been anonymised, constraining the approach to participant matching
- The pipeline must be idempotent: re-running on unchanged input must produce identical results

The specific challenges identified during source profiling:

- 54 spelling variants of `Professional Category` reduce to four canonical values
- 117 spelling variants of `District` reduce to seven canonical districts
- CHW workbooks embed a printed form title in row 0, so the real column headers are on row 1
- The online export uses different column names (`Last Name`, `Location Name`, `Start Date`) compared to the capturing tool (`Participant Surname`, `Facility`, `Start date (YYYY/MM/DD)`)
- 4,067 rows have no parseable event date and are excluded from the Q4 2025 reporting period by definition
- SA ID numbers are synthetic (assessment anonymisation), producing the expected ~9% Luhn pass rate

---

## Solution

Seven Databricks notebooks implement a Medallion architecture with a cross-cutting audit layer:

```
Source files (Unity Catalog volume, unchanged)
        │
        ▼
nb_01_Ingestion ──► audit.file_manifest (content fingerprints, idempotency)
        │
        ▼
nb_02_Bronze ──► bronze.* (source-faithful, all columns as STRING, lineage columns)
        │
        ▼
nb_03_Silver ──► silver.participant_event (conformed, standardised)
                 silver.unmapped_* (surfaced exceptions, not dropped)
        │
        ▼
nb_04_Deduplication ──► silver.participant_event_dedup (Q4 2025, participant_key)
                        silver.excluded_records (outside Q4, with reason)
        │
        ▼
nb_05_Gold ──► gold.report_completions_by_district
               gold.report_enrolments_vs_completions
               gold.reconciliation_results
        │
        ▼
nb_06_Validation ──► gold.validation_results (17 checks)
```

---

## Architecture

### Layer responsibilities

| Layer | Table prefix | Purpose | Preserves source? | Adds conformance? |
|---|---|---|---|---|
| Volume | — | Raw landing of source files, unchanged | Yes | No |
| Bronze | `bronze.*` | Queryable source-faithful representation with lineage | Yes | No |
| Silver | `silver.*` | Conformance, standardisation, deduplication, reportable rule | No | Yes |
| Gold | `gold.*` | Reporting-ready aggregates and reconciliation | No | Yes |
| Audit | `audit.*` | Cross-cutting observability, idempotency, config | n/a | n/a |

---

## Data Sources

| Source | Format | Rows | Key Characteristics |
|---|---|---|---|
| Capturing Tool (3 sheets) | `.xlsx` | 11,607 | Manual capture; 37 columns; 54 profession variants; 117 district variants; sheets Capturer1/2/3 |
| CHW West Coast | `.xlsx` | ~120 | Header on row 1; 49 columns; 15 module columns; no event date |
| CHW KESS | `.xlsx` | ~120 | Same schema as West Coast |
| CHW Witzenberg | `.xlsx` | ~120 | Same schema as West Coast |
| Online Export | `.csv` | 25,824 | System-generated; UTF-8; 27 columns; different naming conventions |
| Course Lookup | `.xlsx` | 340 | Reference data: course codes, names, groups |
| Facility Lookup | `.xlsx` | 809 | Authoritative facility reference; 19 columns including district and sub-district |

The three CHW files share an identical 49-column schema and are unioned into a single `bronze.chw_attendance` table (359 rows combined). The capturing tool's three sheets share a 37-column schema and are unioned into `bronze.capturing_tool` (11,607 rows).

---

## Data Engineering Approach

### Ingestion

`nb_01_Ingestion` reads the `audit.ingestion_config` metadata table to determine which files to process. For each configured source it:

- Resolves the full volume path
- Confirms the file exists and is non-empty (fail-loud on any missing or empty file)
- Computes a content fingerprint using Spark's `xxhash64` via `binaryFile` reader
- Classifies each file as NEW, UNCHANGED, or CHANGED against `audit.file_manifest`
- Upserts the manifest via SQL MERGE (avoiding a Delta API assertion bug on newly created tables)

SHA-256 via Python `hashlib` was abandoned: reading the 10.8 MB CSV over the volume mount took 32 minutes. `xxhash64` via Spark's `binaryFile` reader completes in under 15 seconds.

### Bronze

`nb_02_Bronze` writes one Delta table per logical source. All column values are stored as STRING. Source column names containing spaces, parentheses, commas, and slashes are preserved using Delta column mapping. Four lineage columns are added to every table: `_source_file`, `_source_sheet`, `_ingested_at`, `_run_id`.

Each source has an explicit reader. A single generic reader would require conditional branches for the CHW `header=1` requirement, the multi-sheet union, and the CSV delimiter detection — three explicit readers are more legible.

### Silver

`nb_03_Silver` produces the conformed fact table `silver.participant_event`. Key transformations:

**Column canonicalisation**: all three sources are mapped to a common 20-column schema. The online export's `Last Name` / `Location Name` / `Start Date` are renamed to `surname` / `facility` / `event_date` to match the other sources. CHW rows without an explicit course column receive the synthetic value `Community Health Worker Programme`.

**Type homogenisation**: `event_date` arrives as a datetime in the capturing tool, a string in the online export, and NULL in CHW. All three branches cast to STRING before union, then `try_to_date` with multiple format patterns parses to DATE. `try_to_date` is used rather than `to_date` to return NULL on unparseable input instead of raising `CAST_INVALID_INPUT`.

**Identifier normalisation**: SA IDs are stripped of whitespace, `.0` suffixes (Excel float artefact), and non-digit characters. Only 13-digit strings are retained as `sa_id_normalised`. Luhn validation is implemented and recorded as `sa_id_luhn_valid` but is not used as a matching gate, because the assessment confirmed the data was anonymised with randomised identifiers.

**Facility standardisation**: case-insensitive, trimmed matching against `FacilityName` then `FacilityReportingName` from the standalone `LU_Facility` lookup (809 rows). District placeholder values (`- Other Location` suffix) are flagged separately. Unmatched values are recorded in `silver.unmapped_facilities`.

**Professional category standardisation**: 54 spelling variants of the `Professional Category` field in the capturing tool are reduced to four canonical values (`Doctor`, `Nurse`, `Pharmacist`, `Other`) via the `audit.map_professional_category` mapping table seeded during setup.

**District standardisation**: 117 spelling variants reduced to seven canonical districts via `audit.map_district`.

**Unmapped value handling**: unmatched facility values (507), course values (607), and profession values (6) are recorded in `silver.unmapped_*` tables with occurrence counts and first-seen timestamps. Nothing is silently dropped.

**Event key**: `event_key` is a SHA-256 of `source_system || source_file || source_sheet || sa_id_normalised || surname || first_name || course || event_date_raw`. Including `event_date_raw` was a correction made during validation: the initial formula without it produced 43 `event_key` collisions when participants appeared in both the Q4 2025 and outside-Q4 partitions.

### Deduplication

`nb_04_Deduplication` applies a three-tier deterministic matching rule:

| Tier | Key | Condition |
|---|---|---|
| 1 | `sa_id_normalised` | 13-digit SA ID present and non-null |
| 2 | `sha256(surname \|\| first_name \|\| persal_number)` | SA ID missing; all three fields present |
| 3 | `event_key` | Neither Tier 1 nor Tier 2 applicable |

`participant_match_tier` records which tier produced each `participant_key`, making the matching auditable. No probabilistic or fuzzy matching is used. Rows without a usable identifier are treated as distinct participants, and this is documented as a limitation.

**Reportable rule**: a row is reportable if its `event_date` falls within Q4 2025 (2025-10-01 to 2025-12-31) and represents a completed training event:

| Source | Completion condition |
|---|---|
| `capturing_tool` | `attendance_status IN ('Attended', 'Partial attendance', 'Replacement')` |
| `chw` | Any row (presence on the register implies completion) |
| `online_export` | `End Date IS NOT NULL` (documented proxy) |

Rows outside Q4 2025 are written to `silver.excluded_records` with `exclusion_reason = 'OUTSIDE_REPORTING_PERIOD'`.

### Gold

`nb_05_Gold` produces two reporting tables from `silver.participant_event_dedup`, each including a TOTAL row:

- `gold.report_completions_by_district`: enrolments, completions, not_completed, unique_participants by district
- `gold.report_enrolments_vs_completions`: the same metrics by source system

Five reconciliation checks confirm that district subtotals, source subtotals, and the headline total are all consistent.

### Validation

`nb_06_Validation` runs 17 checks across six categories and writes the results to `gold.validation_results`. All expected values for reconciliation checks are computed from the dedup fact itself, not hard-coded, so the checks test pipeline consistency rather than coincidence with a stale value.

---

## Data Quality Findings

| Issue | Detail | Handling |
|---|---|---|
| Professional Category variants | 54 spelling variants in Capturing Tool | Reduced to 4 canonical values via `audit.map_professional_category` |
| District variants | 117 spelling variants in Capturing Tool | Reduced to 7 canonical districts via `audit.map_district` |
| CHW header offset | Row 0 is a printed form title, not column headers | `header_row = 1` in `audit.ingestion_config` |
| Unmapped facilities | 507 distinct values with no lookup match | Recorded in `silver.unmapped_facilities`; not dropped |
| Unmapped courses | 607 distinct values with no lookup match | Recorded in `silver.unmapped_courses`; not dropped |
| Unmapped professions | 6 distinct values with no mapping | Recorded in `silver.unmapped_professions`; not dropped |
| No event date (CHW) | 4,067 rows have no parseable date | Classified as outside reporting period; recorded in `silver.excluded_records` |
| SA ID as float | Excel renders integer IDs as `7783185163507.0` | `.0` suffix stripped during normalisation |
| SA ID Luhn validity | ~9% pass rate expected for anonymised data | Flagged as `sa_id_luhn_valid`; not used as matching gate |
| Online export completion | `End Date` is 28% null in the full file | `IS NOT NULL` proxy documented as a limitation |
| Race spelling variants | `Coloured` / `coloured` / `Colored` co-exist | Standardised to canonical form in Silver |

---

## Deduplication and Matching

Three-tier deterministic matching. The tier used is recorded on every row in `participant_match_tier`.

**Tier 1** (strongest): 13-digit `sa_id_normalised`. Luhn validity is not required, because the assessment confirmed the data was anonymised with synthetic identifiers. Requiring Luhn validity would incorrectly treat ~91% of valid-format IDs as missing.

**Tier 2**: `sha256(surname_upper || '||' || first_name_upper || '||' || persal_clean)`. Applied only when Tier 1 is unavailable and all three fields are non-empty. A composite of three attributes has a lower collision risk than name alone.

**Tier 3** (weakest): `event_key` (the row's own unique key). Each Tier 3 row is treated as a distinct participant. This is a deliberate limitation: rows without any usable identifier cannot be matched across sources, and this is documented rather than obscured by uncertain probabilistic matching.

**Limitations**:
- The online export `End Date IS NOT NULL` proxy may conflate non-completion with incomplete data capture
- CHW rows without an event date cannot be placed in any reporting period
- Participants who appear in multiple sources but lack a 13-digit SA ID and PERSAL number are counted multiple times under Tier 3

---

## Reconciliation

The pipeline defines one headline total: **4,832 enrolments** in Q4 2025 drawn from `silver.participant_event_dedup`.

Five checks in `gold.reconciliation_results` confirm consistency:

```
Headline enrolments (4,832)
    = Source subtotals: capturing_tool + chw + online_export  ✓
    = District subtotals: 6 districts + UNKNOWN               ✓

Headline completions (3,850)
    = Source subtotals                                         ✓
    = District subtotals                                       ✓

Enrolments (4,832) = Completions (3,850) + Not completed (982) ✓
```

The unique participant count (3,259) is a distinct count of `participant_key`. It is presented separately from the enrolment row count because one participant may have attended multiple courses within Q4 2025.

All 5 reconciliation checks and all 17 validation checks pass.

---

## Project Structure

```
.
├── README.md
├── nb_00_Setup.ipynb          # Schemas, volume, audit tables, mapping seeds
├── nb_01_Ingestion.ipynb      # File validation, fingerprints, file manifest
├── nb_02_Bronze.ipynb         # Source reads, schema validation, Bronze writes
├── nb_03_Silver.ipynb         # Conformance, standardisation, fact table
├── nb_04_Deduplication.ipynb  # Tiered matching, reportable rule, exclusions
├── nb_05_Gold.ipynb           # Reporting tables and reconciliation
├── nb_06_Validation.ipynb     # 17 consolidated validation checks
└── profiling/
    ├── profiling_report.txt
    ├── profiling_column_summary_excel.csv
    └── profiling_column_summary_csv.csv
```

---

## Prerequisites

- Azure Databricks workspace with Unity Catalog enabled
- Serverless notebook compute
- A serverless SQL warehouse (used for interactive queries against Gold tables; not required for the pipeline itself)
- The following catalog and schemas (created by `nb_00_Setup`):
  - Catalog: `ktu_assessment_dev`
  - Schemas: `bronze`, `silver`, `gold`, `audit`
  - Volume: `ktu_assessment_dev.bronze.raw_landing`

---

## How to Run

### 1. Clone or import notebooks

Import the seven notebooks into your Databricks workspace.

### 2. Upload source files

Upload the six source files to the Unity Catalog volume, preserving the folder structure:

```
/Volumes/ktu_assessment_dev/bronze/raw_landing/Training Data/
├── Capturing Tool_V1c V2-27 January 2026_SCRUBBED.xlsx
├── Course and Facility Look Ups.xlsx
├── Online Data Export 17 Dec_SCRUBBED.csv
└── Community Health Worker Completions/
    ├── CHW Training Attendance-West Coast-Oct 2025_SCRUBBED.xlsx
    ├── CHW Training Attendance_KESS_December 2025_SCRUBBED.xlsx
    └── WITZENBERG - July-Dec 2025_SCRUBBED.xlsx
```

### 3. Run the pipeline

**Option A (recommended):** open the Databricks Job `ktu_data_engineer_assessment_pipeline` and click **Run now**.

**Option B:** run the notebooks manually in order on serverless compute:

```
nb_00_Setup → nb_01_Ingestion → nb_02_Bronze → nb_03_Silver
           → nb_04_Deduplication → nb_05_Gold → nb_06_Validation
```

`nb_02_Bronze` and `nb_03_Silver` each require `%pip install openpyxl` at the top, followed by `dbutils.library.restartPython()`. These cells are included in the notebooks.

### 4. Verify results

```sql
-- Headline results
SELECT * FROM ktu_assessment_dev.gold.report_enrolments_vs_completions;

-- Completions by district
SELECT * FROM ktu_assessment_dev.gold.report_completions_by_district;

-- Reconciliation
SELECT * FROM ktu_assessment_dev.gold.reconciliation_results;

-- All validation checks
SELECT * FROM ktu_assessment_dev.gold.validation_results ORDER BY category, check_name;
```

All 17 rows in `validation_results` should show `status = 'PASS'`. All 5 rows in `reconciliation_results` should show `status = 'PASS'`.

---

## Key Engineering Decisions

**Why `xxhash64` instead of SHA-256 for file fingerprints.** Python `hashlib` reading over a Unity Catalog volume mount took 32 minutes for the 10.8 MB CSV. Spark's `binaryFile` reader with `xxhash64` completes in under 15 seconds. The same idempotency guarantee is preserved.

**Why configuration-driven ingestion.** Source-specific parameters (header row, sheet name, subfolder, format) are stored in `audit.ingestion_config` rather than hard-coded in notebooks. This makes the ingestion framework extensible without code changes.

**Why column mapping on Bronze tables.** Delta rejects column names containing spaces, parentheses, commas, and slashes. Source column names contain all of these. Delta column mapping preserves the original names while storing them under safe physical names. Renaming belongs in Silver, not in the read step.

**Why SQL MERGE for manifest upsert rather than the Delta Python API.** `DeltaTable.forName().merge()` raises `AssertionError` against a table that has never had a write commit (first-run scenario). SQL MERGE via temp view is equivalent and avoids that code path.

**Why `try_to_date` with multiple format patterns.** Source dates arrive as ISO (`2025-07-22`) and long-form (`05 February 2025`) strings. `to_date` raises `CAST_INVALID_INPUT` on unparseable input. `try_to_date` returns NULL and allows the pipeline to proceed, with the NULL date correctly excluded from Q4 2025 by the date filter.

**Why source-specific readers rather than one generic reader.** Three sources have genuinely different structural characteristics (Excel with one header row, Excel with form-title row 0, CSV with different naming). A single generic reader would require so many conditional branches that it would be less readable than three explicit functions.

**Why no probabilistic or fuzzy matching.** Given that personal data has been anonymised, a fuzzy match on synthetic identifiers would produce meaningless results. Deterministic matching on available identifiers with an explicit fallback and documented limitation is more defensible than uncertain matches presented without caveat.

---

## Assumptions and Limitations

- **Reporting period**: Q4 2025 (2025-10-01 to 2025-12-31), chosen as the most recent complete quarter at the time of assessment.
- **CHW course name**: `Community Health Worker Programme` is assigned as a synthetic course name because the CHW source files do not contain a course column.
- **Online export completion proxy**: `End Date IS NOT NULL` is used as the completion indicator. This may conflate non-completion with incomplete data entry.
- **SA ID Luhn validity**: ~91% of normalised SA IDs fail Luhn validation, as expected for randomised 13-digit synthetic identifiers. Luhn validity is recorded but not used as a matching gate.
- **Tier 3 matching**: participants without a 13-digit SA ID and complete name+PERSAL combination are treated as distinct. Cross-source duplicates in this tier are not detected.
- **Source files manually landed**: source files were manually placed in the controlled volume for this assessment. The ingestion framework validates, fingerprints, and processes the landed files idempotently on each run.
- **Embedded CHW lookups not ingested**: the `lu_*` sheets embedded in each CHW workbook are identical across all three files and redundant with the standalone lookup file. They are not ingested as separate Bronze tables.

---

## Defects Found and Corrected During Validation

### Event key collision

The initial `event_key` formula excluded `event_date_raw`. Two rows for the same participant on the same course but on different dates produced the same hash. When the dedup/excluded split was applied, 43 `event_key` values appeared in both tables simultaneously.

- **Symptom**: `no_event_key_overlap` check returned 43
- **Fix**: `event_key` now includes `event_date_raw`
- **Impact**: completions increased from 3,849 to 3,850

### Duplicate hash formula in deduplication notebook

After correcting the `event_key` formula in `nb_03_Silver`, online export completions collapsed to zero. `nb_04_Deduplication` Cell 4 had a second, independent `event_key` reconstruction for the online `End Date` join. This second formula retained the old definition, so the join produced zero matches.

- **Symptom**: online export completions = 0; total completions dropped to 2,405
- **Fix**: the hash formula in Cell 4 was aligned to match the corrected formula in `nb_03_Silver` Cell 9

---

## Future Improvements

1. Implement temporal mapping versioning with `valid_from` and `valid_to` on `audit.map_*` tables, so historical reports can be reproduced using the mapping rules that applied at the time.
2. Add per-module unpivot for the CHW 15-module layout to enable module-level completion reporting.
3. Implement an automated regression test that asserts the validation suite passes on each run without manual intervention.
4. Extend the online export completion rule to use `Earliest Approval Date` as an alternative or supplementary indicator.
5. Add Delta Live Tables expectations and automated lineage for the Silver and Gold layers.
6. Add a nightly Databricks Job schedule and alerting on validation failures.
7. Add partitioning on `source_system` and `event_date` for performance at larger data volumes.

---

## Assessment Criteria Mapping

| Criterion | Weight | Evidence |
|---|---|---|
| Reconciliation and correctness | 20% | `gold.reconciliation_results` (5 checks), `gold.validation_results` (17 checks) |
| De-duplication and reportable rule | 15% | `silver.participant_event_dedup`, `participant_match_tier`, tiered matching with documented limitations |
| Transformation, mapping and data quality | 15% | `silver.*_dimension`, `silver.unmapped_*`, `audit.map_*` mapping tables |
| Ingestion, robustness and reproducibility | 15% | `nb_01_Ingestion` with fingerprints, `audit.file_manifest`, fail-loud validation, Databricks Job |
| Pipeline architecture and layering | 15% | Volume → Bronze → Silver → Gold → Validation, plus audit layer |
| Documentation and communication | 10% | This README and inline notebook comments |
| Code quality and maintainability | 10% | Config-driven ingestion, metadata tables, consistent naming and structure |
