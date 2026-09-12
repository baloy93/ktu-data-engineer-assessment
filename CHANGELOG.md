# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-01-15

### Added

#### Architecture and Pipeline
- Seven-notebook Medallion architecture (Volume → Bronze → Silver → Gold) on Azure Databricks
- Cross-cutting audit layer with pipeline_run_log, file_manifest, data_quality_results
- Configuration-driven ingestion framework via audit.ingestion_config metadata table
- Content fingerprinting using xxhash64 for idempotent re-runs

#### Data Ingestion
- Fail-loud validation: RuntimeError on missing, empty, or unreadable source files
- Support for three source formats: Excel (.xlsx), CSV (.csv), with automatic delimiter detection
- CHW header offset handling (row 0 = form title; real headers on row 1)
- Multi-sheet union for capturing tool workbook (3 sheets → 1 Bronze table)

#### Bronze Layer
- Source-faithful representation: all column values stored as STRING
- Delta column mapping enabled to preserve source names with spaces, brackets, slashes
- Lineage columns added to every table: _source_file, _source_sheet, _ingested_at, _run_id
- Schema validation framework (extensible; currently empty audit.schema_registry)

#### Silver Layer (Conformance and Standardisation)
- Column canonicalisation: source naming differences resolved (Last Name → surname, etc.)
- Type homogenisation: multi-format date parsing using try_to_date with ISO and long-form patterns
- Identifier normalisation: SA ID (13-digit check, .0 suffix removal), PERSAL, professional registration
- Facility standardisation: case-insensitive matching against LU_Facility (809 rows) with district placeholder detection
- Professional category reduction: 54 variants → 4 canonical values (audit.map_professional_category)
- District standardisation: 117 variants → 7 canonical districts (audit.map_district)
- Event key: SHA-256 of source_system || source_file || source_sheet || sa_id_normalised || surname || first_name || course || event_date_raw
- Exception capture: unmapped facilities (507), courses (607), professions (6), districts recorded in silver.unmapped_* tables

#### Deduplication and Matching
- Three-tier deterministic participant matching with per-row tier audit:
  - Tier 1: SA ID normalised (13 digits, non-null)
  - Tier 2: SHA-256(surname + first_name + PERSAL)
  - Tier 3: event_key (fallback; documented limitation)
- Source-specific completion rules:
  - capturing_tool: attendance_status IN ('Attended', 'Partial attendance', 'Replacement')
  - chw: any row counts as completed (presence on register implies completion)
  - online_export: End Date IS NOT NULL (documented proxy)
- Q4 2025 reportable rule: event_date falls within 2025-10-01 to 2025-12-31
- Excluded records tracking: silver.excluded_records with exclusion_reason for non-reportable rows

#### Gold Layer Reporting
- gold.report_completions_by_district: rows by district, columns [district, enrolments, completions, not_completed, unique_participants]
- gold.report_enrolments_vs_completions: rows by source system, same columns
- gold.reconciliation_results: 5 checks verifying all paths sum to headline
- Distinct metric handling: unique_participants (COUNT DISTINCT participant_key) presented separately from enrolment row count

#### Validation and Data Quality
- 17 consolidated validation checks across 6 categories (completeness, accounting, integrity, uniqueness, reconciliation, referential)
- 5 reconciliation checks confirming source subtotals, district subtotals, and headline total consistency
- gold.validation_results: one row per check with expected/actual/status/message columns
- All expected values computed at runtime (not hard-coded) to test pipeline consistency

#### Documentation
- README.md: comprehensive overview of architecture, source data, data quality findings, deduplication logic, reconciliation framework, prerequisites, how to run
- Technical_Design_Document.docx: 15-section deep dive covering scope, data assessment, architecture, each layer, data quality findings, reconciliation, validation framework, engineering decisions, defects found and corrected, limitations, future enhancements
- KTU_Pipeline_Presentation.pptx: 14-slide executive presentation with architecture diagrams, metric summaries, validation results

### Fixed

#### Defect 1 — Event Key Collision
- **Issue**: Initial event_key formula excluded event_date_raw; two rows for the same participant on the same course but different dates produced identical hashes
- **Symptom**: 43 event_key values appeared in both dedup fact and excluded records simultaneously; validation check no_event_key_overlap returned 43
- **Fix**: event_key now includes event_date_raw as a hash component
- **Impact**: Completions corrected from 3,849 to 3,850

#### Defect 2 — Duplicate Hash Formula
- **Issue**: After fixing Defect 1, nb_04_Deduplication Cell 4 still used the old event_key formula (without event_date_raw) when reconstructing the key for the online export End Date join
- **Symptom**: Zero matches on the join; online_export completions collapsed to 0; total completions dropped to 2,405
- **Fix**: Hash formula in Cell 4 aligned to match the corrected formula in nb_03_Silver Cell 9
- **Lesson**: Where event_key is reconstructed independently in multiple places, both formulas must be kept in sync — or use a single source-of-truth function

### Known Limitations

- **Online completion proxy**: End Date IS NOT NULL may conflate non-completion with incomplete data entry by training administrators; 28.4% of online rows have no End Date
- **CHW no event date**: All 359 CHW rows lack an event_date column; all excluded from Q4 2025 reporting period by definition
- **Tier 3 matching**: Participants without a 13-digit SA ID and complete name+PERSAL combination treated as distinct; cross-source duplicates in this tier not detected
- **No temporal mapping versioning**: audit.map_* tables lack valid_from/valid_to columns; historical report reproduction assumes current mappings applied historically
- **schema_registry not populated**: Schema validation framework in place but not enforcing expected column sets

### Future Work

- Temporal mapping versioning with valid_from and valid_to columns on audit.map_* tables
- Per-module CHW unpivot for module-level completion reporting
- Extended online export completion rule using Earliest Approval Date
- Delta Live Tables expectations and automated lineage for Silver and Gold
- Automated regression test suite ensuring validation passes on every run
- Nightly Databricks Job schedule with alerting on validation failures
- Partitioning on source_system and event_date for performance at scale
- schema_registry population with expected column sets per source

### Results (Q4 2025)

- **4,832** total enrolments across all three source systems
- **3,850** completions (79.7% completion rate)
- **982** not completed
- **3,259** unique participants
- **17 / 17** validation checks PASS
- **5 / 5** reconciliation checks PASS

---

## [Unreleased]

### Planned

- Integration with Databricks Jobs API for scheduled pipeline execution
- Dynamic alerting on validation failures
- Extended facility matching with fuzzy matching as secondary strategy
- Web UI dashboard for monitoring pipeline runs and data quality

---

**Note**: This project was completed as an assessment demonstrating data engineering best practices including reproducible pipelines, comprehensive data quality validation, and professional documentation.
