# Ralph Progress Log

## Iteration 84 (2026-02-08)
- Normalized allowed/disallowed value comparisons so space/case variants map to rules consistently.
- Documented value normalization behavior in the rules README section.

## Iteration 83 (2026-02-08)
- Persisted review flag counts and review applicant details to Postgres alongside warning rates.
- Expanded audit run metadata in Postgres to capture warning/review rates plus review truncation info.
- Updated seed data and sample rules to include review flags for richer demo outputs.

## Iteration 7 (2026-02-08)
- Fixed missing review models in the audit engine so review flags compile and render correctly.
- Added a smoke test script to validate text and JSON outputs from the CLI.
- Documented the new test runner in the README.

## Iteration 73 (2026-02-08)
- Added database logging for field completeness counts + missing-rate metrics.
- Extended the production seed script to populate completeness rows and handle upgrades.
- Documented the new completeness logging in the README.

## Iteration 84 (2026-02-08)
- Added disallowed value rules that flag blocked field values with a dedicated reason category.
- Updated tracked field completeness to include conditional trigger fields and disallowed lists.
- Expanded sample rules/data plus README documentation to showcase blocked value audits.

## Iteration 93 (2026-02-08)
- Added field alias support to map intake header variants onto canonical rule fields.
- Canonicalized segment/id fields when alias definitions are present.
- Documented alias rules and refreshed the sample rules file.

## Iteration 72 (2026-02-08)
- Added segment-field summaries for eligibility rates by a chosen CSV field.
- Persisted segment breakdowns to Postgres with a new audit_segments table.
- Updated README usage and production seed data to include segments.

## Iteration 40
- Added require-any rule groups to flag applicants missing all fields in a contact bundle.
- Expanded sample rules/intake data and documented the new rule option in the README.

## Iteration 30 (2026-02-08)
- Added run metadata (run name, input path, rules path) to text and JSON outputs.
- Confirmed production seed data exists for Postgres logging.
- Verified CLI output with the new run metadata fields.

## Iteration 39
- Added optional Postgres logging for audit runs, reasons, and failure details.
- Included JDBC driver + updated runner to load the lib classpath.
- Added a seed script and documented database setup in the README.

## Iteration 30
- Created the Eligibility Oracle Java CLI to audit intake CSVs against configurable rules.
- Added a sample intake CSV, a rules template, and a shell runner for quick usage.
- Documented usage, rule syntax, and outputs in the README.

## Iteration 37
- Added eligibility rate percentages and reason category summaries to the audit output.
- Introduced configurable applicant ID fields and optional failure list limits.
- Expanded JSON output with rate metrics, category counts, and truncation metadata.

## Iteration 33
- Added regex pattern rules (e.g., email validation) to eligibility audits.
- Updated sample rules/data and README documentation for pattern checks.

## Iteration 46
- Added unique-field validation to flag duplicates like shared emails.
- Updated sample rules and intake data to demonstrate duplicate detection.
- Documented the new unique rule in the README feature list and rule format.
