# Ralph Progress Log

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
