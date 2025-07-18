# Group Scholar Eligibility Oracle

Eligibility Oracle is a lightweight Java CLI that audits applicant intake CSVs against a human-readable rules file. It flags missing requirements, out-of-range scores, disallowed statuses, and date eligibility windows, then produces an operational summary for scholarship ops teams.

## Features
- Parses CSV intakes without external dependencies
- Rules file supports required fields, numeric ranges, allowed values, and date windows
- Blocks explicitly disallowed values for fields like review notes or flags
- Supports "require any" groups to ensure at least one field is present
- Supports regex pattern validation for fields like email or IDs
- Flags duplicate values for fields that must be unique (ex: email)
- Supports field aliases to map intake header variants to canonical rule fields
- Adds optional segment breakdowns to show eligibility rates by a chosen field
- Outputs concise text summaries or JSON for downstream workflows
- Supports custom applicant ID fields and optional failure list limits
- Includes sample data and rules for fast iteration

## Quickstart

```bash
./scripts/run.sh --input data/sample-intake.csv --rules data/rules.txt
```

```bash
./scripts/run.sh --input data/sample-intake.csv --rules data/rules.txt --format json --output reports/eligibility.json
```

```bash
./scripts/run.sh --input data/sample-intake.csv --rules data/rules.txt --id-field applicant_id --limit 25
```

```bash
./scripts/run.sh --input data/sample-intake.csv --rules data/rules.txt --segment-field status
```

## Testing

```bash
./scripts/run_tests.sh
```

## Database logging (optional)

The Oracle can log audit summaries to Postgres for dashboards or longitudinal tracking.
Field completeness counts are stored alongside reason metrics, warning tallies, and review flag summaries for intake quality monitoring.

```bash
export ELIGIBILITY_DB_URL="jdbc:postgresql://db-acupinir.groupscholar.com:23947/postgres?sslmode=require"
export ELIGIBILITY_DB_USER="ralph"
export ELIGIBILITY_DB_PASSWORD="your-password"
export ELIGIBILITY_DB_SCHEMA="eligibility_oracle"

./scripts/run.sh --input data/sample-intake.csv --rules data/rules.txt --log-db --run-name "fall-2026-import"
```

Seed the production schema once (writes a sample run + failures):

```bash
./scripts/seed-db.py
```

`seed-db.py` accepts the same `ELIGIBILITY_DB_URL` and will strip a leading `jdbc:` prefix if present.

## Rules file format

```
[required]
fields=id,first_name,last_name,email,gpa,grad_year,status,dob

[require_if:status=conditional]
fields=review_notes

[review_missing]
fields=essay

[review_if:status=conditional]
reasons=manual_review

[require_any:contact]
fields=phone,guardian_email

[range:gpa]
min=2.5
max=4.0

[range:grad_year]
min=2024
max=2030

[allowed:status]
values=eligible,conditional

[disallowed:review_notes]
values=unknown,tbd

[date:dob]
earliest=1998-01-01
latest=2008-12-31

[pattern:email]
regex=^.+@.+\..+$

[unique]
fields=email

[aliases]
id=applicant_id,student_id
grad_year=graduation_year
guardian_email=parent_email,guardian_email_address
review_notes=review_note,notes
```

Value comparisons for allowed/disallowed lists are normalized (trimmed, lowercased, spaces replaced with underscores) to reduce casing mismatches.

## Tech
- Java (standard library)
- PostgreSQL (optional analytics logging)
- Shell script wrapper

## Project notes
Store new rules in `data/` and keep outputs in `reports/` for consistency. The Postgres JDBC driver lives in `lib/`.
