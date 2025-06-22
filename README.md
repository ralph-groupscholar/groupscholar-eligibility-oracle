# Group Scholar Eligibility Oracle

Eligibility Oracle is a lightweight Java CLI that audits applicant intake CSVs against a human-readable rules file. It flags missing requirements, out-of-range scores, disallowed statuses, and date eligibility windows, then produces an operational summary for scholarship ops teams.

## Features
- Parses CSV intakes without external dependencies
- Rules file supports required fields, numeric ranges, allowed values, and date windows
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

## Rules file format

```
[required]
fields=id,first_name,last_name,email,gpa,grad_year,status,dob

[range:gpa]
min=2.5
max=4.0

[range:grad_year]
min=2024
max=2030

[allowed:status]
values=eligible,conditional

[date:dob]
earliest=1998-01-01
latest=2008-12-31
```

## Tech
- Java (standard library)
- Shell script wrapper

## Project notes
Store new rules in `data/` and keep outputs in `reports/` for consistency.
