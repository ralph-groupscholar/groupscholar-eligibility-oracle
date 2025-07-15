#!/bin/sh
set -e
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

TEXT_OUTPUT=$(./scripts/run.sh --input data/sample-intake.csv --rules data/rules.txt)
echo "$TEXT_OUTPUT" | grep -q "Eligibility Audit Summary"
echo "$TEXT_OUTPUT" | grep -q "Total applicants"

JSON_OUTPUT=$(./scripts/run.sh --input data/sample-intake.csv --rules data/rules.txt --format json)
echo "$JSON_OUTPUT" | grep -q '"totalApplicants"'

echo "Tests passed."
