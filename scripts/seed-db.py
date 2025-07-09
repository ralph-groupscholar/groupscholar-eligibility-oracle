#!/usr/bin/env python3
import os
import sys
from datetime import datetime, timezone

import psycopg2


def get_env(name, fallback=None):
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return fallback
    return value


def main():
    url = get_env("ELIGIBILITY_DB_URL")
    if not url:
        print("Missing ELIGIBILITY_DB_URL.")
        return 1
    if url.startswith("jdbc:"):
        url = url.replace("jdbc:", "", 1)

    schema = get_env("ELIGIBILITY_DB_SCHEMA", "eligibility_oracle")
    user = get_env("ELIGIBILITY_DB_USER")
    password = get_env("ELIGIBILITY_DB_PASSWORD")

    conn = psycopg2.connect(url, user=user, password=password)
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.audit_runs (
            id BIGSERIAL PRIMARY KEY,
            run_at TIMESTAMPTZ NOT NULL,
            run_name TEXT,
            input_file TEXT,
            rules_file TEXT,
            total_applicants INT NOT NULL,
            eligible INT NOT NULL,
            ineligible INT NOT NULL,
            eligible_rate NUMERIC(6,4) NOT NULL,
            ineligible_rate NUMERIC(6,4) NOT NULL,
            id_field TEXT NOT NULL,
            failure_limit INT,
            failures_truncated BOOLEAN NOT NULL
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.audit_reason_counts (
            run_id BIGINT REFERENCES {schema}.audit_runs(id) ON DELETE CASCADE,
            reason TEXT NOT NULL,
            count INT NOT NULL
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.audit_reason_categories (
            run_id BIGINT REFERENCES {schema}.audit_runs(id) ON DELETE CASCADE,
            category TEXT NOT NULL,
            count INT NOT NULL
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.audit_failures (
            run_id BIGINT REFERENCES {schema}.audit_runs(id) ON DELETE CASCADE,
            applicant_id TEXT NOT NULL,
            reasons TEXT[] NOT NULL
        )
        """
    )

    cur.execute(
        f"SELECT id FROM {schema}.audit_runs WHERE run_name = %s",
        ("seed-sample",),
    )
    existing = cur.fetchone()
    if existing:
        conn.commit()
        conn.close()
        print("Seed already present.")
        return 0

    cur.execute(
        f"""
        INSERT INTO {schema}.audit_runs
        (run_at, run_name, input_file, rules_file, total_applicants, eligible, ineligible, eligible_rate, ineligible_rate, id_field, failure_limit, failures_truncated)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            datetime.now(timezone.utc),
            "seed-sample",
            "data/sample-intake.csv",
            "data/rules.txt",
            12,
            8,
            4,
            0.6667,
            0.3333,
            "applicant_id",
            None,
            False,
        ),
    )
    run_id = cur.fetchone()[0]

    reason_counts = {
        "missing:email": 2,
        "out_of_range:gpa": 1,
        "disallowed:status": 1,
    }
    for reason, count in reason_counts.items():
        cur.execute(
            f"INSERT INTO {schema}.audit_reason_counts (run_id, reason, count) VALUES (%s, %s, %s)",
            (run_id, reason, count),
        )

    category_counts = {
        "missing": 2,
        "out_of_range": 1,
        "disallowed": 1,
    }
    for category, count in category_counts.items():
        cur.execute(
            f"INSERT INTO {schema}.audit_reason_categories (run_id, category, count) VALUES (%s, %s, %s)",
            (run_id, category, count),
        )

    failures = [
        ("GS-1003", ["missing:email"]),
        ("GS-1006", ["out_of_range:gpa"]),
        ("GS-1009", ["missing:email", "disallowed:status"]),
        ("GS-1012", ["disallowed:status"]),
    ]
    for applicant_id, reasons in failures:
        cur.execute(
            f"INSERT INTO {schema}.audit_failures (run_id, applicant_id, reasons) VALUES (%s, %s, %s)",
            (run_id, applicant_id, reasons),
        )

    conn.commit()
    conn.close()
    print("Seed inserted.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
