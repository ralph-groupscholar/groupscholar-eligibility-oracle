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
            warning_applicants INT,
            warning_rate NUMERIC(6,4),
            review_count INT,
            review_rate NUMERIC(6,4),
            id_field TEXT NOT NULL,
            failure_limit INT,
            failures_truncated BOOLEAN NOT NULL,
            review_limit INT,
            reviews_truncated BOOLEAN
        )
        """
    )
    cur.execute(f"ALTER TABLE {schema}.audit_runs ADD COLUMN IF NOT EXISTS warning_applicants INT")
    cur.execute(f"ALTER TABLE {schema}.audit_runs ADD COLUMN IF NOT EXISTS warning_rate NUMERIC(6,4)")
    cur.execute(f"ALTER TABLE {schema}.audit_runs ADD COLUMN IF NOT EXISTS review_count INT")
    cur.execute(f"ALTER TABLE {schema}.audit_runs ADD COLUMN IF NOT EXISTS review_rate NUMERIC(6,4)")
    cur.execute(f"ALTER TABLE {schema}.audit_runs ADD COLUMN IF NOT EXISTS review_limit INT")
    cur.execute(f"ALTER TABLE {schema}.audit_runs ADD COLUMN IF NOT EXISTS reviews_truncated BOOLEAN")
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
        CREATE TABLE IF NOT EXISTS {schema}.audit_warning_counts (
            run_id BIGINT REFERENCES {schema}.audit_runs(id) ON DELETE CASCADE,
            warning TEXT NOT NULL,
            count INT NOT NULL
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.audit_warning_categories (
            run_id BIGINT REFERENCES {schema}.audit_runs(id) ON DELETE CASCADE,
            category TEXT NOT NULL,
            count INT NOT NULL
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.audit_review_counts (
            run_id BIGINT REFERENCES {schema}.audit_runs(id) ON DELETE CASCADE,
            reason TEXT NOT NULL,
            count INT NOT NULL
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.audit_field_completeness (
            run_id BIGINT REFERENCES {schema}.audit_runs(id) ON DELETE CASCADE,
            field_name TEXT NOT NULL,
            missing_count INT NOT NULL,
            missing_rate NUMERIC(6,4) NOT NULL
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
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.audit_reviews (
            run_id BIGINT REFERENCES {schema}.audit_runs(id) ON DELETE CASCADE,
            applicant_id TEXT NOT NULL,
            reasons TEXT[] NOT NULL
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.audit_segments (
            run_id BIGINT REFERENCES {schema}.audit_runs(id) ON DELETE CASCADE,
            segment_field TEXT NOT NULL,
            segment_value TEXT NOT NULL,
            total INT NOT NULL,
            eligible INT NOT NULL,
            ineligible INT NOT NULL,
            eligible_rate NUMERIC(6,4) NOT NULL,
            ineligible_rate NUMERIC(6,4) NOT NULL
        )
        """
    )

    cur.execute(
        f"SELECT id FROM {schema}.audit_runs WHERE run_name = %s",
        ("seed-sample",),
    )
    existing = cur.fetchone()
    if existing:
        run_id = existing[0]
        cur.execute(
            f"SELECT 1 FROM {schema}.audit_field_completeness WHERE run_id = %s LIMIT 1",
            (run_id,),
        )
        completeness_exists = cur.fetchone() is not None
        cur.execute(
            f"SELECT 1 FROM {schema}.audit_segments WHERE run_id = %s LIMIT 1",
            (run_id,),
        )
        segments_exists = cur.fetchone() is not None
        cur.execute(
            f"SELECT 1 FROM {schema}.audit_review_counts WHERE run_id = %s LIMIT 1",
            (run_id,),
        )
        review_counts_exist = cur.fetchone() is not None
        cur.execute(
            f"SELECT 1 FROM {schema}.audit_reviews WHERE run_id = %s LIMIT 1",
            (run_id,),
        )
        review_flags_exist = cur.fetchone() is not None
        if completeness_exists and segments_exists and review_counts_exist and review_flags_exist:
            cur.execute(
                f"""
                UPDATE {schema}.audit_runs
                SET warning_applicants = COALESCE(warning_applicants, %s),
                    warning_rate = COALESCE(warning_rate, %s),
                    review_count = COALESCE(review_count, %s),
                    review_rate = COALESCE(review_rate, %s),
                    review_limit = COALESCE(review_limit, %s),
                    reviews_truncated = COALESCE(reviews_truncated, %s)
                WHERE id = %s
                """,
                (3, 0.2500, 4, 0.3333, None, False, run_id),
            )
            conn.commit()
            conn.close()
            print("Seed already present.")
            return 0
        if not completeness_exists:
            completeness_rows = [
                ("email", 2, 0.1667),
                ("phone", 5, 0.4167),
                ("guardian_email", 7, 0.5833),
                ("review_notes", 3, 0.2500),
            ]
            for field_name, missing_count, missing_rate in completeness_rows:
                cur.execute(
                    f"""
                    INSERT INTO {schema}.audit_field_completeness
                    (run_id, field_name, missing_count, missing_rate)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (run_id, field_name, missing_count, missing_rate),
                )
        if not segments_exists:
            segments = [
                ("status", "eligible", 6, 6, 0, 1.0000, 0.0000),
                ("status", "conditional", 4, 2, 2, 0.5000, 0.5000),
                ("status", "ineligible", 2, 0, 2, 0.0000, 1.0000),
            ]
            for segment in segments:
                cur.execute(
                    f"""
                    INSERT INTO {schema}.audit_segments
                    (run_id, segment_field, segment_value, total, eligible, ineligible, eligible_rate, ineligible_rate)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (run_id, *segment),
                )
        if not review_counts_exist:
            review_counts = {
                "review_missing:review_notes": 3,
                "review_flag:manual_review": 2,
            }
            for reason, count in review_counts.items():
                cur.execute(
                    f"INSERT INTO {schema}.audit_review_counts (run_id, reason, count) VALUES (%s, %s, %s)",
                    (run_id, reason, count),
                )
        if not review_flags_exist:
            review_flags = [
                ("GS-1002", ["review_missing:review_notes"]),
                ("GS-1003", ["review_missing:review_notes", "review_flag:manual_review"]),
                ("GS-1008", ["review_flag:manual_review"]),
                ("GS-1011", ["review_missing:review_notes"]),
            ]
            for applicant_id, reasons in review_flags:
                cur.execute(
                    f"INSERT INTO {schema}.audit_reviews (run_id, applicant_id, reasons) VALUES (%s, %s, %s)",
                    (run_id, applicant_id, reasons),
                )
        cur.execute(
            f"""
            UPDATE {schema}.audit_runs
            SET warning_applicants = COALESCE(warning_applicants, %s),
                warning_rate = COALESCE(warning_rate, %s),
                review_count = COALESCE(review_count, %s),
                review_rate = COALESCE(review_rate, %s),
                review_limit = COALESCE(review_limit, %s),
                reviews_truncated = COALESCE(reviews_truncated, %s)
            WHERE id = %s
            """,
            (3, 0.2500, 4, 0.3333, None, False, run_id),
        )
        conn.commit()
        conn.close()
        if completeness_exists:
            print("Seed segments inserted.")
        elif segments_exists:
            print("Seed field completeness inserted.")
        else:
            print("Seed segments and field completeness inserted.")
        return 0

    cur.execute(
        f"""
        INSERT INTO {schema}.audit_runs
        (run_at, run_name, input_file, rules_file, total_applicants, eligible, ineligible, eligible_rate, ineligible_rate, warning_applicants, warning_rate, review_count, review_rate, id_field, failure_limit, failures_truncated, review_limit, reviews_truncated)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            3,
            0.2500,
            4,
            0.3333,
            "applicant_id",
            None,
            False,
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

    review_counts = {
        "review_missing:review_notes": 3,
        "review_flag:manual_review": 2,
    }
    for reason, count in review_counts.items():
        cur.execute(
            f"INSERT INTO {schema}.audit_review_counts (run_id, reason, count) VALUES (%s, %s, %s)",
            (run_id, reason, count),
        )

    completeness_rows = [
        ("email", 2, 0.1667),
        ("phone", 5, 0.4167),
        ("guardian_email", 7, 0.5833),
        ("review_notes", 3, 0.2500),
    ]
    for field_name, missing_count, missing_rate in completeness_rows:
        cur.execute(
            f"""
            INSERT INTO {schema}.audit_field_completeness
            (run_id, field_name, missing_count, missing_rate)
            VALUES (%s, %s, %s, %s)
            """,
            (run_id, field_name, missing_count, missing_rate),
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

    review_flags = [
        ("GS-1002", ["review_missing:review_notes"]),
        ("GS-1003", ["review_missing:review_notes", "review_flag:manual_review"]),
        ("GS-1008", ["review_flag:manual_review"]),
        ("GS-1011", ["review_missing:review_notes"]),
    ]
    for applicant_id, reasons in review_flags:
        cur.execute(
            f"INSERT INTO {schema}.audit_reviews (run_id, applicant_id, reasons) VALUES (%s, %s, %s)",
            (run_id, applicant_id, reasons),
        )

    segments = [
        ("status", "eligible", 6, 6, 0, 1.0000, 0.0000),
        ("status", "conditional", 4, 2, 2, 0.5000, 0.5000),
        ("status", "ineligible", 2, 0, 2, 0.0000, 1.0000),
    ]
    for segment in segments:
        cur.execute(
            f"""
            INSERT INTO {schema}.audit_segments
            (run_id, segment_field, segment_value, total, eligible, ineligible, eligible_rate, ineligible_rate)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (run_id, *segment),
        )

    conn.commit()
    conn.close()
    print("Seed inserted.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
