"""The user-facing SQL sandbox.

The admin console has had a SQL editor for a while and it is safe there for a
reason that does not transfer: an admin is *supposed* to see every org. Handing
the same thing to a customer would let them read every other customer's traces
with one `WHERE 1=1`.

Scoping here is structural — the query never names a real table — so these tests
are mostly about proving there is no syntax that reaches one.
"""
from __future__ import annotations

import pytest
from fastapi import HTTPException

from routes.sql import (
    MAX_QUERY_CHARS,
    SCHEMA_DOC,
    _readable_error,
    _virtual_tables,
    build_query,
    check_tables,
    clean_sql,
)

ALLOWED = _virtual_tables()


# ══ Tenancy: the whole point ═════════════════════════════════════════════════

def test_a_real_table_cannot_be_named():
    """`fluiq.traces` is unfiltered. Nothing downstream would add the org filter
    for it, so it must not resolve."""
    with pytest.raises(HTTPException) as exc:
        check_tables("SELECT * FROM fluiq.traces", ALLOWED)
    assert "Unknown table" in exc.value.detail


def test_system_tables_cannot_be_named():
    """system.tables would enumerate every other tenant's data shape."""
    for table in ("system.tables", "system.query_log", "system.parts"):
        with pytest.raises(HTTPException):
            check_tables(f"SELECT * FROM {table}", ALLOWED)


def test_a_join_target_is_checked_too():
    """Checking only the FROM would let a JOIN reach anything."""
    with pytest.raises(HTTPException):
        check_tables(
            "SELECT * FROM traces JOIN fluiq.evaluations ON 1=1", ALLOWED,
        )


def test_every_virtual_table_pins_the_organization():
    """The guarantee this module rests on: none of these can be read unfiltered."""
    for name, body in ALLOWED.items():
        assert "organization_id = {org_id:UUID}" in body, name


def test_the_org_id_is_a_bound_parameter_not_interpolated():
    """String-formatting a tenant id into SQL is how a tenancy bug becomes an
    injection bug as well."""
    for body in ALLOWED.values():
        assert "{org_id:UUID}" in body


def test_the_prepared_query_defines_every_virtual_table():
    prepared = build_query("SELECT 1", ALLOWED)
    for name in ALLOWED:
        assert f"{name} AS (" in prepared


# ══ Read-only ════════════════════════════════════════════════════════════════

@pytest.mark.parametrize(
    "statement",
    [
        "DROP TABLE traces",
        "INSERT INTO traces VALUES (1)",
        "ALTER TABLE traces ADD COLUMN x Int",
        "TRUNCATE TABLE traces",
        "CREATE TABLE x (a Int)",
        "OPTIMIZE TABLE traces",
        "SYSTEM SHUTDOWN",
    ],
)
def test_only_select_and_with_may_start_a_query(statement):
    with pytest.raises(HTTPException, match="Read-only"):
        clean_sql(statement)


def test_select_and_with_are_accepted():
    assert clean_sql("SELECT 1")
    assert clean_sql("WITH x AS (SELECT 1) SELECT * FROM x")


def test_a_second_statement_is_refused():
    with pytest.raises(HTTPException, match="single statement"):
        clean_sql("SELECT 1; DROP TABLE traces")


def test_a_statement_hidden_behind_a_comment_is_still_caught():
    """`SELECT 1 --\\nDROP ...` would pass a naive semicolon check, because the
    comment hides the semicolon from it. Comments are stripped first."""
    with pytest.raises(HTTPException, match="single statement"):
        clean_sql("SELECT 1 -- harmless\n; DROP TABLE traces")


def test_a_block_comment_cannot_hide_a_statement():
    with pytest.raises(HTTPException, match="single statement"):
        clean_sql("SELECT 1 /* nothing to see */ ; DROP TABLE traces")


def test_a_trailing_semicolon_is_fine():
    """People end statements with one; refusing that would be pedantry."""
    assert clean_sql("SELECT 1;") == "SELECT 1"


def test_an_empty_query_is_rejected():
    for empty in ("", "   ", "-- just a comment"):
        with pytest.raises(HTTPException, match="Empty query"):
            clean_sql(empty)


def test_an_oversized_query_is_rejected():
    with pytest.raises(HTTPException, match="exceeds"):
        clean_sql("SELECT " + "1," * MAX_QUERY_CHARS)


# ══ Usable for the questions people actually have ════════════════════════════

def test_the_virtual_tables_can_be_joined_to_each_other():
    """"which model regressed on Tuesday" needs traces joined to evaluations."""
    sql = clean_sql("""
        SELECT t.model, avg(e.score)
        FROM traces AS t JOIN evaluations AS e ON t.trace_id = e.trace_id
        GROUP BY t.model
    """)
    check_tables(sql, ALLOWED)   # must not raise


def test_a_user_defined_cte_is_allowed():
    sql = clean_sql("WITH slow AS (SELECT * FROM traces WHERE latency > 5) "
                    "SELECT count() FROM slow")
    check_tables(sql, ALLOWED)


def test_a_user_cte_is_folded_into_the_same_with_clause():
    """Two WITH keywords is a syntax error, so the user's is merged rather than
    stacked."""
    prepared = build_query(
        "WITH slow AS (SELECT * FROM traces) SELECT * FROM slow", ALLOWED,
    )
    assert prepared.lower().count("with ") == 1


def test_a_user_cte_may_not_reach_a_real_table_either():
    with pytest.raises(HTTPException):
        check_tables(
            "WITH sneaky AS (SELECT * FROM fluiq.traces) SELECT * FROM sneaky",
            ALLOWED,
        )


def test_table_names_are_matched_case_insensitively():
    check_tables("SELECT * FROM TRACES", ALLOWED)


def test_the_documented_schema_matches_the_virtual_tables():
    """The editor's help panel is served from SCHEMA_DOC; a table in one and not
    the other means someone reads a column that does not exist."""
    assert set(SCHEMA_DOC) == set(ALLOWED)


def test_every_documented_column_appears_in_its_table():
    for name, columns in SCHEMA_DOC.items():
        body = ALLOWED[name]
        for column in columns:
            assert column in body, f"{name}.{column} documented but not selected"


# ══ Errors a person can act on ═══════════════════════════════════════════════

def test_an_unknown_table_error_lists_the_available_ones():
    with pytest.raises(HTTPException) as exc:
        check_tables("SELECT * FROM orders", ALLOWED)
    for name in ALLOWED:
        assert name in exc.value.detail


def test_engine_errors_are_trimmed_of_the_injected_prefix():
    """A syntax error quoting three hundred characters of injected CTEs would
    bury the one line the author actually wrote."""
    raw = "Syntax error: failed at position 12 (query: WITH traces AS (SELECT ...long...))"
    assert "query:" not in _readable_error(raw)
    assert "Syntax error" in _readable_error(raw)


def test_an_empty_engine_message_still_says_something():
    assert _readable_error("") == "Query failed."
