import pytest
from app.core.segment_compiler import SegmentCompiler, SegmentCompilerError


def test_basic_ast_compilation():
    ast = {
        "logic": "AND",
        "rules": [
            {"field": "country", "op": "eq", "value": "IN"},
            {"field": "last_active_at", "op": "lt", "value": "NOW() - INTERVAL '7 days'"},
            {"field": "metadata.city", "op": "eq", "value": "Mumbai"},
            {"field": "metadata.lifetime_spend", "op": "gte", "value": 5000},
        ],
    }

    sql = SegmentCompiler.compile_sql_string(ast)
    assert "users.country = 'IN'" in sql
    assert "users.metadata @> '{\"city\": \"Mumbai\"}'::jsonb" in sql
    assert "users.metadata ->> 'lifetime_spend'" in sql
    assert ">= 5000" in sql


def test_or_logic_and_in_operator():
    ast = {
        "logic": "OR",
        "rules": [
            {"field": "country", "op": "in", "value": ["US", "GB", "DE"]},
            {"field": "metadata.tier", "op": "eq", "value": "enterprise"},
        ],
    }

    sql = SegmentCompiler.compile_sql_string(ast)
    assert "users.country IN ('US', 'GB', 'DE')" in sql
    assert "OR" in sql
    assert "users.metadata @> '{\"tier\": \"enterprise\"}'::jsonb" in sql


def test_nested_ast_groups():
    ast = {
        "logic": "AND",
        "rules": [
            {"field": "country", "op": "eq", "value": "IN"},
            {
                "logic": "OR",
                "rules": [
                    {"field": "metadata.tier", "op": "eq", "value": "pro"},
                    {"field": "metadata.tier", "op": "eq", "value": "whale"},
                ],
            },
        ],
    }

    sql = SegmentCompiler.compile_sql_string(ast)
    assert "users.country = 'IN'" in sql
    assert "pro" in sql
    assert "whale" in sql


def test_unsupported_field():
    ast = {
        "logic": "AND",
        "rules": [
            {"field": "non_existent_column", "op": "eq", "value": "bad"},
        ],
    }
    with pytest.raises(SegmentCompilerError):
        SegmentCompiler.compile(ast)
