import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Union
from sqlalchemy import (
    and_,
    or_,
    cast,
    text,
    select,
    Numeric,
    String,
    Select,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.elements import ClauseElement

from app.models.users import User
from app.schemas.segments import SegmentFilterAST, SegmentRule


class SegmentCompilerError(Exception):
    pass


class SegmentCompiler:
    """
    Compiles a dynamic JSON AST into parameterized SQLAlchemy 2.0 expressions
    targeting PostgreSQL 16 with GIN index and JSONB path optimization.
    """

    ALLOWED_COLUMNS = {
        "id": User.id,
        "external_user_id": User.external_user_id,
        "email": User.email,
        "phone_e164": User.phone_e164,
        "fcm_token": User.fcm_token,
        "country": User.country,
        "last_active_at": User.last_active_at,
        "created_at": User.created_at,
    }

    INTERVAL_REGEX = re.compile(
        r"NOW\(\)\s*-\s*INTERVAL\s*'(\d+)\s*(day|days|hour|hours|minute|minutes|week|weeks|month|months)'",
        re.IGNORECASE,
    )

    @classmethod
    def compile(cls, ast: Union[SegmentFilterAST, dict]) -> Select:
        """Translates an AST into a SQLAlchemy select(User) statement."""
        if isinstance(ast, dict):
            ast = SegmentFilterAST.model_validate(ast)
        clause = cls._compile_node(ast)
        stmt = select(User)
        if clause is not None:
            stmt = stmt.where(clause)
        return stmt

    @classmethod
    def compile_sql_string(cls, ast: Union[SegmentFilterAST, dict]) -> str:
        """Returns the compiled, formatted SQL string with PostgreSQL dialect."""
        stmt = cls.compile(ast)
        try:
            compiled = stmt.compile(
                dialect=postgresql.dialect(),
                compile_kwargs={"literal_binds": True},
            )
            return str(compiled)
        except Exception:
            compiled = stmt.compile(dialect=postgresql.dialect())
            return str(compiled)

    @classmethod
    def _compile_node(cls, node: Union[SegmentFilterAST, SegmentRule]) -> ClauseElement:
        if isinstance(node, SegmentRule):
            return cls._compile_rule(node)

        # SegmentFilterAST node
        clauses = []
        for child in node.rules:
            if isinstance(child, dict):
                if "field" in child and "op" in child:
                    child_node = SegmentRule.model_validate(child)
                else:
                    child_node = SegmentFilterAST.model_validate(child)
            else:
                child_node = child
            compiled_child = cls._compile_node(child_node)
            if compiled_child is not None:
                clauses.append(compiled_child)

        if not clauses:
            return text("1=1")

        if node.logic.upper() == "OR":
            return or_(*clauses)
        return and_(*clauses)

    @classmethod
    def _resolve_relative_time(cls, val: Any) -> Any:
        if isinstance(val, str):
            match = cls.INTERVAL_REGEX.match(val.strip())
            if match:
                amount = int(match.group(1))
                unit = match.group(2).lower()
                now = datetime.now(timezone.utc)
                if "minute" in unit:
                    return now - timedelta(minutes=amount)
                elif "hour" in unit:
                    return now - timedelta(hours=amount)
                elif "day" in unit:
                    return now - timedelta(days=amount)
                elif "week" in unit:
                    return now - timedelta(weeks=amount)
                elif "month" in unit:
                    return now - timedelta(days=amount * 30)
            # Try ISO string parsing
            try:
                return datetime.fromisoformat(val.replace("Z", "+00:00"))
            except ValueError:
                pass
        return val

    @classmethod
    def _compile_rule(cls, rule: SegmentRule) -> ClauseElement:
        field_name = rule.field.strip()
        op = rule.op.lower()
        val = rule.value

        # Check for metadata JSONB field
        if field_name.startswith("metadata."):
            key_path = field_name.split(".", 1)[1]
            return cls._build_jsonb_filter(key_path, op, val)

        # Check for standard model column
        if field_name in cls.ALLOWED_COLUMNS:
            col = cls.ALLOWED_COLUMNS[field_name]
            # Handle relative timestamps if datetime column
            if field_name in ("last_active_at", "created_at"):
                val = cls._resolve_relative_time(val)
            return cls._apply_operator(col, op, val)

        raise SegmentCompilerError(f"Field '{field_name}' is not queryable. Supported fields: {list(cls.ALLOWED_COLUMNS.keys())} or metadata.*")

    @classmethod
    def _build_jsonb_filter(cls, key: str, op: str, val: Any) -> ClauseElement:
        # If op is eq and value is simple, leverage JSONB containment operator (@>) for GIN index acceleration
        # e.g., metadata @> '{"city": "Mumbai"}'::jsonb
        if op == "eq" and isinstance(val, (str, int, float, bool)):
            payload_json = json.dumps({key: val})
            return User.metadata_.op("@>")(text(f"'{payload_json}'::jsonb"))
        
        # Astext extraction for comparisons
        extracted_text = User.metadata_[key].astext

        if isinstance(val, (int, float)):
            col_target = cast(extracted_text, Numeric)
        elif isinstance(val, bool):
            col_target = cast(extracted_text, String)
            val = str(val).lower()
        else:
            col_target = extracted_text

        return cls._apply_operator(col_target, op, val)

    @classmethod
    def _apply_operator(cls, col: Any, op: str, val: Any) -> ClauseElement:
        if op == "eq":
            return col == val
        elif op == "neq":
            return col != val
        elif op == "gt":
            return col > val
        elif op == "gte":
            return col >= val
        elif op == "lt":
            return col < val
        elif op == "lte":
            return col <= val
        elif op == "in":
            if not isinstance(val, (list, tuple, set)):
                raise SegmentCompilerError(f"'in' operator requires a list or array of values, got {type(val)}")
            return col.in_(val)
        elif op == "contains":
            if isinstance(val, str):
                return col.ilike(f"%{val}%")
            return col.contains(val)
        else:
            raise SegmentCompilerError(f"Unsupported operator '{op}'")
