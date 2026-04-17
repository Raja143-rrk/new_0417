from app.agents.migration_agent import generate_transformed_sql
from app.agents import rag_agent
from app.services.history_service import get_history_match, save_history
from app.services.rule_engine import apply_rules
from app.services.sql_validator import validate


SQL_FIX_RETRY_ATTEMPTS = 2


def _standardize_sql(sql_text: str) -> str:
    return " ".join(str(sql_text or "").split())


def _failed_validation(error_text: str) -> dict:
    return {
        "is_valid": False,
        "errors": [str(error_text or "Unknown validation error.")],
        "warnings": [],
    }


def migrate_sql(
    input_sql: str,
    source: str,
    target: str,
    object_type: str = "table",
    object_name: str = "",
) -> dict:
    if not str(input_sql or "").strip():
        return {
            "status": "error",
            "output_sql": "",
            "validation": {
                "is_valid": False,
                "errors": ["SQL input is empty."],
                "warnings": [],
            },
            "suggestions": [],
            "source": "error",
            "applied_rules": [],
        }

    history_match = get_history_match(input_sql, source, target, object_type, object_name)
    if history_match:
        return {
            "status": "success",
            "output_sql": history_match.get("output_sql", ""),
            "validation": history_match.get("validation") or {"is_valid": True, "errors": [], "warnings": []},
            "suggestions": [],
            "source": "history",
            "applied_rules": [],
            "corrected_sql": history_match.get("output_sql", ""),
            "original_error": history_match.get("error"),
            "retry_count": len(history_match.get("fix_attempts") or []),
            "fix_attempts": history_match.get("fix_attempts") or [],
            "history_key": history_match.get("history_key"),
        }

    existing_output = generate_transformed_sql(
        input_sql,
        source,
        target,
        object_type,
        object_name,
    )
    rule_result = apply_rules(existing_output, source, target)
    final_sql = rule_result["sql"]
    suggestions = []
    fix_attempts = []
    original_error = None
    seen_sql = {_standardize_sql(final_sql)}
    validation = {"is_valid": False, "errors": ["Validation was not executed."], "warnings": []}

    for attempt in range(SQL_FIX_RETRY_ATTEMPTS + 1):
        validation = validate(final_sql, target, object_type)
        if validation.get("is_valid"):
            break
        if original_error is None:
            original_error = "; ".join(validation.get("errors") or ["Validation failed"])
        try:
            suggestions = rag_agent.analyze(
                input_sql,
                final_sql,
                validation,
                source,
                target,
                object_type,
            )
        except Exception:
            suggestions = []
        if attempt >= SQL_FIX_RETRY_ATTEMPTS:
            break
        error_message = "; ".join(validation.get("errors") or ["Validation failed"])
        try:
            corrected_sql = rag_agent.fix_sql(
                final_sql,
                error_message,
                source,
                target,
                object_type,
            )
        except Exception:
            corrected_sql = final_sql
        changed = _standardize_sql(corrected_sql) != _standardize_sql(final_sql)
        fix_attempts.append(
            {
                "attempt": attempt + 1,
                "error": error_message,
                "original_sql": final_sql,
                "corrected_sql": corrected_sql,
                "changed": changed,
            }
        )
        if not changed or _standardize_sql(corrected_sql) in seen_sql:
            break
        seen_sql.add(_standardize_sql(corrected_sql))
        final_sql = corrected_sql

    if validation.get("is_valid"):
        save_history(
            input_sql,
            final_sql,
            source,
            target,
            object_type,
            object_name,
            validation,
            status="SUCCESS",
            error=None,
            fix_attempts=fix_attempts,
        )
    else:
        save_history(
            input_sql,
            final_sql,
            source,
            target,
            object_type,
            object_name,
            validation if validation else _failed_validation(original_error or "Validation failed"),
            status="ERROR",
            error=original_error or "; ".join(validation.get("errors") or ["Validation failed"]),
            fix_attempts=fix_attempts,
        )
    return {
        "status": "success" if validation.get("is_valid") else "partial",
        "output_sql": final_sql,
        "validation": validation,
        "suggestions": suggestions,
        "source": "existing_logic",
        "applied_rules": rule_result.get("applied_rules") or [],
        "corrected_sql": fix_attempts[-1]["corrected_sql"] if fix_attempts else final_sql,
        "original_error": original_error,
        "retry_count": len(fix_attempts),
        "fix_attempts": fix_attempts,
    }
