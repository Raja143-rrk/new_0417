import json
import os
import re
import threading
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - optional dependency
    OpenAI = None

from app.config import load_environment
from app.services.rag_service import build_rag_context


DEFAULT_RAG_AGENT_MODEL = "openai/gpt-oss-120b"

_SUGGESTION_LOCK = threading.Lock()
_SUGGESTION_DIR = Path(__file__).resolve().parents[2] / "app_data"
_SUGGESTION_FILE = _SUGGESTION_DIR / "rag_rule_suggestions.json"

load_environment()


def _ensure_suggestion_file():
    _SUGGESTION_DIR.mkdir(parents=True, exist_ok=True)
    if not _SUGGESTION_FILE.exists():
        _SUGGESTION_FILE.write_text("[]", encoding="utf-8")


def _read_suggestions() -> list:
    _ensure_suggestion_file()
    try:
        data = json.loads(_SUGGESTION_FILE.read_text(encoding="utf-8") or "[]")
    except json.JSONDecodeError:
        data = []
    return data if isinstance(data, list) else []


@lru_cache(maxsize=1)
def _get_client() -> Any:
    if OpenAI is None:
        return None
    api_key = (
        os.getenv("RAG_AGENT_API_KEY")
        or os.getenv("GROQ_API_KEY")
        or os.getenv("OPENAI_API_KEY")
    )
    if not api_key:
        return None
    base_url = os.getenv("RAG_AGENT_BASE_URL") or "https://api.groq.com/openai/v1"
    kwargs = {"api_key": api_key}
    if base_url:
        kwargs["base_url"] = base_url
    return OpenAI(**kwargs)


def get_rag_agent_runtime_info() -> dict[str, str | bool]:
    return {
        "configured": _get_client() is not None,
        "provider": "custom_openai_compatible" if os.getenv("RAG_AGENT_BASE_URL") else "openai",
        "model": os.getenv("RAG_AGENT_MODEL") or DEFAULT_RAG_AGENT_MODEL,
    }


def _normalize_suggestion_record(
    suggestion: dict,
    source: str,
    target: str,
    object_type: str,
    reason: str,
) -> dict:
    return {
        "name": str(suggestion.get("name") or "manual_review_required"),
        "pattern": str(suggestion.get("pattern") or ""),
        "replacement": str(suggestion.get("replacement") or ""),
        "description": str(
            suggestion.get("description")
            or "Review this validation issue and add a deterministic rule if it repeats."
        ),
        "reason": reason,
        "source": source,
        "target": target,
        "object_type": object_type,
        "approved": False,
        "created_at": datetime.utcnow().isoformat(),
    }


def _extract_json_array(text: str) -> list[dict]:
    raw_text = str(text or "").strip()
    if not raw_text:
        return []
    if raw_text.startswith("```"):
        raw_text = raw_text.replace("```json", "").replace("```", "").strip()
    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError:
        start = raw_text.find("[")
        end = raw_text.rfind("]")
        if start == -1 or end == -1 or end <= start:
            return []
        try:
            data = json.loads(raw_text[start : end + 1])
        except json.JSONDecodeError:
            return []
    return data if isinstance(data, list) else []


def _build_llm_prompt(
    input_sql: str,
    output_sql: str,
    validation: dict,
    source: str,
    target: str,
    object_type: str,
) -> str:
    rag_context = build_rag_context(source, target)
    return f"""
You are a SQL migration learning agent.
Analyze validation issues and suggest deterministic regex post-processing rules only when they are safe.

Rules:
- Return standard sql language format only.
- Each item must contain: name, pattern, replacement, description.
- Suggest at most 3 rules.
- Do not suggest semantic rewrites that require AST understanding, data awareness, or procedural logic conversion.
- Do not suggest rules for MERGE rewrites, cursor rewrites, generic procedure-body logic, or broad SQL restructuring.
- If no safe regex rule is appropriate, return [].

Migration:
- Source DB: {source}
- Target DB: {target}
- Object type: {object_type}

Validation:
{json.dumps(validation or {}, ensure_ascii=True, indent=2)}

Input SQL:
{str(input_sql or "")[:3000]}

Output SQL:
{str(output_sql or "")[:3000]}

RAG context:
{rag_context[:4000]}
""".strip()


def _extract_sql_text(text: str) -> str:
    raw_text = str(text or "").strip()
    if not raw_text:
        return ""
    if raw_text.startswith("```"):
        raw_text = re.sub(r"^```[A-Za-z0-9_+-]*\s*", "", raw_text)
        raw_text = re.sub(r"\s*```$", "", raw_text)
    return raw_text.strip()


def _build_fix_sql_prompt(
    sql_text: str,
    error_message: str,
    source: str,
    target: str,
    object_type: str,
) -> str:
    rag_context = build_rag_context(source, target)
    return f"""
You are a SQL migration repair agent.
Return corrected SQL only. Do not return explanations, markdown, or JSON.

Repair goals:
- Fix syntax errors.
- Fix missing DELIMITER wrappers for MySQL routines when needed.
- Fix BEGIN...END wrappers.
- Fix DECLARE placement for MySQL stored procedures.
- Fix source-to-target dialect issues between {source} and {target}.

Constraints:
- Preserve business logic.
- Do not add commentary.
- If no safe correction is possible, return the input SQL unchanged.

Migration:
- Source DB: {source}
- Target DB: {target}
- Object type: {object_type}

Error:
{error_message}

SQL:
{str(sql_text or "")[:12000]}

RAG context:
{rag_context[:5000]}
""".strip()


def _build_llm_suggestions(
    input_sql: str,
    output_sql: str,
    validation: dict,
    source: str,
    target: str,
    object_type: str,
) -> list[dict]:
    client = _get_client()
    if client is None:
        return []
    model = os.getenv("RAG_AGENT_MODEL") or DEFAULT_RAG_AGENT_MODEL
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You return strict JSON for safe deterministic SQL regex suggestions only."
                    ),
                },
                {
                    "role": "user",
                    "content": _build_llm_prompt(
                        input_sql,
                        output_sql,
                        validation,
                        source,
                        target,
                        object_type,
                    ),
                },
            ],
            temperature=0,
        )
    except Exception:
        return []
    content = ""
    if response and response.choices:
        content = response.choices[0].message.content or ""
    parsed = _extract_json_array(content)
    suggestions = []
    reasons = (validation or {}).get("errors") or []
    if not reasons:
        reasons = (validation or {}).get("warnings") or [""]
    default_reason = reasons[0] if reasons else ""
    for item in parsed:
        if not isinstance(item, dict):
            continue
        if not item.get("pattern"):
            continue
        suggestions.append(
            _normalize_suggestion_record(
                item,
                source,
                target,
                object_type,
                str(item.get("reason") or default_reason),
            )
        )
    return suggestions


def _build_llm_sql_fix(
    sql_text: str,
    error_message: str,
    source: str,
    target: str,
    object_type: str,
) -> str:
    client = _get_client()
    if client is None:
        return str(sql_text or "")
    model = os.getenv("RAG_AGENT_MODEL") or DEFAULT_RAG_AGENT_MODEL
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": "You return corrected SQL only with no markdown and no commentary.",
                },
                {
                    "role": "user",
                    "content": _build_fix_sql_prompt(
                        sql_text,
                        error_message,
                        source,
                        target,
                        object_type,
                    ),
                },
            ],
            temperature=0,
        )
    except Exception:
        return str(sql_text or "")
    content = ""
    if response and response.choices:
        content = response.choices[0].message.content or ""
    fixed_sql = _extract_sql_text(content)
    return fixed_sql or str(sql_text or "")


def _build_suggestions(validation: dict, output_sql: str, source: str, target: str) -> list[dict]:
    suggestions = []
    errors = validation.get("errors") or []
    warnings = validation.get("warnings") or []
    for message in errors + warnings:
        lowered = str(message).lower()
        suggestion = {
            "source": source,
            "target": target,
            "reason": message,
            "approved": False,
            "created_at": datetime.utcnow().isoformat(),
        }
        if "escaped formatting" in lowered or "line-continuation" in lowered:
            suggestion.update(
                {
                    "name": "normalize_escaped_sql_formatting",
                    "pattern": r"\\+\n\s*|\\n|\\r|\\t",
                    "replacement": "\n",
                    "description": "Normalize escaped formatting artifacts into standard SQL whitespace.",
                }
            )
        elif "auto_increment" in lowered and str(target).lower() == "snowflake":
            suggestion.update(
                {
                    "name": "snowflake_auto_increment_keyword",
                    "pattern": r"(?is)\bauto_increment\b",
                    "replacement": "AUTOINCREMENT",
                    "description": "Map MySQL AUTO_INCREMENT to Snowflake AUTOINCREMENT.",
                }
            )
        elif "create or replace" in lowered and str(target).lower() == "mysql":
            suggestion.update(
                {
                    "name": "mysql_remove_create_or_replace_routine",
                    "pattern": r"(?is)\bcreate\s+or\s+replace\s+(function|procedure)\b",
                    "replacement": r"CREATE \1",
                    "description": "MySQL routines should use CREATE FUNCTION/PROCEDURE without OR REPLACE.",
                }
            )
        elif "delimiter" in lowered and str(target).lower() == "mysql":
            suggestion.update(
                {
                    "name": "mysql_remove_delimiter_wrappers",
                    "pattern": r"(?im)^\s*delimiter\s+\S+\s*$",
                    "replacement": "",
                    "description": "Remove client-side DELIMITER directives before executing MySQL routine SQL through the driver.",
                }
            )
        else:
            suggestion.update(
                {
                    "name": "manual_review_required",
                    "pattern": "",
                    "replacement": "",
                    "description": "Review this validation issue and add a deterministic rule if it repeats.",
                }
            )
        suggestions.append(suggestion)
    return suggestions


def analyze(input_sql: str, output_sql: str, validation: dict, source: str, target: str, object_type: str = "") -> list[dict]:
    suggestions = _build_llm_suggestions(
        input_sql,
        output_sql,
        validation or {},
        source,
        target,
        object_type,
    )
    if not suggestions:
        suggestions = _build_suggestions(validation or {}, output_sql, source, target)
    if not suggestions:
        return []
    persisted = []
    with _SUGGESTION_LOCK:
        all_suggestions = _read_suggestions()
        for suggestion in suggestions:
            record = {
                **suggestion,
                "object_type": object_type,
                "input_sql_sample": str(input_sql or "")[:1000],
                "output_sql_sample": str(output_sql or "")[:1000],
            }
            all_suggestions.append(record)
            persisted.append(record)
        _SUGGESTION_FILE.write_text(
            json.dumps(all_suggestions, ensure_ascii=True, indent=2),
            encoding="utf-8",
        )
    return persisted


def fix_sql(sql_text: str, error_message: str, source: str, target: str, object_type: str = "") -> str:
    return _build_llm_sql_fix(
        sql_text,
        error_message,
        source,
        target,
        object_type,
    )


def chat(message: str, sql_text: str = "", source: str = "", target: str = "", object_type: str = "") -> str:
    client = _get_client()
    if client is None:
        return "AI chat is not configured."
    model = os.getenv("RAG_AGENT_MODEL") or DEFAULT_RAG_AGENT_MODEL
    rag_context = build_rag_context(source, target) if source or target else ""
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are an enterprise database migration assistant. "
                        "Respond concisely and focus on actionable SQL migration guidance."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"User message:\n{message}\n\n"
                        f"Object type: {object_type}\n"
                        f"Source DB: {source}\n"
                        f"Target DB: {target}\n\n"
                        f"SQL:\n{str(sql_text or '')[:8000]}\n\n"
                        f"RAG context:\n{rag_context[:4000]}"
                    ),
                },
            ],
            temperature=0.2,
        )
    except Exception:
        return "AI chat request failed."
    if response and response.choices:
        return str(response.choices[0].message.content or "").strip() or "AI chat returned no content."
    return "AI chat returned no content."
