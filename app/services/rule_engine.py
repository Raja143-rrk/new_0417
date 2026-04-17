import re

from app.services.rule_mapping_service import get_regex_mappings


_INLINE_FLAG_MAP = {
    "i": re.IGNORECASE,
    "m": re.MULTILINE,
    "s": re.DOTALL,
    "x": re.VERBOSE,
}


def _find_matching_paren(text: str, start_index: int) -> int:
    depth = 0
    in_single = False
    in_double = False
    in_backtick = False
    escape = False
    for index in range(start_index, len(text)):
        char = text[index]
        if escape:
            escape = False
            continue
        if char == "\\" and (in_single or in_double):
            escape = True
            continue
        if char == "'" and not in_double and not in_backtick:
            in_single = not in_single
            continue
        if char == '"' and not in_single and not in_backtick:
            in_double = not in_double
            continue
        if char == "`" and not in_single and not in_double:
            in_backtick = not in_backtick
            continue
        if in_single or in_double or in_backtick:
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index
    return -1


def _detect_mysql_stored_procedure(sql_text: str) -> bool:
    return bool(re.search(r"(?is)\bcreate\s+procedure\b", str(sql_text or "")))


def _repair_mysql_handler_blocks(sql_text: str) -> str:
    lines = str(sql_text or "").split("\n")
    repaired = []
    index = 0

    while index < len(lines):
        line = lines[index]
        if not re.match(r"(?is)^\s*DECLARE\s+(?:EXIT|CONTINUE)\s+HANDLER\b", line):
            repaired.append(line)
            index += 1
            continue

        repaired.append(line)
        index += 1

        if ";" in str(line):
            continue

        blank_lines = []
        while index < len(lines) and not str(lines[index]).strip():
            blank_lines.append(lines[index])
            index += 1

        if index >= len(lines):
            repaired.extend(blank_lines)
            break

        next_line = str(lines[index]).strip()
        if re.match(r"(?is)^BEGIN\b", next_line):
            repaired.extend(blank_lines)
            continue
        if re.match(
            r"(?is)^(DECLARE\b|IF\b|LOOP\b|WHILE\b|REPEAT\b|CASE\b|OPEN\b|CLOSE\b|FETCH\b|START\b|COMMIT\b|ROLLBACK\b|LEAVE\b|ITERATE\b|SELECT\b|INSERT\b|UPDATE\b|DELETE\b|SET\b)",
            next_line,
        ) is None:
            repaired.extend(blank_lines)
            continue

        repaired.append("BEGIN")
        repaired.extend(blank_lines)

        statement_lines = []
        while index < len(lines):
            statement_lines.append(lines[index])
            current_line = str(lines[index]).strip()
            index += 1
            if ";" in current_line:
                break
        repaired.extend(statement_lines)
        repaired.append("END;")

    return "\n".join(repaired)


def _normalize_mysql_procedure_body(body: str) -> str:
    text = standardize_sql(body).rstrip(";").strip()
    if not text:
        return "BEGIN\nEND"
    if re.match(r"(?is)^begin\b", text) and re.search(r"(?is)\bend\s*$", text):
        return text
    if re.match(r"(?is)^begin\b", text):
        return f"{text}\nEND"
    return f"BEGIN\n{text}\nEND"


def _ensure_mysql_procedure_delimiter_wrapper(sql_text: str) -> tuple[str, list[dict]]:
    text = standardize_sql(sql_text)
    if not _detect_mysql_stored_procedure(text):
        return text, []

    header_match = re.search(r"(?is)\bcreate\s+procedure\s+", text)
    if not header_match:
        return text, []

    args_open = text.find("(", header_match.end())
    if args_open == -1:
        return text, []
    args_close = _find_matching_paren(text, args_open)
    if args_close == -1:
        return text, []

    header = text[: args_close + 1].strip()
    body = text[args_close + 1 :].strip()
    body = re.sub(r"(?im)^\s*delimiter\s+\S+\s*$", "", body).strip()
    body = re.sub(r"(?is)\$\$\s*$", "", body).strip()
    normalized_body = _normalize_mysql_procedure_body(body)
    wrapped = f"DELIMITER $$\n{header}\n{normalized_body} $$\nDELIMITER ;"

    applied = []
    if standardize_sql(text) != standardize_sql(wrapped):
        applied.append({"name": "mysql_wrap_stored_procedure_delimiter", "count": 1})
    return wrapped, applied


def standardize_sql(sql_text: str) -> str:
    text = str(sql_text or "")
    if not text.strip():
        return text
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\\+\n\s*", "\n", text)
    text = text.replace("\\r\\n", "\n")
    text = text.replace("\\n", "\n")
    text = text.replace("\\r", "\n")
    text = text.replace("\\t", "\t")
    text = text.replace("\\'", "'")
    text = text.replace('\\"', '"')
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _extract_inline_flags(pattern: str, flags: int) -> tuple[str, int]:
    text = str(pattern or "")
    extracted_flags = int(flags or 0)

    def replace_flag_group(match: re.Match) -> str:
        nonlocal extracted_flags
        flag_text = str(match.group(1) or "")
        for token in flag_text:
            extracted_flags |= _INLINE_FLAG_MAP.get(token.lower(), 0)
        return ""

    normalized = re.sub(r"\(\?([imsx]+)\)", replace_flag_group, text)
    return normalized, extracted_flags


def _build_rule_error(name: str, pattern: str, error: Exception) -> dict:
    return {
        "name": str(name or "unnamed_rule"),
        "pattern": str(pattern or ""),
        "error": str(error),
    }


def _safe_apply_regex_rule(
    text: str,
    pattern: str,
    replacement: str,
    flags: int = 0,
    name: str = "",
) -> tuple[str, int, dict | None]:
    normalized_pattern, normalized_flags = _extract_inline_flags(pattern, flags)
    try:
        compiled = re.compile(normalized_pattern, normalized_flags)
    except re.error as error:
        return text, 0, _build_rule_error(name, pattern, error)
    try:
        new_text, count = compiled.subn(str(replacement or ""), text)
    except re.error as error:
        return text, 0, _build_rule_error(name, pattern, error)
    return new_text, count, None


def _apply_builtin_rules(sql_text: str, target: str) -> tuple[str, list[dict], list[dict]]:
    text = standardize_sql(sql_text)
    applied = []
    errors = []
    target_key = str(target or "").strip().lower()

    replacements = [
        (r";{2,}", ";", "collapse_duplicate_semicolons"),
        (r"(?im)^\s*delimiter\s+\S+\s*$", "", "remove_client_delimiter_directives"),
    ]
    if target_key == "mysql":
        replacements.extend(
            [
                (r"(?is)\bcreate\s+or\s+replace\s+function\b", "CREATE FUNCTION", "mysql_remove_or_replace_function"),
                (r"(?is)\bcreate\s+or\s+replace\s+procedure\b", "CREATE PROCEDURE", "mysql_remove_or_replace_procedure"),
                (r"(?is)\bautoincrement\b", "AUTO_INCREMENT", "mysql_autoincrement_keyword"),
            ]
        )
    if target_key == "snowflake":
        replacements.extend(
            [
                (r"(?is)\bauto_increment\b", "AUTOINCREMENT", "snowflake_autoincrement_keyword"),
                (r"(?is)\bengine\s*=\s*\w+", "", "snowflake_remove_mysql_engine"),
            ]
        )

    for pattern, replacement, name in replacements:
        new_text, count, error = _safe_apply_regex_rule(
            text,
            pattern,
            replacement,
            name=name,
        )
        if error:
            errors.append(error)
            continue
        if count:
            applied.append({"name": name, "count": count})
            text = new_text
    return standardize_sql(text), applied, errors


def apply_rules(sql_text: str, source: str, target: str) -> dict:
    text, applied, rule_errors = _apply_builtin_rules(sql_text, target)
    for mapping in get_regex_mappings(source, target):
        new_text, count, error = _safe_apply_regex_rule(
            text,
            mapping["pattern"],
            str(mapping.get("replacement") or ""),
            flags=int(mapping.get("flags") or 0),
            name=str(mapping.get("name") or mapping["pattern"]),
        )
        if error:
            rule_errors.append(error)
            continue
        if count:
            applied.append({"name": mapping.get("name") or mapping["pattern"], "count": count})
            text = new_text
    if str(target or "").strip().lower() == "mysql":
        text, routine_applied = _ensure_mysql_procedure_delimiter_wrapper(text)
        applied.extend(routine_applied)
    return {
        "sql": standardize_sql(text),
        "applied_rules": applied,
        "rule_errors": rule_errors,
    }
