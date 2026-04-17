import re


def _balanced_parentheses(sql_text: str) -> bool:
    depth = 0
    in_single = False
    in_double = False
    escape = False
    for char in str(sql_text or ""):
        if escape:
            escape = False
            continue
        if char == "\\" and (in_single or in_double):
            escape = True
            continue
        if char == "'" and not in_double:
            in_single = not in_single
            continue
        if char == '"' and not in_single:
            in_double = not in_double
            continue
        if in_single or in_double:
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth < 0:
                return False
    return depth == 0 and not in_single and not in_double


def _select_missing_from(sql_text: str) -> bool:
    for match in re.finditer(r"(?is)\bselect\b(.*?)(?:;|\bend\b|$)", str(sql_text or "")):
        statement = match.group(0)
        if re.search(r"(?is)\bselect\s+('[^']*'|\"[^\"]*\"|\d+)(?:\s+as\s+\w+)?\s*;?$", statement.strip()):
            continue
        if " from " not in f" {statement.lower()} ":
            return True
    return False


def _unsupported_syntax(sql_text: str, target: str) -> list[str]:
    target_key = str(target or "").strip().lower()
    checks = {
        "mysql": [
            (r"(?is)\bcreate\s+or\s+replace\s+(function|procedure)\b", "MySQL does not support CREATE OR REPLACE for routines."),
            (r"(?is)\blanguage\s+javascript\b", "MySQL does not support LANGUAGE JAVASCRIPT routines."),
            (r"(?is)\bqualify\b", "MySQL does not support QUALIFY."),
        ],
        "snowflake": [
            (r"(?is)\bengine\s*=", "Snowflake does not support MySQL ENGINE clauses."),
            (r"(?is)\bauto_increment\b", "Snowflake expects AUTOINCREMENT/IDENTITY syntax, not AUTO_INCREMENT."),
            (r"(?is)`[^`]+`", "Snowflake SQL should not use MySQL backtick identifiers."),
        ],
        "postgresql": [
            (r"(?is)\bauto_increment\b", "PostgreSQL does not support AUTO_INCREMENT."),
            (r"(?is)`[^`]+`", "PostgreSQL SQL should not use MySQL backtick identifiers."),
        ],
        "sql server": [
            (r"(?is)\blimit\s+\d+\b", "SQL Server does not support LIMIT."),
            (r"(?is)`[^`]+`", "SQL Server SQL should not use MySQL backtick identifiers."),
        ],
    }
    errors = []
    for pattern, message in checks.get(target_key, []):
        if re.search(pattern, sql_text or ""):
            errors.append(message)
    return errors


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


def _extract_mysql_routine_body(sql_text: str, routine_keyword: str) -> str:
    text = str(sql_text or "").strip()
    header_match = re.search(
        rf"(?is)\bcreate\s+{routine_keyword}\b\s+.+?\(",
        text,
    )
    if not header_match:
        return ""
    args_open = text.find("(", header_match.start())
    if args_open == -1:
        return ""
    args_close = _find_matching_paren(text, args_open)
    if args_close == -1:
        return ""
    remainder = text[args_close + 1 :].strip()
    if routine_keyword == "function":
        returns_match = re.search(
            r"(?is)\breturns\b\s+[A-Za-z_ ]+(?:\([^)]*\))?",
            remainder,
        )
        if returns_match:
            remainder = remainder[returns_match.end() :].strip()
    remainder = re.sub(r"(?im)^\s*delimiter\s+\S+\s*$", "", remainder).strip()
    remainder = re.sub(r"(?is)\$\$\s*$", "", remainder).strip()
    return remainder


def _validate_mysql_procedure_structure(sql_text: str) -> list[str]:
    errors = []
    text = str(sql_text or "").strip()
    if "create procedure" not in text.lower():
        return errors
    if not re.search(r"(?im)^\s*delimiter\s+\$\$\s*$", text):
        errors.append("MySQL stored procedures must be wrapped with DELIMITER $$ before CREATE PROCEDURE.")
    if not re.search(r"(?im)^\s*delimiter\s+;\s*$", text):
        errors.append("MySQL stored procedures must restore the delimiter with DELIMITER ; after the routine body.")
    body = _extract_mysql_routine_body(text, "procedure")
    if not body:
        errors.append("MySQL stored procedure validator could not parse the procedure body.")
        return errors
    if not re.match(r"(?is)^begin\b", body) or not re.search(r"(?is)\bend(?:\s*\$\$?|\s*;?)\s*$", body):
        errors.append("MySQL stored procedures must use a BEGIN ... END body.")
        return errors
    lowered_body = body.lower()
    has_complex_flow = any(
        token in lowered_body
        for token in (" cursor ", " loop", " fetch ", " handler ", " repeat ", " while ")
    )
    if has_complex_flow:
        return errors
    if re.search(
        r"(?is)\bdeclare\s+(?:exit|continue)\s+handler\b.*?\bfor\b[^;]*;\s*(open|fetch|loop|while|repeat|if|set|select|insert|update|delete|leave|iterate|close)\b",
        body,
    ):
        errors.append("MySQL stored procedure contains a DECLARE HANDLER without a valid action block or statement.")

    declare_lines = list(re.finditer(r"(?im)^\s*declare\b.*$", body))
    cursor_index = None
    handler_index = None
    executable_index = None
    for match in declare_lines:
        line = match.group(0)
        lowered = line.lower()
        if cursor_index is None and " cursor " in f" {lowered} ":
            cursor_index = match.start()
        if handler_index is None and " handler " in f" {lowered} ":
            handler_index = match.start()
    executable_match = re.search(
        r"(?im)^\s*(open|fetch|if|loop|while|repeat|case|select|insert|update|delete|set|call|return|leave|iterate)\b",
        body,
    )
    if executable_match:
        executable_index = executable_match.start()
    if cursor_index is not None and handler_index is not None and handler_index < cursor_index:
        errors.append("MySQL stored procedure declares a handler before a cursor. Cursor declarations must come before handler declarations.")
    if executable_index is not None:
        for match in declare_lines:
            if match.start() > executable_index:
                errors.append("MySQL stored procedure has DECLARE statements after executable statements.")
                break
    return errors


def validate(sql_text: str, target: str, object_type: str = "") -> dict:
    errors = []
    warnings = []
    text = str(sql_text or "").strip()
    normalized_type = str(object_type or "").strip().lower()
    if not text:
        errors.append("SQL output is empty.")
    if text and not _balanced_parentheses(text):
        errors.append("SQL has unbalanced parentheses or quotes.")
    if text and normalized_type in {"view", "table", "storedprocedure", "function", ""} and _select_missing_from(text):
        warnings.append("A SELECT statement may be missing a FROM clause.")
    if re.search(r"\\\r?\n", text) or any(token in text for token in ("\\n", "\\r", "\\t")):
        errors.append("SQL contains escaped formatting artifacts instead of standard SQL text.")
    errors.extend(_unsupported_syntax(text, target))
    target_key = str(target or "").strip().lower()
    if target_key == "mysql" and normalized_type == "storedprocedure":
        errors.extend(_validate_mysql_procedure_structure(text))
    return {
        "is_valid": not errors,
        "errors": errors,
        "warnings": warnings,
    }
