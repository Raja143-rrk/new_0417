import re


def _normalize(value):
    return str(value or "").strip().lower()


def _validate_standard_sql_text(sql_text):
    text = str(sql_text or "")
    if re.search(r"\\\r?\n", text):
        raise Exception(
            "Target SQL validator found line-continuation backslashes in the transformed SQL."
        )
    if "\\n" in text or "\\r" in text or "\\t" in text:
        raise Exception(
            "Target SQL validator found escaped control sequences in the transformed SQL."
        )


def _split_top_level(text):
    parts = []
    current = []
    depth = 0
    in_single = False
    in_double = False
    in_backtick = False
    escape = False
    for char in str(text or ""):
        if escape:
            current.append(char)
            escape = False
            continue
        if char == "\\" and (in_single or in_double):
            current.append(char)
            escape = True
            continue
        if char == "'" and not in_double and not in_backtick:
            in_single = not in_single
            current.append(char)
            continue
        if char == '"' and not in_single and not in_backtick:
            in_double = not in_double
            current.append(char)
            continue
        if char == "`" and not in_single and not in_double:
            in_backtick = not in_backtick
            current.append(char)
            continue
        if in_single or in_double or in_backtick:
            current.append(char)
            continue
        if char == "(":
            depth += 1
        elif char == ")" and depth > 0:
            depth -= 1
        if char == "," and depth == 0:
            piece = "".join(current).strip()
            if piece:
                parts.append(piece)
            current = []
            continue
        current.append(char)
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def _extract_create_table_body(sql_text):
    text = str(sql_text or "")
    match = re.search(r"(?is)\bcreate\s+table\b.*?\(", text)
    if not match:
        return None
    start = text.find("(", match.start())
    if start == -1:
        return None
    depth = 0
    for index in range(start, len(text)):
        char = text[index]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return text[start + 1:index]
    return None


def _validate_mysql_table(sql_text):
    text = str(sql_text or "")
    normalized = text.lower()
    if normalized.count("create table") != 1:
      raise Exception("MySQL table validator requires exactly one CREATE TABLE statement.")
    body = _extract_create_table_body(text)
    if not body:
        raise Exception("MySQL table validator could not parse the CREATE TABLE body.")
    definitions = _split_top_level(body)
    auto_increment_columns = []
    indexed_columns = set()
    for definition in definitions:
        item = str(definition or "").strip()
        if not item:
            continue
        column_match = re.match(
            r'(?is)^((?:`[^`]+`)|(?:"[^"]+")|(?:[A-Za-z0-9_$]+))\s+',
            item,
        )
        if column_match and "auto_increment" in item.lower():
            auto_increment_columns.append(column_match.group(1).strip("`\""))
        primary_match = re.search(r"(?is)\bprimary\s+key\s*\((.*?)\)", item)
        if primary_match:
            indexed_columns.update(
                token.strip().strip("`\"")
                for token in _split_top_level(primary_match.group(1))
            )
        unique_match = re.search(r"(?is)\bunique\b(?:\s+key|\s+index)?(?:\s+[A-Za-z0-9_`\"]+)?\s*\((.*?)\)", item)
        if unique_match:
            indexed_columns.update(
                token.strip().strip("`\"")
                for token in _split_top_level(unique_match.group(1))
            )
        key_match = re.search(r"(?is)\b(?:key|index)\b(?:\s+[A-Za-z0-9_`\"]+)?\s*\((.*?)\)", item)
        if key_match:
            indexed_columns.update(
                token.strip().strip("`\"")
                for token in _split_top_level(key_match.group(1))
            )
    for column in auto_increment_columns:
        if column not in indexed_columns:
            raise Exception(
                f"MySQL semantic validation failed: AUTO_INCREMENT column '{column}' must be indexed."
            )


def _validate_mysql_routine(sql_text, object_type):
    text = str(sql_text or "")
    normalized = text.lower()
    if object_type == "function" and "create function" not in normalized:
        raise Exception("MySQL function validator requires one CREATE FUNCTION statement.")
    if object_type == "storedprocedure" and "create procedure" not in normalized:
        raise Exception("MySQL procedure validator requires one CREATE PROCEDURE statement.")
    if object_type == "function":
        header_match = re.search(
            r"(?is)\bcreate\s+(?:or\s+replace\s+)?function\b\s+.+?\)\s+returns\s+([A-Za-z_ ]+(?:\([^)]*\))?)\b",
            text,
        )
        if not header_match:
            raise Exception(
                "MySQL function validator requires a CREATE FUNCTION header with a RETURNS clause."
            )
    invalid_unsized_strings = re.search(
        r"(?is)\b(varchar|char|binary|varbinary)\b(?!\s*\()",
        text,
    )
    if invalid_unsized_strings:
        raise Exception(
            "MySQL routine validator found a string type without a required length."
        )
    if re.search(r"\\\r?\n", text):
        raise Exception(
            "MySQL routine validator found line-continuation backslashes in the routine body."
        )
    if "\\n" in text or "\\r" in text or "\\t" in text:
        raise Exception(
            "MySQL routine validator found escaped control sequences in the routine body."
        )
    if object_type == "storedprocedure":
        if not re.search(r"(?im)^\s*delimiter\s+\$\$\s*$", text):
            raise Exception(
                "MySQL procedure validator requires DELIMITER $$ before CREATE PROCEDURE."
            )
        if not re.search(r"(?im)^\s*delimiter\s+;\s*$", text):
            raise Exception(
                "MySQL procedure validator requires DELIMITER ; after the routine body."
            )
        if "begin" not in normalized or "end" not in normalized:
            raise Exception(
                "MySQL procedure validator requires a BEGIN ... END routine body."
            )
        has_complex_flow = any(
            token in normalized
            for token in (" cursor ", " loop", " fetch ", " handler ", " repeat ", " while ")
        )
        if has_complex_flow:
            return
        if re.search(
            r"(?is)\bdeclare\s+(?:exit|continue)\s+handler\b.*?\bfor\b[^;]*;\s*(open|fetch|loop|while|repeat|if|set|select|insert|update|delete|leave|iterate|close)\b",
            text,
        ):
            raise Exception(
                "MySQL procedure validator found a handler declaration without a valid action block or statement."
            )


def _validate_snowflake_table(sql_text):
    normalized = str(sql_text or "").lower()
    forbidden_tokens = [
        "auto_increment",
        "autoincrement(",
        "engine=",
        "on update current_timestamp",
    ]
    for token in forbidden_tokens:
        if token in normalized:
            raise Exception(
                f"Snowflake semantic validation failed: unsupported token '{token}' detected in table SQL."
            )


def _validate_snowflake_routine(sql_text, object_type):
    normalized = str(sql_text or "").lower()
    if object_type == "function":
        if "create function" not in normalized and "create or replace function" not in normalized:
            raise Exception("Snowflake function validator requires one CREATE FUNCTION statement.")
        if "$$" not in str(sql_text or ""):
            raise Exception("Snowflake function validator requires $$ routine body delimiters.")
    if object_type == "storedprocedure":
        if "create procedure" not in normalized and "create or replace procedure" not in normalized:
            raise Exception("Snowflake procedure validator requires one CREATE PROCEDURE statement.")
        if "$$" not in str(sql_text or ""):
            raise Exception("Snowflake procedure validator requires $$ routine body delimiters.")
        if "language javascript" in normalized and "language sql" in normalized:
            raise Exception("Snowflake procedure validator found conflicting routine languages.")


def validate_target_sql_semantics(sql_text, source_db, target_db, object_type):
    _validate_standard_sql_text(sql_text)
    normalized_target = _normalize(target_db)
    normalized_object_type = _normalize(object_type)
    if normalized_object_type == "trigger":
        return
    if normalized_target == "mysql":
        if normalized_object_type == "table":
            _validate_mysql_table(sql_text)
            return
        if normalized_object_type in {"function", "storedprocedure"}:
            _validate_mysql_routine(sql_text, normalized_object_type)
            return
    if normalized_target == "snowflake":
        if normalized_object_type == "table":
            _validate_snowflake_table(sql_text)
            return
        if normalized_object_type in {"function", "storedprocedure"}:
            _validate_snowflake_routine(sql_text, normalized_object_type)
