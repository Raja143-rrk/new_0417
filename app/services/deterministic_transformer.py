import json
import re

from app.utils.rule_loader import get_dialect_rule_bundle


_DETERMINISTIC_COVERAGE = {
    ("snowflake", "mysql"): {"table", "view", "function", "storedprocedure", "trigger"},
    ("mysql", "snowflake"): {"table", "view", "function", "storedprocedure", "trigger"},
    ("mysql", "sql server"): {"table", "view"},
    ("postgresql", "mysql"): {"table", "view"},
    ("sql server", "mysql"): {"table", "view"},
    ("sql server", "snowflake"): {"table"},
}


_TYPE_MAPS = {
    ("snowflake", "mysql"): {
        "NUMBER": "DECIMAL",
        "DECIMAL": "DECIMAL",
        "NUMERIC": "DECIMAL",
        "FLOAT": "DOUBLE",
        "DOUBLE": "DOUBLE",
        "VARCHAR": "VARCHAR",
        "STRING": "VARCHAR",
        "TEXT": "TEXT",
        "BOOLEAN": "BOOLEAN",
        "DATE": "DATE",
        "TIMESTAMP_NTZ": "DATETIME",
        "TIMESTAMP_LTZ": "DATETIME",
        "TIMESTAMP_TZ": "DATETIME",
        "TIME": "TIME",
        "BINARY": "BLOB",
        "VARIANT": "JSON",
        "OBJECT": "JSON",
        "ARRAY": "JSON",
    },
    ("mysql", "snowflake"): {
        "INT": "NUMBER",
        "INTEGER": "NUMBER",
        "BIGINT": "NUMBER",
        "SMALLINT": "NUMBER",
        "TINYINT": "NUMBER",
        "DECIMAL": "NUMBER",
        "NUMERIC": "NUMBER",
        "FLOAT": "FLOAT",
        "DOUBLE": "FLOAT",
        "VARCHAR": "VARCHAR",
        "CHAR": "VARCHAR",
        "TEXT": "VARCHAR",
        "LONGTEXT": "VARCHAR",
        "BOOLEAN": "BOOLEAN",
        "BIT": "BOOLEAN",
        "DATE": "DATE",
        "DATETIME": "TIMESTAMP_NTZ",
        "TIMESTAMP": "TIMESTAMP_NTZ",
        "TIME": "TIME",
        "BLOB": "BINARY",
        "JSON": "VARIANT",
    },
    ("mysql", "sql server"): {
        "INT": "INT",
        "INTEGER": "INT",
        "BIGINT": "BIGINT",
        "SMALLINT": "SMALLINT",
        "TINYINT": "TINYINT",
        "DECIMAL": "DECIMAL",
        "NUMERIC": "DECIMAL",
        "FLOAT": "FLOAT",
        "DOUBLE": "FLOAT",
        "VARCHAR": "VARCHAR",
        "CHAR": "CHAR",
        "TEXT": "VARCHAR(MAX)",
        "LONGTEXT": "VARCHAR(MAX)",
        "BOOLEAN": "BIT",
        "BIT": "BIT",
        "DATE": "DATE",
        "DATETIME": "DATETIME2",
        "TIMESTAMP": "DATETIME2",
        "TIME": "TIME",
        "BLOB": "VARBINARY(MAX)",
        "JSON": "NVARCHAR(MAX)",
    },
    ("postgresql", "mysql"): {
        "INTEGER": "INT",
        "BIGINT": "BIGINT",
        "SMALLINT": "SMALLINT",
        "NUMERIC": "DECIMAL",
        "DOUBLE PRECISION": "DOUBLE",
        "REAL": "FLOAT",
        "BOOLEAN": "BOOLEAN",
        "TEXT": "TEXT",
        "CHARACTER VARYING": "VARCHAR",
        "TIMESTAMP WITHOUT TIME ZONE": "DATETIME",
        "TIMESTAMP WITH TIME ZONE": "DATETIME",
        "DATE": "DATE",
        "BYTEA": "BLOB",
        "JSONB": "JSON",
        "JSON": "JSON",
    },
    ("sql server", "mysql"): {
        "INT": "INT",
        "BIGINT": "BIGINT",
        "SMALLINT": "SMALLINT",
        "TINYINT": "TINYINT",
        "DECIMAL": "DECIMAL",
        "NUMERIC": "DECIMAL",
        "FLOAT": "DOUBLE",
        "REAL": "FLOAT",
        "BIT": "BOOLEAN",
        "NVARCHAR": "VARCHAR",
        "VARCHAR": "VARCHAR",
        "NCHAR": "CHAR",
        "TEXT": "TEXT",
        "DATE": "DATE",
        "DATETIME": "DATETIME",
        "DATETIME2": "DATETIME",
        "VARBINARY": "BLOB",
    },
    ("sql server", "snowflake"): {
        "INT": "NUMBER",
        "BIGINT": "NUMBER",
        "SMALLINT": "NUMBER",
        "TINYINT": "NUMBER",
        "DECIMAL": "NUMBER",
        "NUMERIC": "NUMBER",
        "MONEY": "NUMBER",
        "SMALLMONEY": "NUMBER",
        "FLOAT": "FLOAT",
        "REAL": "FLOAT",
        "BIT": "BOOLEAN",
        "NVARCHAR": "VARCHAR",
        "VARCHAR": "VARCHAR",
        "NCHAR": "VARCHAR",
        "CHAR": "VARCHAR",
        "TEXT": "VARCHAR",
        "NTEXT": "VARCHAR",
        "DATE": "DATE",
        "DATETIME": "TIMESTAMP_NTZ",
        "DATETIME2": "TIMESTAMP_NTZ",
        "SMALLDATETIME": "TIMESTAMP_NTZ",
        "TIME": "TIME",
        "VARBINARY": "BINARY",
        "IMAGE": "BINARY",
        "UNIQUEIDENTIFIER": "VARCHAR",
        "XML": "VARCHAR",
        "JSON": "VARIANT",
    },
}


_DB_ALIASES = {
    "mysql": "mysql",
    "mysqldb": "mysql",
    "sqlserver": "sql server",
    "sql_server": "sql server",
    "azure sql": "sql server",
    "azuresql": "sql server",
    "azure_sql": "sql server",
    "postgresql": "postgresql",
    "postgres": "postgresql",
    "redshift": "postgresql",
    "snowflake": "snowflake",
}


def _normalize_db(value: str) -> str:
    normalized = str(value or "").strip().lower()
    return _DB_ALIASES.get(normalized, normalized)


def get_deterministic_coverage_matrix() -> dict[tuple[str, str], set[str]]:
    return {
        (source_db, target_db): set(object_types)
        for (source_db, target_db), object_types in _DETERMINISTIC_COVERAGE.items()
    }


def get_deterministic_supported_object_types(source_db: str, target_db: str) -> set[str]:
    return set(
        _DETERMINISTIC_COVERAGE.get(
            (_normalize_db(source_db), _normalize_db(target_db)),
            set(),
        )
    )


def supports_deterministic_transform(source_db: str, target_db: str, object_type: str) -> bool:
    return str(object_type or "").strip().lower() in get_deterministic_supported_object_types(
        source_db,
        target_db,
    )


def _split_top_level(text: str) -> list[str]:
    parts = []
    current = []
    depth = 0
    in_single = False
    in_double = False
    in_backtick = False
    escape = False
    for char in text:
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


def _split_identifier_tokens(name: str) -> list[str]:
    return [token for token in re.split(r"\.", str(name or "").strip()) if token]


def _strip_identifier_quotes(identifier: str) -> str:
    text = str(identifier or "").strip()
    if len(text) >= 2 and text[0] == "[" and text[-1] == "]":
        return text[1:-1]
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"`", '"'}:
        return text[1:-1]
    return text.strip("[]")


def _quote_identifier(identifier: str, target_db: str) -> str:
    name = _strip_identifier_quotes(identifier)
    normalized_target = _normalize_db(target_db)
    if normalized_target == "mysql":
        return f"`{name}`"
    if normalized_target == "snowflake":
        return f'"{name.upper()}"'
    return name


def _strip_routine_signature(object_name: str) -> str:
    text = str(object_name or "").strip()
    if not text:
        return text
    match = re.match(r"^(.*?)(\s*\(.*\))$", text)
    if match:
        return match.group(1).strip()
    return text


def _qualify_object_name(object_name: str, target_db: str) -> str:
    parts = _split_identifier_tokens(object_name)
    if not parts:
        return _quote_identifier(object_name, target_db)
    return ".".join(_quote_identifier(part, target_db) for part in parts[-3:])


def _clean_sql(text: str) -> str:
    cleaned = str(text or "").strip().rstrip(";").strip()
    cleaned = re.sub(r"(?im)^\s*/\*![0-9]+.*?\*/\s*$", "", cleaned)
    cleaned = re.sub(r"(?is)\bDEFINER\s*=\s*`[^`]+`\s*@\s*`[^`]+`\s*", "", cleaned)
    cleaned = re.sub(r'(?is)\bDEFINER\s*=\s*"[^"]+"\s*@\s*"[^"]+"\s*', "", cleaned)
    cleaned = re.sub(r"(?is)\bDEFINER\s*=\s*[^ ]+\s+", "", cleaned)
    cleaned = re.sub(r"(?im)^\s*create\s+or\s+replace\s+", "CREATE ", cleaned)
    cleaned = re.sub(r"(?im)^\s*create\s+(temporary|transient)\s+", "CREATE ", cleaned)
    cleaned = re.sub(r"(?im)^\s*delimiter\s+\S+\s*$", "", cleaned)
    return cleaned


def _split_args(signature: str) -> list[str]:
    return _split_top_level(signature)


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip())


def _split_type_and_tail(remainder: str) -> tuple[str, str]:
    text = str(remainder or "").strip()
    if not text:
        return "", ""
    keyword_match = re.search(
        r"(?is)\s+(NOT\s+NULL|NULL|DEFAULT|AUTO_INCREMENT|AUTOINCREMENT|IDENTITY|COMMENT|REFERENCES|UNIQUE|PRIMARY\s+KEY|CHECK|COLLATE|CHARACTER\s+SET)\b",
        text,
    )
    if not keyword_match:
        return text, ""
    return text[: keyword_match.start()].strip(), text[keyword_match.start() :].strip()


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


def _parse_mysql_function(source_sql: str):
    cleaned = _clean_sql(source_sql)
    match = re.search(
        r"(?is)\bcreate\s+function\s+([`A-Za-z0-9_$.]+)\s*\((.*?)\)\s+returns\s+([A-Za-z_ ]+(?:\([^)]*\))?)(?:\s+charset\s+[A-Za-z0-9_]+)?(?:\s+collate\s+[A-Za-z0-9_]+)?\s+(.*)$",
        cleaned,
    )
    if not match:
        return None
    body = match.group(4).strip()
    body = re.sub(
        r"(?is)^((deterministic|not deterministic|contains sql|no sql|reads sql data|modifies sql data)(\s+|$))+",
        "",
        body,
    ).strip()
    return_expr = None
    return_match = re.search(r"(?is)\breturn\b\s+(.*?)(?:;|\bend\b|$)", body)
    if return_match:
        return_expr = return_match.group(1).strip()
    return {
        "name": match.group(1),
        "args": match.group(2).strip(),
        "return_type": match.group(3).strip(),
        "body": body,
        "return_expr": return_expr,
    }


def _parse_snowflake_function(source_sql: str):
    cleaned = _clean_sql(source_sql)
    match = re.search(
        r"(?is)\bcreate\s+(?:or\s+replace\s+)?(?:secure\s+)?function\s+"
        r"([`\"A-Za-z0-9_$.]+)\s*\((.*?)\)\s+returns\s+([A-Za-z_]+(?:\([^)]*\))?)\b(.*)$",
        cleaned,
    )
    if not match:
        return None
    trailer = match.group(4).strip()
    body = None
    dollar_body = re.search(r"(?is)\bas\s+\$\$(.*?)\$\$", trailer)
    if dollar_body:
        body = dollar_body.group(1).strip()
    else:
        single_quote_body = re.search(r"(?is)\bas\s+'((?:''|[^'])*)'\s*$", trailer)
        if single_quote_body:
            body = single_quote_body.group(1).replace("''", "'").strip()
    if not body:
        return None
    return {
        "name": match.group(1),
        "args": match.group(2).strip(),
        "return_type": match.group(3).strip(),
        "body": body,
    }


def _convert_snowflake_expression_to_mysql(expression: str) -> str:
    text = str(expression or "").strip()
    if not text:
        return text
    text = text.replace('"', "")
    text = re.sub(r"(?is)\bIFF\s*\(", "IF(", text)
    text = re.sub(r"(?is)\bNVL\s*\(", "IFNULL(", text)

    parts = _split_top_level(text.replace("||", ","))
    if "||" in text and len(parts) > 1:
        concat_parts = [part.strip() for part in re.split(r"(?<!\|)\|\|(?!\|)", text) if part.strip()]
        if len(concat_parts) > 1:
            return f"CONCAT({', '.join(concat_parts)})"
    return text


def _normalize_routine_body_text(body: str) -> str:
    text = str(body or "")
    text = re.sub(r"\\+\r?\n\s*", "\n", text)
    text = text.replace("\\r\\n", "\n")
    text = text.replace("\\n", "\n")
    text = text.replace("\\r", "\n")
    text = text.replace("\\t", "\t")
    text = text.replace("\\'", "'")
    text = text.replace('\\"', '"')
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _parse_mysql_procedure(source_sql: str):
    cleaned = _clean_sql(source_sql)
    match = re.search(
        r"(?is)\bcreate\s+procedure\s+([`A-Za-z0-9_$.]+)\s*\((.*?)\)\s+(.*)$",
        cleaned,
    )
    if not match:
        return None
    body = match.group(3).strip()
    body = re.sub(r"(?im)^\s*begin\s*", "", body)
    body = re.sub(r"(?im)\s*end\s*$", "", body).strip()
    return {
        "name": match.group(1),
        "args": match.group(2).strip(),
        "body": body.strip().rstrip(";"),
    }


def _parse_snowflake_procedure(source_sql: str):
    cleaned = _clean_sql(source_sql)
    match = re.search(
        r"(?is)\bcreate\s+(?:or\s+replace\s+)?(?:secure\s+)?procedure\s+"
        r"([`\"A-Za-z0-9_$.]+)\s*\((.*?)\)\s+returns\s+"
        r"(.+?)(?=\s+(?:language|execute\s+as|comment|copy\s+grants|strict|volatile|immutable|as)\b)(.*)$",
        cleaned,
    )
    if not match:
        return None
    trailer = match.group(4).strip()
    language_match = re.search(r"(?is)\blanguage\s+([A-Za-z_]+)\b", trailer)
    language = (language_match.group(1) if language_match else "SQL").strip().upper()
    body = None
    dollar_body = re.search(r"(?is)\bas\s+\$\$(.*?)\$\$", trailer)
    if dollar_body:
        body = dollar_body.group(1).strip()
    else:
        single_quote_body = re.search(r"(?is)\bas\s+'((?:''|[^'])*)'\s*$", trailer)
        if single_quote_body:
            body = single_quote_body.group(1).replace("''", "'").strip()
    if body is None:
        return None
    body = _normalize_routine_body_text(body)
    body = re.sub(r"(?im)^\s*begin\s*", "", body)
    body = re.sub(r"(?im)\s*end\s*;?\s*$", "", body).strip()
    return {
        "name": match.group(1),
        "args": match.group(2).strip(),
        "return_type": match.group(3).strip(),
        "body": body.rstrip(";"),
        "language": language,
    }


def _extract_sql_from_snowflake_js_procedure(body: str, args_signature: str) -> str | None:
    text = str(body or "").strip()
    if not text:
        return None
    arg_names = []
    for raw_arg in _split_top_level(args_signature or ""):
        raw_arg = str(raw_arg or "").strip()
        if not raw_arg:
            continue
        match = re.match(
            r"(?is)^(?:(INOUT|IN|OUT)\s+)?([`\"A-Za-z0-9_]+)\s+([A-Za-z_ ]+(?:\([^)]*\))?)$",
            raw_arg,
        )
        if match:
            arg_names.append(_strip_identifier_quotes(match.group(2)))
    sql_literal_match = re.search(
        r"(?is)\bsqlText\s*=\s*(?:`([^`]*)`|'((?:\\'|[^'])*)'|\"((?:\\\"|[^\"])*)\")",
        text,
    )
    if not sql_literal_match:
        sql_literal_match = re.search(
            r"(?is)\bcreateStatement\s*\(\s*\{\s*sqlText\s*:\s*(?:`([^`]*)`|'((?:\\'|[^'])*)'|\"((?:\\\"|[^\"])*)\")",
            text,
        )
    if not sql_literal_match:
        return None
    sql_text = next(
        (
            group
            for group in sql_literal_match.groups()
            if group is not None
        ),
        "",
    )
    sql_text = _normalize_routine_body_text(sql_text)
    bind_match = re.search(r"(?is)\bbinds\s*:\s*\[(.*?)\]", text)
    bind_names = []
    if bind_match:
        bind_names = [
            _strip_identifier_quotes(part.strip())
            for part in _split_top_level(bind_match.group(1))
            if part.strip()
        ]
    if not bind_names:
        bind_names = arg_names
    for bind_name in bind_names:
        sql_text = sql_text.replace("?", bind_name, 1)
    sql_text = re.sub(r"(?is)\bsnowflake\.createStatement\s*\(\s*\{.*?\}\s*\)\s*;?", "", sql_text)
    sql_text = re.sub(r"(?is)\bstmt\.execute\s*\(\s*\)\s*;?", "", sql_text)
    sql_text = re.sub(r"(?is)\breturn\b\s+['\"][^'\"]*['\"]\s*;?", "", sql_text)
    return sql_text.strip().rstrip(";") or None


def _parse_mysql_trigger(source_sql: str):
    cleaned = _clean_sql(source_sql)
    match = re.search(
        r"(?is)\bcreate\s+trigger\s+([`A-Za-z0-9_$.]+)\s+"
        r"(before|after)\s+"
        r"(insert|update|delete)\s+on\s+([`A-Za-z0-9_$.]+)\s+"
        r"for\s+each\s+row\s+(.*)$",
        cleaned,
    )
    if not match:
        return None
    body = match.group(5).strip()
    body = re.sub(r"(?im)^\s*begin\s*", "", body)
    body = re.sub(r"(?im)\s*end\s*$", "", body).strip()
    return {
        "name": match.group(1),
        "timing": match.group(2).upper(),
        "event": match.group(3).upper(),
        "table_name": match.group(4),
        "body": body.rstrip(";"),
    }


def _parse_snowflake_trigger_bundle(source_sql: str):
    cleaned = _clean_sql(source_sql)
    stream_match = re.search(
        r"(?is)\bcreate\s+(?:or\s+replace\s+)?stream\s+([`\"A-Za-z0-9_$.]+)\s+on\s+table\s+([`\"A-Za-z0-9_$.]+)",
        cleaned,
    )
    task_match = re.search(
        r"(?is)\bcreate\s+(?:or\s+replace\s+)?task\s+([`\"A-Za-z0-9_$.]+).*?\bas\b\s+(.*)$",
        cleaned,
    )
    if not stream_match or not task_match:
        return None
    task_body = task_match.group(2).strip()
    task_body = task_body.split("-- SQL_BUNDLE_DELIMITER --")[0].strip().rstrip(";")
    return {
        "stream_name": stream_match.group(1),
        "table_name": stream_match.group(2),
        "task_name": task_match.group(1),
        "task_body": task_body,
    }


def _sanitize_js_identifier(name: str) -> str:
    candidate = re.sub(r"[^A-Za-z0-9_]+", "_", str(name or "")).strip("_")
    if not candidate:
        return "arg"
    if candidate[0].isdigit():
        candidate = f"arg_{candidate}"
    return candidate


def _convert_mysql_body_to_snowflake_sql(body: str) -> str:
    normalized = str(body or "").strip()
    normalized = normalized.replace("`", '"')
    normalized = re.sub(r"(?is)\bNOW\(\s*\)", "CURRENT_TIMESTAMP()", normalized)
    normalized = re.sub(r"(?is)\bCURDATE\(\s*\)", "CURRENT_DATE()", normalized)
    normalized = re.sub(r"(?is)\bIFNULL\s*\(", "COALESCE(", normalized)
    return normalized.strip().rstrip(";")


def _parameterize_sql_body(body_sql: str, args_signature: str) -> tuple[str, list[str]]:
    parameterized_sql = str(body_sql or "")
    bind_names = []
    for arg in _split_args(args_signature):
        arg_match = re.match(
            r"(?is)^(?:(INOUT|IN|OUT)\s+)?([`\"A-Za-z0-9_]+)\s+",
            str(arg or "").strip(),
        )
        if not arg_match:
            continue
        param_name = _strip_identifier_quotes(arg_match.group(2))
        safe_name = _sanitize_js_identifier(param_name)
        bind_names.append(safe_name)
        parameterized_sql = re.sub(
            rf"(?is)\b{re.escape(param_name)}\b",
            "?",
            parameterized_sql,
        )
    return parameterized_sql, bind_names


def _to_javascript_template_literal(text: str) -> str:
    value = str(text or "")
    value = value.replace("\\", "\\\\")
    value = value.replace("`", "\\`")
    value = value.replace("${", "\\${")
    return f"`{value}`"


def _transform_args(signature: str, source_db: str, target_db: str, include_modes: bool) -> str:
    if not signature.strip():
        return ""
    transformed = []
    for arg in _split_args(signature):
        token = _normalize_whitespace(arg)
        if not token:
            continue
        match = re.match(
            r"(?is)^(?:(INOUT|IN|OUT)\s+)?([`\"A-Za-z0-9_]+)\s+([A-Za-z_ ]+(?:\([^)]*\))?)$",
            token,
        )
        if not match:
            transformed.append(token)
            continue
        mode = (match.group(1) or "").upper()
        name = _strip_identifier_quotes(match.group(2))
        dtype = _map_type(match.group(3), source_db, target_db)
        if include_modes and mode:
            transformed.append(f"{mode} {name} {dtype}")
        else:
            transformed.append(f"{name} {dtype}")
    return ", ".join(transformed)


def _extract_table_definition(source_sql: str):
    cleaned = _clean_sql(source_sql)
    create_match = re.search(r"(?is)\bcreate\s+table\b", cleaned)
    if not create_match:
        return None, None
    open_index = cleaned.find("(", create_match.end())
    if open_index == -1:
        return None, None
    between = cleaned[create_match.end() : open_index].strip()
    between = re.sub(r"(?is)^if\s+not\s+exists\s+", "", between).strip()
    if not between:
        return None, None
    object_name = between.split()[-1].strip()
    close_index = _find_matching_paren(cleaned, open_index)
    if close_index == -1:
        return None, None
    body = cleaned[open_index + 1 : close_index].strip()
    if not body:
        return None, None
    return object_name, body


def _map_type(type_text: str, source_db: str, target_db: str) -> str:
    source_key = _normalize_db(source_db)
    target_key = _normalize_db(target_db)
    rule_bundle = get_dialect_rule_bundle(source_db, target_db)
    type_map = dict(_TYPE_MAPS.get((source_key, target_key), {}))
    type_map.update({str(k).upper(): str(v) for k, v in (rule_bundle.get("type_mappings") or {}).items()})
    normalized = re.sub(r"(?is)\bunsigned\b", "", str(type_text or "").strip())
    normalized = re.sub(r"\s+", " ", normalized).strip().upper()
    match = re.match(r"([A-Z ]+)(\s*\(.*\))?", normalized)
    if not match:
        return type_text
    base_type = match.group(1).strip()
    suffix = match.group(2) or ""
    mapped = type_map.get(base_type, base_type)
    if target_key == "mysql" and not suffix:
        if mapped == "VARCHAR":
            return "VARCHAR(255)"
        if mapped == "CHAR":
            return "CHAR(1)"
        if mapped == "VARBINARY":
            return "VARBINARY(255)"
        if mapped == "BINARY":
            return "BINARY(1)"
    if target_key == "snowflake" and mapped in {"VARCHAR", "CHAR", "BINARY"}:
        if not suffix:
            return mapped
        if re.search(r"\bMAX\b", suffix, re.IGNORECASE):
            return mapped
        if re.search(r"\(\s*\d+\s*\)", suffix):
            return f"{mapped}{suffix}"
        return mapped
    if mapped in {"JSON", "VARIANT", "BOOLEAN", "DATE", "TIME", "DATETIME", "TIMESTAMP_NTZ", "TEXT", "BLOB"}:
        return mapped
    return f"{mapped}{suffix}"


def _apply_mysql_type_limits(mapped_type: str) -> str:
    normalized = re.sub(r"\s+", " ", str(mapped_type or "").strip()).upper()
    varchar_match = re.match(r"^VARCHAR\s*\(\s*(\d+)\s*\)$", normalized)
    if varchar_match:
        length = int(varchar_match.group(1))
        if length > 16383:
            return "TEXT"
    char_match = re.match(r"^CHAR\s*\(\s*(\d+)\s*\)$", normalized)
    if char_match:
        length = int(char_match.group(1))
        if length > 255:
            return "TEXT"
    return mapped_type


def _normalize_mysql_auto_increment_type(mapped_type: str) -> str:
    normalized = re.sub(r"\s+", " ", str(mapped_type or "").strip()).upper()
    if normalized.startswith(("INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT")):
        return mapped_type
    return "BIGINT"


def _transform_column_definition(definition: str, source_db: str, target_db: str):
    cleaned = str(definition or "").strip()
    upper = cleaned.upper()
    if upper.startswith(("CONSTRAINT ", "PRIMARY KEY", "UNIQUE ", "KEY ", "INDEX ", "FOREIGN KEY", "CLUSTER BY")):
        return None
    match = re.match(r'(?is)^((?:`[^`]+`)|(?:"[^"]+")|(?:\[[^\]]+\])|(?:[A-Za-z0-9_$]+))(\s+.+)$', cleaned)
    if not match:
        return None
    raw_name = match.group(1)
    remainder = match.group(2).strip()
    raw_type, tail = _split_type_and_tail(remainder)
    if not raw_type:
        return None
    mapped_type = _map_type(raw_type, source_db, target_db)
    target_key = _normalize_db(target_db)
    source_key = _normalize_db(source_db)
    is_auto_increment = bool(
        re.search(
            r"(?is)\bauto_increment\b|\bautoincrement\b|\bidentity\s*\([^)]*\)",
            tail,
        )
    )
    if _normalize_db(target_db) == "mysql":
        mapped_type = _apply_mysql_type_limits(mapped_type)
    tail = re.sub(r"(?is)\bauto_increment\b", "", tail)
    tail = re.sub(r"(?is)\bautoincrement\b", "", tail)
    tail = re.sub(r"(?is)\bidentity\s*\([^)]*\)", "", tail)
    tail = re.sub(r"(?is)\bstart\s+-?\d+\b", "", tail)
    tail = re.sub(r"(?is)\bincrement\s+-?\d+\b", "", tail)
    tail = re.sub(r"(?is)\bnoorder\b", "", tail)
    tail = re.sub(r"(?is)\border\b", "", tail)
    tail = re.sub(r"(?is)\bdefault\s+getdate\(\s*\)", " DEFAULT CURRENT_TIMESTAMP", tail)
    tail = re.sub(r"(?is)\bdefault\s+sysdatetime\(\s*\)", " DEFAULT CURRENT_TIMESTAMP", tail)
    tail = re.sub(r"(?is)\bdefault\s+sysdatetimeoffset\(\s*\)", " DEFAULT CURRENT_TIMESTAMP", tail)
    tail = re.sub(r"(?is)\bdefault\s+current_timestamp\(\)", " DEFAULT CURRENT_TIMESTAMP", tail)
    tail = re.sub(
        r"(?is)\s+on\s+update\s+(?:current_timestamp(?:\s*\(\s*\))?|[A-Za-z_][A-Za-z0-9_]*\s*\([^)]*\)|[^,\s]+)",
        "",
        tail,
    )
    tail = re.sub(r"(?is)\bcharacter\s+set\s+[A-Za-z0-9_]+", "", tail)
    tail = re.sub(r"(?is)\bcollate\s+[A-Za-z0-9_]+", "", tail)
    tail = re.sub(r"(?is)\bunsigned\b", "", tail)
    tail = re.sub(r"(?is)\s+comment\s+'[^']*'", "", tail)
    tail = re.sub(r"(?is)\s+references\s+[A-Za-z0-9_`\".\[\]]+\s*\([^)]+\)", "", tail)
    tail = re.sub(r"\s+", " ", tail).strip()
    if source_key == "mysql" and target_key == "snowflake" and is_auto_increment:
        mapped_type = "NUMBER AUTOINCREMENT"
    if source_key in {"snowflake", "sql server"} and target_key == "mysql" and is_auto_increment:
        mapped_type = _normalize_mysql_auto_increment_type(mapped_type)
        tail = f"{tail} AUTO_INCREMENT".strip()
    column_name = _quote_identifier(raw_name, target_db)
    suffix = f" {tail}" if tail else ""
    return f"{column_name} {mapped_type}{suffix}".strip()


def _normalize_constraint_columns(column_list_text: str, target_db: str) -> str:
    columns = []
    for token in _split_top_level(column_list_text or ""):
        part = re.sub(r"(?is)\s+(ASC|DESC)\b", "", str(token or "").strip()).strip()
        if not part:
            continue
        columns.append(_quote_identifier(part, target_db))
    return ", ".join(columns)


def _transform_table_constraint(definition: str, source_db: str, target_db: str):
    cleaned = str(definition or "").strip().rstrip(",")
    upper = cleaned.upper()

    if "FOREIGN KEY" in upper or upper.startswith(("KEY ", "INDEX ")):
        return None

    primary_match = re.search(r"(?is)\bPRIMARY\s+KEY\s*\((.*?)\)", cleaned)
    if primary_match:
        columns = _normalize_constraint_columns(primary_match.group(1), target_db)
        if columns:
            return f"PRIMARY KEY ({columns})"
        return None

    unique_match = re.search(r"(?is)\bUNIQUE\b(?:\s+KEY|\s+INDEX)?(?:\s+[A-Za-z0-9_`\"$]+)?\s*\((.*?)\)", cleaned)
    if unique_match:
        columns = _normalize_constraint_columns(unique_match.group(1), target_db)
        if columns:
            return f"UNIQUE ({columns})"
        return None

    return None


def transform_table_ddl(source_sql: str, source_db: str, target_db: str, object_name: str) -> str | None:
    source_name, body = _extract_table_definition(source_sql)
    if not source_name or not body:
        return None
    lines = []
    auto_increment_columns = []
    has_primary_key = False
    for definition in _split_top_level(body):
        normalized_definition = str(definition or "").strip()
        constraint_line = _transform_table_constraint(
            normalized_definition,
            source_db,
            target_db,
        )
        if constraint_line:
            if constraint_line.upper().startswith("PRIMARY KEY"):
                has_primary_key = True
            lines.append(f"  {constraint_line}")
            continue

        column_match = re.match(
            r'(?is)^((?:`[^`]+`)|(?:"[^"]+")|(?:\[[^\]]+\])|(?:[A-Za-z0-9_$]+))(\s+.+)$',
            normalized_definition,
        )
        if column_match:
            column_name = _strip_identifier_quotes(column_match.group(1))
            remainder = column_match.group(2).strip()
            if re.search(
                r"(?is)\bauto_increment\b|\bautoincrement\b|\bidentity\s*\([^)]*\)|\bstart\s+-?\d+\b.*\bincrement\s+-?\d+\b",
                remainder,
            ):
                auto_increment_columns.append(column_name)

        transformed = _transform_column_definition(definition, source_db, target_db)
        if transformed:
            lines.append(f"  {transformed}")
    if not lines:
        return None
    if (
        _normalize_db(target_db) == "mysql"
        and len(auto_increment_columns) == 1
        and not has_primary_key
    ):
        lines.append(
            f"  PRIMARY KEY ({_quote_identifier(auto_increment_columns[0], target_db)})"
        )
    target_name = _qualify_object_name(object_name or source_name, target_db)
    return "CREATE TABLE {name} (\n{body}\n);".format(
        name=target_name,
        body=",\n".join(lines),
    )


def transform_view_ddl(source_sql: str, source_db: str, target_db: str, object_name: str) -> str | None:
    cleaned = _clean_sql(source_sql)
    match = re.search(
        r"(?is)\bcreate"
        r"(?:\s+or\s+replace)?"
        r"(?:\s+algorithm\s*=\s*[A-Za-z0-9_]+)?"
        r"(?:\s+definer\s*=\s*(?:`[^`]+`|\"[^\"]+\"|[^ ]+)\s*@\s*(?:`[^`]+`|\"[^\"]+\"|[^ ]+))?"
        r"(?:\s+sql\s+security\s+(?:definer|invoker))?"
        r"\s+view\s+"
        r"((?:(?:`[^`]+`)|(?:\"[^\"]+\")|(?:\[[^\]]+\])|(?:[A-Za-z0-9_$]+))(?:\.(?:(?:`[^`]+`)|(?:\"[^\"]+\")|(?:\[[^\]]+\])|(?:[A-Za-z0-9_$]+))){0,2})"
        r"(?:\s*\((.*?)\))?"
        r"\s+as\s+(.*)$",
        cleaned,
    )
    if not match:
        return None
    column_list = match.group(2) or ""
    select_body = match.group(3).strip()
    if not select_body:
        return None
    target_name = _qualify_object_name(object_name or match.group(1), target_db)
    transformed_column_list = ""
    if column_list.strip():
        transformed_columns = _normalize_constraint_columns(column_list, target_db)
        if transformed_columns:
            transformed_column_list = f" ({transformed_columns})"
    if target_db == "mysql":
        select_body = re.sub(r"(?is)\bqualify\b.*$", "", select_body)
        select_body = re.sub(r"(?is)\bilike\b", "LIKE", select_body)
    if _normalize_db(target_db) == "snowflake":
        # MySQL view definitions can retain backtick-quoted identifiers.
        # Leaving those in place causes Snowflake canonicalization to treat the
        # backticks as literal identifier characters.
        select_body = select_body.replace("`", "")
    return f"CREATE VIEW {target_name}{transformed_column_list} AS\n{select_body};"


def transform_function_ddl(source_sql: str, source_db: str, target_db: str, object_name: str) -> str | None:
    source_key = _normalize_db(source_db)
    target_key = _normalize_db(target_db)
    if source_key == "mysql" and target_key == "snowflake":
        parsed = _parse_mysql_function(source_sql)
        if not parsed or not parsed.get("return_expr"):
            return None
        target_name = _qualify_object_name(
            _strip_routine_signature(object_name) or parsed["name"],
            target_db,
        )
        args = _transform_args(parsed["args"], source_db, target_db, include_modes=False)
        return_type = _map_type(parsed["return_type"], source_db, target_db)
        return (
            f"CREATE OR REPLACE FUNCTION {target_name}({args})\n"
            f"RETURNS {return_type}\n"
            "LANGUAGE SQL\n"
            f"AS $$\n{parsed['return_expr']}\n$$;"
        )
    if source_key == "snowflake" and target_key == "mysql":
        parsed = _parse_snowflake_function(source_sql)
        if not parsed:
            return None
        body = parsed["body"].strip().rstrip(";")
        target_name = _qualify_object_name(
            _strip_routine_signature(object_name) or parsed["name"],
            target_db,
        )
        args = _transform_args(parsed["args"], source_db, target_db, include_modes=False)
        return_type = _map_type(parsed["return_type"], source_db, target_db)
        if re.match(r"(?is)^return\s+", body):
            body = re.sub(r"(?is)^return\s+", "", body)
        body = re.sub(r"(?is)^begin\s+", "", body).strip()
        body = re.sub(r"(?is)\s+end\s*$", "", body).strip()
        body = _convert_snowflake_expression_to_mysql(body)
        return (
            f"CREATE FUNCTION {target_name}({args})\n"
            f"RETURNS {return_type}\n"
            "DETERMINISTIC\n"
            f"RETURN {body};"
        )
    return None


def transform_procedure_ddl(source_sql: str, source_db: str, target_db: str, object_name: str) -> str | None:
    source_key = _normalize_db(source_db)
    target_key = _normalize_db(target_db)
    if source_key == "mysql" and target_key == "snowflake":
        parsed = _parse_mysql_procedure(source_sql)
        if not parsed:
            return None
        target_name = _qualify_object_name(
            _strip_routine_signature(object_name) or parsed["name"],
            target_db,
        )
        args = _transform_args(parsed["args"], source_db, target_db, include_modes=False)
        body_sql = _convert_mysql_body_to_snowflake_sql(parsed["body"])
        body_sql, bind_names = _parameterize_sql_body(body_sql, parsed["args"])
        sql_literal = _to_javascript_template_literal(
            f"{body_sql};" if body_sql else "SELECT 'SUCCESS';"
        )
        bind_literal = ", ".join(bind_names)
        return (
            f"CREATE OR REPLACE PROCEDURE {target_name}({args})\n"
            "RETURNS STRING\n"
            "LANGUAGE JAVASCRIPT\n"
            "EXECUTE AS CALLER\n"
            "AS\n"
            "$$\n"
            f"var sqlText = {sql_literal};\n"
            f"var stmt = snowflake.createStatement({{sqlText: sqlText, binds: [{bind_literal}]}});\n"
            "stmt.execute();\n"
            "return 'SUCCESS';\n"
            "$$;"
        )
    if source_key == "snowflake" and target_key == "mysql":
        parsed = _parse_snowflake_procedure(source_sql)
        if not parsed:
            return None
        target_name = _qualify_object_name(
            _strip_routine_signature(object_name) or parsed["name"],
            target_db,
        )
        args = _transform_args(parsed["args"], source_db, target_db, include_modes=True)
        body = _normalize_routine_body_text(parsed["body"] or "SELECT 'SUCCESS';")
        if parsed.get("language") == "JAVASCRIPT":
            body = _extract_sql_from_snowflake_js_procedure(body, parsed["args"]) or "SELECT 'SUCCESS'"
        body = _normalize_routine_body_text(body)
        body = re.sub(r"(?is)\breturn\b\s+[^;]+;?", "SELECT 'SUCCESS';", body)
        body = body.rstrip(";").strip()
        return (
            f"CREATE PROCEDURE {target_name}({args})\n"
            "BEGIN\n"
            f"{body};\n"
            "END;"
        )
    return None


def transform_trigger_ddl(source_sql: str, source_db: str, target_db: str, object_name: str) -> str | None:
    source_key = _normalize_db(source_db)
    target_key = _normalize_db(target_db)
    if source_key == "mysql" and target_key == "snowflake":
        parsed = _parse_mysql_trigger(source_sql)
        if not parsed:
            return None
        base_name = re.sub(r"[^A-Za-z0-9_]+", "_", object_name or parsed["name"]).strip("_") or "trigger_migration"
        base_name = base_name.upper()
        stream_name = _qualify_object_name(f"{base_name}_STREAM", target_db)
        task_name = _qualify_object_name(f"{base_name}_TASK", target_db)
        target_table_name = _qualify_object_name(parsed["table_name"], target_db)
        stream_reference = ".".join(part.strip('"') for part in stream_name.split("."))
        statements = [
            f"CREATE OR REPLACE STREAM {stream_name} ON TABLE {target_table_name};",
            (
                f"CREATE OR REPLACE TASK {task_name} "
                "USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL' "
                "SCHEDULE = '1 MINUTE' "
                f"WHEN SYSTEM$STREAM_HAS_DATA('{stream_reference}') "
                f"AS SELECT '{parsed['event']}' AS TRIGGER_EVENT, CURRENT_TIMESTAMP() AS PROCESSED_AT "
                f"FROM {stream_name};"
            ),
        ]
        return "\n-- SQL_BUNDLE_DELIMITER --\n".join(statements)
    if source_key == "snowflake" and target_key == "mysql":
        parsed = _parse_snowflake_trigger_bundle(source_sql)
        if not parsed:
            return None
        target_name = _qualify_object_name(object_name or parsed["task_name"], target_db)
        table_name = _qualify_object_name(parsed["table_name"], target_db)
        return (
            f"CREATE TRIGGER {target_name}\n"
            f"AFTER UPDATE ON {table_name}\n"
            "FOR EACH ROW\n"
            "SET @last_trigger_sync = CURRENT_TIMESTAMP();"
        )
    return None


def transform_deterministically(source_sql: str, source_db: str, target_db: str, object_type: str, object_name: str | None = None) -> str | None:
    if _normalize_db(source_db) == _normalize_db(target_db):
        normalized_type = str(object_type or "").strip().lower()
        if normalized_type in {"table", "view", "function", "storedprocedure", "trigger"}:
            return _clean_sql(source_sql)
        return None
    normalized_type = str(object_type or "").strip().lower()
    if normalized_type == "table":
        return transform_table_ddl(source_sql, source_db, target_db, object_name or "")
    if normalized_type == "view":
        return transform_view_ddl(source_sql, source_db, target_db, object_name or "")
    if normalized_type == "function":
        return transform_function_ddl(source_sql, source_db, target_db, object_name or "")
    if normalized_type == "storedprocedure":
        return transform_procedure_ddl(source_sql, source_db, target_db, object_name or "")
    if normalized_type == "trigger":
        return transform_trigger_ddl(source_sql, source_db, target_db, object_name or "")
    return None


__all__ = [
    "get_deterministic_coverage_matrix",
    "get_deterministic_supported_object_types",
    "supports_deterministic_transform",
    "transform_deterministically",
]
