from urllib.parse import urlparse


OBJECT_SUMMARIES = {
    "table": 0,
    "view": 0,
    "storedprocedure": 0,
    "function": 0,
    "trigger": 0,
    "cursor": 0,
    "event": 0,
    "sequence": 0,
    "synonym": 0,
}


def empty_object_summary():
    return dict(OBJECT_SUMMARIES)


def first_value(cursor, default=0):
    row = cursor.fetchone()
    if not row:
        return default
    return row[0] if row[0] is not None else default


def snowflake_names(rows, preferred_index=1):
    names = []
    for row in rows or []:
        if row is None:
            continue
        if isinstance(row, (list, tuple)):
            if len(row) > preferred_index and row[preferred_index] is not None:
                names.append(str(row[preferred_index]))
            elif row:
                first_non_null = next((value for value in row if value is not None), None)
                if first_non_null is not None:
                    names.append(str(first_non_null))
        else:
            names.append(str(row))
    return names


def fetch_required(cursor, index, not_found_message):
    row = cursor.fetchone()
    if not row:
        raise Exception(not_found_message)
    return row[index]


def normalize_snowflake_account(account_value):
    if not account_value:
        raise Exception("Snowflake account is required.")

    value = account_value.strip()
    if "://" in value:
        parsed = urlparse(value)
        value = parsed.netloc or parsed.path

    value = value.strip().rstrip("/")
    value = value.split(":")[0]

    suffix = ".snowflakecomputing.com"
    if value.endswith(suffix):
        value = value[: -len(suffix)]

    return value
