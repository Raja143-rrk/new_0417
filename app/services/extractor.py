from app.adapters.registry import get_adapter


def extract_table_ddl(cursor, object_name, db_type, object_type, connection_details=None):
    return get_adapter(db_type).extract_ddl(
        cursor, object_name, object_type, connection_details
    )
