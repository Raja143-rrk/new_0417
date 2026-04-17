from app.adapters.registry import get_adapter


def list_databases(cursor, db_type, details=None):
    return get_adapter(db_type).list_databases(cursor, details)


def list_schemas(cursor, db_type, database_name, details=None):
    return get_adapter(db_type).list_schemas(cursor, database_name, details)


def get_object_summary(cursor, db_type, database_name, schema_name):
    return get_adapter(db_type).get_object_summary(cursor, database_name, schema_name)


def list_objects(cursor, db_type, database_name, schema_name, object_type):
    return get_adapter(db_type).list_objects(cursor, database_name, schema_name, object_type)
