import json
import re
from functools import lru_cache
from pathlib import Path


RULES_DIR = Path(__file__).resolve().parent.parent / "rules"


def _normalize_key(value):
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def _normalize_db_name(value):
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")


@lru_cache(maxsize=None)
def _load_json(path_str):
    path = Path(path_str)
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def get_column_aliases():
    data = _load_json(str(RULES_DIR / "columns" / "normalization.json"))
    aliases = data.get("identifier_aliases", {})
    return {
        _normalize_key(source): _normalize_key(target)
        for source, target in aliases.items()
    }


def normalize_identifier_with_rules(identifier):
    normalized = _normalize_key(identifier)
    aliases = get_column_aliases()
    return aliases.get(normalized, normalized)


def get_dialect_rule_bundle(source_db, target_db):
    file_name = f"{_normalize_db_name(source_db)}_to_{_normalize_db_name(target_db)}.json"
    return _load_json(str(RULES_DIR / "dialect" / file_name))


def get_target_rules(source_db, target_db):
    rule_bundle = get_dialect_rule_bundle(source_db, target_db)
    return [str(item).strip() for item in (rule_bundle.get("target_rules") or []) if str(item).strip()]


def get_object_rules(source_db, target_db, object_type):
    rule_bundle = get_dialect_rule_bundle(source_db, target_db)
    object_rules = rule_bundle.get("object_rules") or {}
    return [str(item).strip() for item in (object_rules.get(object_type) or []) if str(item).strip()]


def get_unsupported_object_rule(source_db, target_db, object_type):
    rule_bundle = get_dialect_rule_bundle(source_db, target_db)
    unsupported = rule_bundle.get("unsupported_object_types", {})
    return str(unsupported.get(object_type) or "").strip()


def get_error_repair_rules(target_db):
    file_name = f"{_normalize_db_name(target_db)}.json"
    data = _load_json(str(RULES_DIR / "errors" / file_name))
    return data.get("rules", [])
