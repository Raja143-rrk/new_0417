import json
import re
from pathlib import Path


_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_RULE_DIRS = [
    _PROJECT_ROOT / "app" / "rules" / "mappings",
    _PROJECT_ROOT / "app_data" / "rule_mappings",
]


def _parse_scalar(value: str):
    text = str(value or "").strip()
    if (text.startswith('"') and text.endswith('"')) or (text.startswith("'") and text.endswith("'")):
        return text[1:-1]
    return text


def _parse_simple_yaml(text: str):
    items = []
    current = None
    for raw_line in str(text or "").splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        if line.lstrip().startswith("- "):
            if current:
                items.append(current)
            current = {}
            line = line.lstrip()[2:].strip()
            if ":" in line:
                key, value = line.split(":", 1)
                current[key.strip()] = _parse_scalar(value)
            continue
        if current is not None and ":" in line:
            key, value = line.strip().split(":", 1)
            current[key.strip()] = _parse_scalar(value)
    if current:
        items.append(current)
    return items


def _load_rule_file(path: Path):
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        data = json.loads(text or "[]")
    else:
        data = _parse_simple_yaml(text)
    if isinstance(data, dict):
        return data.get("rules") if isinstance(data.get("rules"), list) else [data]
    return data if isinstance(data, list) else []


def get_regex_mappings(source: str, target: str, rule_dirs: list[Path] | None = None) -> list[dict]:
    source_key = str(source or "").strip().lower().replace(" ", "_")
    target_key = str(target or "").strip().lower().replace(" ", "_")
    file_stems = {
        "common",
        f"{source_key}_to_{target_key}",
        f"{source_key}-{target_key}",
    }
    mappings = []
    for directory in rule_dirs or _DEFAULT_RULE_DIRS:
        if not directory.exists():
            continue
        for path in directory.iterdir():
            if path.suffix.lower() not in {".yaml", ".yml", ".json"}:
                continue
            if path.stem.lower() not in file_stems:
                continue
            try:
                rules = _load_rule_file(path)
            except Exception:
                continue
            for rule in rules:
                if not isinstance(rule, dict):
                    continue
                pattern = rule.get("pattern") or rule.get("find") or rule.get("regex")
                replacement = rule.get("replacement") or rule.get("replace") or ""
                if not pattern:
                    continue
                pattern = str(pattern).replace("\\\\", "\\")
                replacement = str(replacement).replace("\\\\", "\\")
                flags_text = str(rule.get("flags") or "i").lower()
                flags = re.IGNORECASE if "i" in flags_text else 0
                if "s" in flags_text:
                    flags |= re.DOTALL
                if "m" in flags_text:
                    flags |= re.MULTILINE
                mappings.append(
                    {
                        "name": rule.get("name") or pattern,
                        "pattern": pattern,
                        "replacement": replacement,
                        "flags": flags,
                        "description": rule.get("description") or "",
                    }
                )
    return mappings
