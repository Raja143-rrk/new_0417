import json
from pathlib import Path

try:
    import yaml
except Exception:  # pragma: no cover - optional dependency
    yaml = None


_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_RAG_DIRS = [
    _PROJECT_ROOT / "app" / "rag" / "mappings",
]


def _parse_scalar(value: str):
    text = str(value or "").strip()
    if text in {"true", "True"}:
        return True
    if text in {"false", "False"}:
        return False
    if (text.startswith('"') and text.endswith('"')) or (text.startswith("'") and text.endswith("'")):
        return text[1:-1]
    return text


def _parse_simple_yaml(text: str):
    # Minimal YAML subset for rule files: list items with scalar key/value pairs.
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
    if items:
        return items
    return {}


def _load_mapping_file(path: Path):
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        data = json.loads(text or "[]")
    else:
        if yaml is not None:
            data = yaml.safe_load(text) or []
        else:
            data = _parse_simple_yaml(text)
    if isinstance(data, dict):
        if isinstance(data.get("rules"), list):
            return data["rules"]
        mappings = []
        metadata = data.get("metadata")
        for key, value in data.items():
            if key == "metadata":
                continue
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        mapping = {"category": key, **item}
                        if isinstance(metadata, dict):
                            mapping["metadata"] = metadata
                        mappings.append(mapping)
            elif isinstance(value, dict):
                mapping = {"category": key, **value}
                if isinstance(metadata, dict):
                    mapping["metadata"] = metadata
                mappings.append(mapping)
        if mappings:
            return mappings
        return [data]
    if isinstance(data, list):
        return data
    return []


def load_rag_mappings(source: str, target: str, mapping_dirs: list[Path] | None = None) -> list[dict]:
    source_key = str(source or "").strip().lower().replace(" ", "_")
    target_key = str(target or "").strip().lower().replace(" ", "_")
    file_stems = {
        f"{source_key}_to_{target_key}",
        f"{source_key}-{target_key}",
    }
    mappings = []
    for directory in mapping_dirs or _DEFAULT_RAG_DIRS:
        if not directory.exists():
            continue
        for path in directory.iterdir():
            if path.suffix.lower() not in {".yaml", ".yml", ".json"}:
                continue
            if path.stem.lower() not in file_stems:
                continue
            try:
                mappings.extend(_load_mapping_file(path))
            except Exception:
                continue
    return mappings


def build_rag_context(source: str, target: str) -> str:
    sections = []
    for mapping in load_rag_mappings(source, target):
        if not isinstance(mapping, dict):
            continue
        category = str(mapping.get("category") or "").strip()
        name = str(mapping.get("name") or "").strip()
        source_value = mapping.get("source") or mapping.get("input") or mapping.get("issue")
        target_value = mapping.get("target") or mapping.get("output") or mapping.get("fix")
        notes = mapping.get("notes") or mapping.get("description")
        if not source_value and not target_value:
            continue
        prefix_parts = []
        if category:
            prefix_parts.append(f"[{category}]")
        if name:
            prefix_parts.append(name)
        prefix = " ".join(prefix_parts)
        line = f"- {prefix} " if prefix else "- "
        line += f"Source: {source_value}\n  Target/Fix: {target_value}"
        if notes:
            line += f"\n  Notes: {notes}"
        sections.append(line)
    return "\n".join(sections)
