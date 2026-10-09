"""
Join inventory.yaml to snowcap's code without a Snowflake connection.

For each modeled object, write L1 (does the spec have the field?) and a list of candidate gaps
for each row. Candidates are leads for live_test.py, not results:

- L2: the field has no prop, so CREATE does not render it.
- L3: the fetch function does not return the field.
- L4: ALTER cannot change the property but snowcap does not refuse the change, or the
  ALTER that snowcap renders does not match the docs syntax.

Also report spec fields that no inventory row maps, so the inventory stays complete.

Usage: uv run python tools/snowflake_coverage/static_map.py [OBJECT[:variant] ...]
"""

import ast
import inspect
import re
import sys
from dataclasses import fields

from common import LEVELS, build_resource, load_inventory, object_label, resource_class, save_inventory, select_objects

from snowcap import data_provider, lifecycle
from snowcap.identifiers import resource_label_for_type
from snowcap.resources.column import Column
from snowcap.resources.tag import TaggableResource

# Fields that every resource carries outside its props: the name is in the CREATE header and
# ownership moves through GRANT OWNERSHIP.
HEADER_FIELDS = {"name", "owner"}


def fetched_keys(cls) -> set[str]:
    """String keys of the dict literals that fetch_<label> returns."""
    fetch = getattr(data_provider, f"fetch_{resource_label_for_type(cls.resource_type)}")
    tree = ast.parse(inspect.getsource(fetch).lstrip())
    keys = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Return) and isinstance(node.value, ast.Dict):
            keys |= {k.value for k in node.value.keys if isinstance(k, ast.Constant)}
    return keys


def head_pattern(alter_syntax: str):
    """Regex for the ALTER action that follows the object name in a docs syntax line."""
    match = re.search(r"<name>\s*(?:\(\s*<arg_types>\s*\))?\s*(.*)", alter_syntax)
    if not match:
        return None
    parts = []
    for token in re.findall(r"\{[^}]*\}|\S+", match.group(1)):
        if token.startswith(("<", "(", "=", "'", "[")):
            break
        if token.startswith("{"):
            options = [re.escape(o.strip()) for o in token.strip("{}").split("|")]
            parts.append("(?:" + "|".join(options) + ")")
        else:
            parts.append(re.escape(token))
    return re.compile(r"\s+".join(parts), re.IGNORECASE) if parts else None


def l1(cls, field: str) -> bool:
    if field.startswith("columns[]."):
        return field.split(".", 1)[1] in {f.name for f in fields(Column.spec)}
    if field == "tags":
        # Tags are a relationship: snowcap models them on the resource class (snowcap/resources/tag.py), not in the spec.
        return issubclass(cls, TaggableResource)
    return field in {f.name for f in fields(cls.spec)}


def l4_candidates(obj: dict, row: dict, cls) -> list[str]:
    field = row["snowcap"]["field"]
    if row["alter"] == "none":
        if not cls.spec.get_metadata(field).triggers_replacement:
            return [
                "L4: ALTER cannot change it, but the spec does not set triggers_replacement, so snowcap plans an ALTER"
            ]
        return []
    sample = row["snowcap"].get("sample")
    if not sample or field in HEADER_FIELDS:
        return []
    before = build_resource(obj, "P", "COVERAGE_STATIC", "ACCOUNTADMIN", {field: sample[0]})
    after = build_resource(obj, "P", "COVERAGE_STATIC", "ACCOUNTADMIN", {field: sample[1]})
    try:
        rendered = lifecycle.update_resource(before.urn, {field: after.to_dict()[field]}, cls.props)
    except Exception as err:
        return [f"L4: the update handler raises {type(err).__name__}: {err}"]
    statements = [rendered] if isinstance(rendered, str) else rendered
    fqn = str(before.urn.fqn).upper()
    actions = [" ".join(s.split()).upper().split(fqn, 1)[-1].strip() for s in statements]
    pattern = head_pattern(row["alter_syntax"])
    if pattern and not any(pattern.match(a) for a in actions):
        return [f"L4: snowcap renders {statements!r}, docs syntax is {row['alter_syntax']!r}"]
    return []


def map_object(obj: dict) -> None:
    cls = resource_class(obj)
    keys = fetched_keys(cls)
    mapped = set()
    for row in obj["properties"]:
        meta = row["snowcap"]
        field = meta["field"]
        if field:
            mapped.add(field.split("[]")[0])
        if meta.get("n_a"):
            meta.pop("levels", None)
            continue
        levels = meta.get("levels") or dict.fromkeys(LEVELS, "untested")
        candidates = []
        if not field:
            levels.update(L1="fail")
        else:
            levels["L1"] = "pass" if l1(cls, field) else "fail"
            top = field.split("[]")[0]
            if levels["L1"] == "pass" and top in {f.name for f in fields(cls.spec)}:
                if top not in cls.props.props and top not in HEADER_FIELDS:
                    candidates.append(f"L2: {cls.__name__}.props has no '{top}' prop, so CREATE does not render it")
                if cls.spec.get_metadata(top).fetchable and top not in keys:
                    candidates.append(f"L3: the fetch function does not return '{top}'")
                if "[]" not in field:
                    candidates += l4_candidates(obj, row, cls)
        meta["levels"] = levels
        meta["candidates"] = candidates
    spec_fields = {f.name for f in fields(cls.spec)}
    unmapped = sorted(spec_fields - mapped - set(obj["snowcap"].get("creation_options", [])))
    obj["snowcap"]["unmapped_fields"] = unmapped


def main(labels: list[str]) -> None:
    inventory = load_inventory()
    for obj in select_objects(inventory, labels):
        map_object(obj)
        label = object_label(obj)
        if obj["snowcap"]["unmapped_fields"]:
            print(f"{label}: spec fields with no inventory row: {obj['snowcap']['unmapped_fields']}")
        for row in obj["properties"]:
            meta = row["snowcap"]
            if meta.get("n_a"):
                continue
            if meta["levels"]["L1"] == "fail":
                print(f"{label}.{row['name']}: L1 fail (not modeled)")
            for candidate in meta["candidates"]:
                print(f"{label}.{row['name']}: {candidate}")
    save_inventory(inventory)


if __name__ == "__main__":
    main(sys.argv[1:])
