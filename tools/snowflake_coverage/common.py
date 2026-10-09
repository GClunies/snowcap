"""
Shared helpers for the Snowflake coverage audit.

inventory.yaml is the coverage source of truth. Each object entry holds the Snowflake docs facts
(from the CREATE, ALTER, SHOW, and DESCRIBE pages) and a `snowcap` block:

    snowcap:
      class: Table            # snowcap resource class, or null when snowcap does not model the object
      test: live              # live | docs_only | untested
      create_args: {...}      # the minimum arguments to create a test object
      truth: [SQL, ...]       # queries that report Snowflake's own values; {name} {database} {container} {fqn}
      creation_options: [...] # spec fields that are CREATE options, not state (for example copy_grants)

Each property row has its own `snowcap` block:

    snowcap:
      field: data_retention_time_in_days  # spec field, or null when snowcap does not model the property
      n_a: reason                         # set when the row is not a manageable state of this object
      sample: [3, 5]                      # create value, changed value (rows without it stay untested)
      truth: retention_time               # key in the truth query output
      levels: {L1: pass, L2: ..., L5: ...}
      evidence: {L3: ...}
      candidates: [...]                   # static findings to confirm live

static_map.py writes L1 and candidates. live_test.py writes L2 to L5, evidence, the object's
`combined` result, and `tested_inputs`, a digest of the inputs that produced those results.
"""

import copy
import hashlib
import json
from dataclasses import fields
from pathlib import Path
from typing import Iterator, Optional

import yaml

from snowcap import resources
from snowcap.scope import DatabaseScope, SchemaScope

INVENTORY = Path(__file__).with_name("inventory.yaml")
LEVELS = ("L1", "L2", "L3", "L4", "L5")
LIVE_LEVELS = LEVELS[1:]


def load_inventory() -> dict:
    """Read the inventory and clear the live results of every object whose test inputs changed since its run."""
    inventory = yaml.safe_load(INVENTORY.read_text())
    for obj in select_objects(inventory, None):
        tested = obj["snowcap"].get("tested_inputs")
        if tested and tested != live_inputs_digest(obj):
            clear_live_results(obj)
            print(f"{object_label(obj)}: test inputs changed since the live run, live results cleared")
    return inventory


def live_inputs_digest(obj: dict) -> str:
    """A short digest of every inventory value that decides the object's live results."""
    rows = [
        {k: row["snowcap"].get(k) for k in ("field", "sample", "truth")} | {k: row[k] for k in ("alter", "combinable")}
        for row in testable_rows(obj)
    ]
    inputs = [obj["snowcap"]["create_args"], obj["snowcap"]["truth"], rows]
    return hashlib.sha256(json.dumps(inputs, sort_keys=True, default=str).encode()).hexdigest()[:12]


def clear_live_results(obj: dict) -> None:
    for key in ("tested_inputs", "combined"):
        obj["snowcap"].pop(key, None)
    for row in obj["properties"]:
        row["snowcap"].pop("evidence", None)
        if "levels" in row["snowcap"]:
            row["snowcap"]["levels"].update(dict.fromkeys(LIVE_LEVELS, "untested"))


def save_inventory(inventory: dict) -> None:
    INVENTORY.write_text(yaml.safe_dump(inventory, sort_keys=False, width=120, allow_unicode=True))


def object_label(obj: dict) -> str:
    return obj["object"] + (f":{obj['variant']}" if obj["variant"] else "")


def select_objects(inventory: dict, labels: Optional[list[str]]) -> Iterator[dict]:
    """Yield the modeled objects whose label (OBJECT or OBJECT:variant) is in labels, or all when labels is empty.

    Objects without a snowcap block have docs rows only and are not mapped to snowcap yet.
    """
    for obj in inventory["objects"]:
        if obj.get("snowcap", {}).get("class") and (not labels or object_label(obj) in labels):
            yield obj


def resource_class(obj: dict) -> type:
    return getattr(resources, obj["snowcap"]["class"])


def build_resource(obj: dict, name: str, database: str, owner: str, values: Optional[dict] = None):
    """Create the snowcap resource for a test object in database, with the given spec field values."""
    cls = resource_class(obj)
    kwargs = copy.deepcopy(obj["snowcap"]["create_args"])
    kwargs.update(name=name, owner=owner)
    if isinstance(cls.scope, SchemaScope):
        kwargs.update(database=database, schema="PUBLIC")
    elif isinstance(cls.scope, DatabaseScope):
        kwargs.update(database=database)
    kwargs.update(copy.deepcopy(values or {}))
    return cls(**kwargs)


def testable_rows(obj: dict) -> list[dict]:
    """Rows that map to a top-level spec field and carry samples. Column and tag rows stay untested."""
    spec_fields = {f.name for f in fields(resource_class(obj).spec)}
    return [
        p
        for p in obj["properties"]
        if p["snowcap"].get("sample") and not p["snowcap"].get("n_a") and p["snowcap"]["field"] in spec_fields
    ]
