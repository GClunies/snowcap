"""
Run L2 to L5 on the test account for each inventory row that has samples, and write the results
into inventory.yaml.

For each row the script creates one object with the first sample value, then:

- L2 Created: Snowflake reports the declared value (truth queries). Without a truth key, L2 passes
  only through L3.
- L3 Fetched: snowcap's fetch, read through the spec as plan does, equals the declared value.
- L4 Updated: a change to the second sample value lands in Snowflake. When ALTER cannot change the
  property (docs `alter: none`), L4 passes only if snowcap refuses the change with an error before
  it sends SQL. A refusal is an error that snowcap raises on purpose: a bare Exception,
  NotImplementedError, or a snowcap.exceptions class. Any other error is a crash and fails.
- L5 Round trip: plan shows no change after the create and after the change. When snowcap
  correctly refuses the change, nothing is applied, so L5 is n/a.

It also runs one combined test per object: every combinable SET property changes in one plan, and
each value must land.

Every object runs in its own throwaway database, which is dropped at the end, also on failure.

Usage: uv run python tools/snowflake_coverage/live_test.py [OBJECT[:variant] ...]
"""

import contextlib
import copy
import datetime
import io
import re
import sys
import uuid
from pathlib import Path

import snowflake.connector

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from common import (
    build_resource,
    live_inputs_digest,
    load_inventory,
    object_label,
    resource_class,
    save_inventory,
    select_objects,
    testable_rows,
)  # noqa: E402
from conftest import TEST_ROLE, connection_params  # noqa: E402

from snowcap import data_provider  # noqa: E402
from snowcap.blueprint import Blueprint, TransferOwnership, UpdateResource, compile_plan_to_sql  # noqa: E402
from snowcap.client import reset_cache  # noqa: E402

BOOLEAN_WORDS = {"y": "true", "yes": "true", "on": "true", "n": "false", "no": "false", "off": "false"}


class Refused(Exception):
    """snowcap stopped the change on purpose, with its own error, before it sent SQL."""


class Crashed(Exception):
    """snowcap failed with an internal error, for example a KeyError in an update handler."""


def snowcap_error(err: Exception) -> Exception:
    """Classify an error that snowcap raised while it planned or compiled a change."""
    deliberate = type(err) in (Exception, NotImplementedError) or type(err).__module__ == "snowcap.exceptions"
    return (Refused if deliberate else Crashed)(short(err))


class NotPlanned(Exception):
    """A changed value produced no change for the field in the plan."""


def short(err: BaseException) -> str:
    text = " ".join(str(err).split())
    return f"{type(err).__name__}: {text[:300]}"


def norm(value) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    text = str(value).strip().strip("'\"").lower().replace("_", " ")
    return BOOLEAN_WORDS.get(text, text)


class Account:
    def __init__(self, conn):
        self.conn = conn
        self.cur = conn.cursor(snowflake.connector.DictCursor)
        self.edition = data_provider.fetch_session(conn)["account_edition"]

    def provenance(self) -> dict:
        """The account facts that the live results depend on."""
        session = data_provider.fetch_session(self.conn)
        return {
            "edition": str(session["account_edition"]),
            "cloud": str(session["cloud"]),
            "cloud_region": session["cloud_region"],
            "snowflake_version": session["version"],
            "test_role": str(session["role"]),
            "checked_on": datetime.date.today().isoformat(),
        }

    def plan(self, resource) -> tuple[Blueprint, list]:
        blueprint = Blueprint(resources=[resource])
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                return blueprint, blueprint.plan(self.conn)
        except snowflake.connector.errors.Error:
            raise
        except Exception as err:
            raise snowcap_error(err) from err

    def sql(self, plan: list) -> list[str]:
        """The SQL that apply would run."""
        try:
            # Some update handlers pop fields off change.delta, so compile a copy and leave the plan for apply.
            compiled, _ = compile_plan_to_sql(data_provider.fetch_session(self.conn), copy.deepcopy(plan))
        except Exception as err:
            raise snowcap_error(err) from err
        return [command for change in compiled for command in change["commands"]]

    def apply(self, blueprint: Blueprint, plan: list) -> None:
        """Run the plan. sql() already compiled it, so any snowcap error here comes after the SQL started."""
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                blueprint.apply(self.conn, plan)
        except snowflake.connector.errors.Error:
            raise
        except Exception as err:
            raise Crashed(short(err)) from err
        finally:
            self.cur.execute(f"USE ROLE {TEST_ROLE}")

    def create(self, resource) -> list[str]:
        blueprint, plan = self.plan(resource)
        sql = self.sql(plan)
        self.apply(blueprint, plan)
        return sql

    def truth(self, obj: dict, resource, database: str) -> dict:
        """Snowflake's own view of the object, from the inventory's truth queries."""
        name = str(resource.name).strip('"')
        values = {
            "name": name,
            "database": database,
            "container": f"{database}.PUBLIC" if "schema" in obj["scope_level"] else database,
            "fqn": str(resource.fqn),
        }
        truth = {}
        for query in obj["snowcap"]["truth"]:
            for row in self.cur.execute(query.format(**values)).fetchall():
                row = {k.lower(): v for k, v in row.items()}
                if "key" in row and "value" in row:
                    truth[row["key"].lower()] = row["value"]
                elif "property" in row and "value" in row:
                    truth[row["property"].lower()] = row["value"]
                elif str(row.get("name", "")).upper() == name.upper():
                    truth.update(row)
        return truth

    def fetched(self, cls, resource):
        """The field values snowcap reads back, normalized through the spec the way plan does."""
        reset_cache()
        data = data_provider.fetch_resource(self.conn, resource.urn)
        return None if data is None else cls.spec(**data).to_dict(self.edition)


def is_for(change, resource) -> bool:
    # resource.urn has an empty account locator and plan URNs carry the real one, so compare the rest.
    return change.urn.resource_type == resource.resource_type and change.urn.fqn == resource.urn.fqn


def changes_for(plan: list, resource) -> list[str]:
    described = []
    for change in plan:
        if not is_for(change, resource):
            continue
        if isinstance(change, UpdateResource):
            described.append(f"update {sorted(change.delta)}")
        else:
            described.append(type(change).__name__)
    return described


def touches(plan: list, resource, field: str) -> bool:
    return any(
        is_for(change, resource)
        and (
            (isinstance(change, UpdateResource) and field in change.delta)
            or (field == "owner" and isinstance(change, TransferOwnership))
        )
        for change in plan
    )


def landed(account: Account, obj: dict, cls, resource, database: str, field: str, value, truth_key) -> tuple[bool, str]:
    """Whether Snowflake holds value for field. Uses the truth query when the row has a key."""
    if truth_key:
        reported = account.truth(obj, resource, database).get(truth_key)
        return norm(reported) == norm(value), f"Snowflake reports {truth_key}={reported!r}"
    fetched = (account.fetched(cls, resource) or {}).get(field)
    declared = resource.to_dict(account.edition)[field]
    return fetched == declared, f"snowcap fetches {fetched!r}"


def test_row(account: Account, obj: dict, row: dict, database: str, index: int) -> None:
    meta = row["snowcap"]
    field, (before, after), truth_key = meta["field"], meta["sample"], meta.get("truth")
    cls = resource_class(obj)
    name = f"R{index:02d}_{re.sub(r'[^A-Za-z0-9]', '_', row['name']).upper()}"
    levels = meta.setdefault("levels", {})
    evidence = {}
    meta["evidence"] = evidence

    def make(value):
        # A blueprint finalizes its resources, so each plan needs a new instance.
        return build_resource(obj, name, database, TEST_ROLE, {field: value})

    resource = make(before)
    try:
        evidence["create_sql"] = account.create(resource)
    except (Refused, Crashed, snowflake.connector.errors.Error) as err:
        levels.update(L2="fail", L3="untested", L4="untested", L5="untested")
        evidence["L2"] = f"create failed: {short(err)}"
        return

    declared = resource.to_dict(account.edition)[field]
    fetched = (account.fetched(cls, resource) or {}).get(field)
    if truth_key:
        # L2 and L3 are separate checks against Snowflake's own value.
        reported = account.truth(obj, resource, database).get(truth_key)
        levels["L2"] = "pass" if norm(reported) == norm(before) else "fail"
        levels["L3"] = "pass" if norm(fetched) == norm(reported) else "fail"
        if levels["L2"] == "fail":
            evidence["L2"] = f"declared {before!r}, Snowflake reports {truth_key}={reported!r}"
        if levels["L3"] == "fail":
            evidence["L3"] = f"Snowflake reports {truth_key}={reported!r}, snowcap fetches {fetched!r}"
    else:
        # Without an independent read, a matching fetch is the only proof that CREATE wrote the value.
        levels["L3"] = "pass" if fetched == declared else "fail"
        levels["L2"] = "pass" if levels["L3"] == "pass" else "untested"
        if levels["L3"] == "fail":
            evidence["L3"] = f"declared {declared!r}, snowcap fetches {fetched!r}"

    _, plan = account.plan(make(before))
    drift_after_create = changes_for(plan, resource)

    # When CREATE did not write the first value, Snowflake can already hold the second one, so a
    # change toward it is a no-op. Change toward the first value instead: Snowflake does not hold it.
    target = before if levels["L2"] == "fail" else after
    changed = make(target)
    applied = False
    try:
        blueprint, plan = account.plan(changed)
        if not touches(plan, changed, field):
            raise NotPlanned(f"a change to {target!r} plans {changes_for(plan, changed) or 'nothing'}")
        evidence["L4_sql"] = account.sql(plan)
        account.apply(blueprint, plan)
        applied = True
        ok, detail = landed(account, obj, cls, changed, database, field, target, truth_key)
        levels["L4"] = "pass" if ok else "fail"
        if not ok:
            evidence["L4"] = f"apply ran but the change did not land: {detail}"
        elif row["alter"] == "none":
            evidence["L4"] = "the docs say ALTER cannot change it, but the ALTER ran and the change landed"
    except Refused as err:
        refused_ok = row["alter"] == "none"
        levels["L4"] = "pass" if refused_ok else "fail"
        evidence["L4"] = ("snowcap refuses: " if refused_ok else "snowcap refuses a change ALTER supports: ") + str(err)
    except Crashed as err:
        levels["L4"] = "fail"
        evidence["L4"] = f"snowcap crashes: {err}"
    except snowflake.connector.errors.Error as err:
        levels["L4"] = "fail"
        evidence["L4"] = f"Snowflake rejects the SQL: {short(err)}"
    except NotPlanned as err:
        levels["L4"] = "fail"
        evidence["L4"] = str(err)

    drift_after_change = changes_for(account.plan(make(target))[1], changed) if applied else []
    if drift_after_create or drift_after_change:
        levels["L5"] = "fail"
        evidence["L5"] = f"plan after create: {drift_after_create}; plan after change: {drift_after_change}"
    elif not applied and levels["L4"] == "pass":
        levels["L5"] = "n/a"
        evidence["L5"] = "no round trip: the property is immutable and snowcap correctly refuses the change"
    else:
        levels["L5"] = levels["L4"]
        if levels["L5"] == "fail":
            evidence["L5"] = "fails because L4 fails"


def test_combined(account: Account, obj: dict, database: str) -> dict:
    """Change every combinable SET property in one plan, and report the values that did not land."""
    rows = [r for r in testable_rows(obj) if r["alter"] == "set" and r["combinable"] is True]
    if len(rows) < 2:
        return {"fields": [r["snowcap"]["field"] for r in rows], "result": "untested (needs two or more fields)"}
    cls = resource_class(obj)
    before = {r["snowcap"]["field"]: r["snowcap"]["sample"][0] for r in rows}
    after = {r["snowcap"]["field"]: r["snowcap"]["sample"][1] for r in rows}
    result = {"fields": sorted(after)}
    try:
        account.create(build_resource(obj, "COMBINED", database, TEST_ROLE, before))
        changed = build_resource(obj, "COMBINED", database, TEST_ROLE, after)
        blueprint, plan = account.plan(changed)
        result["sql"] = account.sql(plan)
        account.apply(blueprint, plan)
    except (Refused, Crashed, snowflake.connector.errors.Error) as err:
        result["result"] = "fail"
        result["error"] = short(err)
        return result
    missed = [
        r["snowcap"]["field"]
        for r in rows
        if not landed(
            account,
            obj,
            cls,
            changed,
            database,
            r["snowcap"]["field"],
            after[r["snowcap"]["field"]],
            r["snowcap"].get("truth"),
        )[0]
    ]
    result["result"] = "fail" if missed else "pass"
    if missed:
        result["missed"] = missed
    return result


def test_object(account: Account, obj: dict) -> None:
    label = object_label(obj)
    database = f"COVERAGE_{re.sub(r'[^A-Za-z0-9]', '_', label).upper()}_{uuid.uuid4().hex[:8].upper()}"
    account.cur.execute(f"CREATE DATABASE {database}")
    try:
        for index, row in enumerate(testable_rows(obj)):
            test_row(account, obj, row, database, index)
            levels = row["snowcap"]["levels"]
            print(f"{label}.{row['name']}: " + " ".join(f"{k}={v}" for k, v in levels.items()), flush=True)
        obj["snowcap"]["combined"] = test_combined(account, obj, database)
        obj["snowcap"]["tested_inputs"] = live_inputs_digest(obj)
        print(f"{label} combined: {obj['snowcap']['combined']['result']}", flush=True)
    finally:
        account.cur.execute(f"USE ROLE {TEST_ROLE}")
        account.cur.execute(f"DROP DATABASE IF EXISTS {database}")


def main(labels: list[str]) -> None:
    inventory = load_inventory()
    conn = snowflake.connector.connect(**connection_params())
    try:
        conn.cursor().execute("ALTER SESSION SET QUERY_TAG = 'snowcap_coverage_audit'")
        account = Account(conn)
        inventory["account"] = account.provenance()
        for obj in select_objects(inventory, labels):
            if obj["snowcap"]["test"] == "live":
                test_object(account, obj)
                save_inventory(inventory)
    finally:
        conn.close()


if __name__ == "__main__":
    main(sys.argv[1:])
