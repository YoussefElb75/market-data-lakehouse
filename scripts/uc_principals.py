"""Create the project's service principals once (idempotent) and grant catalog access.

Roles (see docs/governance.md):

* ``mdl-pipeline``: runs the prod job and pipeline; writes every layer.
* ``mdl-analyst``: reads the gold marts only.

Unity Catalog accepts service principals as grantees, but on Free Edition not
workspace-local groups, so roles are service principals. Their application ids
are never written to the repository: the bundle resolves them by display name
at deploy time (``lookup: service_principal``).

With ``--grant-deployer`` the current user also gets the ``servicePrincipal.user``
role on ``mdl-pipeline``, which a pipeline ``run_as`` binding requires.

Usage: python scripts/uc_principals.py --profile mdl --env prod [--grant-deployer]
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from urllib.parse import quote

from uc_bootstrap import ENVIRONMENTS, _api, _cli, _warehouse, run_sql

PRINCIPALS = ("mdl-pipeline", "mdl-analyst")


def ensure_principal(profile: str, name: str) -> str:
    """Return the application id of ``name``, creating the service principal if needed."""
    found = _api(
        profile, "get", f"/api/2.0/preview/scim/v2/ServicePrincipals?filter=displayName%20eq%20%22{name}%22"
    ).get("Resources", [])
    if found:
        print(f"service principal {name}: exists")
        return str(found[0]["applicationId"])
    created = _api(
        profile,
        "post",
        "/api/2.0/preview/scim/v2/ServicePrincipals",
        {
            "displayName": name,
            "active": True,
            "entitlements": [{"value": "workspace-access"}],
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"],
        },
    )
    print(f"service principal {name}: created")
    return str(created["applicationId"])


def _account_id(profile: str) -> str:
    out = subprocess.run([_cli(), "auth", "describe", "--profile", profile, "-o", "json"],
                         capture_output=True, text=True, check=True)  # fmt: skip
    value = json.loads(out.stdout)["details"]["configuration"]["account_id"]["value"]
    return str(value)


def grant_sp_user_role(profile: str, app_id: str) -> None:
    """Let the deploying user act as the service principal (needed for bundle ``run_as``).

    Pipelines refuse ``run_as`` unless the user creating or updating them holds the
    ``servicePrincipal.user`` role on that service principal. Idempotent. Only run with
    ``--grant-deployer`` (a permission grant, so it is opt-in).
    """
    me = _api(profile, "get", "/api/2.0/preview/scim/v2/Me")["userName"]
    name = f"accounts/{_account_id(profile)}/servicePrincipals/{app_id}/ruleSets/default"
    path = "/api/2.0/preview/accounts/access-control/rule-sets"
    current = _api(profile, "get", f"{path}?name={quote(name, safe='')}&etag=")
    rules = current.get("grant_rules", [])
    principal = f"users/{me}"
    role = "roles/servicePrincipal.user"
    if any(r.get("role") == role and principal in r.get("principals", []) for r in rules):
        print("  -> deploying user already holds servicePrincipal.user")
        return
    for rule in rules:
        if rule.get("role") == role:
            rule["principals"] = [*rule.get("principals", []), principal]
            break
    else:
        rules.append({"role": role, "principals": [principal]})
    body = {"name": name, "rule_set": {"name": name, "etag": current["etag"], "grant_rules": rules}}
    _api(profile, "put", path, body)
    print("  -> servicePrincipal.user granted to the deploying user")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--profile", required=True)
    parser.add_argument("--env", choices=ENVIRONMENTS, required=True)
    parser.add_argument(
        "--grant-deployer",
        action="store_true",
        help="also give the current user servicePrincipal.user on mdl-pipeline (for prod run_as)",
    )
    args = parser.parse_args(argv)
    catalog = f"mdl_{args.env}"
    ids = {name: ensure_principal(args.profile, name) for name in PRINCIPALS}
    if args.grant_deployer:
        grant_sp_user_role(args.profile, ids["mdl-pipeline"])
    warehouse = _warehouse(args.profile)
    # Catalog-level privileges (the catalog is not a bundle resource on Free Edition);
    # schema and volume privileges are declared in resources/uc.yml.
    for name, app_id in ids.items():
        run_sql(args.profile, warehouse, f"GRANT USE CATALOG ON CATALOG {catalog} TO `{app_id}`", quiet=True)
        print(f"  -> USE CATALOG on {catalog} granted to {name}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
