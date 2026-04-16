"""
Rucio MCP Server for EIC/ePIC

MCP server providing Rucio data management tools for the Electron Ion Collider
(EIC) ePIC experiment. Exposes DIDs, replicas, RSEs, replication rules, and
account usage via the Model Context Protocol.

Authentication: X509 proxy certificate or username/password against BNL or
JLab Rucio instances.

Based on the Belle II rucio-mcp server by Cedric Serfon and Wouter Verkerke.
"""

import argparse
import os
import re
import time
from datetime import datetime
from json import loads
from typing import Any, Generator, Optional, Union

import requests
from urllib.parse import quote

from mcp.server.fastmcp import FastMCP


# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------

CERT_PATH = os.getenv("X509_USER_PROXY", "/tmp/x509")
RUCIO_ACCOUNT = os.getenv("RUCIO_ACCOUNT", "rucioddm")
RUCIO_USERNAME = os.getenv("RUCIO_USERNAME", "")
RUCIO_PASSWORD = os.getenv("RUCIO_PASSWORD", "")
RUCIO_AUTH_TYPE = os.getenv("RUCIO_AUTH_TYPE", "x509")  # "x509" or "userpass"
TOKEN_FILE_PATH = os.getenv("TOKEN_FILE_PATH", "/tmp/rucio_eic_token.txt")
RUCIO_URL = os.getenv("RUCIO_URL", "https://nprucio01.sdcc.bnl.gov:443")
# Use system CA bundle by default; override with RUCIO_CA_BUNDLE if needed.
# Set to "false" to disable verification (not recommended).
_ca_env = os.getenv("RUCIO_CA_BUNDLE", os.getenv("REQUESTS_CA_BUNDLE", ""))
if _ca_env.lower() == "false":
    CA_BUNDLE = False
elif _ca_env:
    CA_BUNDLE = _ca_env
else:
    CA_BUNDLE = True

AUTH_X509_URL = f"{RUCIO_URL}/auth/x509"
AUTH_USERPASS_URL = f"{RUCIO_URL}/auth/userpass"
DIDS_URL = f"{RUCIO_URL}/dids"
ACCOUNT_URL = f"{RUCIO_URL}/accounts"
RULES_URL = f"{RUCIO_URL}/rules"
RSES_URL = f"{RUCIO_URL}/rses"
REPLICAS_URL = f"{RUCIO_URL}/replicas"


# ---------------------------------------------------------------------------
# Response parsing helpers
# ---------------------------------------------------------------------------

def _load_json_data(response: requests.Response) -> Generator[Any, Any, Any]:
    """Parse streaming JSON responses (application/x-json-stream)."""
    if (
        "content-type" in response.headers
        and response.headers["content-type"] == "application/x-json-stream"
    ):
        for line in response.iter_lines():
            if line:
                yield _parse_response(line)
    else:
        if response.text:
            yield response.text


def _datetime_parser(dct: dict[Any, Any]) -> dict[Any, Any]:
    """JSON object_hook that converts '... UTC' strings to datetime objects."""
    for k, v in list(dct.items()):
        if isinstance(v, str) and re.search(" UTC", v):
            try:
                dct[k] = datetime.strptime(v, "%Y-%m-%d %H:%M:%S UTC")
            except Exception:
                pass
    return dct


def _parse_response(data: Union[str, bytes, bytearray]) -> Any:
    """Decode and parse a JSON response line."""
    if isinstance(data, (bytes, bytearray)):
        data = data.decode("utf-8")
    return loads(data, object_hook=_datetime_parser)


# ---------------------------------------------------------------------------
# Rucio HTTP helpers
# ---------------------------------------------------------------------------

def _make_rucio_request(
    url: str,
    method: str = "GET",
    headers: dict = None,
    payload: dict = None,
    params: dict = None,
) -> dict:
    """
    Make a request to the Rucio REST API.

    Returns dict with 'status' and 'data' on success, or 'error' on failure.
    """
    if headers is None:
        headers = {}
    try:
        response = requests.request(
            method, url, headers=headers, json=payload, params=params,
            verify=CA_BUNDLE, timeout=60,
        )
        response.raise_for_status()
        if response.headers.get("Content-Type") == "application/x-json-stream":
            return {"status": response.status_code, "data": list(_load_json_data(response))}
        if response.text:
            return {"status": response.status_code, "data": _parse_response(response.text)}
        return {"status": response.status_code, "data": None}
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Authentication — X509 or userpass
# ---------------------------------------------------------------------------

def _get_token_x509() -> dict:
    """Authenticate with Rucio via X509 proxy certificate."""
    headers = {"X-Rucio-Account": RUCIO_ACCOUNT}
    try:
        cert = (CERT_PATH, CERT_PATH)
        response = requests.get(
            AUTH_X509_URL, headers=headers, verify=CA_BUNDLE, stream=True,
            cert=cert, timeout=15,
        )
        response.raise_for_status()
        token = response.headers.get("X-Rucio-Auth-Token")
        if not token:
            return {"error": "Token not found in Rucio response headers."}
        with open(TOKEN_FILE_PATH, "w") as f:
            f.write(token)
        return {"status": response.status_code, "message": "Token stored successfully."}
    except requests.exceptions.RequestException as e:
        return {"error": f"Rucio X509 authentication failed: {e}"}


def _get_token_userpass() -> dict:
    """Authenticate with Rucio via username/password."""
    headers = {
        "X-Rucio-Account": RUCIO_ACCOUNT,
        "X-Rucio-Username": RUCIO_USERNAME,
        "X-Rucio-Password": RUCIO_PASSWORD,
    }
    try:
        response = requests.get(
            AUTH_USERPASS_URL, headers=headers, verify=CA_BUNDLE, timeout=15,
        )
        response.raise_for_status()
        token = response.headers.get("X-Rucio-Auth-Token")
        if not token:
            return {"error": "Token not found in Rucio response headers."}
        with open(TOKEN_FILE_PATH, "w") as f:
            f.write(token)
        return {"status": response.status_code, "message": "Token stored successfully."}
    except requests.exceptions.RequestException as e:
        return {"error": f"Rucio userpass authentication failed: {e}"}


def _get_token() -> dict:
    """Obtain a Rucio auth token using the configured auth method."""
    if RUCIO_AUTH_TYPE == "userpass":
        return _get_token_userpass()
    return _get_token_x509()


def _get_token_from_file() -> str:
    """
    Retrieve the cached Rucio auth token, refreshing if expired (>1 hour).

    Raises RuntimeError if authentication fails.
    """
    refresh = False
    if not os.path.exists(TOKEN_FILE_PATH):
        refresh = True
    else:
        stat = os.stat(TOKEN_FILE_PATH)
        if time.time() - stat.st_mtime > 3600:
            refresh = True

    if refresh:
        result = _get_token()
        if "error" in result:
            raise RuntimeError(result["error"])

    with open(TOKEN_FILE_PATH, "r") as f:
        token = f.read().strip()
    if not token:
        raise RuntimeError("Token file is empty. Rucio authentication may have failed.")
    return token


def _rucio_headers(accept: str = "application/json") -> dict:
    """Build standard Rucio API headers with a valid auth token."""
    token = _get_token_from_file()
    return {
        "X-Rucio-Auth-Token": token,
        "Content-Type": "application/json",
        "Accept": accept,
    }


# ---------------------------------------------------------------------------
# EIC/ePIC scope extraction
# ---------------------------------------------------------------------------

def _extract_scope_eic(did: str) -> dict[str, str]:
    """
    Extract scope and name from an EIC/ePIC DID string.

    EIC naming conventions:
    - group.EIC:dataset_name       — ePIC production datasets
    - group.daq:swf.NNNNNN.run    — streaming workflow / DAQ datasets
    - user.<username>:name         — user datasets
    - group.<group>:name           — group datasets

    If the DID contains a colon, it's already scope:name.
    Otherwise, infer from path conventions.
    """
    # Already has explicit scope
    if ":" in did:
        scope, name = did.split(":", 1)
        return {"scope": scope, "name": name}

    # Path-based inference for EIC conventions
    if did.startswith("/eic/") or did.startswith("/EIC/"):
        parts = did.split("/")
        # /eic/user/<username>/... → user.<username>
        if len(parts) > 3 and parts[2] == "user":
            return {"scope": f"user.{parts[3]}", "name": did}
        # /eic/group/<group>/... → group.<group>
        if len(parts) > 3 and parts[2] == "group":
            return {"scope": f"group.{parts[3]}", "name": did}
        return {"scope": "group.EIC", "name": did}

    # Streaming workflow datasets
    if did.startswith("swf."):
        return {"scope": "group.daq", "name": did}

    # Default to group.EIC for unrecognized patterns
    return {"scope": "group.EIC", "name": did}


# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "rucio-eic",
    stateless_http=True,
    json_response=True,
    host="0.0.0.0",
    port=8000,
)


@mcp.tool(description="List available Rucio scopes.")
def list_scopes() -> dict:
    """
    Fetch all available scopes from the Rucio instance.

    Returns the list of scopes (e.g., group.EIC, group.daq, user.wenaus, ...).
    Use this first to discover what data is available on the server.
    """
    try:
        headers = _rucio_headers()
    except RuntimeError as e:
        return {"error": str(e)}

    return _make_rucio_request(f"{RUCIO_URL}/scopes", headers=headers)


@mcp.tool(description="Search for DIDs (datasets/containers) within a scope, filtered by name pattern and/or metadata key=value filters.")
def list_dids(
    scope: str,
    name: Optional[str] = None,
    type: str = "DATASET",
    filters: Optional[dict[str, str]] = None,
    long: bool = False,
) -> dict:
    """
    Search for DIDs (Data Identifiers) within a given scope, with optional
    name-pattern and metadata-filter criteria.

    Equivalent to the Rucio CLI:
        rucio did list "scope:pattern" --filter "key=value,key2=value2"

    Args:
        scope: Rucio scope (e.g., 'group.EIC', 'group.daq', 'user.wenaus', 'epic').
        name: Optional name pattern filter. Supports Rucio wildcards '*' and '?'.
              Example: '*26.03.1*' matches any DID name containing '26.03.1'.
        type: DID type filter — DATASET (default), CONTAINER, FILE, or ALL.
              Note: COLLECTION (datasets+containers) is not supported by all Rucio
              versions; DATASET is the reliable default. Use ALL to span everything
              (can be huge — combine with name or filters).
        filters: Optional dict of metadata key=value filters. Any DID metadata field
              set on the server can be used (e.g., {"pwg": "inclusive"},
              {"datatype": "RECO", "campaign": "26.03"}). The server silently
              returns 0 matches for unpopulated keys — call get_did_metadata on a
              sample DID first to see which fields are populated for the scope.
        long: If True, return full DID info (type, bytes, length, ...) instead of
              just name. Useful when pairing a metadata search with inspection.
    """
    try:
        headers = _rucio_headers("application/x-json-stream")
    except RuntimeError as e:
        return {"error": str(e)}

    url = f"{DIDS_URL}/{scope}/dids/search"
    params: dict[str, str] = {"type": type}
    if name:
        params["name"] = name
    if long:
        params["long"] = "True"
    if filters:
        for k, v in filters.items():
            if k in params:
                return {"error": f"filter key '{k}' conflicts with a reserved parameter"}
            params[k] = v
    result = _make_rucio_request(url, headers=headers, params=params)

    # Auto-retry: if the caller asked for CONTAINER with a name or filter
    # and the server returned an empty list, retry as DATASET. Many Rucio
    # scopes (notably JLab's 'epic') hold flat datasets, not containers,
    # so a CONTAINER search silently returns nothing.
    if (
        type == "CONTAINER"
        and (name or filters)
        and "error" not in result
        and result.get("data") in (None, [], "")
    ):
        params["type"] = "DATASET"
        retry = _make_rucio_request(url, headers=headers, params=params)
        if "error" not in retry and retry.get("data"):
            retry["hint"] = (
                "type=CONTAINER returned 0 results; showing DATASET results "
                "instead. This scope appears to hold datasets, not containers."
            )
            return retry

    return result


@mcp.tool(description="List files within a Rucio dataset or container.")
def list_files(scope: str, name: str) -> dict:
    """
    Fetch the file listing for a Rucio DID (dataset or container).

    Args:
        scope: Rucio scope (e.g., 'group.EIC').
        name: DID name (e.g., 'epic.26.02.0.ePIC_craterlake.p1001.e1.s1.r1').
    """
    try:
        headers = _rucio_headers("application/x-json-stream")
    except RuntimeError as e:
        return {"error": str(e)}

    url = f"{DIDS_URL}/bulkfiles"
    payload = {"dids": [{"scope": scope, "name": name}]}
    return _make_rucio_request(url, method="POST", headers=headers, payload=payload)


@mcp.tool(description="List immediate children of a Rucio container or dataset.")
def list_content(scope: str, name: str) -> dict:
    """
    List the child DIDs within a container or dataset (one level).

    For containers: returns child datasets and/or containers.
    For datasets: returns child files.
    Use list_files for a recursive listing of all files.

    Args:
        scope: Rucio scope (e.g., 'epic', 'group.EIC').
        name: DID name (may contain slashes, e.g., '/RECO/26.03.1/...').
    """
    try:
        headers = _rucio_headers("application/x-json-stream")
    except RuntimeError as e:
        return {"error": str(e)}

    url = f"{DIDS_URL}/{quote(scope, safe='')}/{quote(name, safe='')}/dids"
    return _make_rucio_request(url, headers=headers)


@mcp.tool(description="Get DID details including type, size, file count, and custom fields.")
def get_did_metadata(scope: str, name: str) -> dict:
    """
    Fetch full details for a DID (Data Identifier).

    Args:
        scope: Rucio scope.
        name: DID name.

    Returns type, creation date, file count, total size, and any custom fields
    set on the DID.
    """
    try:
        headers = _rucio_headers()
    except RuntimeError as e:
        return {"error": str(e)}

    url = f"{DIDS_URL}/{quote(scope, safe='')}/{quote(name, safe='')}/meta"
    return _make_rucio_request(url, headers=headers)


@mcp.tool(description="Get storage quota limits for a Rucio account.")
def get_account_limits(account: str) -> dict:
    """
    Fetch account storage limits across all RSEs.

    Args:
        account: Rucio account name (e.g., 'wenaus', 'rucioddm').
    """
    try:
        headers = _rucio_headers()
    except RuntimeError as e:
        return {"error": str(e)}

    url = f"{ACCOUNT_URL}/{account}/limits"
    return _make_rucio_request(url, headers=headers)


@mcp.tool(description="Get storage usage for a Rucio account at a specific RSE.")
def get_account_usage(account: str, rse: str) -> dict:
    """
    Fetch storage usage for an account at a specific RSE.

    Args:
        account: Rucio account name.
        rse: RSE name (e.g., 'BNL_SDCC_EIC', 'JLAB_EIC').
    """
    try:
        headers = _rucio_headers()
    except RuntimeError as e:
        return {"error": str(e)}

    url = f"{ACCOUNT_URL}/{account}/usage/local/{rse}"
    return _make_rucio_request(url, headers=headers)


@mcp.tool(description="List all Rucio Storage Elements (RSEs).")
def list_rses() -> dict:
    """
    Fetch the list of all RSEs (Rucio Storage Elements).

    Returns RSE names, availability, and basic configuration.
    EIC RSEs include BNL_SDCC_EIC, JLAB_EIC, etc.
    """
    try:
        headers = _rucio_headers()
    except RuntimeError as e:
        return {"error": str(e)}

    return _make_rucio_request(RSES_URL, headers=headers)


@mcp.tool(description="Get storage usage statistics for a specific RSE.")
def get_rse_usage(rse: str) -> dict:
    """
    Fetch usage statistics for an RSE (used, free, total bytes).

    Args:
        rse: RSE name (e.g., 'BNL_SDCC_EIC').
    """
    try:
        headers = _rucio_headers("application/x-json-stream")
    except RuntimeError as e:
        return {"error": str(e)}

    url = f"{RSES_URL}/{rse}/usage"
    return _make_rucio_request(url, headers=headers)


@mcp.tool(description="List replication rules with optional filters (account, state).")
def list_rules(filters: Optional[dict[str, str]] = None) -> dict:
    """
    Fetch replication rules, optionally filtered.

    Args:
        filters: Optional dict of filters:
            - account: Filter by Rucio account.
            - state: Filter by rule state — 'O' (OK), 'R' (Replicating), 'S' (Stuck).
            Example: {"account": "wenaus", "state": "R"}
    """
    try:
        headers = _rucio_headers("application/x-json-stream")
    except RuntimeError as e:
        return {"error": str(e)}

    params = filters if filters else {}
    return _make_rucio_request(RULES_URL, headers=headers, params=params)


@mcp.tool(description="Get replica lock details for a replication rule.")
def get_rule_locks(rule_id: str) -> dict:
    """
    Fetch replica locks associated with a replication rule.

    Args:
        rule_id: The Rucio rule ID (UUID).
    """
    try:
        headers = _rucio_headers()
    except RuntimeError as e:
        return {"error": str(e)}

    url = f"{RULES_URL}/{rule_id}/locks"
    return _make_rucio_request(url, headers=headers)


@mcp.tool(description="Find where file replicas are located across RSEs.")
def list_file_replicas(dids: list[dict[str, str]]) -> dict:
    """
    Fetch replica locations for a list of DIDs.

    Args:
        dids: List of DIDs, each a dict with 'scope' and 'name'.
              Example: [{"scope": "group.EIC", "name": "file.root"}]

    Returns replica locations (RSEs and PFNs) for each file.
    """
    try:
        headers = _rucio_headers("application/x-json-stream")
    except RuntimeError as e:
        return {"error": str(e)}

    url = f"{REPLICAS_URL}/list"
    payload = {"dids": dids}
    return _make_rucio_request(url, method="POST", headers=headers, payload=payload)


@mcp.tool(description="Extract scope and name from an EIC DID string.")
def extract_scope(did: str) -> dict[str, str]:
    """
    Parse an EIC/ePIC DID string into scope and name components.

    Handles:
    - Explicit scope:name format (e.g., 'group.EIC:dataset_name')
    - EIC path conventions (/eic/user/..., /eic/group/...)
    - SWF dataset names (swf.NNNNNN.run)

    Args:
        did: The DID string to parse.
    """
    return _extract_scope_eic(did)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    """Run the MCP server. Supports stdio (for pandabot) and SSE transports."""
    parser = argparse.ArgumentParser(description="Rucio EIC MCP Server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse"],
        default="stdio",
        help="Transport protocol (default: stdio)",
    )
    args = parser.parse_args()
    mcp.run(transport=args.transport)


if __name__ == "__main__":
    main()
