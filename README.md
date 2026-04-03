# Rucio MCP Server for EIC/ePIC

MCP server providing [Rucio](https://rucio.cern.ch/) data management tools for the Electron Ion Collider (EIC) ePIC experiment.

## Tools

| Tool | Description |
|------|-------------|
| `list_scopes` | List available Rucio scopes |
| `list_dids` | Search for datasets/containers within a scope |
| `list_files` | List files within a dataset or container |
| `get_did_metadata` | Get DID details (type, size, file count, custom fields) |
| `get_account_limits` | Get storage quota limits for an account |
| `get_account_usage` | Get storage usage for an account at a specific RSE |
| `list_rses` | List all Rucio Storage Elements |
| `get_rse_usage` | Get storage usage statistics for an RSE |
| `list_rules` | List replication rules with optional filters |
| `get_rule_locks` | Get replica lock details for a replication rule |
| `list_file_replicas` | Find where file replicas are located across RSEs |
| `extract_scope` | Parse an EIC DID string into scope and name |

## Requirements

- Python 3.10+
- Valid X509 proxy certificate for Rucio authentication
- Network access to BNL Rucio instance (`blrucio.sdcc.bnl.gov`)

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `X509_USER_PROXY` | `/tmp/x509` | Path to X509 proxy certificate |
| `RUCIO_ACCOUNT` | `rucioddm` | Rucio account name |
| `RUCIO_URL` | `https://blrucio.sdcc.bnl.gov:443` | Rucio server URL |
| `TOKEN_FILE_PATH` | `/tmp/rucio_eic_token.txt` | Path to cache auth token |
| `RUCIO_CA_BUNDLE` | System default | CA bundle for TLS verification |

## Usage

### stdio (for pandabot / Claude Code integration)

```bash
python rucio_eic_mcp_server.py
```

### SSE (standalone HTTP server on port 8000)

```bash
python rucio_eic_mcp_server.py --sse
```

## Setup for sys admins

The server needs:

1. **X509 proxy certificate** — a service certificate or auto-renewed proxy valid for `blrucio.sdcc.bnl.gov`. Set `X509_USER_PROXY` to the path.
2. **Rucio account** with read access to EIC scopes (`group.EIC`, `group.daq`, `user.*`).
3. **Network access** from the host to `blrucio.sdcc.bnl.gov:443`.

Adapted from the Belle II Rucio MCP server for EIC/ePIC conventions.
