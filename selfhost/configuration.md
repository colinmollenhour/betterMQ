# Configuration reference

Generate the authoritative JSON Schema from the installed binary:

```sh
bettermq config schema > bettermq.schema.json
bettermq config init --template local --output bettermq.json
bettermq config validate --config bettermq.json
bettermq doctor --config bettermq.json --data-dir ./data
```

The schema is generated from the same strict Rust types as the loader, not from
template examples. Unknown keys, including nested retry/backoff keys, are errors.
V1 files and the legacy storage `rocksdb`/`wal` and auth `panel` aliases remain
readable. Loading never rewrites an existing file. The schema describes types;
`config validate` additionally enforces cross-field rules and supported roles.

## V2 sections

| Section / field | Meaning |
|---|---|
| `version` | 1 or 2; new templates use 2 |
| `node.name`, `node.publicUrl` | Local node identity and advertised URL |
| `node.listen` | Compatibility bind fallback, default `0.0.0.0:8080` |
| `dataDir` | Durable node data directory, default `./data` |
| `storage` | Tagged `mode`: `local`, or `slate` with `s3` settings |
| `storage.s3` | `endpoint`, `bucket`, `payloadBucket`, `accessKey`, `secretKey`, optional `region` (default `auto`) |
| `auth` | `mode: local` with optional `authFile`; `cloud` with `databaseUrl` requires a cloud build |
| `dispatch` | `httpTimeoutSecs` (300), `longHttpTimeoutSecs` (7200), optional `retry` |
| `dispatch.retry` | `maxRetries` (3), `backoff`: `kind` (`fixed`/`exponential`), `initialMs` (500), `maxMs` (30000), `multiplier` (2) |
| `cluster` | `enabled`, optional `id`, named `nodes` with `publicUrl`, optional `sharedMetaDir` |
| `components` | `profile`: `all`, `broker`, `gateway`, `dispatch`, `panel`; optional `include` component names |
| `listeners` | Optional `public`, `admin`, `internal` socket addresses |
| `panel` | Optional `mode`: `embedded`, `separateListener`, `disabled`, `standalone`; `listen`, `controller` HTTP(S) URL |
| `replication` | `factor`, `minIsr`; requires `1 <= minIsr <= factor` and must match the controller policy at startup |

CLI overrides take priority over supported environment inputs, then file values,
then defaults. `listeners.public` overrides `node.listen`. Admin and panel share
one role socket: an explicit admin address wins over the panel-listen alias.
Addresses shared by multiple roles are bound once. Separate panel mode requires
an explicit panel/admin address. Controller-only operation is unsupported.

`--profile broker` / `--broker-only` retains admin/panel but disables local delivery.
`--no-panel` hides the UI. Dispatch fleet workers and gateways do not open broker
storage; standalone panels proxy administration to `panel.controller`.

See [self-host settings](README.md) for supported environment variables, topology
examples and secret-file persistence requirements.
