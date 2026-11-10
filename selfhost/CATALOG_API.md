# Catalog mutation API

In a cell, queue, flow, group, member, and cron mutations return success only
after controller quorum commit and application to the requesting node's cache.
Standalone servers without a cluster retain local-only behavior.

## Request and response headers

| Header | Direction | Meaning |
| --- | --- | --- |
| `X-BetterMQ-Operation-Id` | Request and response | UUID identifying one logical mutation. Generated when omitted; echoed on responses. |
| `X-BetterMQ-Catalog-Revision` | Response | Global controller revision committed by this operation. Also present when commit succeeded but local application failed. |
| `X-BetterMQ-Catalog-Version` | Response | Version of the primary identity changed by this operation. |
| `If-Match` | Request | Optional numeric identity version, optionally quoted. A mismatched version returns 409. |

Without `If-Match`, the server captures the identity version before preparing the
change and rejects concurrent changes with 409. A global catalog revision is not
an identity version. Standalone writes do not return controller revision/version
headers and do not persist operation receipts.

## Retries

Supply an operation UUID before the first request when a lost response must be
retryable. Retry with the same UUID, HTTP method, path, tenant, and JSON body.
Changing the request while reusing the UUID returns 409. JSON object field order
does not affect request identity. An optional `If-Match` value is not part of
request identity, so a committed retry retains its original outcome.

A committed retry returns HTTP 200 with a receipt instead of the original object
response. `resources` identifies the affected primary objects without exposing
credentials. The revision and identity version remain those of the original
operation, even if the object has subsequently changed or been deleted.

```json
{
  "operation_id": "4cc1be77-c798-42f0-a756-3187bea98214",
  "catalog_revision": 42,
  "catalog_version": 1,
  "replayed": true,
  "resources": [
    {"kind": "queue", "id": "dc17190d-848e-4661-a93e-c76c9f7e07cb", "deleted": false}
  ]
}
```

Inline-flow publish keeps its normal ingest response on retry; its unchanged
definition does not submit another catalog mutation. Use message idempotency
keys independently for message acceptance.

## Errors

| HTTP status | Condition | Retry behavior |
| --- | --- | --- |
| 400 | Invalid definition, operation UUID, or version header | Correct the request. |
| 409 | Stale version, duplicate name, missing parent, deleted identity, reused operation UUID, or legacy internal write after activation | Refresh the definition; use a new UUID for a new logical operation. |
| 503 | No controller/quorum, activation freeze, or unavailable local application | Retry the same operation UUID. |
| 503, message includes `outcome unknown` | Transport or timeout after controller submission | The operation may have committed. Retry the same UUID; do not assume rollback. |
| 503, message includes `committed at revision` | Commit succeeded but local application failed | The change remains committed. The node is fenced until application recovers. Retry the same UUID. |

Other nodes normally observe a committed change within one apply interval.
See [cell catalog consistency](CATALOG.md) for activation, recovery, and costs.
