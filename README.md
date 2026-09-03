# agent-api-lane-protocol (AALP)

A provider-agnostic, compression-only external API transport. AALP
admits requests strictly FIFO, applies per-flow/turn locking, and
enforces per-provider bounded concurrency declared by each provider's
own non-secret JSON definition (`providers/<id>.json`) — no round robin,
priority, weight, or per-agent fairness scheduling, and no provider
branching in core logic. AALP owns provider credentials, one per
provider id, under ignored repo-local `.aalp/credential/<provider id>`
state.

v1 ships exactly one provider definition: `ci` (CheapestInference,
active, `concurrency_limit: 1`, preserving today's single-flight
behavior).

AALP is one of three protocols defined in `agent_protocols_v1`:

- **ADP** (`agent-delegation-protocol`) — native-only delegation
  enforcement.
- **ACP** (`agent-compression-protocol`) — host-wide context compression,
  the primary caller of this transport.
- **AALP** (this repository) — the compression-only external API
  transport.

See the `agent_protocols_v1` project metadata for the full architecture
and implementation phases.

## License

CC BY 4.0 — see [LICENSE](LICENSE).
