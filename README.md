# agent-api-lane-protocol (AALP)

A provider-agnostic, compression-only external API transport. Each
provider admits its own requests strictly FIFO and enforces its own
bounded concurrency, both declared by that provider's non-secret JSON
definition (`providers/<id>.json`) and both gated by nothing but that
provider's own lane — no round robin, priority, weight, or per-agent
fairness scheduling, no cross-provider reservation, and no provider
branching in core logic. A request is single-turn: it is admitted,
forwarded, and its slot released once that one request's outcome is
known, with no mechanism to hold a reservation open across requests.
AALP owns provider credentials, one per provider id, under ignored
repo-local `.aalp/credential/<provider id>` state.

v1 ships exactly one provider definition: `ci` (CheapestInference,
active, `concurrency_limit: 1`, single-flight for that provider
specifically — not a system-wide constraint).

Run AALP as a standalone process with `python -m aalp` (see
`aalp/serve.py`); it constructs `Gateway` from `providers/` and starts
`Ingress` on it, publishing `.aalp/state/ingress.json` +
`.aalp/state/ingress.secret` for a client to bootstrap against (see
`interface/v1/README.md`'s Bootstrap section). `--providers-dir`,
`--root`, `--host`, and `--port` override the `AALP_PROVIDERS_DIR`/
`AALP_HOME`-derived defaults.

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
