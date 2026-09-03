# AALP interface v1

This directory is AALP's published, versioned, cross-protocol interface
description. It is the *only* surface a cross-protocol client — in practice
ACP (`agent-compression-protocol`) — is entitled to depend on when talking to
AALP. `contract.json` is the machine-validatable schema; this file is the
human-readable explanation of the same contract.

## Why this exists

AALP's real implementation (`aalp/lane.py`, `credential.py`,
`errors.py`, `forwarder.py`, `ingress.py`, `audit.py`, `migrate_ci.py`,
`gateway.py`) is provider-agnostic, socket-aware where it needs to be, and
free to change its internals at any time. A client built against those
private modules — importing them directly, instantiating `Gateway` or
`Lane`, calling an undocumented function, or reading `.aalp/`
private state on disk — would be coupled to implementation details that were
never meant to be a contract. Interface v1 exists so a conforming fake AALP
service can be built and tested against *this document alone*, and so ACP's
own client tests can run against that fake without ever touching AALP's real
code or state.

Nothing described here requires reading `providers/*.json` from outside this
repository. Everything a client needs to know about a provider — is it
active, what's its concurrency ceiling, what paths does it accept — is
available through `provider.status`.

## Bootstrap: discovering AALP and authenticating

Every operation below requires a live loopback HTTP connection to AALP and a
bearer secret. A client discovers both the same way, and this is the one
deliberate, narrow exception to "never read another protocol's private
state": these two files, and only these two files, are a published part of
interface v1, carved out of `.aalp/` specifically for this purpose
(adjustment metadata §4).

- **`<AALP root>/.aalp/state/ingress.json`** — written atomically by AALP's
  ingress on startup, before it accepts any connection. Contains
  `{"host": "127.0.0.1", "port": <int>, "secret_file": "<absolute path>"}`.
  `<AALP root>` resolves to the `AALP_HOME` environment variable if set,
  else the working directory AALP was started from; a client must be told
  this root out-of-band (shared deployment configuration) — interface v1
  defines no operation for locating an unknown root.
- **`<secret_file>`** (default `<AALP root>/.aalp/state/ingress.secret`) —
  an opaque bearer token, `0600`, owner-only, generated once. A client
  reads its raw contents and sends them back as
  `Authorization: Bearer <contents>` on every request below.

A client reads exactly these two paths and nothing else under `.aalp/`.
Credentials, the audit log, and provider-lane internals remain off-limits in
every version of this interface.

## The three operations

### `service.capabilities`

`GET /_aalp/v1/capabilities` → `{"service": "aalp", "interface_version": 1, "capabilities": [...]}`

The discovery entry point. A client should call this first (and may cache
the result) to learn AALP's interface version and which capability strings
are in force before assuming any interface v1 behavior is available.

v1 declares exactly these capabilities:

- `request.forward` — the forwarding operation below is supported.
- `provider.status` — the provider discovery operation below is supported.
- `provider.concurrency` — `provider.status` responses include live
  concurrency fields (`in_flight`, `queued`, `idle`, `idle_seconds`), not
  just static `active`/`concurrency_limit`.
- `request.timeout_outcomes` — the outcome set distinguishes
  `queue_timeout`, `compression_timeout`, and `total_timeout` rather than
  collapsing every timeout into one generic failure.

**`request.batch` is deliberately not a v1 capability.** No genuine
finite-batch need from ACP has been demonstrated yet. Adding it later would
be a normal additive change (a new operation, a new capability string,
existing clients unaffected) — but it does not belong in v1 today, and a
client must not assume batching is available just because it isn't
explicitly forbidden.

### `provider.status`

`GET /_aalp/v1/providers` (all providers) or
`GET /_aalp/v1/providers/{provider_id}` (one, 404 if unknown).

Returns, per provider: `id`, `display_name`, `active`, `concurrency_limit`,
`in_flight`, `queued`, `idle`, `idle_seconds`, and `accepted_paths` (the
exact upstream paths `request.forward` will accept for that provider). This
is deliberately everything ACP needs and nothing more — it never exposes a
provider's `endpoint` URL, its AALP-internal `client` selection, or any
credential/credential-existence information. Credentials are covered below.

### `request.forward`

`{METHOD} /{provider_id}/{upstream_path}` — submit one provider request and
get back exactly one outcome.

The first path segment names the target provider and is stripped before
forwarding; the remainder must be one of that provider's `accepted_paths`
(discoverable via `provider.status`). An optional `X-Aalp-Flow-Id` header
may accompany the request — see **Scheduling** below for exactly what it
does and does not mean. On success, AALP passes the upstream response
through byte-for-byte, including the upstream's own status code (a
provider-side 4xx/5xx is still an AALP-level *success*: AALP only classifies
transport-level results, never the provider's own application response).

Because a passthrough success can itself carry a 502/504-shaped upstream
status, status codes alone are not enough to tell an AALP-level outcome
apart from a provider's own response. **Every `request.forward` response
carries an `X-Aalp-Outcome` header** — one of the seven outcome values below
— so a client can always distinguish them unambiguously. This header is a
v1 requirement; a client should treat its absence as a contract violation
rather than guessing from the status code.

On a non-success outcome, AALP synthesizes a small JSON body,
`{"outcome": "...", "message": "..."}`; `message` is diagnostic text only —
a client must never key behavior off it, only off `outcome`.

## Outcomes

`request.forward` reports exactly one of the seven values already
implemented in `aalp/errors.py`'s `Outcome` enum. No client-facing code
should ever need to handle an eighth value for this operation, and a future
v1.x addition may never repurpose one of these to mean something new — that
would require a new major interface version instead.

| Outcome | Meaning | Response status |
|---|---|---|
| `success` | Upstream response read back completely; its own status code is passed through as-is. | passthrough |
| `unavailable` | Provider id unknown or `active: false`. No network attempt was made. | 503 |
| `queue_timeout` | Admission into the target provider's own FIFO lane did not complete before its queue-timeout budget elapsed. No network attempt was made. | 504 |
| `compression_timeout` | A connection-level timeout occurred while sending the request or reading the response, once an upstream attempt started. | 504 |
| `total_timeout` | The overall queue+upstream timeout budget elapsed, whether or not an upstream attempt ever started. | 504 |
| `invalid_response` | A connection was established but the response could not be read as well-formed HTTP. | 502 |
| `upstream_error` | The connection/transport itself failed (DNS/TCP/TLS/protocol) before any response was read. | 502 |

`provider.status` and `service.capabilities` are plain discovery reads and
are not modeled with this enum; they use ordinary HTTP 200/404 semantics
(see `contract.json` for the 404 body shape).

## Scheduling: submitted-request FIFO

- Requests targeting the same provider are served strictly in the order
  they are **submitted** to that provider — across every logical flow,
  caller, and agent. There is no round robin, no priority, no per-agent
  fairness, and critically, no way to reserve a lane slot ahead of
  actually submitting the request that will use it. This is a per-provider
  guarantee, not a single global queue: it holds among requests contending
  for the same provider's slot(s), not as a system-wide bottleneck across
  unrelated providers.
- A request may optionally carry an `X-Aalp-Flow-Id` header purely as an
  **audit/grouping label**. It has zero effect on scheduling order. Two
  requests sharing a `flow_id` are not guaranteed to run adjacently, and a
  request with no `flow_id` at all is scheduled exactly like one that has
  one.
- **There is no renewal operation in this interface.** Earlier AALP builds
  had an open-ended per-flow reservation (`X-Aalp-Flow-Token`, renewed
  across a flow's successive requests) that let an idle flow hold a lane
  slot open for a not-yet-submitted continuation request. That mechanism
  has been removed, and no future v1.x addition may reintroduce an
  equivalent reservation without a new major version.
- Provider concurrency is bounded by each provider's declared
  `concurrency_limit` (discoverable via `provider.status`) — and that
  bound is the *only* thing that gates it: a provider's own concurrency
  ceiling determines how many of its own requests genuinely run at once,
  with no separate global admission step narrowing it further. The
  default `ci` provider is `concurrency_limit: 1` — single-flight, as
  before — but a provider declared with a higher limit, or a second
  provider entirely, now executes concurrently rather than being
  serialized behind `ci`'s own requests. FIFO order and the concurrency
  ceiling are independent constraints: FIFO governs the order a given
  provider's requests are attempted in, the ceiling governs how many of
  that provider's requests may be in flight upstream at once.

## What this interface explicitly does not cover

Credential read/write/probe operations remain AALP-owned administrative
functionality (used internally by `aalp/credential.py` and
`aalp/migrate_ci.py`) and are never exposed to a cross-protocol client
through this interface, in any form, in any version. A client that needs a
provider's live status has `provider.status`; it never needs, and must never
be given, a path to a provider credential.

More generally: a conforming client only ever calls the three HTTP
operations documented above and in `contract.json`. It never imports an
`aalp.*` Python module, never instantiates `Gateway` or `Lane`, never
calls a function not named on this page, and never reads `.aalp/` state
from disk beyond the two bootstrap files named above. If a future need
can't be met through this interface, the fix is to extend the interface
(additively, if possible), not to reach
around it.

## Compatibility rules

Interface major versions are protocol-local to AALP — this repository's `v1`
need not track ACP's or ADP's own version numbers.

A change **stays within `interface_version: 1`** when all of the following
hold:

- existing valid requests remain valid;
- existing outcomes retain their documented meaning;
- existing clients keep working without adopting any new capability;
- new fields are optional and/or gated behind a new capability string;
- `service.capabilities` exposes the addition, so clients can detect it.

A change **requires a new major interface version** when any of the
following is true:

- an operation or field is removed or renamed;
- an existing operation's or outcome's semantics change incompatibly;
- a previously valid request becomes invalid;
- an outcome's meaning changes;
- authentication/bootstrap semantics change incompatibly.

`request.batch`, if it is ever added, is expected to be a same-major-version
additive change under these rules — a new operation, a new capability
string, no effect on existing clients — once a genuine need is demonstrated.
