# AALP interface v2

This directory is AALP's published, versioned, cross-protocol interface
description. It is the *only* surface a cross-protocol client — in practice
ACP (`agent-compression-protocol`) — is entitled to depend on when talking to
AALP. `contract.json` is the machine-validatable schema; this file is the
human-readable explanation of the same contract.

**Interface v1 is superseded, not kept alongside v2.** `interface/v1/`
remains in this repository as a historical record, but the daemon that
served it (`aalp.service`, an `AF_UNIX` socket authenticated by a bearer
secret) is being removed entirely — on every platform, not only Windows,
where `systemd --user` units don't exist and gave the daemonless migration
its original push. There is no dual-stack period: once a host's AALP moves
to v2, there is nothing left listening for a v1 client to connect to. Every
cross-protocol client must move to v2. See "Migration from v1" below for
exactly what that requires.

## Why this exists

AALP's real implementation (`aalp/lane.py`, `aalp/file_lane.py`,
`aalp/filelock_compat.py`, `credential.py`, `errors.py`, `forwarder.py`,
`gateway.py`, `audit.py`, `migrate_ci.py`) is provider-agnostic and free to
change its internals at any time. A client built against those private
modules — importing them directly, instantiating `Gateway` or `FileLane`,
calling an undocumented function, or reading `.aalp/` private state on disk
— would be coupled to implementation details that were never meant to be a
contract. Interface v2 exists so a conforming fake AALP CLI can be built and
tested against *this document alone*, and so ACP's own client tests can run
against that fake without ever touching AALP's real code or state.

Nothing described here requires reading `providers/*.json` from outside this
repository. Everything a client needs to know about a provider — is it
active, what's its concurrency ceiling, what paths does it accept — is
available through `provider.status`.

## Why v2: the daemon is gone

AALP ran interface v1 as a resident `aalp.service` process holding all
admission state (`aalp/lane.py`'s in-memory, condition-variable-based `Lane`)
and a listening socket in memory for as long as the service was up. Removing
that daemon — so that "clone the repo and run" works with no supervised
service and no `systemd --user` dependency, on Windows included — means
every AALP invocation is now its own short-lived process. Two independent
processes cannot share an in-memory dict and a `threading.Condition`; there
is no shared waiter registry anymore. That single change is what forces
almost everything below: it is not one isolated field going away, it is the
removal of the one piece of shared state (a live process) that several v1
guarantees were quietly built on top of.

`aalp/file_lane.py`'s `FileLane` is the daemonless replacement for
per-provider admission: a provider with `concurrency_limit` N owns N
`lane.<provider_id>.<slot>.lock` files under `.aalp/state/`, and holding a
non-blocking OS lock (`aalp/filelock_compat.py` — `fcntl.flock` on POSIX,
`msvcrt.locking` on Windows) on one of them is what it means to occupy that
slot. Everything that changes in this document traces back to what that
mechanism can and cannot offer that `Lane`'s in-memory FIFO+condition-variable
design could.

## Bootstrap: none

Interface v1 required discovering a live socket and a bearer secret before
any call could be made — `<AALP root>/.aalp/state/ingress.json` and
`.aalp/state/ingress.secret`, the one deliberate, narrow exception to "never
read another protocol's private state" (adjustment metadata §4). Interface
v2 has no bootstrap step at all: those two files, and the socket descriptor
file `ingress.sock`, are not written, read, or meaningful in v2. A client
that already knows the AALP root (the same `AALP_HOME` environment variable,
or an explicit `--root` argument, it would have needed under v1 to interpret
`ingress.json`) simply invokes the CLI directly.

## Authentication: OS identity, not a bearer secret

v1's bearer secret authenticated an otherwise-anonymous socket client — any
process on the host that could reach the loopback `AF_UNIX` socket path. A
CLI subprocess has no equivalent anonymity to resolve: the OS starts it
running as the caller's own real uid/gid, and it always runs as that
identity for its entire lifetime. Every piece of state it touches —
`providers/*.json`, the credential store under `.aalp/credential/`, the
admission lock files under `.aalp/state/`, the maintenance flag — is already
gated by an OS-level filesystem permission check (the same 0600/0700
discipline `aalp/credential.py` and `aalp/ingress.py` already enforce).

**The new trust boundary is stated explicitly, not left implicit:** whichever
OS user (or root) owns and can read/write the target AALP root's `.aalp/`
tree is implicitly authorized to invoke this interface against it. There is
no separate per-call credential check inside AALP itself in v2, because there
is no longer a network-reachable listener for an unauthorized peer to reach
in the first place — the entire class of threat a bearer secret defended
against (an unauthenticated local socket client) does not exist when there
is no socket. This is an authentication/bootstrap semantics change and is,
on its own, sufficient reason for a new major interface version.

## Wire protocol: a CLI, not a socket

**Status note, kept prominent deliberately:** everything in this section —
the invocation form, the stdin/stdout envelopes, the exit codes — is new
design produced for this document, not a transcription of something already
implemented and exercised. Unlike, say, `provider.status`'s admission
model (which builds directly on `aalp/file_lane.py` and
`aalp/filelock_compat.py`, already-written and already-tested code), there
is no CLI/stdin entrypoint anywhere in this repository today —
`aalp/serve.py` only builds the socket-based `Ingress`, and `aalp/__main__.py`
just calls `serve.main()`. The same caveat applies to the `provider.status`
read-only probe (`LOCK_SH|LOCK_NB` against each admission lock file,
described below): `aalp/file_lane.py` explicitly does not implement a
`status()` of any kind and says as much in its own docstring. Both pieces
need review against an actual implementation before anyone builds a client
against them as if they were already load-bearing, tested behavior.

Interface v2 speaks JSON over a child process's own stdin/stdout, not a
length-prefixed frame over `AF_UNIX`:

```
python -m aalp call <operation> [--root PATH] [--providers-dir PATH]
```

where `<operation>` is one of `request.forward`, `provider.status`,
`service.capabilities`, `service.maintenance`. The process reads a single
JSON object from stdin (until EOF — there is no length prefix; a subprocess
pipe has no framing ambiguity for a length prefix to resolve, unlike the
multiplexed socket connections v1's framing was written for), performs
exactly one operation, writes a single JSON object to stdout, and exits.
There is no keep-alive and no batching of multiple operations into one
process lifetime — one invocation, one call, one exit.

```
request:  {"provider_id": str?, "method": str?, "path": str?, "headers": {str: str}?, "body": <base64 str>?}
response: {"status": int, "headers": {str: str}, "body": <base64 str>}
```

`status` and `headers`/`X-Aalp-Outcome` mean exactly what they meant in v1 —
only the transport underneath changed. `body` is still base64-encoded so it
can carry arbitrary, non-UTF8-safe passthrough bytes safely inside JSON.

**Closing stdin after writing the request is a client obligation, stated
here as a hard requirement, not left to be inferred.** The child's read loop
blocks until stdin reaches EOF; from inside that loop, "the client has more
to write" and "the client is never going to close this" look identical.
A client that writes the envelope and leaves its end of stdin open hangs the
child indefinitely, with nothing the child itself can do to detect or
recover from it. A client-side timeout on how long it waits for the child to
exit (see "Exit codes" below) limits the damage from getting this wrong, but
does not substitute for it — a conforming client **must** close stdin
immediately after writing the complete request envelope.

### Nothing sensitive travels in argv — a hard invariant

`argv` may contain only the fixed literal tokens (`call`, the operation
name) and non-secret filesystem-path overrides (`--root`,
`--providers-dir`). Every field that could carry sensitive content —
`provider_id`, `method`, `path`, `headers` (including any value AALP would
otherwise need to forward as an auth header), and `body` — travels on stdin
instead. This is not a style preference: process argv is visible via `ps` or
`/proc/<pid>/cmdline` to **every user on the host**, unlike a byte stream
piped directly between two processes' own file descriptors. A conforming
client implementation must never format any of these fields into a
command-line argument.

### Exit codes

| Exit code | Meaning |
|---|---|
| `0` | The process ran the operation to completion and wrote a complete response envelope to stdout — even when that envelope's own outcome is an AALP-level failure (e.g. `unavailable`). A client reads the envelope's own fields to learn the outcome; exit `0` says only that the envelope is trustworthy. |
| `1` | The invocation itself was malformed (unknown operation, bad argv, unparseable stdin) before any operation ran. Nothing is written to stdout; a diagnostic appears on stderr only. |
| `2` | An internal error occurred while producing a response for an otherwise well-formed invocation (v2's analog of v1's ingress-level 500). Nothing meaningful is written to stdout. |
| other | Reserved. A conforming client treats any nonzero exit code as "no trustworthy response envelope was produced" and does not otherwise distinguish among them. |

`stderr` carries diagnostic text only; a conforming client must never parse
it or key behavior off its contents.

AALP's CLI process still imposes no read/write deadline of its own on the
stdin/stdout exchange — `queue_timeout`/`compression_timeout`/`total_timeout`
budget enforcement remains the client's responsibility, exactly as in v1.
What's new: a client must also bound how long it waits for the child process
to exit at all (its own process-level timeout, with a kill as a last resort),
since there is no longer a persistent server-side connection whose closure
alone signals "done."

### No more `_aalp` reserved path prefix

v1 disambiguated discovery calls from provider-passthrough calls by
reserving a `_aalp` path prefix inside one shared HTTP-style path namespace
(`/{provider_id}/{upstream_path}` vs. `/_aalp/v1/...`). v2's request
envelope selects the operation explicitly via the CLI's `<operation>`
argument, and `provider_id` is a separate JSON field rather than folded into
one combined path string — there is no shared namespace left to
disambiguate, so this mechanism is dropped along with the "no provider id
may begin with `_`" restriction it existed to enforce.

## The four operations

### `service.capabilities`

`python -m aalp call service.capabilities` →
`{"service": "aalp", "interface_version": 2, "capabilities": [...]}`

Unchanged in purpose from v1: call this first (and cache the result) to
learn AALP's interface version and capability set before assuming any v2
behavior is available. `request.queue` is no longer among the capabilities
(see "Queue coalescing: removed" below); everything else v1 declared is
still declared.

### `provider.status`

`python -m aalp call provider.status`, with `provider_id` absent from the
request envelope for the list form or present for the single form (`404` if
unknown).

Returns, per provider: `id`, `display_name`, `active`, `concurrency_limit`,
`in_flight`, `idle`, and `accepted_paths`. **`queued` and `idle_seconds` are
both gone** — see "The forcing changes: `queued` and `idle_seconds` have no
analog" below. Everything else here means what it meant in v1.

### `request.forward`

Submit one provider request via the `request.forward` operation and get back
exactly one outcome, identically to v1 in every respect except the
transport: `provider_id`, `method`, `path`, and `headers` are now separate
top-level fields of the stdin JSON request rather than a combined path
string plus a headers map inside a socket frame. An optional
`X-Aalp-Flow-Id` header may still accompany the request as a pure
audit/grouping label — see "Scheduling" below for what it does and does not
mean, unchanged from v1. On success, AALP passes the upstream response
through byte-for-byte, including the upstream's own status code.

Every `request.forward` response still carries `X-Aalp-Outcome` in its
`headers` field — one of the seven outcome values below — for the same
reason it did in v1: a passthrough success can itself carry a
502/504-shaped upstream status, so status code alone cannot disambiguate an
AALP-level failure from a provider's own response. This remains a v2
requirement.

On a non-success outcome, AALP synthesizes the same small JSON body,
`{"outcome": "...", "message": "..."}`; `message` remains diagnostic-only.

### `service.maintenance`

`python -m aalp call service.maintenance` → `{"maintenance": true|false}`.
Unchanged from v1 in every respect except transport.

## Outcomes

`request.forward` reports exactly one of the same seven values v1 defined,
reusing `aalp.errors.Outcome` verbatim. No client-facing code should ever
need to handle an eighth value, and no future v2.x addition may repurpose
one of these to mean something new.

| Outcome | Meaning | Response status |
|---|---|---|
| `success` | Upstream response read back completely; its own status code is passed through as-is. | passthrough |
| `unavailable` | Provider id unknown or `active: false`. No network attempt was made. | 503 |
| `queue_timeout` | **Changed.** Admission into one of the target provider's `concurrency_limit` slots did not complete before its queue-timeout budget elapsed. No network attempt was made. | 504 |
| `compression_timeout` | A connection-level timeout occurred while sending the request or reading the response, once an upstream attempt started. | 504 |
| `total_timeout` | The overall queue+upstream timeout budget elapsed, whether or not an upstream attempt ever started. | 504 |
| `invalid_response` | A connection was established but the response could not be read as a well-formed response. | 502 |
| `upstream_error` | The connection/transport itself failed (DNS/TCP/TLS/protocol) before any response was read. | 502 |

`queue_timeout`'s **value name and response status are unchanged**; only its
meaning narrows. v1 described it as failing to be admitted into the
provider's own FIFO lane — a lane no longer exists in v2. v2's admission is
a bounded, best-effort race among independent waiters for a free slot (see
"Scheduling" below), and `queue_timeout` now means that race did not resolve
before the deadline. A client that keys behavior only off the `outcome`
string (as instructed in both versions) needs no code change here; a client
that had baked in an assumption of *why* this outcome occurs does.

## The forcing changes: `queued` and `idle_seconds` have no analog

v1's `queued` meant, verbatim, "requests currently waiting in this
provider's FIFO lane." That sentence describes a concept — a FIFO lane with
countable waiters — that the daemonless model does not have. Admission is
now `concurrency_limit` independent OS lock files, probed by `concurrency_limit`
independent, short-lived processes with no shared waiter list anywhere for
anyone to count. There is no data structure in this design that a `queued`
count could be read from.

Reporting a permanent `0` was considered and rejected: it would read to any
client using it for backpressure as "nothing is waiting right now," which is
a *false, confident-looking* claim — there is no way to know whether zero,
one, or a hundred other processes are currently polling for the same
provider's slots. A wrong non-zero number would be one kind of lie; a
plausible-looking permanent zero is a worse one, because nothing about it
signals "this value is meaningless" to a client reading it. **`queued` is
removed outright.** This is the change that forced the version bump in the
first place.

`in_flight` and `idle` remain reportable, because — unlike "how many
processes are currently waiting" — "is this specific slot currently held"
*is* something a lock file can answer without a shared registry: AALP probes
each of a provider's `concurrency_limit` admission lock files with a
non-blocking, non-exclusive (`LOCK_SH|LOCK_NB`) attempt. Against a slot no
one holds exclusively, that probe succeeds and is released immediately
(read-only, leaves nothing held); against a slot some process currently
holds, it fails. `in_flight` is the count of slots that failed the probe;
`idle` is true exactly when none did.

This probe is deliberately a **shared**-lock attempt, not the same
exclusive-lock probe `FileLane.acquire()` itself uses to find a free slot to
occupy — a status read must never itself compete with, or be mistaken for,
a real admission attempt. Even so, it is a **best-effort, non-atomic**
read: each of the `concurrency_limit` slots is probed one at a time, with no
single moment at which all of them are observed simultaneously, so a value
read under active contention can be stale by the time it's returned. v1's
`Lane.status()` held one process-wide lock across its entire snapshot and
was therefore atomic in a way this is not.

### `idle_seconds` is also removed, and for a sharper reason than "approximate"

An earlier draft of this document tried to keep `idle_seconds`, first by
claiming it could be read from the admission lock files' own filesystem
metadata (wrong: `flock()`/`msvcrt.locking()` touch no timestamp on a file,
ever — a lock file's mtime only reflects when it was first created), then,
once that was caught, by making the field nullable and deriving it from a
timestamp record a slot's holder would write deliberately on acquire and on
release. Both attempts are gone. The owner's call, after reviewing the
nullable design, was to drop `idle_seconds` outright, on the same grounds as
`queued` — not keep a plain number with a caveat, and not keep a nullable
field that returns a number most of the time and `null` only after a crash.

**The specific reason a deliberately-written timestamp doesn't rescue this
field:** any record of "this slot became free at T" has to be written by
somebody, and the only somebody in a position to write it is the slot's own
holder, on its way out. The entire reason this design uses OS locks in the
first place is that they survive a holder being `SIGKILL`ed — the kernel
releases the lock unconditionally on process death, running no code of the
dead process's at all. That is exactly the moment a release-timestamp write
needed to happen and exactly the moment nothing can make it happen. A probe
reading that slot afterward sees "free" (true, and known instantly) with no
honest record of *when* it became free — only that it did, somewhere between
the last recorded acquire and now. Any number derived from that gap
overstates idleness, in the unsafe direction: a caller using `idle` plus
`idle_seconds` to infer safe dormancy would be told the provider has been
quiet for longer than it actually has, by exactly the interval between the
crash and whoever next happens to probe it. A nullable field only narrows
*when* this shows up — it doesn't change that the crash case is the case
the field exists to be useful for (a live client generally checks status
after suspecting something is wrong, which is disproportionately likely to
be exactly when a holder died badly). There is no honest number obtainable
here at any precision, so v2 reports none.

`idle` survives this because it asks a strictly easier question: "is every
slot free *right now*," answered completely by the same read-only probe
`in_flight` already performs, with no history and no timestamp involved.
`idle_seconds` asks a question about the past — how long has it been this
way — and answering that honestly requires a write that survives the one
failure mode (an unclean death) this design most needs to tolerate. Those
are different questions with different answerability, which is why one
field is kept and the other isn't, rather than both living or dying
together.

**A genuine simplification falls out of this, not just a subtraction: with
no timestamp to record, nothing in interface v2 ever needs to write to an
admission lock file.** No `utime` on acquire, none on release, no write of
any kind by anything other than the OS's own lock-state bookkeeping. The
lock files become pure lock tokens whose only meaningful state is
held-or-free — content-free, in fact, since nothing is ever written into
them — and the status probe (`in_flight`/`idle`) becomes strictly read-only:
it cannot mutate what it observes, on a free slot or a held one. That
removes an entire category of design questions a write-on-transition scheme
would have introduced: write races between a slot's holder and a concurrent
status probe, `utime`/mtime semantics differing between POSIX and Windows
(this repo already carries one POSIX/Windows split in
`aalp/filelock_compat.py`; a second one for timestamp writes would have been
avoidable complexity), and any requirement that a probing process have write
permission on state it is only trying to read. Dropping `idle_seconds` is
what makes the probe this clean.

## Scheduling: concurrency ceiling, no ordering

- Each provider still enforces its own declared `concurrency_limit`
  (discoverable via `provider.status`) as the sole gate on how many of its
  requests run at once — no round robin, no priority, no per-agent
  fairness, and (as in v1) no way to reserve a slot ahead of actually
  submitting the request that will use it. This part is unchanged.
- **What is gone: FIFO order.** v1 guaranteed that requests targeting the
  same provider were served strictly in submission order. That guarantee
  came from `Lane`'s single in-process waiter list — the same shared state
  a daemonless design does not have. `FileLane` admission is `concurrency_limit`
  independent processes each polling `concurrency_limit` independent lock
  files with no shared waiter list to order them by; which waiter is
  admitted next depends on OS scheduling and polling timing, not arrival
  order. **v2 makes no ordering promise of any kind among requests
  contending for the same provider's slots**, and a waiting caller is not
  guaranteed to make progress before its own deadline under sustained
  contention from other waiters. A client whose correctness — not just its
  throughput — depended on FIFO ordering cannot be ported to v2 without
  redesigning that dependency out; this is a genuine behavioral loss, not
  only a documentation change.
- `X-Aalp-Flow-Id` remains a purely optional audit/grouping label with zero
  effect on scheduling — unchanged from v1, and unaffected by the ordering
  guarantee's own removal, since it never provided ordering in v1 either.
- **There is still no renewal operation, and that is now an explicit
  decision, not a carried-over silence.** v1's README stated that the
  earlier per-flow reservation mechanism (`X-Aalp-Flow-Token`) had been
  removed and could not be reintroduced without a new major version.
  Interface v2 *is* that new major version, so this document makes the
  decision the v1 text anticipated needing: **the prohibition is carried
  forward, not lifted.** No v2.x addition may introduce a renewal or
  reservation operation without a new major version (v3). If anything, the
  daemonless model has less to build a reservation mechanism on top of than
  v1 did — a `FileLease` is a held OS lock for as long as one short-lived
  process runs, with no notion of "the same logical holder reconnecting
  from a later call" to renew against; supporting reservation semantics
  here would mean designing a new cross-process renewal primitive from
  scratch, not un-deprecating v1's old one.
- Provider concurrency is bounded by each provider's declared
  `concurrency_limit`, and that bound is still the *only* thing that gates
  it — no separate global admission step narrows it further. The default
  `ci` provider is still `concurrency_limit: 1`.

## Queue coalescing: removed

v1's `request.queue` operation (and its `X-Aalp-Queue-Key`,
`X-Aalp-Queue-Generation-Id`, `X-Aalp-Queue-Member-Count` headers) let
concurrent callers sharing a queue key coalesce into one physical provider
call, with the first caller becoming a generation's leader and later callers
joining as members. That mechanism depended on one resident process holding
a shared in-memory generation registry (`aalp/queue.py`'s
`QueueGeneration`) that every concurrent caller's connection could see and
join.

**v2 removes this operation outright — it is not redefined, and none of its
three headers are retained with new meaning.** Coalescing only ever paid off
for genuinely concurrent identical requests observed *inside one long-lived
process*; a process-per-invocation CLI has no equivalent to observe. By the
time one invocation's process has started, run, and could theoretically
notice a second one, there is no shared place either process could have
registered itself for the other to find — there is no daemonless analog to
build this on top of, not merely an unimplemented one.

**What a v1 client relying on this should expect:** `request.queue` as an
operation does not exist in v2's operation set at all. `X-Aalp-Queue-Key` is
no longer inspected or special-cased by `request.forward` — if a v1 client
sends it, it is simply forwarded upstream verbatim like any other ordinary
header it didn't intend AALP to treat specially, exactly as any
unrecognized header already was. `X-Aalp-Queue-Generation-Id` and
`X-Aalp-Queue-Member-Count` never appear in a v2 response. A client whose
correctness depends on coalescing (e.g. assuming a shared response across
concurrent same-key callers) must be redesigned to submit each request
independently via plain `request.forward`.

## What this interface explicitly does not cover

Credential read/write/probe operations remain AALP-owned administrative
functionality and are never exposed to a cross-protocol client through this
interface, in any form, in any version. A client that needs a provider's
live status has `provider.status`; it never needs, and must never be given,
a path to a provider credential. Unchanged from v1.

More generally: a conforming client only ever calls the four operations
documented above and in `contract.json`. It never imports an `aalp.*` Python
module, never instantiates `Gateway` or `FileLane`, never calls a function
not named on this page, and never reads `.aalp/` state from disk at all (v2
has no bootstrap carve-out — see "Bootstrap: none" above). If a future need
can't be met through this interface, the fix is to extend the interface
(additively, if possible), not to reach around it.

## Migration from v1

**Nothing about this migration is source-compatible.** Every one of the
changes below is a wire-protocol and/or field-shape break, not an additive
change a v1 client can ignore:

| v1 | v2 | Client action required |
|---|---|---|
| Discover socket + bearer secret via `ingress.json`/`ingress.secret` | No bootstrap; invoke the CLI directly with a known `AALP_HOME`/`--root` | Remove all bootstrap/discovery code; replace the transport client with a subprocess invocation |
| Length-prefixed JSON frame over `AF_UNIX` | JSON on stdin, JSON on stdout, one process per call | Replace the socket client entirely |
| `Authorization: Bearer <secret>` on every call | None — OS process identity + filesystem permissions | Remove secret-reading and header-injection code |
| `provider_id` folded into the request `path` string | `provider_id` a separate top-level field | Stop concatenating provider id into a path |
| `provider.status` response includes `queued` | `queued` removed, no replacement (first removed field) | Remove any code that reads `queued`; do not substitute a hardcoded `0` |
| `provider.status` response includes `idle_seconds` | `idle_seconds` removed, no replacement (second removed field, same grounds as `queued`) | Remove any code that reads `idle_seconds`; there is no v2 equivalent to migrate to — redesign that dependency out, the same treatment FIFO ordering below gets |
| `queue_timeout` means "FIFO lane admission timed out" | `queue_timeout` means "concurrency-slot admission timed out"; same name, same status code | No code change if the client only keys off the `outcome` string (as it always should have); update any comments/docs asserting FIFO |
| Submitted-request FIFO ordering guaranteed per provider | No ordering guarantee among waiters for the same provider | Remove any correctness dependency on submission order; a throughput-only dependency needs no change |
| `request.queue` + `X-Aalp-Queue-*` headers coalesce concurrent identical requests | Operation removed; the headers do nothing | Submit each request independently via `request.forward`; remove coalescing-dependent logic |
| `_aalp` reserved path prefix disambiguates discovery from passthrough | No shared path namespace; operation is explicit | Nothing to port — this was a wire-format internal, not client-visible behavior beyond the "no leading `_` in provider id" rule, which no longer applies |

**What is unchanged and needs no client code change**, beyond the transport
swap itself: the outcome *value set* (all seven names), `X-Aalp-Outcome`'s
requirement and purpose, `X-Aalp-Flow-Id`'s audit-only semantics, the
absence of any renewal/reservation operation, `accepted_paths` semantics,
`concurrency_limit` semantics, and the credential-exclusion guarantees.

## Compatibility rules

Interface major versions are protocol-local to AALP — this repository's
`v2` need not track ACP's or ADP's own version numbers.

A change **stays within `interface_version: 2`** when all of the following
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
A renewal/reservation operation is explicitly **not** expected to be a
same-major-version change; see "Scheduling" above.
