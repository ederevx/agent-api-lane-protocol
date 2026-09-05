"""File-lock-based provider admission lane (phase 2 of the daemonless
migration).

`aalp.lane.Lane` is an in-memory, condition-variable-based admission
primitive that only coordinates within one resident process. That was
fine while AALP ran as a long-lived `aalp.service` daemon holding all
admission state itself. Removing the daemon (driven by Windows
portability -- `systemd --user` units don't exist there) means every
agent invocation becomes its own short-lived process, so an in-memory
dict and `threading.Condition` stop coordinating anything: two
processes each have their own independent, unsynchronized `Lane`.
Admission has to move somewhere both processes can see it -- the
filesystem, under `.aalp/state/`.

This module is a from-scratch replacement, not a subclass or a drop-in
for `Lane`. It is additive: nothing in this package imports or calls it
yet, `aalp/lane.py` is untouched, and both continue to exist side by
side until a caller is deliberately switched over.

Mechanism
---------
A provider with `concurrency_limit` N owns N lock files,
``lane.<provider_id>.<slot>.lock`` for ``slot`` in ``0..N-1``, under
``.aalp/state/``. Acquiring a slot means holding a non-blocking OS
lock (via this repo's own `aalp.filelock_compat` -- `fcntl.flock` on
POSIX, `msvcrt.locking` on Windows; see that module for why this is a
~40-line in-repo shim rather than the third-party `filelock` package)
on one of those N files; N simultaneous holders is exactly N processes
each holding a distinct file's lock. `concurrency_limit` -- and
therefore N -- is config, so raising it is a config change here too,
never a code change: the acquire loop is already written to walk
however many slots exist.

To avoid every waiter racing to try slot 0 first, each attempt round
starts at a random slot index and wraps around. Free slots are found
with a single non-blocking probe per slot (`FileLock.acquire()`, which
never blocks and raises `LockBusy` immediately if that slot is taken);
if every slot is taken, the caller sleeps a jittered interval and
tries again, bounded by an overall deadline -- see "Timeout
discipline" below.

What this trades away
----------------------
`Lane` reclaims an abandoned lease itself: a lease that is never
released is still forcibly expired once `lease_seconds` elapses, so one
misbehaving holder can only block others for that long, no matter why
it never released (crashed, hung, forgot). That self-healing depended
entirely on there being one resident process to run the expiry check.

An OS file lock has no such concept. It is released in exactly two
ways: the holder calls `release()`, or the holder's process exits (for
any reason, including SIGKILL) and the OS reclaims every file
descriptor -- and therefore every `flock` -- that process held. Those
two cases cover a *crashed* holder perfectly and instantly, with zero
code required (verified experimentally: SIGKILLing a holder lets a
waiting process acquire its slot immediately, before any cleanup code
runs anywhere). They do **not** cover a *hung* holder -- one that is
still alive but wedged on a network call, blocked in a syscall, or
otherwise never reaches its own `release()`. That process's lock is
held for as long as the process itself is running, which could be
forever.

Net effect: with `concurrency_limit: 1`, a single wedged-but-alive
holder can disable that provider's lane until it dies, something the
old TTL-based `Lane` would have recovered from on its own after
`lease_seconds`. This module does not attempt to paper over that gap
(there is no reliable, skew-proof way to detect "alive but wedged" from
outside the process without reintroducing exactly the kind of
timestamp-based heuristic this design is trying to get away from).
Instead it makes sure the failure mode this produces is bounded and
inert rather than open-ended: every wait in this module has an
explicit, configurable overall timeout (`FileLane.acquire`'s
`timeout_seconds`), so a caller that cannot get in degrades to a clean
`FileLaneTimeout` -- "admission unavailable" -- and never to an
unbounded hang. We gain instant, code-free crash recovery; we lose
TTL-based self-healing of a live-but-wedged holder. That is a
deliberate trade, not an oversight.

Windows note: the byte-range-lock behavior `msvcrt.locking` requires
(seek to a fixed offset, lock a fixed byte count, consistently across
acquire/release/retry) is handled entirely inside `filelock_compat.py`
and is UNVERIFIED -- no Windows machine was available to run it in the
session that wrote it. Everything in *this* module is platform-neutral
on top of that shim's interface, so it needs no separate Windows
caveat beyond deferring to that module's.

Two more properties worth calling out because they are easy to get
wrong with file locks specifically:

* Nothing here is FIFO. `Lane` guarantees strict admission order --
  it is the single queueing algorithm this codebase uses specifically
  *because* it is fair. A pool of independent processes polling N lock
  files with no shared waiter list cannot offer that guarantee cheaply,
  and this design does not try to fake it: which waiter gets a slot
  next depends on scheduling and timing, not arrival order. Under
  sustained contention a particular waiter is not guaranteed to make
  progress before its own deadline. See "Not preserved" below.
* Correctness never depends on wall-clock timestamps -- only on
  whether the OS still considers a given file locked, which is
  skew-proof across processes/machines by construction. `time.sleep`
  and `time.monotonic()` are used only to pace polling and enforce the
  caller's own deadline, never to decide who holds what.

Concurrent first run
---------------------
Two processes can both start before `.aalp/state/` exists. This module
creates it with `Path.mkdir(parents=True, exist_ok=True)`, which is
specified to succeed whether or not the directory (or its parents)
already exist, so both racing processes converge without either
raising. The lock files themselves are never created with
`O_CREAT | O_EXCL`: a lock file's mere existence carries no meaning,
only its lock state does, so there is nothing to race over there
either (`filelock_compat.FileLock` opens each one with plain `O_CREAT`,
tolerating -- indeed expecting -- the file to already exist).

Not preserved from `Lane`
--------------------------
Called out explicitly, per design review, rather than smoothed over:

* **FIFO ordering** is gone, per above -- admission is best-effort and
  randomized, not queue-ordered.
* **TTL self-healing of a live-but-wedged holder** is gone, per above
  -- only process death releases a wedged holder's slot.
* **Heartbeat / reentrant-token renewal.** `Lane.acquire`'s `token=`
  parameter lets a caller (e.g. AALP's flow admission) renew an
  existing lease in place instead of requeuing, so a long-running
  continuation can't be overtaken by a newer waiter's ticket, and
  `Lane.heartbeat()` extends a lease without releasing it. Neither has
  an equivalent here: a `FileLease` is just a held OS lock for as long
  as the acquiring process keeps it, with no notion of "the same
  logical holder reconnecting from a new call" to renew against. If
  file-lock admission is ever extended to flow admission (the other
  caller of `Lane`, per `lane.py`'s own docstring), that reentrancy
  need has to be designed for separately -- it is out of scope here.
* **`queued`, specifically, out of `status()`.** `Lane.status()`
  reports `leased`, `queued`, `idle`, and `idle_seconds` from its own
  in-process state. `FileLane.status()` (below) reports the analogues
  of the first three under the names `in_flight`, `idle`, and
  `idle_seconds`, plus `capacity` -- but never `queued`: nothing
  registers a waiter anywhere here (that's what makes this lock-free
  of a shared waiter list, and therefore not FIFO), and there is no
  honest way to produce that count. `in_flight` is filled in by
  probing every slot's lock via `filelock_compat.probe()`, which is
  specifically designed (see that module) to do this without
  perturbing a real holder or a free slot -- unlike a "leased" count,
  a momentary shared-lock check does not have the disturbance problem
  called out in earlier revisions of this paragraph. `idle_seconds`
  has no in-process timestamp to read either; see `FileLane.status()`
  for how it is approximated from a small on-disk marker instead, and
  what that approximation cannot promise across a crashed holder.
"""
from __future__ import annotations

import os
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .filelock_compat import FileLock, LockBusy, probe

DEFAULT_POLL_INTERVAL_SECONDS = 0.05
DEFAULT_JITTER_SECONDS = 0.05


class FileLaneTimeout(TimeoutError):
    """Raised when acquire() could not obtain a slot before its deadline.

    Mirrors `aalp.lane.LaneTimeout`: in this case too, nothing is held
    and no cleanup is required by the caller.
    """


def _default_root() -> Path:
    # Mirrors aalp/maintenance.py's and aalp/audit.py's root-resolution
    # convention (AALP_HOME env var if set, else the caller's own
    # `root`, else cwd) rather than inventing a second one.
    configured = os.environ.get("AALP_HOME")
    if configured:
        return Path(configured).expanduser()
    return Path.cwd()


def state_dir(root: str | Path | None = None) -> Path:
    base = Path(root) if root is not None else _default_root()
    return base / ".aalp" / "state"


def lock_path(directory: str | Path, provider_id: str, slot: int) -> Path:
    return Path(directory) / f"lane.{provider_id}.{slot}.lock"


def activity_path(directory: str | Path, provider_id: str) -> Path:
    """One small marker file per provider, touched (mtime bumped, no
    content that matters) on every successful acquire and every
    release. Not a lock -- never opened with any `fcntl`/`msvcrt` call
    -- purely a persisted "something happened at this wall-clock time"
    stamp that survives process death, used by `FileLane.status()` to
    approximate `idle_seconds`. See that method's docstring for what
    this can and cannot promise."""
    return Path(directory) / f"lane.{provider_id}.activity"


@dataclass
class FileLease:
    """A held admission slot for one provider.

    Unlike `Lane.Lease`, this is not a serializable token another call
    can look up later -- it owns a live OS file descriptor and must be
    released (at most once; a second `release()` is a harmless no-op)
    by the same process that acquired it. Use as a context manager or
    call `release()` explicitly on the normal path; an abnormal exit
    (including SIGKILL) releases it for you via the OS, per this
    module's docstring.
    """

    provider_id: str
    slot: int
    path: Path
    _lock: FileLock
    _activity_path: Path | None = None

    def release(self) -> None:
        if self._lock.is_locked:
            self._lock.release()
            if self._activity_path is not None:
                # Best-effort activity stamp for FileLane.status()'s
                # idle_seconds; see that method's docstring. Touching
                # this is not part of the release's correctness (a
                # missing or stale stamp never affects mutual
                # exclusion), so a failure here must never mask the
                # release that already happened above.
                try:
                    self._activity_path.touch()
                except OSError:
                    pass

    def __enter__(self) -> "FileLease":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


class FileLane:
    """One provider's file-lock-based admission lane.

    `capacity` plays exactly the role `Lane(capacity=...)` does --
    provider.concurrency_limit, per registry.py -- it is just enforced
    via N lock files instead of an in-process counter.
    """

    def __init__(
        self,
        provider_id: str,
        capacity: int,
        *,
        root: str | Path | None = None,
        directory: str | Path | None = None,
        poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
        jitter_seconds: float = DEFAULT_JITTER_SECONDS,
        rng: random.Random | None = None,
    ) -> None:
        if not provider_id:
            raise ValueError("provider_id is required")
        if capacity < 1:
            raise ValueError("capacity must be positive")
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        if jitter_seconds < 0:
            raise ValueError("jitter_seconds must not be negative")

        self.provider_id = provider_id
        self.capacity = capacity
        self.directory = Path(directory) if directory is not None else state_dir(root)
        self.poll_interval_seconds = poll_interval_seconds
        self.jitter_seconds = jitter_seconds
        self._rng = rng if rng is not None else random.Random()
        self._lock_paths = [
            lock_path(self.directory, provider_id, slot) for slot in range(capacity)
        ]
        self.activity_path = activity_path(self.directory, provider_id)

    def _ensure_directory(self) -> None:
        # exist_ok=True (rather than a bare mkdir + FileExistsError
        # catch) is the same guarantee either way: both racing
        # first-run processes converge without either erroring.
        self.directory.mkdir(parents=True, exist_ok=True)

    def _touch_activity(self) -> None:
        # Shared by a successful acquire and a release -- see
        # `status()` for why both ends of a hold, not just release,
        # bump this stamp. Best-effort: never let a failure here mask
        # the acquire/release that already succeeded.
        try:
            self.activity_path.touch()
        except OSError:
            pass

    def _try_slots_once(self) -> FileLease | None:
        start = self._rng.randrange(self.capacity)
        for offset in range(self.capacity):
            slot = (start + offset) % self.capacity
            candidate = FileLock(self._lock_paths[slot])
            try:
                candidate.acquire()
            except LockBusy:
                continue
            self._touch_activity()
            return FileLease(
                self.provider_id, slot, self._lock_paths[slot], candidate,
                self.activity_path)
        return None

    def acquire(self, timeout_seconds: float) -> FileLease:
        """Admit this caller to one of the `capacity` slots.

        Every wait this makes is bounded by `timeout_seconds`,
        including the very first attempt's directory setup, per this
        module's "no unbounded wait" rule -- there is no code path here
        that can block past the caller's own deadline. Raises
        `FileLaneTimeout` if no slot freed up in time; in that case
        nothing is held and no cleanup is required by the caller.
        """
        if timeout_seconds < 0:
            raise ValueError("timeout_seconds must not be negative")
        deadline = time.monotonic() + timeout_seconds
        self._ensure_directory()
        while True:
            lease = self._try_slots_once()
            if lease is not None:
                return lease
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise FileLaneTimeout(
                    f"file lane acquire timed out for provider {self.provider_id!r}")
            sleep_for = self.poll_interval_seconds + self._rng.uniform(0, self.jitter_seconds)
            time.sleep(min(remaining, sleep_for))

    def status(self) -> dict[str, Any]:
        """Best-effort introspection, filling in every field
        `Lane.status()` reports except `queued` -- see this module's
        docstring ("Not preserved from `Lane`") for why no honest
        `queued` count exists in a daemonless, waiter-registry-free
        design. Returns `capacity`, `in_flight`, `idle`, `idle_seconds`.

        `in_flight` is a fresh probe, not cached state: it calls
        `filelock_compat.probe()` once per slot, right now, in this
        call. Each probe is a momentary shared-lock check (a brief
        exclusive try-then-release on Windows -- see that function's
        docstring) that never blocks and never itself counts as a
        holder, so calling `status()` while real holders are working
        does not disturb them and does not perturb a free slot either.
        `idle` is simply `in_flight == 0` from that same probe pass.

        `idle_seconds` cannot be read from any in-process timestamp --
        there is no resident process here to hold one, by design (see
        this module's docstring). Instead it is read from
        `self.activity_path`'s mtime: a small marker file this lane
        touches on every successful `acquire()` and every `release()`
        (both ends of a hold, mirroring `Lane.status()`'s own
        `last_activity`, which likewise updates on any transition --
        acquire, release, or heartbeat -- not only on "went idle"
        events). A file's mtime is OS-maintained metadata, so this
        survives process death without any code needing to run at
        exit, and every read/write of it happens against this one
        machine's own clock -- there is no attempt, and no need, to
        compare timestamps written by different machines.

        Known limitation, stated plainly rather than hidden: a holder
        that crashes without releasing leaves the marker at the time
        of its own acquire. Once that slot is later found free (by an
        OS-level reclaim this module did not initiate), the next
        `status()` call's idle_seconds will overstate true idle time
        by roughly that crashed hold's own duration, because nothing
        observed the actual moment of death. That overstatement is
        bounded by a single hold's length and is not cumulative -- the
        very next acquire or release recorded anywhere in this lane
        resets the stamp to a real event again. If the marker has
        never been touched at all (this lane has never completed a
        single acquire or release since `.aalp/state/` was created),
        this call creates it now and reports 0.0, the same way
        `Lane.__init__` sets `last_activity = clock()` at construction
        and likewise cannot know how long a never-used lane was idle
        before it existed.
        """
        self._ensure_directory()
        in_flight = sum(1 for path in self._lock_paths if probe(path))
        idle = in_flight == 0
        if not self.activity_path.exists():
            self._touch_activity()
        idle_seconds = max(0.0, time.time() - self.activity_path.stat().st_mtime)
        return {
            "capacity": self.capacity,
            "in_flight": in_flight,
            "idle": idle,
            "idle_seconds": idle_seconds,
        }
