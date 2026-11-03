"""
Reconnection and redirection attempt generations for clients.

An :class:`AttemptEpoch` is a stack of :class:`AttemptGeneration` ledgers
shared by client connection factories — :func:`~websockets.asyncio.client.connect`
in :mod:`asyncio` and :mod:`trio`, and :func:`~websockets.sync.client.connect`
/ :class:`~websockets.sync.client.reconnect` in :mod:`threading`.

Each generation owns two cursors that must never be shared across generation
boundaries:

* the *redirect cursor* of an opening handshake: the URI rewritten by 3xx
  responses, the credential headers stripped on cross-origin redirects, and
  the redirect-following budget;
* the *reconnect cursor* of a retry loop: the ``reconnect_delays`` backoff
  generator and the retry/fatal judgment returned by ``process_exception``.

A generation is *pushed* for a reconnect loop and for every one-shot open,
*popped* when that scope exits, and can be *advanced* at any time: the current
generation is closed — its unfinished redirect rewrite, its backoff sleep, and
its exception judgment stop driving anything — and a fresh generation takes
over immediately. A one-shot connect nested inside a foreign generation pushes
its own child generation, so its redirects and failures never touch the outer
reconnect ledger; an abnormal exit pops the child just the same.

Without an :class:`AttemptEpoch`, every open and every reconnect loop uses a
detached generation with the same rules, preserving the historical behavior.

The single-open ``open_timeout``, the inter-attempt ``reconnect_delays``
cursor, and the connection ``close_timeout`` remain independent.

"""

from __future__ import annotations

import contextlib
import itertools
import os
import threading
import urllib.parse
from collections.abc import Callable, Generator, Iterator
from dataclasses import dataclass
from typing import Protocol, TypeVar, cast, runtime_checkable

from .client import (
    REDIRECT_STATUS_CODES,
    STRIPPED_REDIRECT_HEADERS,
    backoff,
    process_exception,
)
from .datastructures import Headers, HeadersLike
from .exceptions import InvalidStatus, SecurityError
from .uri import WebSocketURI, parse_uri


__all__ = [
    "AttemptEpoch",
    "AttemptGeneration",
    "GenerationStale",
    "Redirect",
    "redirect_decision",
    "commit_redirect",
    "loop_generation_scope",
    "open_generation_scope",
    "sync_reconnect_wait",
    "asyncio_reconnect_wait",
    "trio_reconnect_wait",
]

MAX_REDIRECTS = int(os.environ.get("WEBSOCKETS_MAX_REDIRECTS", "10"))


class GenerationStale(RuntimeError):
    """
    A popped or superseded attempt generation was driven.

    Raised when an opening handshake or a reconnect loop tries to consume the
    redirect budget, the backoff cursor, or an exception judgment of a
    generation that was already closed by :meth:`AttemptEpoch.pop` or
    :meth:`AttemptEpoch.advance`.

    """


@dataclass
class Redirect:
    """A validated redirect target plus the headers to send to it."""

    uri: str
    ws_uri: WebSocketURI
    additional_headers: HeadersLike | None
    cross_origin: bool


def redirect_decision(
    exc: Exception,
    *,
    base_uri: str,
    additional_headers: HeadersLike | None,
    preexisting_endpoint: bool,
    unix: bool,
    explicit_destination: bool,
    endpoint_noun: str,
    destination_noun: str,
) -> Redirect | Exception:
    """
    Evaluate a failed opening handshake against the redirect table.

    Return a :class:`Redirect` when ``exc`` is a 3xx response with a
    ``Location`` header that may be followed, or the exception to raise
    otherwise. TLS downgrades and cross-origin redirects over a preexisting
    socket/stream, a Unix socket, or an explicit destination are rejected.

    The redirect table (:data:`~websockets.client.REDIRECT_STATUS_CODES`) is
    deliberately separate from the transient-status table
    (:data:`~websockets.client.RETRYABLE_STATUS_CODES`) used by
    :func:`~websockets.client.process_exception`.

    """
    # Redirects are followed during the opening handshake.
    if not (
        isinstance(exc, InvalidStatus)
        and exc.response.status_code in REDIRECT_STATUS_CODES
        and "Location" in exc.response.headers
    ):
        return exc

    old_ws_uri = parse_uri(base_uri)
    new_uri = urllib.parse.urljoin(base_uri, exc.response.headers["Location"])
    new_ws_uri = parse_uri(new_uri)

    # If the caller provided a socket or stream, it is closed and cannot be
    # reused to establish a connection to a different endpoint.
    if preexisting_endpoint:
        return ValueError(
            f"cannot follow redirect to {new_uri} with a preexisting {endpoint_noun}"
        )

    # TLS downgrade is forbidden.
    if old_ws_uri.secure and not new_ws_uri.secure:
        return SecurityError(f"cannot follow redirect to non-secure URI {new_uri}")

    cross_origin = (old_ws_uri.secure, old_ws_uri.host, old_ws_uri.port) != (
        new_ws_uri.secure,
        new_ws_uri.host,
        new_ws_uri.port,
    )

    if cross_origin:
        # Cross-origin redirects on Unix sockets don't quite make sense.
        if unix:
            return ValueError(
                f"cannot follow cross-origin redirect to {new_uri} with a Unix socket"
            )
        # Cross-origin redirects when host and port are overridden are ill-defined.
        if explicit_destination:
            return ValueError(
                f"cannot follow cross-origin redirect to {new_uri} "
                f"with an explicit {destination_noun}"
            )
        # Strip credentials from requests when the redirect changes the origin.
        if additional_headers is not None:
            additional_headers = Headers(
                (key, value)
                for key, value in Headers(additional_headers).raw_items()
                if key.lower() not in STRIPPED_REDIRECT_HEADERS
            )

    return Redirect(new_uri, new_ws_uri, additional_headers, cross_origin)


@runtime_checkable
class _AsyncInterruptEvent(Protocol):
    def set(self) -> None: ...
    def is_set(self) -> bool: ...
    async def wait(self) -> None: ...


# Unbound: the concrete event is asyncio.Event or trio.Event; trio's type
# stubs don't expose an async ``wait`` structurally, so callers keep the exact
# event type while storage only needs the protocol above.
_AsyncEventT = TypeVar("_AsyncEventT")


class AttemptGeneration:
    """
    One generation of connection attempts.

    A generation binds the redirect cursor (:attr:`uri`, :attr:`ws_uri`,
    :attr:`additional_headers`, :attr:`redirects`) and the reconnect cursor
    (:attr:`delays`, :attr:`judgment`) for every open performed while it is
    current. Hosts do not instantiate generations directly; they are created
    by :meth:`AttemptEpoch.push` or by the generation scopes attached to the
    client connection factories.

    """

    def __init__(
        self,
        number: int,
        *,
        epoch: AttemptEpoch | None,
        owner: object | None,
        uri: str,
        additional_headers: HeadersLike | None,
        process_exception: Callable[[Exception], Exception | None],
        reconnect_delays: Callable[[], Generator[float]],
        max_redirects: int,
        is_detached: bool = False,
    ) -> None:
        # Identity and nesting.
        self.number = number
        self.epoch = epoch
        self.owner = owner
        self.is_detached = is_detached

        # Redirect cursor.
        self.uri = uri
        self.ws_uri = parse_uri(uri)
        self.additional_headers = additional_headers
        self.redirects = 0
        self.max_redirects = max_redirects

        # Reconnect cursor.
        self.process_exception = process_exception
        self.reconnect_delays = reconnect_delays
        self.delays: Generator[float] | None = None

        # Observability / verification.
        self.opens = 0
        self.judgment: tuple[Exception, Exception | None] | None = None

        # Liveness.
        self.stale = False
        self.interrupt = threading.Event()
        self._async_event: _AsyncInterruptEvent | None = None

    @classmethod
    def detached(
        cls,
        *,
        uri: str,
        additional_headers: HeadersLike | None,
        process_exception: Callable[[Exception], Exception | None],
        reconnect_delays: Callable[[], Generator[float]],
        max_redirects: int,
    ) -> AttemptGeneration:
        """Create a generation that is not attached to any epoch."""
        return cls(
            0,
            epoch=None,
            owner=None,
            uri=uri,
            additional_headers=additional_headers,
            process_exception=process_exception,
            reconnect_delays=reconnect_delays,
            max_redirects=max_redirects,
            is_detached=True,
        )

    def begin_open(self) -> None:
        """Bind one opening handshake sequence to this generation."""
        if self.stale:
            raise GenerationStale(
                f"cannot open a connection with stale attempt generation {self.number}"
            )
        self.opens += 1
        # The redirect budget is spent per opening handshake, even though the
        # URI cursor itself survives across reconnects within the generation.
        self.redirects = 0

    def follow_redirect(self, redirect: Redirect) -> None:
        """Commit a followed redirect to this generation's cursor."""
        if self.stale:
            raise GenerationStale(
                f"cannot follow redirect with stale attempt generation {self.number}"
            )
        self.uri = redirect.uri
        self.ws_uri = redirect.ws_uri
        self.additional_headers = redirect.additional_headers
        self.redirects += 1

    def classify_exception(self, exc: Exception) -> Exception | None:
        """
        Bind a retry/fatal judgment to this generation.

        Invoke the factory's ``process_exception`` callback, supporting its
        convention of raising instead of returning an exception.
        """
        if self.stale:
            raise GenerationStale(
                f"cannot classify exception with stale attempt generation {self.number}"
            )
        try:
            decision = self.process_exception(exc)
        except Exception as raised:
            decision = raised
        self.judgment = (exc, decision)
        return decision

    def next_delay(self) -> float:
        """Consume one delay from the generation's reconnect cursor."""
        if self.stale:
            raise GenerationStale(
                f"cannot wait for reconnect with stale attempt generation {self.number}"
            )
        if self.delays is None:
            self.delays = self.reconnect_delays()
        return next(self.delays)

    def reset_backoff(self) -> None:
        """Restart the reconnect cursor after a successful connection."""
        self.delays = None

    def host_event(
        self, event_factory: Callable[[], _AsyncEventT]
    ) -> _AsyncEventT:
        """Lazily create the event-loop invalidation event for this generation."""
        event = self._async_event
        if event is None:
            new_event = event_factory()
            event_view = cast(_AsyncInterruptEvent, new_event)
            if self.stale:
                event_view.set()
            self._async_event = event_view
            return new_event
        return cast(_AsyncEventT, event)

    def invalidate(self) -> None:
        """Close this generation and interrupt any reconnect wait on it."""
        if self.stale:
            return
        self.stale = True
        self.interrupt.set()
        if self._async_event is not None:
            self._async_event.set()

    def __repr__(self) -> str:
        state = "stale" if self.stale else "active"
        return f"<AttemptGeneration {self.number} {state} {self.uri}>"


class _GenerationConnecter(Protocol):
    attempt_epoch: AttemptEpoch | None
    _loop_generation: AttemptGeneration | None
    _open_generation: AttemptGeneration | None
    uri: str
    ws_uri: WebSocketURI
    additional_headers: HeadersLike | None
    process_exception: Callable[[Exception], Exception | None]
    reconnect_delays: Callable[[], Generator[float]]


class AttemptEpoch:
    """
    Stack of client attempt generations.

    Pass an instance as the ``attempt_epoch`` argument of
    :func:`~websockets.asyncio.client.connect` (:mod:`asyncio`, :mod:`trio`) or
    :func:`~websockets.sync.client.connect` /
    :class:`~websockets.sync.client.reconnect` (:mod:`threading`) to bind
    opening handshakes and reconnect loops to explicit generations. Factories
    push and pop generations themselves; hosts call :meth:`advance` to replace
    the current generation when its retry policy must stop driving new opens.

    Args:
        process_exception: Default policy for generations pushed directly.
        reconnect_delays: Default backoff cursor factory for generations.
        max_redirects: Default redirect budget for one opening handshake.

    """

    def __init__(
        self,
        *,
        process_exception: Callable[[Exception], Exception | None] = process_exception,
        reconnect_delays: Callable[[], Generator[float]] = backoff,
        max_redirects: int = MAX_REDIRECTS,
    ) -> None:
        self.process_exception = process_exception
        self.reconnect_delays = reconnect_delays
        self.max_redirects = max_redirects
        self._numbers = itertools.count(1)
        self._stack: list[AttemptGeneration] = []
        #: Every generation ever pushed, including popped ones.
        self.history: list[AttemptGeneration] = []

    @property
    def current(self) -> AttemptGeneration | None:
        """The generation at the top of the stack, or :obj:`None`."""
        return self._stack[-1] if self._stack else None

    @property
    def generation_number(self) -> int:
        """Number of the current generation (0 when the stack is empty)."""
        return self._stack[-1].number if self._stack else 0

    def push(
        self,
        uri: str,
        *,
        additional_headers: HeadersLike | None = None,
        owner: object | None = None,
        process_exception: Callable[[Exception], Exception | None] | None = None,
        reconnect_delays: Callable[[], Generator[float]] | None = None,
        max_redirects: int | None = None,
    ) -> AttemptGeneration:
        """Push a new generation on top of the stack."""
        generation = AttemptGeneration(
            next(self._numbers),
            epoch=self,
            owner=owner,
            uri=uri,
            additional_headers=additional_headers,
            process_exception=(
                process_exception
                if process_exception is not None
                else self.process_exception
            ),
            reconnect_delays=(
                reconnect_delays
                if reconnect_delays is not None
                else self.reconnect_delays
            ),
            max_redirects=max_redirects
            if max_redirects is not None
            else self.max_redirects,
        )
        self._stack.append(generation)
        self.history.append(generation)
        return generation

    def pop(self, generation: AttemptGeneration) -> None:
        """
        Pop ``generation`` if it is current, then invalidate it.

        Popping is idempotent: popping a generation that was already replaced
        only marks it stale; it never pops a different, newer generation.
        """
        if self._stack and self._stack[-1] is generation:
            self._stack.pop()
        generation.invalidate()

    def advance(self, uri: str | None = None) -> AttemptGeneration:
        """
        Replace the current generation with a fresh one.

        The old generation is popped and invalidated — a reconnect wait on it
        returns immediately and its redirect cursor, backoff generator, and
        exception judgment are closed — and a new generation is pushed with a
        fresh backoff cursor. The URI cursor, credential headers, retry
        policy, redirect budget, and loop ownership are inherited from the
        replaced generation.

        """
        if not self._stack:
            raise RuntimeError("cannot advance an empty attempt epoch")
        old = self._stack[-1]
        self.pop(old)
        return self.push(
            old.uri if uri is None else uri,
            additional_headers=old.additional_headers,
            owner=old.owner,
            process_exception=old.process_exception,
            reconnect_delays=old.reconnect_delays,
            max_redirects=old.max_redirects,
        )

    def close(self) -> None:
        """Pop and invalidate every generation on the stack."""
        while self._stack:
            self.pop(self._stack[-1])


@contextlib.contextmanager
def loop_generation_scope(
    connecter: _GenerationConnecter,
    *,
    max_redirects: int,
) -> Iterator[AttemptGeneration]:
    """
    Bind one reconnect loop to a generation.

    With an epoch, push a loop-owned generation and pop it (including
    generations adopted via :meth:`AttemptEpoch.advance`) on exit. Without an
    epoch, use a detached generation.

    """
    epoch = connecter.attempt_epoch
    owner = object()
    if epoch is None:
        generation = AttemptGeneration.detached(
            uri=connecter.uri,
            additional_headers=connecter.additional_headers,
            process_exception=connecter.process_exception,
            reconnect_delays=connecter.reconnect_delays,
            max_redirects=max_redirects,
        )
    else:
        # The redirect budget is declared on the epoch; it survives advances.
        generation = epoch.push(
            connecter.uri,
            additional_headers=connecter.additional_headers,
            owner=owner,
            process_exception=connecter.process_exception,
            reconnect_delays=connecter.reconnect_delays,
        )
    connecter._loop_generation = generation
    try:
        yield generation
    finally:
        connecter._loop_generation = None
        if epoch is None:
            generation.invalidate()
        else:
            # Pop the original generation and every generation that an
            # advance() adopted for this loop, but never a foreign child.
            while True:
                current = epoch.current
                if current is None or current.owner is not owner:
                    break
                epoch.pop(current)


@contextlib.contextmanager
def open_generation_scope(
    connecter: _GenerationConnecter,
    *,
    max_redirects: int,
) -> Iterator[AttemptGeneration]:
    """
    Bind one opening handshake sequence to a generation.

    Inside the reconnect loop of this connecter, spend the redirect budget of
    the loop generation while keeping its backoff cursor. Otherwise, push a
    child generation (or use a detached one) so a one-shot open never rewrites
    a foreign generation's URI cursor.

    """
    epoch = connecter.attempt_epoch
    loop_generation = connecter._loop_generation
    if loop_generation is not None and (
        epoch is None or epoch.current is loop_generation
    ):
        connecter._open_generation = loop_generation
        loop_generation.begin_open()
        try:
            yield loop_generation
        finally:
            connecter._open_generation = None
        return

    if epoch is None:
        generation = AttemptGeneration.detached(
            uri=connecter.uri,
            additional_headers=connecter.additional_headers,
            process_exception=connecter.process_exception,
            reconnect_delays=connecter.reconnect_delays,
            max_redirects=max_redirects,
        )
    else:
        # A nested one-shot open gets its own child generation; its pop on
        # exit — including exceptional exit — leaves the foreign ledger intact.
        generation = epoch.push(
            connecter.uri,
            additional_headers=connecter.additional_headers,
            owner=connecter,
            process_exception=connecter.process_exception,
            reconnect_delays=connecter.reconnect_delays,
        )
    connecter._open_generation = generation
    generation.begin_open()
    try:
        yield generation
    finally:
        connecter._open_generation = None
        if epoch is None:
            generation.invalidate()
        else:
            epoch.pop(generation)


def commit_redirect(
    connecter: _GenerationConnecter,
    exc: Exception,
    *,
    preexisting_endpoint: bool,
    unix: bool,
    explicit_destination: bool,
    endpoint_noun: str,
    destination_noun: str,
) -> Exception | str:
    """
    Run the redirect decision for an opening handshake and commit it.

    This is the body of the host ``process_redirect`` methods: evaluate
    ``exc`` against the redirect table and, when it may be followed, rewrite
    the URI and credential cursor of the current opening generation. The
    rewrite is visible only on that generation; a superseded generation's
    :meth:`AttemptGeneration.follow_redirect` raises :class:`GenerationStale`.

    """
    generation = connecter._open_generation
    if generation is None:  # pragma: no cover - always set by open_generation_scope
        return exc
    decision = redirect_decision(
        exc,
        base_uri=generation.uri,
        additional_headers=generation.additional_headers,
        preexisting_endpoint=preexisting_endpoint,
        unix=unix,
        explicit_destination=explicit_destination,
        endpoint_noun=endpoint_noun,
        destination_noun=destination_noun,
    )
    if isinstance(decision, Exception):
        return decision
    generation.follow_redirect(decision)
    # Mirror the cursor on the factory for backwards compatibility with hosts
    # reading connecter.uri / connecter.ws_uri after a redirect.
    connecter.uri = generation.uri
    connecter.ws_uri = generation.ws_uri
    connecter.additional_headers = generation.additional_headers
    return decision.uri


def _reconcile_after_wait(
    connecter: _GenerationConnecter,
    generation: AttemptGeneration,
) -> AttemptGeneration:
    epoch = connecter.attempt_epoch
    new_generation = epoch.current if epoch is not None else None
    if (
        new_generation is None
        or new_generation is generation
        or new_generation.owner is not generation.owner
    ):
        raise GenerationStale(
            f"attempt generation {generation.number} was closed "
            "while waiting to reconnect"
        )
    connecter._loop_generation = new_generation
    connecter._open_generation = None
    return new_generation


def sync_reconnect_wait(
    connecter: _GenerationConnecter,
    generation: AttemptGeneration,
    delay: float,
) -> AttemptGeneration:
    """Wait for ``delay`` in :mod:`threading`; adopt a new generation if advanced."""
    # threading.Event.wait() is interrupted by invalidate()/advance() from
    # another thread; otherwise it is equivalent to time.sleep(delay).
    if generation.interrupt.wait(delay):
        return _reconcile_after_wait(connecter, generation)
    return generation


async def asyncio_reconnect_wait(
    connecter: _GenerationConnecter,
    generation: AttemptGeneration,
    delay: float,
) -> AttemptGeneration:
    """Wait for ``delay`` in :mod:`asyncio`; adopt a new generation if advanced."""
    import asyncio

    event = generation.host_event(asyncio.Event)
    try:
        await asyncio.wait_for(event.wait(), timeout=delay)
    except TimeoutError:
        # The delay elapsed; the generation is still current.
        return generation
    # The generation was invalidated before the delay elapsed.
    return _reconcile_after_wait(connecter, generation)


async def trio_reconnect_wait(
    connecter: _GenerationConnecter,
    generation: AttemptGeneration,
    delay: float,
) -> AttemptGeneration:
    """Wait for ``delay`` in :mod:`trio`; adopt a new generation if advanced."""
    import trio

    event = generation.host_event(trio.Event)
    with trio.move_on_after(delay):
        await event.wait()
    if event.is_set():
        return _reconcile_after_wait(connecter, generation)
    return generation
