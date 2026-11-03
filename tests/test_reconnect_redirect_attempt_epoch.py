"""
Tests for reconnect / redirect attempt generations.

These tests cover the shared ledger in
``websockets.reconnect_redirect_attempt_epoch`` and its causal behavior across
the :mod:`asyncio` and :mod:`threading` client factories:

* a backoff cursor of generation N stops driving opens after advancing to
  generation N+1;
* within one generation, a 3xx redirect (open ledger) and a transient status
  (reconnect ledger) take different forks;
* a cross-origin redirect strips credentials while a same-origin redirect
  keeps them;
* ``open_timeout`` (one open) and ``reconnect_delays`` (between attempts) are
  separate budgets;
* a one-shot connect nested in a foreign reconnect loop pops its own
  generation, including on exceptional exit, without touching the outer
  ledger.

All servers are local TCP sockets; no external service is used.

"""

import asyncio
import http
import socket
import threading
import time
import unittest

from websockets.asyncio.client import connect as asyncio_connect
from websockets.asyncio.server import serve as asyncio_serve
from websockets.client import backoff, process_exception
from websockets.datastructures import Headers
from websockets.exceptions import InvalidStatus
from websockets.http11 import Response
from websockets.reconnect_redirect_attempt_epoch import (
    AttemptEpoch,
    AttemptGeneration,
    GenerationStale,
    Redirect,
    asyncio_reconnect_wait,
    commit_redirect,
    loop_generation_scope,
    open_generation_scope,
    redirect_decision,
    sync_reconnect_wait,
    trio_reconnect_wait,
)
from websockets.sync.client import connect as sync_connect, reconnect as sync_reconnect
from websockets.uri import parse_uri

from .asyncio.server import args as asyncio_args, get_uri as asyncio_get_uri
from .sync.server import get_uri as sync_get_uri, handler as sync_handler, run_server
from .utils import MS


# A reconnect_delays factory that records every generator it produced and how
# many delays each generator consumed.
class DelayCursor:
    def __init__(self, *delays, on_next=None, later_delays=None):
        self.delays = delays or (0.0,)
        # Delay sequence used by generators created after the first one, i.e.
        # by fresh backoff cursors of advanced generations.
        self.later_delays = later_delays
        self.on_next = on_next
        self.generators = []

    def __call__(self):
        state = {"consumed": 0}
        is_first = not self.generators
        self.generators.append(state)
        delays = (
            self.delays if is_first or self.later_delays is None else self.later_delays
        )
        on_next = self.on_next

        def gen():
            i = 0
            while True:
                state["consumed"] += 1
                if on_next is not None:
                    on_next()
                yield delays[min(i, len(delays) - 1)]
                i += 1

        return gen()

    @property
    def total_consumed(self):
        return sum(state["consumed"] for state in self.generators)


def redirect_exception(location, status_code=302):
    return InvalidStatus(
        Response(
            status_code,
            "Found",
            Headers([("Location", location)]),
            b"",
        )
    )


CREDENTIAL_HEADERS = [
    ("Authorization", "Bearer secret"),
    ("Cookie", "session=secret"),
    ("Proxy-Authorization", "Basic secret"),
    ("X-Custom", "keep"),
]


class FakeConnecter:
    """Minimal client factory satisfying the generation connecter protocol."""

    def __init__(self, epoch=None, uri="ws://localhost/", headers=None):
        self.attempt_epoch = epoch
        self._loop_generation = None
        self._open_generation = None
        self.uri = uri
        self.ws_uri = parse_uri(uri)
        self.additional_headers = headers
        self.process_exception = process_exception
        self.reconnect_delays = backoff


def unused_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("localhost", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class EpochLedgerTests(unittest.TestCase):
    def test_push_current_number_history_and_close(self):
        epoch = AttemptEpoch()
        self.assertIsNone(epoch.current)
        self.assertEqual(epoch.generation_number, 0)
        gen1 = epoch.push("ws://localhost/1")
        gen2 = epoch.push("ws://localhost/2")
        self.assertIs(epoch.current, gen2)
        self.assertEqual(epoch.generation_number, 2)
        self.assertEqual([g.number for g in epoch.history], [1, 2])
        self.assertEqual(repr(gen2), "<AttemptGeneration 2 active ws://localhost/2>")
        epoch.close()
        self.assertIsNone(epoch.current)
        self.assertTrue(gen1.stale and gen2.stale)
        self.assertEqual(repr(gen1), "<AttemptGeneration 1 stale ws://localhost/1>")

    def test_pop_is_idempotent_and_never_pops_younger(self):
        epoch = AttemptEpoch()
        gen1 = epoch.push("ws://localhost/1")
        gen2 = epoch.push("ws://localhost/2")
        # Popping the older generation invalidates it but leaves gen2 current.
        epoch.pop(gen1)
        self.assertTrue(gen1.stale)
        self.assertIs(epoch.current, gen2)
        self.assertFalse(gen2.stale)
        # Popping again is a no-op.
        epoch.pop(gen1)
        self.assertIs(epoch.current, gen2)
        # Pop generations in stack order.
        epoch.pop(gen2)
        self.assertIs(epoch.current, gen1)
        epoch.pop(gen1)
        self.assertIsNone(epoch.current)
        self.assertTrue(gen2.stale)

    def test_advance_invalidates_old_and_inherits_ledger(self):
        epoch = AttemptEpoch(max_redirects=3)
        gen1 = epoch.push("ws://localhost/", additional_headers={"X": "y"})
        gen1.follow_redirect(
            Redirect(
                "ws://localhost/redir",
                parse_uri("ws://localhost/redir"),
                {"X": "y"},
                False,
            )
        )
        gen1.next_delay()
        gen2 = epoch.advance()
        self.assertIs(epoch.current, gen2)
        self.assertTrue(gen1.stale)
        self.assertEqual(gen2.uri, "ws://localhost/redir")
        self.assertEqual(gen2.additional_headers, {"X": "y"})
        self.assertEqual(gen2.max_redirects, 3)
        self.assertIs(gen2.owner, gen1.owner)
        self.assertIsNone(gen2.delays)

    def test_advance_with_uri_replaces_the_cursor(self):
        epoch = AttemptEpoch()
        epoch.push("ws://localhost/")
        gen2 = epoch.advance("ws://localhost/other")
        self.assertEqual(gen2.uri, "ws://localhost/other")

    def test_advance_empty_epoch(self):
        with self.assertRaises(RuntimeError):
            AttemptEpoch().advance()

    def test_stale_generation_rejects_every_cursor(self):
        epoch = AttemptEpoch()
        gen = epoch.push("ws://localhost/")
        epoch.pop(gen)
        with self.assertRaises(GenerationStale):
            gen.begin_open()
        with self.assertRaises(GenerationStale):
            gen.follow_redirect(
                Redirect("ws://localhost/x", parse_uri("ws://localhost/x"), None, False)
            )
        with self.assertRaises(GenerationStale):
            gen.classify_exception(OSError())
        with self.assertRaises(GenerationStale):
            gen.next_delay()

    def test_redirect_cursor_budget_and_backoff_reset(self):
        epoch = AttemptEpoch()
        gen = epoch.push("ws://localhost/")
        gen.begin_open()
        self.assertEqual(gen.opens, 1)
        gen.follow_redirect(
            Redirect("ws://localhost/1", parse_uri("ws://localhost/1"), None, False)
        )
        gen.follow_redirect(
            Redirect("ws://localhost/2", parse_uri("ws://localhost/2"), None, False)
        )
        self.assertEqual(gen.redirects, 2)
        # A new open restarts the redirect budget but not the generation.
        gen.begin_open()
        self.assertEqual(gen.redirects, 0)
        self.assertEqual(gen.opens, 2)
        d1 = gen.next_delay()
        self.assertIsInstance(d1, float)
        self.assertIsNotNone(gen.delays)
        gen.reset_backoff()
        self.assertIsNone(gen.delays)

    def test_classify_exception_records_judgment_and_supports_raising(self):
        def raising(exc):
            raise RuntimeError("boom")

        gen = AttemptGeneration.detached(
            uri="ws://localhost/",
            additional_headers=None,
            process_exception=raising,
            reconnect_delays=backoff,
            max_redirects=10,
        )
        exc = OSError()
        decision = gen.classify_exception(exc)
        self.assertIsInstance(decision, RuntimeError)
        self.assertIs(gen.judgment[0], exc)
        self.assertIs(decision, gen.judgment[1])
        # The ordinary convention is returning the decision.
        gen.process_exception = process_exception
        self.assertIsNone(gen.classify_exception(OSError()))

    def test_invalidate_is_idempotent_and_interrupts_async_event(self):
        gen = AttemptGeneration.detached(
            uri="ws://localhost/",
            additional_headers=None,
            process_exception=process_exception,
            reconnect_delays=backoff,
            max_redirects=10,
        )

        async def scenario():
            event = gen.host_event(asyncio.Event)
            self.assertIs(event, gen.host_event(asyncio.Event))
            self.assertFalse(event.is_set())
            gen.invalidate()
            self.assertTrue(event.is_set())
            # A second invalidation is a no-op.
            gen.invalidate()
            # The event is pre-set when the generation is already stale.
            self.assertTrue(gen.host_event(asyncio.Event).is_set())

        asyncio.run(scenario())


class RedirectDecisionTests(unittest.TestCase):
    def test_non_redirect_is_returned(self):
        decision = redirect_decision(
            InvalidStatus(Response(503, "Service Unavailable", Headers(), b"")),
            base_uri="ws://localhost/",
            additional_headers=None,
            preexisting_endpoint=False,
            unix=False,
            explicit_destination=False,
            endpoint_noun="socket",
            destination_noun="host or port",
        )
        self.assertIsInstance(decision, InvalidStatus)

    def test_preexisting_endpoint(self):
        decision = redirect_decision(
            redirect_exception("/there"),
            base_uri="ws://localhost/",
            additional_headers=None,
            preexisting_endpoint=True,
            unix=False,
            explicit_destination=False,
            endpoint_noun="socket",
            destination_noun="host or port",
        )
        self.assertEqual(
            str(decision),
            "cannot follow redirect to ws://localhost/there with a preexisting socket",
        )

    def test_tls_downgrade_is_forbidden(self):
        decision = redirect_decision(
            redirect_exception("ws://localhost/"),
            base_uri="wss://localhost/",
            additional_headers=None,
            preexisting_endpoint=False,
            unix=False,
            explicit_destination=False,
            endpoint_noun="stream",
            destination_noun="host or port",
        )
        self.assertEqual(
            str(decision),
            "cannot follow redirect to non-secure URI ws://localhost/",
        )

    def test_cross_origin_unix_socket(self):
        decision = redirect_decision(
            redirect_exception("ws://other:1/"),
            base_uri="ws://localhost/",
            additional_headers=None,
            preexisting_endpoint=False,
            unix=True,
            explicit_destination=False,
            endpoint_noun="socket",
            destination_noun="host and port",
        )
        self.assertEqual(
            str(decision),
            "cannot follow cross-origin redirect to ws://other:1/ with a Unix socket",
        )

    def test_cross_origin_explicit_destination(self):
        decision = redirect_decision(
            redirect_exception("ws://other:1/"),
            base_uri="ws://localhost/",
            additional_headers=None,
            preexisting_endpoint=False,
            unix=False,
            explicit_destination=True,
            endpoint_noun="socket",
            destination_noun="host and port",
        )
        self.assertEqual(
            str(decision),
            "cannot follow cross-origin redirect to ws://other:1/ "
            "with an explicit host and port",
        )

    def test_cross_origin_strips_credentials(self):
        decision = redirect_decision(
            redirect_exception("ws://localhost:200/"),
            base_uri="ws://localhost:100/",
            additional_headers=Headers(CREDENTIAL_HEADERS),
            preexisting_endpoint=False,
            unix=False,
            explicit_destination=False,
            endpoint_noun="socket",
            destination_noun="host or port",
        )
        self.assertIsInstance(decision, Redirect)
        self.assertTrue(decision.cross_origin)
        self.assertEqual(decision.uri, "ws://localhost:200/")
        self.assertEqual(
            [key.lower() for key, _ in decision.additional_headers.raw_items()],
            ["x-custom"],
        )

    def test_cross_origin_without_headers(self):
        decision = redirect_decision(
            redirect_exception("ws://localhost:200/"),
            base_uri="ws://localhost:100/",
            additional_headers=None,
            preexisting_endpoint=False,
            unix=False,
            explicit_destination=False,
            endpoint_noun="socket",
            destination_noun="host or port",
        )
        self.assertIsInstance(decision, Redirect)
        self.assertTrue(decision.cross_origin)
        self.assertIsNone(decision.additional_headers)

    def test_same_origin_keeps_credentials(self):
        decision = redirect_decision(
            redirect_exception("/there"),
            base_uri="ws://localhost/",
            additional_headers=Headers(CREDENTIAL_HEADERS),
            preexisting_endpoint=False,
            unix=False,
            explicit_destination=False,
            endpoint_noun="socket",
            destination_noun="host or port",
        )
        self.assertIsInstance(decision, Redirect)
        self.assertFalse(decision.cross_origin)
        self.assertEqual(
            [key.lower() for key, _ in decision.additional_headers.raw_items()],
            ["authorization", "cookie", "proxy-authorization", "x-custom"],
        )


class GenerationScopeTests(unittest.TestCase):
    def test_detached_loop_scope_reuses_generation_for_opens(self):
        connecter = FakeConnecter()
        with loop_generation_scope(connecter, max_redirects=7) as gen:
            self.assertTrue(gen.is_detached)
            self.assertIs(connecter._loop_generation, gen)
            with open_generation_scope(connecter, max_redirects=7) as open_gen:
                self.assertIs(open_gen, gen)
            self.assertIsNone(connecter._open_generation)
        self.assertTrue(gen.stale)
        self.assertIsNone(connecter._loop_generation)

    def test_epoch_loop_scope_reuses_generation_for_opens(self):
        epoch = AttemptEpoch(max_redirects=4)
        connecter = FakeConnecter(epoch)
        with loop_generation_scope(connecter, max_redirects=10) as gen:
            self.assertEqual(gen.max_redirects, 4)
            self.assertIs(epoch.current, gen)
            with open_generation_scope(connecter, max_redirects=10) as open_gen:
                self.assertIs(open_gen, gen)
        self.assertIsNone(epoch.current)
        self.assertTrue(gen.stale)

    def test_detached_one_shot_open(self):
        connecter = FakeConnecter()
        with open_generation_scope(connecter, max_redirects=3) as gen:
            self.assertTrue(gen.is_detached)
            self.assertEqual(gen.max_redirects, 3)
        self.assertTrue(gen.stale)

    def test_child_open_pushes_and_pops_even_on_exception(self):
        epoch = AttemptEpoch()
        connecter = FakeConnecter(epoch)
        with self.assertRaises(RuntimeError):
            with open_generation_scope(connecter, max_redirects=10) as gen:
                self.assertIs(epoch.current, gen)
                self.assertIs(connecter._open_generation, gen)
                raise RuntimeError("open failed")
        self.assertIsNone(epoch.current)
        self.assertTrue(gen.stale)

    def test_nested_open_under_foreign_loop_pushes_child(self):
        epoch = AttemptEpoch()
        outer = FakeConnecter(epoch)
        inner = FakeConnecter(epoch, uri="ws://localhost/inner")
        # The inner factory has its own loop generation that is not current in
        # the epoch: its open must not spend the outer loop's budget.
        foreign_loop = AttemptGeneration.detached(
            uri="ws://elsewhere/",
            additional_headers=None,
            process_exception=process_exception,
            reconnect_delays=backoff,
            max_redirects=10,
        )
        inner._loop_generation = foreign_loop
        with loop_generation_scope(outer, max_redirects=10) as outer_gen:
            outer_gen.begin_open()
            with open_generation_scope(inner, max_redirects=10) as child:
                self.assertIsNot(child, outer_gen)
                self.assertIs(epoch.current, child)
                self.assertEqual(child.uri, "ws://localhost/inner")
            # Child popped; outer ledger is current again and untouched.
            self.assertIs(epoch.current, outer_gen)
            self.assertEqual(outer_gen.redirects, 0)
            self.assertEqual(outer_gen.opens, 1)

    def test_loop_scope_leaves_foreign_generation_on_top(self):
        epoch = AttemptEpoch()
        connecter = FakeConnecter(epoch)
        with loop_generation_scope(connecter, max_redirects=10) as gen1:
            gen2 = epoch.advance()
            self.assertIs(epoch.current, gen2)
            # Adopted generations are owned by the loop and popped on exit.
            # A foreign-owned generation left on top must not be popped here;
            # consequently the adopted gen below it is left for its own scope.
            foreign = epoch.push("ws://foreign/", owner=object())
        self.assertIs(epoch.current, foreign)
        self.assertFalse(foreign.stale)
        self.assertTrue(gen1.stale)
        self.assertFalse(gen2.stale)
        epoch.pop(foreign)
        epoch.pop(gen2)

    def test_commit_redirect_mirrors_cursor_on_connecter(self):
        epoch = AttemptEpoch()
        connecter = FakeConnecter(
            epoch, uri="ws://localhost/", headers=Headers(CREDENTIAL_HEADERS)
        )
        with open_generation_scope(connecter, max_redirects=10) as gen:
            result = commit_redirect(
                connecter,
                redirect_exception("ws://localhost:200/"),
                preexisting_endpoint=False,
                unix=False,
                explicit_destination=False,
                endpoint_noun="socket",
                destination_noun="host or port",
            )
            self.assertEqual(result, "ws://localhost:200/")
            self.assertEqual(connecter.uri, "ws://localhost:200/")
            self.assertEqual(connecter.ws_uri.port, 200)
            self.assertEqual(gen.redirects, 1)
            self.assertNotIn(
                "authorization",
                [key.lower() for key, _ in connecter.additional_headers.raw_items()],
            )

    def test_commit_redirect_error_leaves_cursor_untouched(self):
        epoch = AttemptEpoch()
        connecter = FakeConnecter(epoch)
        with open_generation_scope(connecter, max_redirects=10):
            result = commit_redirect(
                connecter,
                redirect_exception("ws://other:1/"),
                preexisting_endpoint=False,
                unix=True,
                explicit_destination=False,
                endpoint_noun="socket",
                destination_noun="host or port",
            )
            self.assertIsInstance(result, ValueError)
            self.assertEqual(connecter.uri, "ws://localhost/")


class ReconnectWaitTests(unittest.TestCase):
    def test_sync_wait_elapses(self):
        connecter = FakeConnecter()
        with loop_generation_scope(connecter, max_redirects=10) as gen:
            result = sync_reconnect_wait(connecter, gen, 0)
            self.assertIs(result, gen)

    def test_sync_wait_adopts_advanced_generation(self):
        epoch = AttemptEpoch()
        connecter = FakeConnecter(epoch)
        with loop_generation_scope(connecter, max_redirects=10) as gen1:
            timer = threading.Timer(10 * MS, epoch.advance)
            timer.start()
            gen2 = sync_reconnect_wait(connecter, gen1, 30.0)
            timer.join()
            self.assertIsNot(gen2, gen1)
            self.assertIs(epoch.current, gen2)
            self.assertIs(connecter._loop_generation, gen2)

    def test_sync_wait_stale_when_popped(self):
        epoch = AttemptEpoch()
        connecter = FakeConnecter(epoch)
        with loop_generation_scope(connecter, max_redirects=10) as gen:
            timer = threading.Timer(10 * MS, epoch.pop, args=(gen,))
            timer.start()
            with self.assertRaises(GenerationStale):
                sync_reconnect_wait(connecter, gen, 30.0)
            timer.join()

    def test_sync_wait_stale_under_foreign_generation(self):
        epoch = AttemptEpoch()
        connecter = FakeConnecter(epoch)
        with loop_generation_scope(connecter, max_redirects=10) as gen:
            epoch.push("ws://foreign/", owner=object())
            timer = threading.Timer(10 * MS, gen.invalidate)
            timer.start()
            with self.assertRaises(GenerationStale):
                sync_reconnect_wait(connecter, gen, 30.0)
            timer.join()

    def test_sync_wait_stale_detached(self):
        connecter = FakeConnecter()
        with loop_generation_scope(connecter, max_redirects=10) as gen:
            timer = threading.Timer(10 * MS, gen.invalidate)
            timer.start()
            with self.assertRaises(GenerationStale):
                sync_reconnect_wait(connecter, gen, 30.0)
            timer.join()


class AsyncioReconnectWaitTests(unittest.IsolatedAsyncioTestCase):
    async def test_asyncio_wait_elapses(self):
        connecter = FakeConnecter()
        with loop_generation_scope(connecter, max_redirects=10) as gen:
            result = await asyncio_reconnect_wait(connecter, gen, 0)
            self.assertIs(result, gen)

    async def test_asyncio_wait_adopts_advanced_generation(self):
        epoch = AttemptEpoch()
        connecter = FakeConnecter(epoch)
        with loop_generation_scope(connecter, max_redirects=10) as gen1:
            loop = asyncio.get_running_loop()
            loop.call_later(10 * MS, epoch.advance)
            gen2 = await asyncio_reconnect_wait(connecter, gen1, 30.0)
            self.assertIsNot(gen2, gen1)
            self.assertIs(epoch.current, gen2)

    async def test_asyncio_wait_stale_when_popped(self):
        epoch = AttemptEpoch()
        connecter = FakeConnecter(epoch)
        with loop_generation_scope(connecter, max_redirects=10) as gen:

            async def pop():
                await asyncio.sleep(10 * MS)
                epoch.pop(gen)

            task = asyncio.ensure_future(pop())
            with self.assertRaises(GenerationStale):
                await asyncio_reconnect_wait(connecter, gen, 30.0)
            await task

    async def test_asyncio_wait_stale_under_foreign_generation(self):
        epoch = AttemptEpoch()
        connecter = FakeConnecter(epoch)
        with loop_generation_scope(connecter, max_redirects=10) as gen:
            epoch.push("ws://foreign/", owner=object())

            async def invalidate():
                await asyncio.sleep(10 * MS)
                gen.invalidate()

            task = asyncio.ensure_future(invalidate())
            with self.assertRaises(GenerationStale):
                await asyncio_reconnect_wait(connecter, gen, 30.0)
            await task

    async def test_asyncio_wait_stale_detached(self):
        connecter = FakeConnecter()
        with loop_generation_scope(connecter, max_redirects=10) as gen:

            async def invalidate():
                await asyncio.sleep(10 * MS)
                gen.invalidate()

            task = asyncio.ensure_future(invalidate())
            with self.assertRaises(GenerationStale):
                await asyncio_reconnect_wait(connecter, gen, 30.0)
            await task


class TrioReconnectWaitTests(unittest.TestCase):
    def test_trio_wait_elapses_adopts_and_stales(self):
        try:
            import trio
            import trio.testing
        except ImportError:  # pragma: no cover
            self.skipTest("trio isn't installed")

        epoch = AttemptEpoch()
        connecter = FakeConnecter(epoch)

        async def scenario():
            # The delay elapsed: the generation remains current.
            with loop_generation_scope(connecter, max_redirects=10) as gen1:
                gen1_result = await trio_reconnect_wait(connecter, gen1, 1.0)
                self.assertIs(gen1_result, gen1)

                # An advance during the wait adopts the new generation.
                async def advance():
                    await trio.sleep(0.5)
                    epoch.advance()

                async with trio.open_nursery() as nursery:
                    nursery.start_soon(advance)
                    gen2 = await trio_reconnect_wait(connecter, gen1, 10.0)
                    self.assertIsNot(gen2, gen1)
                    self.assertIs(epoch.current, gen2)
                    # Stop the loop scope from waiting on an adopted generation
                    # by ending the scenario; the scope pops gen2 on exit.

        trio.run(scenario, clock=trio.testing.MockClock(autojump_threshold=0.0))

        connecter = FakeConnecter()

        async def stale_scenario():
            with loop_generation_scope(connecter, max_redirects=10) as gen:

                async def invalidate():
                    await trio.sleep(0.5)
                    gen.invalidate()

                async with trio.open_nursery() as nursery:
                    nursery.start_soon(invalidate)
                    with self.assertRaises(GenerationStale):
                        await trio_reconnect_wait(connecter, gen, 10.0)

        trio.run(
            stale_scenario,
            clock=trio.testing.MockClock(autojump_threshold=0.0),
        )


# ---------------------------------------------------------------------------
# Cross-host acceptance tests with local TCP servers.
# ---------------------------------------------------------------------------


CREDENTIALS = {
    "Authorization": "Bearer secret",
    "Cookie": "session=secret",
    "Proxy-Authorization": "Basic secret",
    "X-Custom": "keep",
}


class AsyncioEpochTests(unittest.IsolatedAsyncioTestCase):
    async def test_backoff_cursor_stops_after_advance(self):
        gen1_waiting = asyncio.Event()
        gen2_waiting = asyncio.Event()

        def on_next():
            (gen1_waiting if len(cursor.generators) == 1 else gen2_waiting).set()

        cursor = DelayCursor(30.0, on_next=on_next, later_delays=(MS,))
        epoch = AttemptEpoch(reconnect_delays=cursor)
        attempts = 0
        succeed = False

        async def process_request(connection, request):
            nonlocal attempts
            attempts += 1
            if not succeed:
                return connection.respond(http.HTTPStatus.SERVICE_UNAVAILABLE, "🚒")

        async def runner():
            async for _connection in asyncio_connect(
                asyncio_get_uri(server),
                attempt_epoch=epoch,
                reconnect_delays=cursor,
            ):
                break

        async with asyncio_serve(
            *asyncio_args, process_request=process_request
        ) as server:
            task = asyncio.ensure_future(runner())
            await gen1_waiting.wait()
            gen1 = epoch.current
            self.assertEqual(gen1.number, 1)
            self.assertEqual(cursor.generators[0]["consumed"], 1)
            # Advance while generation 1 waits for 30 seconds: that wait is
            # abandoned. Generation 2 must fail once (driving its own fresh
            # cursor) before the server accepts the connection.
            gen2 = epoch.advance()
            await gen2_waiting.wait()
            self.assertEqual(len(cursor.generators), 2)
            succeed = True
            await task

        self.assertTrue(gen1.stale)
        self.assertTrue(gen2.stale)
        self.assertIsNone(epoch.current)
        self.assertEqual([g.number for g in epoch.history], [1, 2])
        self.assertEqual(gen1.opens, 1)
        self.assertEqual(gen2.opens, 2)
        # Generation 1's generator was never consumed after the advance;
        # generation 2 opened from a fresh generator.
        self.assertEqual(cursor.generators[0]["consumed"], 1)
        self.assertEqual(cursor.generators[1]["consumed"], 1)

    async def test_redirect_fork_and_transient_reconnect_share_generation(self):
        cursor = DelayCursor(2 * MS)
        epoch = AttemptEpoch(reconnect_delays=cursor)
        attempts = 0
        paths = []

        async def process_request(connection, request):
            nonlocal attempts
            attempts += 1
            paths.append(request.path)
            if attempts == 1:
                response = connection.respond(http.HTTPStatus.FOUND, "")
                response.headers["Location"] = "/redir"
                return response
            if attempts == 2:
                return connection.respond(http.HTTPStatus.SERVICE_UNAVAILABLE, "🚒")

        async def runner():
            async for _connection in asyncio_connect(
                asyncio_get_uri(server),
                attempt_epoch=epoch,
                reconnect_delays=cursor,
            ):
                break

        async with asyncio_serve(
            *asyncio_args, process_request=process_request
        ) as server:
            await runner()

        (gen,) = epoch.history
        # The 3xx redirect rewrote the open ledger of this generation; the
        # 503 drove the reconnect ledger of the same generation.
        self.assertEqual(paths, ["/", "/redir", "/redir"])
        self.assertTrue(gen.uri.endswith("/redir"))
        self.assertEqual(cursor.total_consumed, 1)
        self.assertIsInstance(gen.judgment[0], InvalidStatus)
        self.assertEqual(gen.judgment[0].response.status_code, 503)
        self.assertIsNone(gen.judgment[1])
        # The successful open restarted the redirect budget of the open.
        self.assertEqual(gen.redirects, 0)
        self.assertEqual(gen.opens, 2)

    async def test_cross_origin_strips_and_same_origin_keeps_credentials(self):
        cross_seen = []
        same_seen = []

        def record(target):
            def process_request(connection, request):
                target.append(
                    {key.lower(): value for key, value in request.headers.raw_items()}
                )

            return process_request

        def redirect_to(uri):
            def process_request(connection, request):
                response = connection.respond(http.HTTPStatus.FOUND, "")
                response.headers["Location"] = uri
                return response

            return process_request

        epoch = AttemptEpoch()
        async with asyncio_serve(
            *asyncio_args, process_request=record(cross_seen)
        ) as target:
            target_uri = asyncio_get_uri(target)
            async with asyncio_serve(
                *asyncio_args, process_request=redirect_to(target_uri)
            ) as redirector:
                async with asyncio_connect(
                    asyncio_get_uri(redirector),
                    attempt_epoch=epoch,
                    additional_headers=dict(CREDENTIALS),
                ):
                    self.assertNotIn("authorization", cross_seen[0])
                    self.assertNotIn("cookie", cross_seen[0])
                    self.assertNotIn("proxy-authorization", cross_seen[0])
                    self.assertEqual(cross_seen[0]["x-custom"], "keep")
            cross_gen = epoch.history[-1]
            self.assertTrue(cross_gen.stale)

            # A same-origin redirect is a path-only redirect on one server.
            def same_origin_request(connection, request):
                if request.path == "/redirect":
                    response = connection.respond(http.HTTPStatus.FOUND, "")
                    response.headers["Location"] = "/there"
                    return response
                same_seen.append(
                    {key.lower(): value for key, value in request.headers.raw_items()}
                )

            async with asyncio_serve(
                *asyncio_args, process_request=same_origin_request
            ) as same_server:
                same_uri = asyncio_get_uri(same_server)
                async with asyncio_connect(
                    same_uri + "/redirect",
                    attempt_epoch=epoch,
                    additional_headers=dict(CREDENTIALS),
                ):
                    self.assertEqual(same_seen[0]["authorization"], "Bearer secret")
                    self.assertEqual(same_seen[0]["cookie"], "session=secret")
                    self.assertEqual(
                        same_seen[0]["proxy-authorization"], "Basic secret"
                    )

        # The two one-shot opens owned two distinct child generations, and the
        # cross-origin cursor ended at the target origin.
        self.assertEqual(len(epoch.history), 2)
        cross_gen, same_gen = epoch.history
        self.assertEqual(cross_gen.uri, target_uri)
        self.assertNotEqual(cross_gen.uri, same_gen.uri)
        self.assertEqual(same_gen.uri, same_uri + "/there")

    async def test_open_timeout_and_reconnect_delay_are_separate_budgets(self):
        delay = 10 * MS
        cursor = DelayCursor(delay)
        epoch = AttemptEpoch(reconnect_delays=cursor)
        attempts = 0

        async def process_request(connection, request):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                await asyncio.sleep(30 * MS)

        async def runner():
            async for _connection in asyncio_connect(
                asyncio_get_uri(server),
                attempt_epoch=epoch,
                open_timeout=3 * MS,
                reconnect_delays=cursor,
            ):
                break

        async with asyncio_serve(
            *asyncio_args, process_request=process_request
        ) as server:
            t0 = time.monotonic()
            await runner()
            elapsed = time.monotonic() - t0

        (gen,) = epoch.history
        self.assertIsInstance(gen.judgment[0], TimeoutError)
        self.assertEqual(str(gen.judgment[0]), "timed out during opening handshake")
        # The reconnect delay was waited for; the open timeout did not replace it.
        self.assertGreaterEqual(elapsed + 1e-6, delay)
        self.assertEqual(cursor.total_consumed, 1)
        self.assertEqual(gen.opens, 2)

    async def test_nested_one_shot_connect_does_not_touch_outer_loop(self):
        cursor = DelayCursor(2 * MS)
        epoch = AttemptEpoch(reconnect_delays=cursor)
        attempts = 0
        bodies = 0

        async def process_request(connection, request):
            nonlocal attempts
            attempts += 1
            if attempts in (1, 3):
                return connection.respond(http.HTTPStatus.SERVICE_UNAVAILABLE, "🚒")

        refused_uri = f"ws://localhost:{unused_port()}/"

        async def runner():
            nonlocal bodies
            async for _outer in asyncio_connect(
                asyncio_get_uri(outer_server),
                attempt_epoch=epoch,
                reconnect_delays=cursor,
            ):
                bodies += 1
                if bodies == 1:
                    # An inner open that fails pops its child generation
                    # cleanly when unwinding.
                    with self.assertRaises(OSError):
                        await asyncio_connect(refused_uri, attempt_epoch=epoch)
                    self.assertEqual(epoch.generation_number, 1)
                    # An inner open that succeeds pushed its own generation
                    # and popped it by the time the connection is handed over.
                    async with asyncio_connect(
                        asyncio_get_uri(inner_server), attempt_epoch=epoch
                    ):
                        self.assertEqual(epoch.generation_number, 1)
                    self.assertEqual(epoch.generation_number, 1)
                else:
                    break

        async with asyncio_serve(
            *asyncio_args, process_request=process_request
        ) as outer_server:
            async with asyncio_serve(*asyncio_args) as inner_server:
                outer_uri = asyncio_get_uri(outer_server)
                inner_uri = asyncio_get_uri(inner_server)
                await runner()

        outer_gen, refused_gen, inner_gen = epoch.history
        self.assertEqual(bodies, 2)
        self.assertEqual(attempts, 4)
        self.assertIsNone(epoch.current)
        self.assertEqual(outer_gen.number, 1)
        self.assertEqual(outer_gen.opens, 4)
        self.assertEqual(refused_gen.number, 2)
        self.assertTrue(refused_gen.stale)
        self.assertEqual(inner_gen.number, 3)
        self.assertTrue(inner_gen.stale)
        self.assertEqual(inner_gen.uri, inner_uri)
        # The outer cursor and URI cursor were never rewritten by inner opens.
        self.assertEqual(outer_gen.uri, outer_uri)
        self.assertIsNone(inner_gen.delays)
        self.assertIsNone(inner_gen.judgment)


class SyncEpochTests(unittest.TestCase):
    def _run_worker(self, target):
        errors = []

        def runner():
            try:
                target()
            except BaseException as exc:  # captured and re-raised below
                errors.append(exc)

        thread = threading.Thread(target=runner)
        thread.start()
        return thread, errors

    def test_backoff_cursor_stops_after_advance(self):
        gen1_waiting = threading.Event()
        gen2_waiting = threading.Event()

        def on_next():
            (gen1_waiting if len(cursor.generators) == 1 else gen2_waiting).set()

        cursor = DelayCursor(30.0, on_next=on_next, later_delays=(5 * MS,))
        epoch = AttemptEpoch(reconnect_delays=cursor)
        attempts = 0
        succeed = False

        def process_request(connection, request):
            nonlocal attempts
            attempts += 1
            if not succeed:
                return connection.respond(http.HTTPStatus.SERVICE_UNAVAILABLE, "🚒")

        with run_server(process_request=process_request) as server:
            uri = sync_get_uri(server)

            def runner():
                for _connection in sync_reconnect(
                    uri,
                    attempt_epoch=epoch,
                    reconnect_delays=cursor,
                ):
                    break

            thread, errors = self._run_worker(runner)
            self.assertTrue(gen1_waiting.wait(5))
            gen1 = epoch.current
            self.assertEqual(gen1.number, 1)
            self.assertEqual(cursor.generators[0]["consumed"], 1)
            # Generation 2 must fail once on its own fresh cursor as well.
            gen2 = epoch.advance()
            self.assertTrue(gen2_waiting.wait(5))
            self.assertEqual(len(cursor.generators), 2)
            succeed = True
            thread.join(5)
            self.assertFalse(thread.is_alive())

        self.assertEqual(errors, [])
        self.assertTrue(gen1.stale and gen2.stale)
        self.assertEqual([g.number for g in epoch.history], [1, 2])
        self.assertEqual(gen1.opens, 1)
        self.assertEqual(gen2.opens, 2)
        self.assertEqual(cursor.generators[0]["consumed"], 1)
        self.assertEqual(cursor.generators[1]["consumed"], 1)

    def test_redirect_fork_and_transient_reconnect_share_generation(self):
        cursor = DelayCursor(2 * MS)
        epoch = AttemptEpoch(reconnect_delays=cursor)
        attempts = 0
        paths = []

        def process_request(connection, request):
            nonlocal attempts
            attempts += 1
            paths.append(request.path)
            if attempts == 1:
                response = connection.respond(http.HTTPStatus.FOUND, "")
                response.headers["Location"] = "/redir"
                return response
            if attempts == 2:
                return connection.respond(http.HTTPStatus.SERVICE_UNAVAILABLE, "🚒")

        with run_server(process_request=process_request) as server:
            for _connection in sync_reconnect(
                sync_get_uri(server),
                attempt_epoch=epoch,
                reconnect_delays=cursor,
            ):
                break

        (gen,) = epoch.history
        self.assertEqual(paths, ["/", "/redir", "/redir"])
        self.assertTrue(gen.uri.endswith("/redir"))
        self.assertEqual(cursor.total_consumed, 1)
        self.assertIsInstance(gen.judgment[0], InvalidStatus)
        self.assertEqual(gen.judgment[0].response.status_code, 503)
        self.assertIsNone(gen.judgment[1])
        self.assertEqual(gen.redirects, 0)
        self.assertEqual(gen.opens, 2)

    def test_cross_origin_strips_and_same_origin_keeps_credentials(self):
        cross_seen = []
        same_seen = []

        def record(target):
            def process_request(connection, request):
                target.append(
                    {key.lower(): value for key, value in request.headers.raw_items()}
                )

            return process_request

        def redirect_to(uri):
            def process_request(connection, request):
                response = connection.respond(http.HTTPStatus.FOUND, "")
                response.headers["Location"] = uri
                return response

            return process_request

        epoch = AttemptEpoch()
        with run_server(process_request=record(cross_seen)) as target:
            target_uri = sync_get_uri(target)
            with run_server(process_request=redirect_to(target_uri)) as redirector:
                with sync_connect(
                    sync_get_uri(redirector),
                    attempt_epoch=epoch,
                    additional_headers=dict(CREDENTIALS),
                ):
                    self.assertNotIn("authorization", cross_seen[0])
                    self.assertNotIn("cookie", cross_seen[0])
                    self.assertNotIn("proxy-authorization", cross_seen[0])
                    self.assertEqual(cross_seen[0]["x-custom"], "keep")

            # A same-origin redirect is a path-only redirect on one server.
            def same_origin_request(connection, request):
                if request.path == "/redirect":
                    response = connection.respond(http.HTTPStatus.FOUND, "")
                    response.headers["Location"] = "/there"
                    return response
                same_seen.append(
                    {key.lower(): value for key, value in request.headers.raw_items()}
                )

            with run_server(process_request=same_origin_request) as same_server:
                same_uri = sync_get_uri(same_server)
                with sync_connect(
                    same_uri + "/redirect",
                    attempt_epoch=epoch,
                    additional_headers=dict(CREDENTIALS),
                ):
                    self.assertEqual(same_seen[0]["authorization"], "Bearer secret")
                    self.assertEqual(same_seen[0]["cookie"], "session=secret")
                    self.assertEqual(
                        same_seen[0]["proxy-authorization"], "Basic secret"
                    )

        self.assertEqual(len(epoch.history), 2)
        cross_gen, same_gen = epoch.history
        self.assertEqual(cross_gen.uri, target_uri)
        self.assertNotEqual(cross_gen.uri, same_gen.uri)
        self.assertEqual(same_gen.uri, same_uri + "/there")

    def test_open_timeout_and_reconnect_delay_are_separate_budgets(self):
        delay = 10 * MS
        cursor = DelayCursor(delay)
        epoch = AttemptEpoch(reconnect_delays=cursor)
        attempts = 0

        def process_request(connection, request):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                import time as _time

                _time.sleep(30 * MS)

        with run_server(process_request=process_request) as server:
            t0 = time.monotonic()
            for _connection in sync_reconnect(
                sync_get_uri(server),
                attempt_epoch=epoch,
                open_timeout=3 * MS,
                reconnect_delays=cursor,
            ):
                break
            elapsed = time.monotonic() - t0

        (gen,) = epoch.history
        self.assertIsInstance(gen.judgment[0], TimeoutError)
        self.assertEqual(
            str(gen.judgment[0]),
            "timed out while waiting for handshake response",
        )
        self.assertGreaterEqual(elapsed + 1e-6, delay)
        self.assertEqual(cursor.total_consumed, 1)
        self.assertEqual(gen.opens, 2)

    def test_nested_one_shot_connect_does_not_touch_outer_loop(self):
        cursor = DelayCursor(2 * MS)
        epoch = AttemptEpoch(reconnect_delays=cursor)
        attempts = 0
        bodies = 0

        def process_request(connection, request):
            nonlocal attempts
            attempts += 1
            if attempts in (1, 3):
                return connection.respond(http.HTTPStatus.SERVICE_UNAVAILABLE, "🚒")

        refused_uri = f"ws://localhost:{unused_port()}/"

        with run_server(sync_handler, process_request=process_request) as outer_server:
            with run_server(sync_handler) as inner_server:
                outer_uri = sync_get_uri(outer_server)
                inner_uri = sync_get_uri(inner_server)
                for _outer in sync_reconnect(
                    sync_get_uri(outer_server),
                    attempt_epoch=epoch,
                    reconnect_delays=cursor,
                ):
                    bodies += 1
                    if bodies == 1:
                        # A failed inner open pops its child while unwinding.
                        with self.assertRaises(OSError):
                            sync_connect(refused_uri, attempt_epoch=epoch)
                        self.assertEqual(epoch.generation_number, 1)
                        # A successful inner open popped its child by handover.
                        with sync_connect(
                            sync_get_uri(inner_server), attempt_epoch=epoch
                        ):
                            self.assertEqual(epoch.generation_number, 1)
                        self.assertEqual(epoch.generation_number, 1)
                    else:
                        break

        outer_gen, refused_gen, inner_gen = epoch.history
        self.assertEqual(bodies, 2)
        self.assertEqual(attempts, 4)
        self.assertIsNone(epoch.current)
        self.assertEqual(outer_gen.number, 1)
        self.assertEqual(outer_gen.opens, 4)
        self.assertEqual(refused_gen.number, 2)
        self.assertTrue(refused_gen.stale)
        self.assertEqual(inner_gen.number, 3)
        self.assertTrue(inner_gen.stale)
        self.assertEqual(inner_gen.uri, inner_uri)
        self.assertEqual(outer_gen.uri, outer_uri)
        self.assertIsNone(inner_gen.delays)
        self.assertIsNone(inner_gen.judgment)


if __name__ == "__main__":
    unittest.main()
