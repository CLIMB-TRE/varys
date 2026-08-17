"""Live-broker tests for `Varys.try_receive` / `Consumer._check_exchange`.

Runs against the no-TLS profile only; every defect covered here is
transport-independent.
"""

import logging
import os
import tempfile
import unittest

import pika
import pytest
from pika import exceptions as pika_exceptions

from varys import Varys
from tests.helpers import (
    admin_channel,
    file_handler_count,
    make_temp_consumer,
    purge_exchange_logger,
    reset_broker_objects,
    write_config,
    varys_file_handlers,
)

DIR = os.path.dirname(__file__)
LOG_FILENAME = os.path.join(DIR, "test.log")

pytestmark = pytest.mark.broker

ABSENT = "try_recv_absent"
PRESENT = "try_recv_present"
UNBOUND = "try_recv_unbound"
EXCHANGES = (ABSENT, PRESENT, UNBOUND)


def result_flags(result, exchange, queue_suffix):
    """Read (exchange_exists, queue_exists) out of a `try_receive` result.

    Deliberately tolerant of both the current internal keying
    (`exchange` / `exchange.suffix`) and the intended public keying
    (`"exchange"` / `"queue"`), so the behavioural tests below keep passing
    across the fix for the key-naming defect. The key shape itself is pinned
    by `test_try_receive_result_keys_match_public_api`.
    """
    internal_queue_key = f"{exchange}.{queue_suffix}"

    if "exchange" in result and "queue" in result:
        return result["exchange"], result["queue"]
    if exchange in result and internal_queue_key in result:
        return result[exchange], result[internal_queue_key]

    raise AssertionError(f"unrecognised try_receive result shape: {result!r}")


class TestTryReceive(unittest.TestCase):
    def setUp(self):
        handle, self.config_path = tempfile.mkstemp()
        os.close(handle)
        write_config(self.config_path, tls=False)

        reset_broker_objects(
            *EXCHANGES, *(f"{name}.q" for name in EXCHANGES), f"{PRESENT}.nope"
        )

        self.v = Varys("test", LOG_FILENAME, config_path=self.config_path)

    def tearDown(self):
        self.v.close()

        reset_broker_objects(
            *EXCHANGES, *(f"{name}.q" for name in EXCHANGES), f"{PRESENT}.nope"
        )

        for exchange in EXCHANGES:
            purge_exchange_logger(exchange)

        os.remove(self.config_path)

    def declare_exchange(self, exchange):
        with admin_channel() as channel:
            channel.exchange_declare(
                exchange=exchange,
                exchange_type=pika.exchange_type.ExchangeType.fanout,
                durable=True,
            )

    def declare_bound_queue(self, exchange, queue_suffix="q"):
        self.declare_exchange(exchange)
        with admin_channel() as channel:
            channel.queue_declare(queue=f"{exchange}.{queue_suffix}", durable=True)
            channel.queue_bind(
                queue=f"{exchange}.{queue_suffix}",
                exchange=exchange,
                routing_key="arbitrary_string",
            )

    # --- log handler leak on the 404 early return ---------------------------

    def test_try_receive_missing_exchange_releases_log_handler(self):
        """The 404 early return must still release the log FileHandler.

        `_check_exchange` returns before reaching `self.stop()`, so
        `_stop_logger()` never runs and the refcount on the per-exchange
        handler is never decremented.
        """
        logger = logging.getLogger(ABSENT)
        self.assertEqual(file_handler_count(logger), 0, "dirty precondition")

        self.v.try_receive(ABSENT, "q")

        self.assertEqual(file_handler_count(logger), 0)

    def test_try_receive_missing_exchange_repeated_does_not_leak(self):
        """Polling a not-yet-created exchange must not leak once per call.

        This is the intended usage pattern, so the refcount grows without
        bound and the log file handle is never closed.
        """
        self.v.try_receive(ABSENT, "q")

        handlers = varys_file_handlers(ABSENT)
        self.assertEqual(len(handlers), 1, "expected one handler after first poll")
        handler = handlers[0]

        for _ in range(4):
            self.v.try_receive(ABSENT, "q")

        self.assertEqual(
            handler.count, 0, "handler refcount grows by one per poll iteration"
        )
        self.assertNotIn(handler, logging.getLogger(ABSENT).handlers)
        self.assertTrue(handler.stream is None or handler.stream.closed)

    # --- stop() never actually closes anything ------------------------------

    def test_check_exchange_actually_closes_connection(self):
        """`stop()` only queues closes via `add_callback_threadsafe`.

        pika dispatches those from `process_data_events()`/`start_consuming()`,
        and this Consumer thread is never started, so the connection and
        channel stay open -- contradicting "Closes connection after checks".
        """
        self.declare_bound_queue(PRESENT)
        consumer = make_temp_consumer(self.v, PRESENT, "q")
        self.addCleanup(purge_exchange_logger, PRESENT)

        consumer._check_exchange()

        self.assertTrue(consumer._channel.is_closed, "channel left open")
        self.assertTrue(consumer._connection.is_closed, "connection left open")

    # --- passive queue_declare does not prove a binding ---------------------

    @unittest.expectedFailure
    def test_try_receive_unbound_queue_reports_not_bound(self):
        """A queue that exists but is not bound must report False.

        Both docstrings promise "queue bound to that exchange", but
        `queue_declare(passive=True)` only proves the queue exists. A leftover
        unbound queue reports True and the caller then receives nothing
        forever.

        Marked expectedFailure rather than fixed: there is no AMQP-only way to
        enumerate bindings (`queue_unbind` of an absent binding is a silent
        no-op), so a real check needs the management HTTP API, which means the
        plugin plus a published port 15672 in CI.
        """
        self.declare_exchange(UNBOUND)
        with admin_channel() as channel:
            channel.queue_declare(queue=f"{UNBOUND}.q", durable=True)

        result = self.v.try_receive(UNBOUND, "q")
        exchange_exists, queue_bound = result_flags(result, UNBOUND, "q")

        self.assertTrue(exchange_exists)
        self.assertFalse(queue_bound)

    # --- dead channel reuse -------------------------------------------------

    def test_check_exchange_reuse_after_404_raises(self):
        """Reusing a Consumer after a 404 must not raise.

        The broker closes the channel on a 404 passive declare, but
        `if not self._channel` cannot detect that -- a closed BlockingChannel
        is still truthy. The dead channel is reused and pika raises
        ChannelWrongStateError, which is not a ChannelClosed subclass and so
        escapes both handlers.
        """
        consumer = make_temp_consumer(self.v, ABSENT, "q")
        self.addCleanup(purge_exchange_logger, ABSENT)

        first = consumer._check_exchange()
        try:
            second = consumer._check_exchange()
        except pika_exceptions.ChannelWrongStateError as e:
            self.fail(f"second _check_exchange raised {e!r} on a dead channel")

        self.assertEqual(first, second)

    # --- result dict keying -------------------------------------------------

    def test_try_receive_result_keys_match_public_api(self):
        """Result keys must be usable without knowing varys' internals.

        `Process.__init__` sets `self._queue = exchange + "." + queue_suffix`
        and `_check_exchange` keys on that, so the queue key is
        "try_recv_present.q" while the public API takes a bare suffix.
        """
        self.declare_bound_queue(PRESENT)

        result = self.v.try_receive(PRESENT, "q")

        self.assertEqual(set(result), {"exchange", "queue"})
        self.assertIs(result["exchange"], True)
        self.assertIs(result["queue"], True)

    # --- behavioural matrix (regression guards) -----------------------------

    def test_try_receive_all_present(self):
        self.declare_bound_queue(PRESENT)

        result = self.v.try_receive(PRESENT, "q")

        self.assertEqual(result_flags(result, PRESENT, "q"), (True, True))

    def test_try_receive_exchange_present_queue_absent(self):
        """Exercises the queue-404 branch, which no existing test touches."""
        self.declare_exchange(PRESENT)

        result = self.v.try_receive(PRESENT, "nope")

        self.assertEqual(result_flags(result, PRESENT, "nope"), (True, False))

    def test_try_receive_nothing_present(self):
        result = self.v.try_receive(ABSENT, "q")

        self.assertEqual(result_flags(result, ABSENT, "q"), (False, False))

    def test_try_receive_does_not_register_channel(self):
        """Confirms the "Does not store Consumer instance" docstring."""
        self.declare_bound_queue(PRESENT)

        self.v.try_receive(PRESENT, "q")

        self.assertNotIn(PRESENT, self.v.get_channels()["consumer_channels"])

    def test_try_receive_does_not_start_thread(self):
        """The premise of the `stop()`-is-a-no-op defect: nothing dispatches."""
        self.declare_bound_queue(PRESENT)
        consumer = make_temp_consumer(self.v, PRESENT, "q")
        self.addCleanup(purge_exchange_logger, PRESENT)

        consumer._check_exchange()

        self.assertFalse(consumer.is_alive())


class TestReceiveBlockRegression(unittest.TestCase):
    """The honest behavioural test for the shifted `block`/`timeout` positions.

    Disabled by default: against current code `receive("ex", "q", 1)` sets
    `block=1, timeout=None` and blocks forever on `queue.Queue.get`. A watchdog
    does not rescue it -- `v.close()` will not interrupt the stuck get, and
    tearDown's `join()` would then hang too -- so this would burn the CI
    `timeout 120s` and take the whole suite down with it. CI relies on
    test_api_signature.py instead.

    Run with VARYS_TEST_HANGS=1 to see the hang for yourself.
    """

    @unittest.skipUnless(
        os.getenv("VARYS_TEST_HANGS"),
        "hangs against current code; see test_api_signature.py",
    )
    def test_receive_positional_timeout_returns_none(self):
        handle, config_path = tempfile.mkstemp()
        os.close(handle)
        write_config(config_path, tls=False)
        v = Varys("test", LOG_FILENAME, config_path=config_path)

        try:
            # 1.2.x semantics: third positional arg is the timeout
            self.assertIsNone(v.receive(ABSENT, "q", 1))
        finally:
            v.close()
            os.remove(config_path)


if __name__ == "__main__":
    unittest.main()
