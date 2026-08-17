"""Unit tests for `Consumer._check_exchange`, with pika mocked out.

These are deliberately broker-free. Two of the defects covered here are not
observable against a live broker:

* whether `stop()` actually *closes* the connection or merely queues a
  callback that nothing ever dispatches, and
* the non-404 re-raise branch, which cannot be provoked over AMQP because
  passive declares require no permissions on RabbitMQ 4.2.1 (a restricted
  user still gets 404/success, never 403).
"""

import os
import tempfile
import types
import unittest
from unittest import mock

from pika import exceptions as pika_exceptions

from tests.helpers import make_temp_consumer, purge_exchange_logger

EXCHANGE = "unit_check_ex"


def fake_credentials():
    """A configurator stand-in; enough for `pika.ConnectionParameters`."""
    return types.SimpleNamespace(
        use_tls=False,
        ampq_url="127.0.0.1",
        port=5672,
        username="guest",
        password="guest",
        ca_certificate=None,
    )


class TestCheckExchangeUnit(unittest.TestCase):
    def setUp(self):
        handle, self.log_file = tempfile.mkstemp()
        os.close(handle)

        # make_temp_consumer only reads these three attributes off the Varys
        self.varys = types.SimpleNamespace(
            routing_key="arbitrary_string",
            _credentials=fake_credentials(),
            _logfile=self.log_file,
            _log_level="DEBUG",
        )

    def tearDown(self):
        purge_exchange_logger(EXCHANGE)
        os.remove(self.log_file)

    def patched_connection(self):
        """Patch `pika.BlockingConnection` and return (mock_conn, mock_channel)."""
        patcher = mock.patch("varys.consumer.pika.BlockingConnection")
        mock_connection_class = patcher.start()
        self.addCleanup(patcher.stop)

        mock_conn = mock_connection_class.return_value
        mock_channel = mock.MagicMock()
        mock_channel.is_open = True
        mock_conn.channel.return_value = mock_channel
        mock_conn.is_open = True

        return mock_conn, mock_channel

    def test_check_exchange_closes_synchronously(self):
        """`_check_exchange` must actually close the channel and connection.

        `stop()` only hands the closes to `add_callback_threadsafe`, which pika
        dispatches from `process_data_events()`/`start_consuming()`. This
        Consumer is never `start()`ed, so nothing ever dispatches them and the
        socket leaks once per call, despite the docstring's "Closes connection
        after checks".
        """
        mock_conn, mock_channel = self.patched_connection()
        consumer = make_temp_consumer(self.varys, EXCHANGE, "q")

        consumer._check_exchange()

        mock_channel.close.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_check_exchange_is_reusable_after_channel_closed(self):
        """A second `_check_exchange` must not reuse a dead channel.

        A 404 passive declare makes the broker close the channel (the
        connection survives). `if not self._channel` tests existence, not
        liveness -- a closed BlockingChannel is still truthy, since it defines
        neither __bool__ nor __len__ -- so the dead channel is reused and pika
        raises ChannelWrongStateError. That is not a ChannelClosed subclass, so
        it escapes both handlers in `_check_exchange`.

        Unreachable through `try_receive` today, which builds a fresh Consumer
        per call, but it is a live landmine if that ever caches or a caller
        holds onto a Consumer. `run()` gets this right; `_check_exchange`
        does not.
        """
        mock_conn, mock_channel = self.patched_connection()
        mock_channel.exchange_declare.side_effect = (
            pika_exceptions.ChannelClosedByBroker(404, "NOT_FOUND")
        )
        consumer = make_temp_consumer(self.varys, EXCHANGE, "q")

        first = consumer._check_exchange()
        # the broker closed the channel out from under us on the 404
        mock_channel.is_open = False
        second = consumer._check_exchange()

        self.assertEqual(first, second)
        self.assertEqual(
            mock_conn.channel.call_count,
            2,
            "_check_exchange should open a fresh channel when the current one "
            "is closed, i.e. guard on `is None or not is_open`",
        )

    def test_check_exchange_propagates_non_404_channel_error(self):
        """Coverage for the `if e.reply_code != 404: raise` branch.

        Mock-only: passive declares need no permissions on RabbitMQ 4.2.1, so
        a 403 is unreachable through this code path with a live broker.
        """
        _, mock_channel = self.patched_connection()
        mock_channel.exchange_declare.side_effect = (
            pika_exceptions.ChannelClosedByBroker(403, "ACCESS_REFUSED")
        )
        consumer = make_temp_consumer(self.varys, EXCHANGE, "q")

        with self.assertRaises(pika_exceptions.ChannelClosed):
            consumer._check_exchange()

    def test_check_exchange_propagates_non_404_on_queue_declare(self):
        """Same branch, but on the queue declare after the exchange succeeds."""
        _, mock_channel = self.patched_connection()
        mock_channel.queue_declare.side_effect = (
            pika_exceptions.ChannelClosedByBroker(403, "ACCESS_REFUSED")
        )
        consumer = make_temp_consumer(self.varys, EXCHANGE, "q")

        with self.assertRaises(pika_exceptions.ChannelClosed):
            consumer._check_exchange()


if __name__ == "__main__":
    unittest.main()
