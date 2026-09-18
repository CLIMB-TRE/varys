"""Unit tests for Producer.publish_message.

These drive the producer against stub connection/channel objects so they need no
broker, unlike the integration tests in test_varys.py.
"""

import os
import tempfile
import unittest

from pika import exceptions as pika_exceptions

from varys.exceptions import (
    ProducerNotReadyError,
    PublishFailedError,
    PublishTimeoutError,
)
from varys.producer import Producer


class StubConfig:
    use_tls = False
    ampq_url = "localhost"
    port = 5672
    username = "guest"
    password = "guest"


class StubChannel:
    """Records publishes, and optionally fails a number of them first."""

    def __init__(self, fail_with=None, fail_times=0):
        self.published = []
        self._fail_with = fail_with
        self._fail_times = fail_times

    def basic_publish(self, exchange, routing_key, body, properties, mandatory=False):
        if self._fail_times > 0:
            self._fail_times -= 1
            raise self._fail_with
        self.published.append(body)


class StubConnection:
    """Runs threadsafe callbacks inline, standing in for the I/O thread."""

    def __init__(self, run_callbacks=True):
        self.is_closed = False
        self._run_callbacks = run_callbacks

    def add_callback_threadsafe(self, callback):
        if self._run_callbacks:
            callback()


def make_producer(ready=True, connection=None, channel=None, reconnect_wait=0):
    handle, log_file = tempfile.mkstemp(suffix=".log")
    os.close(handle)

    producer = Producer(
        message_queue=None,
        exchange="test_exchange",
        configuration=StubConfig(),
        log_file=log_file,
        log_level="DEBUG",
        queue_suffix="q",
        exchange_type="fanout",
        reconnect_wait=reconnect_wait,
    )

    producer._connection = connection if connection is not None else StubConnection()
    producer._channel = channel if channel is not None else StubChannel()
    if ready:
        producer._ready.set()

    return producer


class TestPublishMessage(unittest.TestCase):

    def test_publish_succeeds(self):
        channel = StubChannel()
        producer = make_producer(channel=channel)

        producer.publish_message({"hello": "world"})

        self.assertEqual(len(channel.published), 1)
        self.assertEqual(producer._message_number, 1)

    def test_not_ready_raises_and_does_not_count_the_message(self):
        """Regression test for the silent drop.

        A producer that is not ready used to consume its single attempt without
        publishing, then increment the counter and log 'Published message #N'.
        """
        channel = StubChannel()
        producer = make_producer(ready=False, channel=channel)

        with self.assertRaises(ProducerNotReadyError):
            producer.publish_message({"hello": "world"}, max_attempts=1, ready_timeout=0.01)

        self.assertEqual(channel.published, [])
        self.assertEqual(producer._message_number, 0)

    def test_max_attempts_one_still_attempts_once(self):
        """max_attempts=1 must mean one attempt, not zero."""
        channel = StubChannel()
        producer = make_producer(channel=channel)

        producer.publish_message({"hello": "world"}, max_attempts=1)

        self.assertEqual(len(channel.published), 1)

    def test_unroutable_message_raises(self):
        channel = StubChannel(
            fail_with=pika_exceptions.UnroutableError([]), fail_times=99
        )
        producer = make_producer(channel=channel)

        with self.assertRaises(PublishFailedError):
            producer.publish_message({"hello": "world"}, max_attempts=2)

        self.assertEqual(channel.published, [])
        self.assertEqual(producer._message_number, 0)

    def test_retry_then_succeed(self):
        channel = StubChannel(
            fail_with=pika_exceptions.AMQPConnectionError(), fail_times=2
        )
        producer = make_producer(channel=channel)

        producer.publish_message({"hello": "world"}, max_attempts=3)

        self.assertEqual(len(channel.published), 1)
        self.assertEqual(producer._message_number, 1)

    def test_no_confirmation_times_out(self):
        """A callback that never runs must not be reported as published."""
        channel = StubChannel()
        producer = make_producer(
            connection=StubConnection(run_callbacks=False), channel=channel
        )

        with self.assertRaises(PublishTimeoutError):
            producer.publish_message(
                {"hello": "world"}, max_attempts=1, confirm_timeout=0.01
            )

        self.assertEqual(producer._message_number, 0)

    def test_unserialisable_message_raises(self):
        producer = make_producer()

        with self.assertRaises(TypeError):
            producer.publish_message({"when": object()})

    def test_wait_until_ready(self):
        producer = make_producer(ready=False)
        self.assertFalse(producer.wait_until_ready(timeout=0.01))

        producer._ready.set()
        self.assertTrue(producer.wait_until_ready(timeout=0.01))


if __name__ == "__main__":
    unittest.main()
