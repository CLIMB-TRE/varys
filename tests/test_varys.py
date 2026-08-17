import unittest
import time
import tempfile
import os
import json
import logging
from varys import Varys
import pytest

from tests.helpers import admin_channel, file_handler_count, write_config

DIR = os.path.dirname(__file__)
LOG_FILENAME = os.path.join(DIR, "test.log")
TMP_HANDLE, TMP_FILENAME = tempfile.mkstemp()
TEXT = "Hello, world!"

pytestmark = pytest.mark.broker


class TestVarys(unittest.TestCase):

    def tearDown(self):
        # this seems to prevent some hanging
        # or errors related to closing connections that haven't opened yet
        # I presume because some operations are so fast
        # that we try to close the connections before they've opened
        # 0.01s seems to be sufficient; 0.1s is just a bit conservative
        time.sleep(0.1)

        self.v.close()
        os.remove(TMP_FILENAME)
        time.sleep(0.1)

        with admin_channel() as channel:
            channel.queue_delete(queue="test_varys.q")

        time.sleep(0.5)

        # check that all file handles were dropped
        logger = logging.getLogger("test_varys")
        self.assertEqual(file_handler_count(logger), 0)

    def send_and_receive(self):
        self.v.send(TEXT, "test_varys", queue_suffix="q")
        message = self.v.receive("test_varys", queue_suffix="q")
        self.assertEqual(TEXT, json.loads(message.body))

        logger = logging.getLogger("test_varys")
        self.assertEqual(file_handler_count(logger), 1)

    def manual_ack(self):

        self.v.auto_ack = False

        time.sleep(0.5)

        self.v.send(TEXT, "test_varys", queue_suffix="q")

        message = self.v.receive("test_varys", queue_suffix="q")

        self.v.acknowledge_message(message)

    def nack(self):
        self.v.auto_ack = False

        self.v.send(TEXT, "test_varys", queue_suffix="q")

        message = self.v.receive("test_varys", queue_suffix="q")

        self.v.nack_message(message)

        # check that the message has been requeued
        message_2 = self.v.receive("test_varys", queue_suffix="q")

        self.v.acknowledge_message(message_2)

        self.assertEqual(message.body, message_2.body)

    def send_and_receive_batch(self):
        self.v.send(TEXT, "test_varys", queue_suffix="q")
        self.v.send(TEXT, "test_varys", queue_suffix="q")

        messages = self.v.receive_batch("test_varys", queue_suffix="q", timeout=1)
        parsed_messages = [json.loads(m.body) for m in messages]
        self.assertListEqual([TEXT, TEXT], parsed_messages)

    def receive_no_message(self):
        self.assertIsNone(self.v.receive("test_varys", queue_suffix="q", timeout=1))

    def send_no_suffix(self):
        self.assertRaises(Exception, self.v.send, TEXT, "test_varys")

    def receive_no_suffix(self):
        self.assertRaises(Exception, self.v.receive, "test_varys")

    def receive_batch_no_suffix(self):
        self.assertRaises(Exception, self.v.receive_batch, "test_varys")

    def quick_turnaround(self):
        """Regression test for GitHub issue #28:

        https://github.com/CLIMB-TRE/varys/issues/28

        Quickly sends a lot of messages, closes the client, then
        checks that all the messages can be received.
        """
        sent_messages = [str(i) for i in range(1000)]

        for message in sent_messages:
            self.v.send(message, "test_varys", queue_suffix="q")

        self.v.close()

        # we re-use the setUp method to get the same configuration
        self.setUp()
        # timeout seems to need to be at least 0.01s
        received_messages = [
            message.body.decode()[1:-1]
            for message in self.v.receive_batch(
                "test_varys", queue_suffix="q", timeout=0.1
            )
        ]

        self.assertEqual(received_messages, sent_messages)


@pytest.mark.tls
class TestVarysTLS(TestVarys):

    def setUp(self):
        write_config(TMP_FILENAME, tls=True)

        self.v = Varys("test", LOG_FILENAME, config_path=TMP_FILENAME)

    def test_send_and_receive(self):
        self.send_and_receive()

    def test_manual_ack(self):
        self.manual_ack()

    def test_nack(self):
        self.nack()

    def test_send_and_receive_batch(self):
        self.send_and_receive_batch()

    def test_receive_no_message(self):
        self.receive_no_message()

    def test_send_no_suffix(self):
        self.send_no_suffix()

    def test_receive_no_suffix(self):
        self.receive_no_suffix()

    def test_receive_batch_no_suffix(self):
        self.receive_batch_no_suffix()

    def test_quick_turnaround(self):
        self.quick_turnaround()


class TestVarysNoTLS(TestVarys):

    def setUp(self):
        write_config(TMP_FILENAME, tls=False)

        self.v = Varys("test", LOG_FILENAME, config_path=TMP_FILENAME)

    def test_send_and_receive(self):
        self.send_and_receive()

    def test_manual_ack(self):
        self.manual_ack()

    def test_nack(self):
        self.nack()

    def test_send_and_receive_batch(self):
        self.send_and_receive_batch()

    def test_receive_no_message(self):
        self.receive_no_message()

    def test_send_no_suffix(self):
        self.send_no_suffix()

    def test_receive_no_suffix(self):
        self.receive_no_suffix()

    def test_receive_batch_no_suffix(self):
        self.receive_batch_no_suffix()

    def test_quick_turnaround(self):
        self.quick_turnaround()


@pytest.mark.permissions
class TestVarysPermissions(unittest.TestCase):

    def setUp(self):
        write_config(
            TMP_FILENAME, tls=False, profiles={"test": "guest2", "admin": "guest"}
        )

        # Setup exchange
        admin_varys = Varys("admin", LOG_FILENAME, config_path=TMP_FILENAME)
        admin_varys.send("setup message", "test-exchange", queue_suffix="test_queue")
        admin_varys.close()

        with admin_channel() as channel:
            channel.queue_purge(queue="test-exchange.test_queue")

        self.v = Varys("test", LOG_FILENAME, config_path=TMP_FILENAME)

    def tearDown(self):
        # this seems to prevent some hanging
        # or errors related to closing connections that haven't opened yet
        # I presume because some operations are so fast
        # that we try to close the connections before they've opened
        # 0.01s seems to be sufficient; 0.1s is just a bit conservative
        time.sleep(0.1)

        self.v.close()
        os.remove(TMP_FILENAME)
        time.sleep(0.1)

        with admin_channel() as channel:
            channel.queue_purge(queue="test-exchange.test_queue")

        time.sleep(0.5)

        # check that all file handles were dropped for relevant loggers
        for logger_name in ["test-exchange", "test-exchange-2", "test-exchange-3"]:
            logger = logging.getLogger(logger_name)
            self.assertEqual(file_handler_count(logger), 0)

    def test_not_permitted_declare_fail(self):
        self.v.send(TEXT, "test-exchange-2", queue_suffix="test_queue")
        time.sleep(0.5)
        with open(LOG_FILENAME, "r") as f:
            loglines = f.readlines()

        self.assertTrue(
            any(
                "pika.exceptions.ChannelClosedByBroker: (403, " in message
                for message in loglines
            )
        )

    def test_send_receive_extant_queue(self):
        self.v.send(TEXT, "test-exchange", queue_suffix="test_queue")
        message = self.v.receive("test-exchange", queue_suffix="test_queue")
        self.assertEqual(TEXT, json.loads(message.body))

        logger = logging.getLogger("test-exchange")
        self.assertEqual(file_handler_count(logger), 1)

    def test_send_nonexistant_queue(self):
        self.v.send(TEXT, "test-exchange", queue_suffix="test_queue_2")
        message = self.v.receive("test-exchange", queue_suffix="test_queue_2")
        self.assertEqual(TEXT, json.loads(message.body))

        logger = logging.getLogger("test-exchange")
        self.assertEqual(file_handler_count(logger), 1)

    def test_send_nonexistant_exchange(self):
        self.v.send(TEXT, "test-exchange-3", queue_suffix="test_queue")
        message = self.v.receive("test-exchange-3", queue_suffix="test_queue")
        self.assertEqual(TEXT, json.loads(message.body))

        logger = logging.getLogger("test-exchange-3")
        self.assertEqual(file_handler_count(logger), 1)


if __name__ == "__main__":
    unittest.main()
