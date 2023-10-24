import unittest
import time
import tempfile
import os
import json
import logging
from varys import varys
import pika

DIR = os.path.dirname(__file__)
LOG_FILENAME = os.path.join(DIR, "test.log")
TMP_HANDLE, TMP_FILENAME = tempfile.mkstemp()
TEXT = "Hello, world!"


class TestVarys(unittest.TestCase):
    def setUp(self):
        config = {
            "version": "0.1",
            "profiles": {
                "test": {
                    "username": "guest",
                    "password": "guest",
                    "amqp_url": "127.0.0.1",
                    "port": 5672,
                }
            },
        }

        with open(TMP_FILENAME, "w") as f:
            json.dump(config, f, ensure_ascii=False)

        self.v = varys("test", LOG_FILENAME, config_path=TMP_FILENAME)

    def tearDown(self):
        # this seems to prevent some hanging
        # or errors related to closing connections that haven't opened yet
        # I presume because some operations are so fast
        # that we try to close the connections before they've opened
        # 0.01s seems to be sufficient; 0.1s is just a bit conservative
        time.sleep(0.1)

        credentials = pika.PlainCredentials("guest", "guest")

        connection = pika.BlockingConnection(
            pika.ConnectionParameters("localhost", credentials=credentials)
        )
        channel = connection.channel()

        channel.queue_delete(queue="test_varys")

        connection.close()

        self.v.close()
        os.remove(TMP_FILENAME)
        time.sleep(0.1)

        # check that all file handles were dropped
        logger = logging.getLogger("test_varys")
        self.assertEqual(len(logger.handlers), 0)

    def test_send_and_receive(self):
        self.v.send(TEXT, "test_varys", queue_suffix="q")
        message = self.v.receive("test_varys", queue_suffix="q")
        self.assertEqual(TEXT, json.loads(message.body))

        logger = logging.getLogger("test_varys")
        self.assertEqual(len(logger.handlers), 1)

    def test_manual_ack(self):
        self.v.auto_ack = False

        self.v.send(TEXT, "test_varys", queue_suffix="q")

        message = self.v.receive("test_varys", queue_suffix="q")

        self.v.acknowledge_message(message)

    def test_nack(self):
        self.v.auto_ack = False

        self.v.send(TEXT, "test_varys", queue_suffix="q")

        message = self.v.receive("test_varys", queue_suffix="q")

        self.v.nack_message(message)

        # check that the message has been requeued
        message_2 = self.v.receive("test_varys", queue_suffix="q")

        self.v.acknowledge_message(message_2)

        self.assertEqual(message.body, message_2.body)

    def test_send_and_receive_batch(self):
        self.v.send(TEXT, "test_varys", queue_suffix="q")
        self.v.send(TEXT, "test_varys", queue_suffix="q")
        messages = self.v.receive_batch("test_varys", queue_suffix="q")
        parsed_messages = [json.loads(m.body) for m in messages]
        self.assertListEqual([TEXT, TEXT], parsed_messages)

    def test_receive_no_message(self):
        self.assertIsNone(
            self.v.receive("test_varys_no_message", queue_suffix="q", timeout=1)
        )

    def test_send_no_suffix(self):
        self.assertRaises(Exception, self.v.send, TEXT, "test_varys")

    def test_receive_no_suffix(self):
        self.assertRaises(Exception, self.v.receive, "test_varys")

    def test_receive_batch_no_suffix(self):
        self.assertRaises(Exception, self.v.receive_batch, "test_varys")


class TestVarysConfig(unittest.TestCase):
    def tearDown(self):
        os.remove(TMP_FILENAME)

    def test_config_not_json(self):
        with open(TMP_FILENAME, "w") as f:
            f.write("asdf9υ021ζ3;-ö×=()[]{}∇Δοo")

        # use a context manager so we can check SystemExit code
        with self.assertRaises(SystemExit) as cm:
            v = varys("test", LOG_FILENAME, config_path=TMP_FILENAME)

        self.assertEqual(cm.exception.code, 11)

    def test_config_profile_missing(self):
        config = {
            "version": "0.2",  # bad version prints warning but doesn't raise error
            "profiles": {"asdfadsf": {}},
        }

        with open(TMP_FILENAME, "w") as f:
            json.dump(config, f, ensure_ascii=False)

        with self.assertRaises(SystemExit) as cm:
            v = varys("test", LOG_FILENAME, config_path=TMP_FILENAME)

        self.assertEqual(cm.exception.code, 2)

    def test_config_profile_incomplete(self):
        config = {
            "version": "0.1",
            "profiles": {
                "test": {
                    "username": "username",
                    "extra": "unnecessary",
                }
            },
        }

        with open(TMP_FILENAME, "w") as f:
            json.dump(config, f, ensure_ascii=False)

        with self.assertRaises(SystemExit) as cm:
            v = varys("test", LOG_FILENAME, config_path=TMP_FILENAME)

        self.assertEqual(cm.exception.code, 11)
