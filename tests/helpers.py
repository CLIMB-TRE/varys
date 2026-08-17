"""Shared helpers for the varys test suite.

Importing this module must not open a broker connection, so that the
broker-free test modules (e.g. test_api_signature.py) can import it.
"""

import contextlib
import json
import logging
import os

import pika

from varys.consumer import Consumer

# Read lazily rather than at import time: conftest.py publishes these once it
# has resolved (or launched) the broker, which happens after collection.
DEFAULT_CERT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".rabbitmq"
)


def host():
    return os.getenv("VARYS_TEST_HOST", "127.0.0.1")


def port(tls=False):
    if tls:
        return int(os.getenv("VARYS_TEST_TLS_PORT", "5671"))
    return int(os.getenv("VARYS_TEST_PORT", "5672"))


def cert_path(name):
    """Absolute, so tests work regardless of the working directory."""
    directory = os.path.abspath(os.getenv("VARYS_TEST_CERT_DIR", DEFAULT_CERT_DIR))
    return os.path.join(directory, name)


def write_config(path, username="guest", tls=False, profiles=None):
    """Write a varys configuration JSON to `path` and return the config dict.

    `profiles` overrides the default single "test" profile, as a mapping of
    profile name to username.
    """

    def profile_for(user):
        entry = {
            "username": user,
            "password": "guest",
            # `localhost` is a dual A/AAAA name; the certs carry SANs for both
            # spellings, so either works. 127.0.0.1 keeps TLS errors legible.
            "amqp_url": "localhost" if tls else host(),
            "port": port(tls=tls),
            "use_tls": tls,
        }
        if tls:
            entry.update(
                {
                    "ca_certificate": cert_path("ca_certificate.pem"),
                    "client_certificate": cert_path("client_certificate.pem"),
                    "client_key": cert_path("client_key.pem"),
                }
            )
        else:
            entry["ca_certificate"] = "this-value-shouldn't-matter"
        return entry

    if profiles is None:
        profiles = {"test": username}

    config = {
        "version": "0.1",
        "profiles": {name: profile_for(user) for name, user in profiles.items()},
    }

    with open(path, "w") as f:
        json.dump(config, f, ensure_ascii=False)

    return config


@contextlib.contextmanager
def admin_channel():
    """Yield a plain `guest` channel for raw declare/bind/delete operations."""
    credentials = pika.PlainCredentials("guest", "guest")
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host(), port=port(), credentials=credentials)
    )
    try:
        yield connection.channel()
    finally:
        connection.close()


def file_handler_count(logger):
    """Count only the FileHandlers varys itself manages, ignoring any
    handlers other tools (e.g. pytest's log capture) attach to the same
    non-propagating logger."""
    return sum(1 for h in logger.handlers if isinstance(h, logging.FileHandler))


def varys_file_handlers(exchange):
    """Return the varys-managed FileHandlers on the logger for `exchange`."""
    logger = logging.getLogger(exchange)
    return [h for h in logger.handlers if isinstance(h, logging.FileHandler)]


def purge_exchange_logger(exchange):
    """Force-detach and close any varys FileHandler left on `exchange`'s logger.

    varys' handler refcount lives on the process-wide logger registry, so a
    leaked handler in one test would otherwise poison refcount assertions in
    every later test in the same pytest process. tearDown only.
    """
    logger = logging.getLogger(exchange)
    for handler in varys_file_handlers(exchange):
        logger.handlers.remove(handler)
        handler.close()


def reset_broker_objects(*exchanges_and_queues):
    """Delete the named exchanges and queues, ignoring any that don't exist.

    CI reuses one broker for the whole session, so tests that assert on
    absence must clean up after themselves. Each name is deleted as both an
    exchange and a queue; a fresh channel per attempt because the broker
    closes the channel on a failed operation.
    """
    for name in exchanges_and_queues:
        for delete in ("exchange_delete", "queue_delete"):
            with contextlib.suppress(Exception):
                with admin_channel() as channel:
                    getattr(channel, delete)(name)


def make_temp_consumer(varys_instance, exchange, queue_suffix):
    """Build a Consumer with exactly the arguments `Varys.try_receive` uses,
    but return the instance so tests can assert on its post-call state.

    `try_receive` discards its Consumer, which makes the connection/channel
    lifecycle unobservable through the public API.
    """
    import queue as queue_module

    return Consumer(
        message_queue=queue_module.Queue(),
        routing_key=varys_instance.routing_key,
        exchange=exchange,
        configuration=varys_instance._credentials,
        log_file=varys_instance._logfile,
        log_level=varys_instance._log_level,
        queue_suffix=queue_suffix,
        exchange_type="fanout",
        prefetch_count=0,
        reconnect_wait=10,
    )
