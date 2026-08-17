"""Broker provisioning for the test suite.

Locally, launches a RabbitMQ container on demand and removes it afterwards.
In CI, where the workflow already provides a broker, sets nothing up and
fails loudly if that broker is missing or degraded.

Modes, via VARYS_TEST_BROKER:
    auto      (default) reuse a running broker if there is one, else launch
    external  use a broker someone else provides; never launch  [CI]
    docker    always launch, don't probe for an existing one
    none      launch nothing; skip every broker-dependent test

Other knobs:
    VARYS_TEST_KEEP_BROKER=1     leave the container up after the run
    VARYS_TEST_BROKER_TIMEOUT    readiness budget in seconds (default 60)
    VARYS_TEST_RABBITMQ_IMAGE    image to run (default rabbitmq:4.2.1)
"""

import atexit
import os

import pytest

from tests import broker

PORT = 5672
TLS_PORT = 5671

_state = {"resolved": False, "launched": False, "skip": None,
          "has_tls": True, "has_definitions": True}


def in_ci():
    return bool(os.getenv("CI") or os.getenv("GITHUB_ACTIONS"))


def _publish_env(certs):
    """Hand the resolved endpoint to tests.helpers via the environment."""
    os.environ.setdefault("VARYS_TEST_HOST", "127.0.0.1")
    os.environ.setdefault("VARYS_TEST_PORT", str(PORT))
    os.environ.setdefault("VARYS_TEST_TLS_PORT", str(TLS_PORT))
    if certs:
        os.environ.setdefault("VARYS_TEST_CERT_DIR", broker.cert_dir())


def _fail_or_skip(reason):
    """Degraded broker: a local inconvenience, but a CI defect.

    Skipping in CI would let a broken broker masquerade as a green run.
    """
    if in_ci():
        raise pytest.UsageError(f"broker unavailable in CI: {reason}")
    _state["skip"] = reason


def _resolve():
    if _state["resolved"]:
        return
    _state["resolved"] = True

    mode = os.getenv("VARYS_TEST_BROKER", "auto").lower()
    if mode not in {"auto", "external", "docker", "none"}:
        raise pytest.UsageError(
            f"VARYS_TEST_BROKER={mode!r}; expected auto, external, docker or none"
        )

    if mode == "none":
        _state["skip"] = "VARYS_TEST_BROKER=none"
        return

    certs = None
    if broker._certs_usable(broker.cert_dir()):
        certs = ensure_cert_paths()

    if mode == "external":
        error = broker.probe_amqp("127.0.0.1", PORT)
        if error is not None:
            # never a skip: external mode means someone promised us a broker
            raise pytest.UsageError(
                f"VARYS_TEST_BROKER=external but no broker on 127.0.0.1:{PORT} "
                f"({error!r})"
            )
        _state["has_tls"], _state["has_definitions"] = broker.capabilities(
            PORT, TLS_PORT, certs
        )
        _publish_env(certs)
        return

    if mode == "auto" and broker.probe_amqp("127.0.0.1", PORT) is None:
        # reuse whatever is already there, but only run what it can support
        _state["has_tls"], _state["has_definitions"] = broker.capabilities(
            PORT, TLS_PORT, certs
        )
        _publish_env(certs)
        return

    if not broker.docker_available():
        owner = broker.port_owner(PORT)
        detail = f"; note {owner}" if owner else ""
        _fail_or_skip(f"docker is not available and no broker is running{detail}")
        return

    if mode == "auto":
        owner = broker.port_owner(PORT)
        if owner:
            # something answers TCP but not AMQP as guest/guest -- publishing
            # our container onto that port would fail confusingly
            _fail_or_skip(
                f"{owner} but it did not complete an AMQP handshake as "
                "guest/guest; stop it or set VARYS_TEST_BROKER=external"
            )
            return

    certs = broker.launch(port=PORT, tls_port=TLS_PORT)
    _state["launched"] = True
    atexit.register(_teardown)
    # a broker we chose to launch and that then failed is an error everywhere
    broker.wait_ready(port=PORT, tls_port=TLS_PORT, certs=certs)
    _publish_env(certs)


def ensure_cert_paths():
    return broker.ensure_certs()


def _teardown():
    if not _state["launched"]:
        return
    if os.getenv("VARYS_TEST_KEEP_BROKER"):
        return
    broker.reap()
    _state["launched"] = False


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config, items):
    """Provision the broker only if a broker-dependent test was selected.

    Doing this here rather than in a fixture means `pytest -m "not broker"`
    never touches docker at all, and that skip reasons appear against the
    individual tests.

    trylast is required: pytest applies `-m` deselection in its own
    implementation of this hook, so running earlier would still see the
    broker-marked items and provision a broker nobody asked for.
    """
    needs_broker = [i for i in items if i.get_closest_marker("broker")]
    if not needs_broker:
        return

    _resolve()

    if _state["skip"]:
        mark = pytest.mark.skip(reason=_state["skip"])
        for item in needs_broker:
            item.add_marker(mark)
        return

    if not _state["has_tls"]:
        mark = pytest.mark.skip(reason="broker has no usable TLS listener on 5671")
        for item in needs_broker:
            if item.get_closest_marker("tls"):
                item.add_marker(mark)

    if not _state["has_definitions"]:
        mark = pytest.mark.skip(
            reason="broker lacks the guest2 user from .rabbitmq/definitions.json"
        )
        for item in needs_broker:
            if item.get_closest_marker("permissions"):
                item.add_marker(mark)


def pytest_unconfigure(config):
    _teardown()
