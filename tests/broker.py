"""RabbitMQ test-broker lifecycle: certificates, container, readiness probes.

Deliberately free of pytest imports so it can be driven from a script or from
CI as easily as from conftest.py.

Two behaviours here are non-obvious and both are load-bearing:

* Readiness and detection complete a full AMQP handshake. A TCP connect is not
  enough -- under Colima (and Docker Desktop) the port forwarder accepts
  connections on a published port even when nothing inside the container is
  listening on it, so `connect_ex` reports success against a broker with no
  TLS listener.
* The certificates must satisfy Python's stricter chain validation, not just
  `openssl s_client`. See `ensure_certs`.
"""

import os
import shutil
import socket
import ssl
import subprocess
import time

import pika
from pika import exceptions as pika_exceptions

DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(DIR)

CONTAINER_NAME = "varys-test-broker"
DEFAULT_IMAGE = "rabbitmq:4.2.1"
CERT_DIR = os.path.join(REPO_ROOT, ".rabbitmq")

# CI's tls-gen `mv` steps produce exactly these names, and the committed
# rabbitmq.conf refers to them, so keep them identical.
CA_CERT = "ca_certificate.pem"
SERVER_CERT = "server_localhost_certificate.pem"
SERVER_KEY = "server_localhost_key.pem"
CLIENT_CERT = "client_certificate.pem"
CLIENT_KEY = "client_key.pem"
CERT_FILES = (CA_CERT, SERVER_CERT, SERVER_KEY, CLIENT_CERT, CLIENT_KEY)

RENEW_WITHIN_DAYS = 30
CERT_LIFETIME_DAYS = 365


def image():
    return os.getenv("VARYS_TEST_RABBITMQ_IMAGE", DEFAULT_IMAGE)


def cert_dir():
    return os.path.abspath(os.getenv("VARYS_TEST_CERT_DIR", CERT_DIR))


def cert_path(name):
    return os.path.join(cert_dir(), name)


# --------------------------------------------------------------------------
# certificates
# --------------------------------------------------------------------------


def _openssl(*args, stdin=None):
    return subprocess.run(
        ("openssl",) + args,
        input=stdin,
        capture_output=True,
        text=True,
        check=True,
    )


def _certs_usable(directory):
    """True if every cert exists and none expires within RENEW_WITHIN_DAYS."""
    for name in CERT_FILES:
        if not os.path.exists(os.path.join(directory, name)):
            return False

    horizon = RENEW_WITHIN_DAYS * 24 * 60 * 60
    for name in (CA_CERT, SERVER_CERT, CLIENT_CERT):
        try:
            _openssl(
                "x509",
                "-in",
                os.path.join(directory, name),
                "-noout",
                "-checkend",
                str(horizon),
            )
        except subprocess.CalledProcessError:
            return False
    return True


def ensure_certs(directory=None, force=False):
    """Generate a CA plus server and client leaf certs, idempotently.

    Two extensions are mandatory rather than cosmetic:

    * The CA must carry `keyUsage=critical,digitalSignature,cRLSign,keyCertSign`.
      Without it Python 3.13 / OpenSSL 3.6 rejects the chain with
      "CA cert does not include key usage extension", even though
      `openssl s_client` validates it happily -- so never verify these certs
      with s_client alone.
    * The server leaf needs `subjectAltName=DNS:localhost,IP:127.0.0.1`,
      because varys sets `check_hostname = True` and passes the configured
      `amqp_url` as the SNI name (varys/process.py), and the suite uses both
      spellings of the loopback address.

    Returns a dict of absolute paths.
    """
    directory = os.path.abspath(directory or cert_dir())
    os.makedirs(directory, exist_ok=True)

    paths = {
        "ca_certificate": os.path.join(directory, CA_CERT),
        "server_certificate": os.path.join(directory, SERVER_CERT),
        "server_key": os.path.join(directory, SERVER_KEY),
        "client_certificate": os.path.join(directory, CLIENT_CERT),
        "client_key": os.path.join(directory, CLIENT_KEY),
    }

    if not force and _certs_usable(directory):
        return paths

    if shutil.which("openssl") is None:
        raise BrokerError(
            "openssl not found on PATH; cannot generate test certificates. "
            "Provide certs in .rabbitmq/ manually, or run with "
            "VARYS_TEST_BROKER=none to skip broker tests."
        )

    ca_key = os.path.join(directory, "ca_key.pem")
    days = str(CERT_LIFETIME_DAYS)

    # certificate authority
    _openssl("genrsa", "-out", ca_key, "2048")
    _openssl(
        "req",
        "-x509",
        "-new",
        "-key",
        ca_key,
        "-days",
        days,
        "-sha256",
        "-subj",
        "/CN=varys-test-ca",
        "-addext",
        "basicConstraints=critical,CA:TRUE",
        # required by Python's chain validation; see docstring
        "-addext",
        "keyUsage=critical,digitalSignature,cRLSign,keyCertSign",
        "-out",
        paths["ca_certificate"],
    )

    for kind, cert, key, extra in (
        (
            "server",
            paths["server_certificate"],
            paths["server_key"],
            (
                "subjectAltName=DNS:localhost,IP:127.0.0.1",
                "extendedKeyUsage=serverAuth",
            ),
        ),
        (
            "client",
            paths["client_certificate"],
            paths["client_key"],
            (
                "subjectAltName=DNS:localhost,IP:127.0.0.1",
                "extendedKeyUsage=clientAuth",
            ),
        ),
    ):
        csr = os.path.join(directory, f"{kind}.csr")
        ext_file = os.path.join(directory, f"{kind}.ext")
        _openssl("genrsa", "-out", key, "2048")
        _openssl(
            "req",
            "-new",
            "-key",
            key,
            "-subj",
            "/CN=localhost",
            "-out",
            csr,
        )

        # `x509 -req` does not accept -addext (unlike `req -x509`), so leaf
        # extensions have to go through an -extfile.
        extensions = extra + (
            "basicConstraints=CA:FALSE",
            "keyUsage=digitalSignature,keyEncipherment",
        )
        with open(ext_file, "w") as f:
            f.write("\n".join(extensions) + "\n")

        _openssl(
            "x509",
            "-req",
            "-in",
            csr,
            "-CA",
            paths["ca_certificate"],
            "-CAkey",
            ca_key,
            "-CAcreateserial",
            "-days",
            days,
            "-sha256",
            "-extfile",
            ext_file,
            "-out",
            cert,
        )
        os.remove(csr)
        os.remove(ext_file)

    # the broker runs as a different uid inside the container
    for path in paths.values():
        os.chmod(path, 0o644)

    return paths


# --------------------------------------------------------------------------
# probes
# --------------------------------------------------------------------------


class BrokerError(RuntimeError):
    pass


def _tls_options(paths, host):
    context = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH, cafile=paths["ca_certificate"]
    )
    context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = True
    context.load_cert_chain(paths["client_certificate"], paths["client_key"])
    return pika.SSLOptions(context, host)


def probe_amqp(host, port, username="guest", password="guest", tls_paths=None,
               timeout=2.0):
    """Complete an AMQP (optionally TLS) handshake. Returns None on success,
    else the exception, so callers can report *why* a probe failed.

    Never use a bare TCP connect for this: container port forwarders accept
    connections for published ports with no listener behind them.
    """
    parameters = pika.ConnectionParameters(
        host=host,
        port=port,
        credentials=pika.PlainCredentials(username, password),
        ssl_options=_tls_options(tls_paths, host) if tls_paths else None,
        socket_timeout=timeout,
        blocked_connection_timeout=timeout,
        connection_attempts=1,
        retry_delay=0,
    )
    try:
        connection = pika.BlockingConnection(parameters)
    except Exception as e:  # pika raises a wide variety here
        return e
    else:
        try:
            connection.close()
        except Exception:
            pass
        return None


def port_owner(port):
    """Best-effort description of whatever holds `port`, for error messages."""
    with socket.socket() as probe:
        probe.settimeout(0.5)
        try:
            probe.connect(("127.0.0.1", port))
        except OSError:
            return None
    return f"something is listening on 127.0.0.1:{port}"


# --------------------------------------------------------------------------
# container lifecycle
# --------------------------------------------------------------------------


def _docker(*args, check=False):
    return subprocess.run(
        ("docker",) + args, capture_output=True, text=True, check=check
    )


def docker_available():
    if shutil.which("docker") is None:
        return False
    return _docker("version").returncode == 0


def is_running(name=CONTAINER_NAME):
    result = _docker("inspect", "-f", "{{.State.Running}}", name)
    return result.returncode == 0 and result.stdout.strip() == "true"


def logs(name=CONTAINER_NAME, tail=40):
    result = _docker("logs", "--tail", str(tail), name)
    return (result.stdout + result.stderr).strip()


def reap(name=CONTAINER_NAME):
    """Remove any container with our name, running or not.

    Called before every launch, which is what makes a crashed previous run
    self-healing. Deliberately not `docker run --rm`: that would destroy the
    logs needed to diagnose a broker that dies during startup.
    """
    _docker("rm", "-f", name)


def launch(port=5672, tls_port=5671, name=CONTAINER_NAME, certs=None):
    """Start the broker container. Returns the cert paths in use."""
    certs = certs or ensure_certs()
    reap(name)

    result = _docker(
        "run",
        "-d",
        "--name",
        name,
        "-v",
        f"{cert_dir()}:/.rabbitmq",
        "-e",
        "RABBITMQ_CONFIG_FILE=/.rabbitmq/rabbitmq.conf",
        "-p",
        f"{tls_port}:5671",
        "-p",
        f"{port}:5672",
        image(),
    )
    if result.returncode != 0:
        raise BrokerError(
            f"failed to start {image()}:\n{result.stderr.strip()}"
        )

    return certs


def wait_ready(port=5672, tls_port=5671, certs=None, name=CONTAINER_NAME,
               budget=None):
    """Block until the broker serves plain AMQP, TLS, and the guest2 user.

    All three are checked because a bind mount that silently fails to land
    (Colima only shares $HOME by default) yields a broker that boots with no
    config at all: no TLS listener and no definitions, yet plain AMQP works.
    Checking only 5672 would call that success.
    """
    budget = budget or float(os.getenv("VARYS_TEST_BROKER_TIMEOUT", "60"))
    deadline = time.monotonic() + budget
    last = {}

    while time.monotonic() < deadline:
        if not is_running(name):
            raise BrokerError(
                f"broker container died during startup.\n\n{logs(name)}"
            )

        checks = [
            ("amqp", lambda: probe_amqp("127.0.0.1", port)),
            # 127.0.0.1 rather than localhost: with a dual A/AAAA name pika
            # reports cert-verification failures as ConnectionRefusedError,
            # which hides the real cause.
            (
                "tls",
                lambda: probe_amqp("127.0.0.1", tls_port, tls_paths=certs),
            ),
            (
                "definitions (guest2)",
                lambda: probe_amqp("127.0.0.1", port, username="guest2"),
            ),
        ]

        last = {}
        for label, check in checks:
            error = check()
            if error is not None:
                last[label] = error
                break

        if not last:
            return

        time.sleep(0.5)

    failed = ", ".join(f"{k}: {v!r}" for k, v in last.items()) or "unknown"
    raise BrokerError(
        f"broker not ready within {budget:.0f}s ({failed}).\n\n{logs(name)}"
    )


def capabilities(port=5672, tls_port=5671, certs=None):
    """Probe an already-running broker we did not start.

    Returns (has_tls, has_definitions) so the caller can skip only the test
    classes whose requirements are actually missing.
    """
    has_tls = False
    if certs and all(os.path.exists(p) for p in certs.values()):
        has_tls = probe_amqp("127.0.0.1", tls_port, tls_paths=certs) is None

    has_definitions = probe_amqp("127.0.0.1", port, username="guest2") is None

    return has_tls, has_definitions
