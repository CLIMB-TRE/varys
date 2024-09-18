from threading import Thread
from os import path
import ssl
import logging

import pika


class Process(Thread):
    def __init__(
        self,
        message_queue,
        exchange,
        configuration,
        log_file,
        log_level,
        queue_suffix,
        exchange_type,
        routing_key="arbitrary_string",
        reconnect_wait=10,
    ):
        super().__init__()

        self._message_queue = message_queue
        self._routing_key = routing_key
        self._exchange = exchange
        self._queue = exchange + "." + queue_suffix
        self._log_file = path.abspath(log_file)  # so we know which file handle to drop when we stop
        self._setup_logger(log_level)

        self._connection = None
        self._channel = None
        self._stopping = False
        self._reconnect_wait = reconnect_wait

        if exchange_type == "fanout":
            self._exchange_type = pika.exchange_type.ExchangeType.fanout
        elif exchange_type == "topic":
            self._exchange_type = pika.exchange_type.ExchangeType.topic
        elif exchange_type == "direct":
            self._exchange_type = pika.exchange_type.ExchangeType.direct
        elif exchange_type == "headers":
            self._exchange_type = pika.exchange_type.ExchangeType.headers
        else:
            raise ValueError(
                "Exchange type must be one of: fanout, topic, direct, headers"
            )

        if configuration.use_tls:
            context = ssl.create_default_context(
                purpose=ssl.Purpose.SERVER_AUTH,
                cafile=configuration.ca_certificate,
            )
            # default behaviour for Purpose.SERVERAUTH
            context.verify_mode = ssl.CERT_REQUIRED
            context.check_hostname = True

            context.load_cert_chain(configuration.client_certificate,
                                    configuration.client_key)

            ssl_options = pika.SSLOptions(context, configuration.ampq_url)
        else:
            ssl_options = None

        self._parameters = pika.ConnectionParameters(
            host=configuration.ampq_url,
            port=configuration.port,
            credentials=pika.PlainCredentials(
                username=configuration.username, password=configuration.password
            ),
            ssl_options=ssl_options,
        )

    def _setup_logger(self, log_level):
        name = self._exchange
        log_path = self._log_file

        self._log = logging.getLogger(name)
        self._log.propagate = False
        self._log.setLevel(log_level)

        # if the filename is already associated with a handler, increase the count
        try:
            handler_filenames = [fh.baseFilename for fh in self._log.handlers]
            index = handler_filenames.index(log_path)
            self._log.handlers[index].count += 1
        # otherwise create a new filehandler (with initial count 1)
        except ValueError:
            logging_fh = logging.FileHandler(log_path)
            logging_fh.setFormatter(
                logging.Formatter(
                    "%(name)s\t::%(levelname)s::%(asctime)s::\t%(message)s"
                )
            )
            self._log.addHandler(logging_fh)
            self._log.handlers[-1].count = 1

    def _stop_logger(self):
        log_path = self._log_file

        handler_filenames = [fh.baseFilename for fh in self._log.handlers]
        index = handler_filenames.index(log_path)
        self._log.handlers[index].count -= 1

        if self._log.handlers[index].count == 0:
            handler = self._log.handlers.pop(index)
            handler.close()
