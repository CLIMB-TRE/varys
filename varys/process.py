from threading import Thread
from functools import partial
import logging

import pika


class Process(Thread):
    def __init__(
        self,
        exchange,
        log_file,
        log_level,
        queue_suffix,
        exchange_type,
    ):
        super().__init__()

        Thread.daemon = True

        self._exchange = exchange
        self._queue = exchange + "." + queue_suffix
        self._log_file = log_file  # so we know which file handle to drop when we stop
        self._setup_logger(log_level)

        self._connection = None
        self._channel = None

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
            self._log.handlers.pop(index)

    def _connect(self):
        self._log.info("Connecting to broker")
        return pika.SelectConnection(
            self._parameters,
            on_open_callback=self._on_connection_open,
            on_open_error_callback=self._on_connection_open_error,
            on_close_callback=self._on_connection_closed,
        )

    def _on_connection_open(self, _unused_connection):
        self._log.info("Successfully opened connection to broker")
        self._open_channel()

    def _open_channel(self):
        self._log.info("Creating a new channel")
        self._connection.channel(on_open_callback=self._on_channel_open)

    def _on_channel_open(self, channel):
        self._log.info("Channel successfully opened")
        self._channel = channel
        self._add_on_channel_close_callback()
        self._setup_exchange(self._exchange)

    def _add_on_channel_close_callback(self):
        self._log.info("Adding channel close callback")
        self._channel.add_on_close_callback(self._on_channel_closed)

    def _setup_exchange(self, exchange_name):
        self._log.info(f"Declaring exchange: {exchange_name}")
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self._exchange_type,
            callback=self._on_declare_exchangeok,
            durable=True,
        )

    def _nack_message(self, delivery_tag):
        self._log.info(f"Nacking message: {delivery_tag}")
        self._channel.basic_nack(
            delivery_tag=delivery_tag,
            multiple=False,
            requeue=True,
        )

    def _on_declare_exchangeok(self, _unused_frame):
        self._log.info("Exchange declared")
        self._setup_queue(self._queue)

    def _setup_queue(self, queue_name):
        self._log.info(f"Declaring queue: {queue_name}")
        q_callback = partial(self._on_queue_declareok, queue_name=queue_name)
        self._channel.queue_declare(
            queue=queue_name,
            callback=q_callback,
            durable=True,
        )

    def _on_queue_declareok(self, _unused_frame, queue_name):
        self._log.info(
            f"Binding queue {queue_name} to exchange: {self._exchange} with routing key {self._routing_key}"
        )
        self._channel.queue_bind(
            queue_name,
            self._exchange,
            routing_key=self._routing_key,
            callback=self._on_bindok,
        )
