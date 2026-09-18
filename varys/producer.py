import functools
import threading
import pika
from pika import exceptions as pika_exceptions
import time
import json

from varys.exceptions import (
    ProducerNotReadyError,
    PublishFailedError,
    PublishTimeoutError,
    VarysPublishError,
)
from varys.process import Process

DEFAULT_READY_TIMEOUT = 30
DEFAULT_CONFIRM_TIMEOUT = 30


class Producer(Process):
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
        super().__init__(
            message_queue,
            exchange,
            configuration,
            log_file,
            log_level,
            queue_suffix,
            exchange_type,
            routing_key=routing_key,
            reconnect_wait=reconnect_wait,
        )

        self._message_number = 0

        # Set once the channel is open, bound and in confirm mode; cleared whenever
        # the connection is lost. Publishing before this is set is the startup race
        # that used to silently drop the first message to each exchange.
        self._ready = threading.Event()

        self._message_properties = pika.BasicProperties(
            content_type="json",
            delivery_mode=pika.DeliveryMode.Persistent,
        )

    def wait_until_ready(self, timeout=DEFAULT_READY_TIMEOUT):
        """Block until this producer can publish.

        Returns True once the channel is open, bound to the exchange and in confirm
        mode, or False if that has not happened within timeout seconds.
        """
        return self._ready.wait(timeout)

    def _publish_and_confirm(self, message_str, timeout):
        """Publish on the connection's I/O thread and block until the broker answers.

        The channel is in confirm mode and we publish with mandatory=True, so
        basic_publish raises UnroutableError or NackError instead of returning. Those
        are raised on the I/O thread, where nothing can catch them on the caller's
        behalf, so capture the outcome there and re-raise it on the calling thread.
        """
        outcome = {}
        completed = threading.Event()

        def _publish():
            try:
                self._channel.basic_publish(
                    self._exchange,
                    self._routing_key,
                    message_str,
                    self._message_properties,
                    mandatory=True,
                )
            except BaseException as e:
                outcome["error"] = e
            finally:
                completed.set()

        self._connection.add_callback_threadsafe(_publish)

        if not completed.wait(timeout):
            raise PublishTimeoutError(
                f"No confirmation from the broker within {timeout}s when publishing to "
                f"exchange {self._exchange}; the message may or may not have been delivered"
            )

        error = outcome.get("error")
        if error is not None:
            raise error

    def publish_message(
        self,
        message,
        max_attempts=3,
        ready_timeout=DEFAULT_READY_TIMEOUT,
        confirm_timeout=DEFAULT_CONFIRM_TIMEOUT,
    ):
        """Publish a message, blocking until the broker has confirmed it.

        Returns only once the broker holds the message, and raises VarysPublishError
        otherwise, so a caller consuming from another queue can safely acknowledge its
        inbound message after this returns.
        """
        try:
            message_str = json.dumps(message, ensure_ascii=False)
        except TypeError:
            self._log.exception(f"Unable to serialise message into json: {str(message)}")
            raise

        last_error = None

        for attempt in range(1, max_attempts + 1):
            if not self._ready.wait(ready_timeout):
                self._log.warning(
                    f"Connection is not ready, cannot publish message (attempt "
                    f"{attempt}/{max_attempts})"
                )
                last_error = ProducerNotReadyError(
                    f"Producer for exchange {self._exchange} was not ready to publish "
                    f"within {ready_timeout}s"
                )
            else:
                try:
                    self._log.info(
                        f"Sending message (attempt {attempt}/{max_attempts}): {message_str}"
                    )
                    self._publish_and_confirm(message_str, confirm_timeout)
                except Exception as e:
                    self._log.exception(
                        f"Exception while trying to publish message on attempt "
                        f"{attempt}/{max_attempts}!:"
                    )
                    last_error = e
                else:
                    self._message_number += 1
                    self._log.info(f"Published message #{self._message_number}")
                    return

            if attempt < max_attempts and self._reconnect_wait > 0:
                time.sleep(self._reconnect_wait)

        self._log.error(
            f"Failed to publish message to exchange {self._exchange} after "
            f"{max_attempts} attempt(s), giving up: {last_error}"
        )

        if isinstance(last_error, VarysPublishError):
            raise last_error

        raise PublishFailedError(
            f"Failed to publish message to exchange {self._exchange} after "
            f"{max_attempts} attempt(s)"
        ) from last_error

    def run(self):
        while not self._stopping:
            try:
                self._connection = pika.BlockingConnection(self._parameters)
                self._channel = self._connection.channel()
                try:
                    self._channel.exchange_declare(
                        exchange=self._exchange,
                        exchange_type=self._exchange_type,
                        durable=True,
                        passive=True,
                    )
                except pika_exceptions.ChannelClosed as e:
                    if e.reply_code != 404:
                        raise

                    self._log.info(
                        f"Exchange {self._exchange} does not exist, creating it..."
                    )
                    self._channel = self._connection.channel()
                    self._channel.exchange_declare(
                        exchange=self._exchange,
                        exchange_type=self._exchange_type,
                        durable=True,
                    )

                try:
                    self._channel.queue_declare(
                        queue=self._queue, durable=True, passive=True
                    )
                except pika_exceptions.ChannelClosed as e:
                    if e.reply_code != 404:
                        raise

                    self._log.info(
                        f"Queue {self._queue} does not exist, creating it..."
                    )
                    self._channel = self._connection.channel()
                    self._channel.queue_declare(queue=self._queue, durable=True)

                self._channel.queue_bind(
                    queue=self._queue,
                    exchange=self._exchange,
                    routing_key=self._routing_key,
                )
                self._channel.confirm_delivery()

                # Everything a publish needs is now in place, so let waiting callers go
                self._ready.set()

                # time_limit=None leads to the connection being dropped for inactivity
                # not sure if this should be while not self._stopping
                # while true:
                while not self._stopping:
                    self._connection.process_data_events(time_limit=1)
            except Exception:
                self._log.exception("Producer caught exception:")
            finally:
                # Whatever took us out of the loop, this connection can no longer
                # publish; block callers until it has been re-established
                self._ready.clear()

            if self._stopping:
                self._connection.process_data_events(time_limit=0)
                break
            elif self._reconnect_wait < 0:
                # connection has been broken but we don't want to reconnect
                # so there's no connection with data events to process
                break
            else:
                time.sleep(self._reconnect_wait)
                continue

    def stop(self):
        self._log.info("Stopping producer as instructed...")
        # probably have to say we're closing so run doesn't try to reopen connection
        self._stopping = True
        self._ready.clear()

        try:
            self._connection.add_callback_threadsafe(
                functools.partial(self._connection.process_data_events, time_limit=3)
            )

            self._connection.add_callback_threadsafe(self._channel.close)
            self._connection.add_callback_threadsafe(self._connection.close)
        finally:
            self._log.debug("Stopping producer logger...")
            self._stop_logger()

            self._log.info("Stopped producer as instructed.")
