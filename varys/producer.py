import pika
import time
import queue
import json

from varys.process import Process


class producer(Process):
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
        sleep_interval=10,
    ):
        # username, password, queue, ampq_url, port, log_file, exchange="", routing_key="default", sleep_interval=5
        super().__init__(
            message_queue,
            routing_key,
            exchange,
            configuration,
            log_file,
            log_level,
            queue_suffix,
            exchange_type,
            sleep_interval=sleep_interval,
        )

        self._deliveries = None
        self._acked = None
        self._nacked = None
        self._message_number = None

        self._stopping = False

        self._message_properties = pika.BasicProperties(
            content_type="json", delivery_mode=2
        )

    def _on_connection_open_error(self, _unused_connection, error):
        self._log.error(
            f"Connection attempt to broker failed, attempting to re-open in {self._sleep_interval} seconds: {error}"
        )
        self._connection.ioloop.stop()
        time.sleep(self._sleep_interval)
        self._connection = self._connect()
        self._connection.ioloop.start()

    def _on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        if self._stopping:
            self._connection.ioloop.stop()
        else:
            self._log.warning(
                f"Connection to broker closed, will attempt to re-connect in 10 seconds: {reason}"
            )
            self._connection.ioloop.call_later(10, self._connection.ioloop.start)

    def _on_channel_closed(self, channel, reason):
        self._log.warning(f"Channel {channel} was closed by broker: {reason}")
        self._channel = None
        if not self._stopping:
            self._connection.close()

    def _on_bindok(self, _unused_frame):
        self._log.info("Queue successfully bound")
        self._start_publishing()

    def _start_publishing(self):
        self._log.info(
            "Issuing consumer delivery confirmation commands and sending first message"
        )
        self._enable_delivery_confirmations()
        self._send_if_queued()

    def _enable_delivery_confirmations(self):
        self._log.info("Issuing Confirm.Select RPC command")
        self._channel.confirm_delivery(self._on_delivery_confirmation)

    def _on_delivery_confirmation(self, method_frame):
        confirmation_type = method_frame.method.NAME.split(".")[1].lower()
        self._log.info(
            f"Received {confirmation_type} for delivery tag: {method_frame.method.delivery_tag}"
        )
        if confirmation_type == "ack":
            self._acked += 1
        elif confirmation_type == "nack":
            self._nacked += 1
        self._deliveries.remove(method_frame.method.delivery_tag)
        self._log.info(
            f"Published {self._message_number} messages, {len(self._deliveries)} have yet to be confirmed, "
            f"{self._acked} were acked, {self._nacked} were nacked"
        )

    def _send_if_queued(self):
        try:
            to_send = self._message_queue.get(block=False)
            self.publish_message(to_send)
        except queue.Empty:
            self._connection.ioloop.call_later(
                self._sleep_interval, self._send_if_queued
            )

    def _close_channel(self):
        if self._channel is not None:
            self._log.info("Closing the channel")
            self._channel.close()

    def _close_connection(self):
        if self._connection is not None:
            self._log.info("Closing connection")
            self._connection.close()

    def publish_message(self, message):
        if self._channel is None or not self._channel.is_open:
            return False

        try:
            message_str = json.dumps(message, ensure_ascii=False)
        except TypeError:
            self._log.error(f"Unable to serialise message into json: {str(message)}")

        self._log.info(f"Sending message: {json.dumps(message)}")
        self._channel.basic_publish(
            self._exchange,
            self._routing_key,
            message_str,
            self._message_properties,
            mandatory=True,
        )

        self._message_number += 1
        self._deliveries.append(self._message_number)
        self._message_queue.task_done()
        self._log.info(f"Published message # {self._message_number}")

        self._send_if_queued()

    def run(self):
        while not self._stopping:
            self._connection = None
            self._deliveries = []
            self._acked = 0
            self._nacked = 0
            self._message_number = 0

            try:
                self._connection = self._connect()
                self._connection.ioloop.start()
            except KeyboardInterrupt:
                self.stop()
                if self._connection is not None and not self._connection.is_closed:
                    # Finish closing
                    self._connection.ioloop.stop()

            return True

    def stop(self):
        self._log.info("Stopping publisher")
        self._stopping = True
        self._close_channel()
        self._close_connection()
        self._stop_logger()
