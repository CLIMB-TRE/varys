import functools
import pika
from pika import exceptions as pika_exceptions
import time

from varys.utils import varys_message
from varys.process import Process


class Consumer(Process):
    def __init__(
        self,
        message_queue,
        exchange,
        configuration,
        log_file,
        log_level,
        queue_suffix,
        exchange_type,
        prefetch_count,
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

        self._closing = False
        self._prefetch_count = prefetch_count

    def _on_message(self, _unused_channel, basic_deliver, properties, body):
        message = varys_message(basic_deliver, properties, body)
        self._log.info(
            f"Received Message: #{message.basic_deliver.delivery_tag} from {message.properties.app_id}, {message.body}"
        )
        self._message_queue.put(message)

    def _acknowledge_message(self, delivery_tag):
        self._log.info(f"Acknowledging message: {delivery_tag}")
        self._connection.add_callback_threadsafe(
            functools.partial(
                self._channel.basic_ack,
                delivery_tag=delivery_tag,
            )
        )

    def _nack_message(self, delivery_tag, requeue):
        self._log.info(f"Nacking message: {delivery_tag}")
        self._connection.add_callback_threadsafe(
            functools.partial(
                self._channel.basic_nack,
                delivery_tag=delivery_tag,
                multiple=False,
                requeue=requeue,
            )
        )

    def run(self):
        while not self._stopping:
            try:
                # clear the queue, otherwise acking can cause problems on new channel
                while not self._message_queue.empty():
                    self._message_queue.get()

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
                self._channel.basic_qos(prefetch_count=self._prefetch_count)
                self._channel.basic_consume(
                    self._queue, self._on_message, auto_ack=False
                )
                self._channel.start_consuming()
            except Exception as e:
                self._log.exception(f"Consumer caught exception: {e}")

            if self._stopping or self._reconnect_wait < 0:
                break
            else:
                time.sleep(self._reconnect_wait)
                continue

    def stop(self):
        self._log.info("Stopping consumer as instructed...")
        self._stopping = True

        self._connection.add_callback_threadsafe(self._channel.stop_consuming)

        self._connection.add_callback_threadsafe(self._channel.close)

        self._connection.add_callback_threadsafe(self._connection.close)

        self._log.debug("Stopping consumer logger...")
        self._stop_logger()

        self._log.info("Stopped consumer as instructed.")
