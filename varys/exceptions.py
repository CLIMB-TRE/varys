class VarysPublishError(Exception):
    """Base class for publish failures raised to the caller of Varys.send()."""


class ProducerNotReadyError(VarysPublishError):
    """The producer had no open, confirm-mode channel in time to publish."""


class PublishFailedError(VarysPublishError):
    """The broker rejected the message, or it could not be routed to a queue."""


class PublishTimeoutError(VarysPublishError):
    """No publisher confirmation arrived within the timeout.

    The message may still have reached the broker, so this outcome is
    indeterminate rather than a definite failure.
    """
