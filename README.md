![varys logo](varys_logo_scaled.png)

## Varys - A python RabbitMQ client for CLIMB-TRE

![Varys CI Status](https://github.com/CLIMB-TRE/varys/actions/workflows/pytest.yml/badge.svg)

TODO:
* Test SSL support with CA-signed certificates

---
### Installation

Pip or Conda installation will be simplest for most users, and can be achieved with the following commands:

```
pip install varys-client
```
or
```
conda install -c conda-forge varys
```

Alternatively, varys can be installed directly from this repository by doing the following:

```
git clone https://github.com/CLIMB-TRE/varys.git

cd varys

pip install .
```

---

### Configuration

Varys uses a JSON format configuration file to provide credentials to connect to RabbitMQ, the path to this configuration file should either be provided with the `VARYS_CFG` environmental variable or using `config_path` argument when instantiating varys, this will override an environmental variable.

If you wish to use TLS (highly recommended) you should provide a ca certificates file, a client certificate, and a client key to the client, this can be achieved in one of two ways, via the config file or via environmental variables (the config file will always override environmental variables), the environmental variables are as follows:
* `VARYS_CA_CERTIFICATE` -> the ca certificate path
* `VARYS_CLIENT_CERTIFICATE` -> the client certificate
* `VARYS_CLIENT_KEY` -> the client key
These must match the certificates and keys which belong to the server, if you are using this within the context of CLIMB-TRE the certificates / keys will already be present within a jupyter notebook with the appropriate environmental variables pointing to them.

An example of the configuration file format [is available here](example_config.json).

---

### Basic Usage

First the varys object must be instantiated, like so:

```python
from varys import Varys

varys_client = Varys(
    profile="test_user",
    logfile="/var/log/varys_test.log",
    log_level="DEBUG"
)
```

Profile will control which set of credentials the client will read from the config file.

By default Varys will automatically acknowledge received messages instantly however if this is not the desired behaviour then the argument `auto_acknowledge` can be set to `False`, it is important that messages are later acknowledged if this setting is turned off or you risk the RabbitMQ server deciding that you have timed out and closing the connection, see more in the "Message Acknowledgement" section below.

Once the base object has been instantiated you are ready to send or receive messages from the RabbitMQ server:

The `queue_suffix` argument must be provided the first time a message is sent or receeived to/from a queue after varys is instantiated so that the varys may create or bind the queue (`exchange + . + queue_suffix`) if it already exists.

#### Sending
```python
message = {"foo": "bar"}

varys_client.send(message=message,
    exchange="test_exchange",
    queue_suffix="test_suffix"
)
```

Messages must be a python object that can be serialised into JSON format using [json.dumps()](https://docs.python.org/3/library/json.html#json.dumps) which includes the following types: dict, list, tuple, str, int, float, True, False, None.

#### Receiving one message at a time
```python
import json

message = varys_client.receive(exchange="test_exchange",
    queue_suffix="test_suffix",
    block=True,
)

deserialised_message = json.loads(message.body)

print(deserialised_message)
```
This will block execution until a message is received unless the `block` argument is set to `False`, in this case if there are no messages to be received the return value will be `None` object.

If a message is received it will be a `varys_message` object which has the following attributes:

basic_deliver -> A [pika.spec.Basic.Deliver](https://pika.readthedocs.io/en/stable/modules/spec.html#pika.spec.Basic.Deliver)
    Should be irrelevant for normal usage but documentation available at this link.

properties -> A `pika.BasicProperties` object, containing header information about the message if provided when sending. Should be irrelevant for normal usage.

body -> The message body in serialised JSON format, generally a user will wish to convert this to a python object equivalent for ease of use with `json.loads()`

### Message acknowledgement

Where `auto_acknowledge` is set to `False` when instantiating varys, messages must be acknowledged after being received manually like so:

```python

message = varys_client.receive(exchange="test_exchange",
    queue_suffix="test_suffix",
    block=True
)

varys_client.acknowledge_message(message)
```

If you wish to instead reject the message meaning that it will be pushed back onto the first position of the queue, you can `nack` a message:

```python

message = varys_client.receive(exchange="test_exchange",
    queue_suffix="test_suffix",
    block=True
)

varys_client.nack_message(message)
```

### Prefetch count

If `auto_acknowledge` is not set, another useful argument is the `prefetch_count` this allows the user to change the number of messages which will be made available to the user at any given time; when a message is acknowledged another message will be made available to maintain this number so long as there are messages available in the rabbitMQ queue. This setting is particularly useful where messages may be acted on in parallel; for example: if 5 messages can be proccessed at a time then the appropriate `prefetch_count` would be `5`, if 10 messages may be processed in parallel then the appropriate `prefetch_count` would be `10`.

#### Receiving multiple messages at a time
```python
import json

messages = varys_client.receive_batch(exchange="test_exchange",
    queue_suffix="test_suffix",
)


for message in messages:
    deserialised_message = json.loads(message.body)

    print(deserialised_message)
```

This will never block execution and will always return a python list object containing all available messages as `varys_message` objects which should then be iterated through and treated as above. In the case of there being no messages available, this list will be empty and will evaluate to `False`.

