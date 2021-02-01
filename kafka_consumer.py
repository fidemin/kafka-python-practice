from confluent_kafka import DeserializingConsumer, Message
from confluent_kafka.serialization import IntegerDeserializer


def consume(consumer: DeserializingConsumer, timeout) -> iter:
    while True:
        # Waiting for message until timeout reached if there is no message.
        # If message exists, message will be returned.
        message = consumer.poll(timeout)
        # print('[kafka] polling...')
        if message is None:
            continue
        if message.error():
            print('Consumer error: {}'.format(message.error()))
            continue
        yield message


if __name__ == '__main__':
    c = DeserializingConsumer(
        {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'my-consumer-group',
            'auto.offset.reset': 'earliest',
            'key.deserializer': IntegerDeserializer()
        }
    )
    c.subscribe(topics=['sample-topic'])
    try:
        for msg in consume(c, timeout=1.0):
            print(f'{msg.key()},{msg.value().decode("utf-8")}')
    finally:
        print('Consumer will be closed')
        c.close()

