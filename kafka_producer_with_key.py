from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import IntegerSerializer, StringSerializer


def callback(err, msg):
    if err is not None:
        print(f'Message deliver failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


p = SerializingProducer({
    'bootstrap.servers': 'localhost:9092',
    'key.serializer': IntegerSerializer(),
    'value.serializer': StringSerializer()
})


for i in range(100):
    polling_result = p.poll(0)
    if polling_result:
        print(f'Polling result: {polling_result}')
    p.produce('sample-topic', key=i, value=f'hello world {i}', on_delivery=callback)

p.flush()

