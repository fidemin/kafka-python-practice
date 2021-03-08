from confluent_kafka import Producer


def callback(err, msg):
    if err is not None:
        print(f'Message deliver failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


p = Producer({
    'bootstrap.servers': 'localhost:9092',
    # With linger.ms=0 (default),
    # Kafka producer sends messages immediately, even if there is additional unused space in the buffer.
    'linger.ms': '20',
    # Python producer has different value of default batch size. 
    # Default value in confluent doc is 16384(16KB). 
    # But with batch.size given expliciply with 16384, batch size from callback log is different
    'batch.size': '32768', # 32KB
    'compression.type': 'snappy'
})


for i in range(100000):
    # You need to call poll() at regular intervals to serve the producer's delivery report callbacks.
    # Without poll(), all callback is queued (internal Queue) until flush method is executed.
    # In case of large # of messages, the queue can be full, 'BufferError: Local: Queue full' can occur.
    # poll(0) is a cheap call if nothing needs to be done. Therefore it is typically put in the producer loop
    # When timeout is not 0 (e.g. poll(10)), the process is blocked until any callback is returned or timeout is reached

    # Return value of poll() is # of batches sent which is caught by p.poll (Maybe not number of messages...)
    # In case of no key is given,
    # note that in kafka producer with later version, messages are assigned with SAME partition,
    # if a batch of records is not full and has not yet been sent to the broker.
    # https://docs.confluent.io/platform/current/clients/producer.html#concepts
    polling_result = p.poll(0)
    if polling_result:
        print(f'Polling result: {polling_result}')
    p.produce('sample-topic', f'hello world {i}', on_delivery=callback)

# flush(): Waiting for all messages are sent
# Should be called for application teardown
# returned value is # of messages not to be sent.
# With the very short timeout, some message could not be sent.
p.flush()

