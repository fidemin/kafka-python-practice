from confluent_kafka import Producer


def callback(err, msg):
    if err is not None:
        print(f'Message deliver failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


p = Producer({'bootstrap.servers': 'localhost:9092'})

for i in range(200000):
    # You need to call poll() at regular intervals to serve the producer's delivery report callbacks
    # Without poll(), all callback is queued until flush method is executed.
    # In case of large # of messages, if the queue is full, 'BufferError: Local: Queue full' can occur.
    # poll(0) is a cheap call if nothing needs to be done. Therefore it is typically put in the producer loop
    # When timeout is not 0, the process is blocked until any callback is returned or timeout is reached

    # Return value of poll() is # of batches sent which is caught by p.poll (Maybe not number of messages...)
    polling_result = p.poll(0)
    if polling_result:
        print(f'Polling result: {polling_result}')
    p.produce('sample-topic', f'hello world {i}', callback=callback)

# flush(): Waiting for all messages are sent
# Should be called for application teardown
# returned value is # of messages not to be sent.
# With the very short timeout, some message could not be sent.
p.flush()
