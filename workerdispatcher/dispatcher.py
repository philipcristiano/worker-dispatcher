from gevent import monkey; monkey.patch_all()

import time

from gevent.pool import Pool
import gevent
import puka
import pymongo

from mongo_pool import put as mongo_put


PUBLISH_QUEUE = gevent.queue.Queue()

def insert(data):
    result = gevent.event.AsyncResult()

    def func(connection):
        connection.test_db.task_queue.insert(data, safe=True)

    mongo_put(func, result)
    result.get()

def publish(*args, **kwargs):
    result = gevent.event.AsyncResult()
    PUBLISH_QUEUE.put((args, kwargs, result))
    return result.get()

def handle_publish_queue_items(client):
    while True:
        for _ in range(PUBLISH_QUEUE.qsize()):
            try:
                args, kwargs, async_result = PUBLISH_QUEUE.get(timeout=.1)
            except gevent.queue.Empty:
                continue

            def callback(promise, result):
                async_result.set(result)

            promise=client.basic_publish(callback=callback, *args, **kwargs)
        client.loop(.001)
        #promise = client.basic_publish(*args, **kwargs)
        #result = client.wait(promise)
        #async_result.set(result)

def callback(queue_name, msg_result):
    insert({'body': msg_result['body']})
    publish(exchange='', routing_key='results', body=msg_result['body'])
    print queue_name, " [x] Received %r" % (msg_result['body'],)
    return msg_result

def publisher():
    client = puka.Client("amqp://guest:guest@33.33.33.10:5672/")
    promise = client.connect()
    print 'publisher connecting'
    time.sleep(1)
    client.wait(promise)
    while True:
        handle_publish_queue_items(client)

def build_ack_callback(ack_queue, messages):
    acks = set()
    def finish(glet):
        tag = messages[glet]['delivery_tag']
        assert tag not in acks
        acks.add(tag)
        print messages[glet]
        ack_queue.put(messages[glet])

    return finish

def consumer(queue_name):
    """Spawn a new consumer"""
    client = puka.Client("amqp://guest:guest@33.33.33.10:5672/")
    promise = client.connect()
    print 'connecting'
    time.sleep(1)
    client.wait(promise)
    promise = client.queue_declare(
            queue=queue_name,
            durable=True
    )
    client.wait(promise)
    size = 10
    client.wait(client.exchange_declare('events', 'direct', True))
    client.wait(client.queue_bind(queue_name, 'events', routing_key='task_queue'))
    print ' [*] Waiting for messages. To exit press CTRL+C'

    consume_promise = client.basic_consume(queue=queue_name, prefetch_count=size+5)
    pool = Pool(size=size)
    ack_queue = gevent.queue.Queue()
    messages = {}
    while True:
        msg_result = client.wait(consume_promise, timeout=1)
        if msg_result:
            greenlet = pool.spawn(callback, queue_name, msg_result)
            messages[greenlet] = msg_result

            func = build_ack_callback(ack_queue, messages)

            greenlet.link(func)
        for _ in range(ack_queue.qsize()):
            client.basic_ack(ack_queue.get())

greenlet_1 = gevent.spawn(consumer, 'queue_1')
greenlet_2 = gevent.spawn(consumer, 'queue_2')
greenlet_3 = gevent.spawn(consumer, 'queue_3')
greenlet_publisher = gevent.spawn(publisher)
greenlet_1.join()
