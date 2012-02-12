from gevent import monkey; monkey.patch_all()

import time

from gevent.pool import Pool
import gevent
import puka
import pymongo

from mongo_pool import put as mongo_put


def insert(data):
    result = gevent.event.AsyncResult()

    def func(connection):
        connection.test_db.task_queue.insert(data, safe=True)

    mongo_put(func, result)
    result.get()

def callback(queue_name, msg_result):
    insert({'body': msg_result['body']})
    print queue_name, " [x] Received %r" % (msg_result['body'],)
    return msg_result

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

            def finish(glet):
                ack_queue.put(messages[glet])

            greenlet.link(finish)
        #client.basic_ack(msg_result)
        for _ in range(ack_queue.qsize()):
            client.basic_ack(ack_queue.get())

greenlet_1 = gevent.spawn(consumer, 'queue_1')
greenlet_2 = gevent.spawn(consumer, 'queue_2')
greenlet_1.join()
