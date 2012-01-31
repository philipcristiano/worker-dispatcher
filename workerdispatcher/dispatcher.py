from gevent import monkey; monkey.patch_all()
import gevent
from gevent.pool import Pool
import puka
import time

def callback(queue_name, msg_result):
    print queue_name, " [x] Received %r" % (msg_result['body'],)
    gevent.sleep(1)
    time.sleep( msg_result['body'].count('.') )

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
    client.wait(client.exchange_declare('events', 'direct', True))
    client.wait(client.queue_bind(queue_name, 'events', routing_key='task_queue'))
    print ' [*] Waiting for messages. To exit press CTRL+C'

    consume_promise = client.basic_consume(queue=queue_name, prefetch_count=1)
    pool = Pool(size=10)
    while True:
        msg_result = client.wait(consume_promise)
        pool.spawn(callback, queue_name, msg_result)
        client.basic_ack(msg_result)

greenlet_1 = gevent.spawn(consumer, 'queue_1')
greenlet_2 = gevent.spawn(consumer, 'queue_2')
greenlet_1.join()
