from gevent import monkey; monkey.patch_socket()
import gevent
import puka
import time


def consumer():
    """Spawn a new consumer"""
    client = puka.Client("amqp://guest:guest@33.33.33.10:5672/")
    promise = client.connect()
    print 'connecting'
    time.sleep(1)
    client.wait(promise)
    promise = client.queue_declare(queue='task_queue', durable=True)
    client.wait(promise)
    print ' [*] Waiting for messages. To exit press CTRL+C'

    consume_promise = client.basic_consume(queue='task_queue', prefetch_count=1)
    while True:
        msg_result = client.wait(consume_promise)
        print " [x] Received %r" % (msg_result['body'],)
        time.sleep( msg_result['body'].count('.') )
        print " [x] Done"
        client.basic_ack(msg_result)


consumer()
