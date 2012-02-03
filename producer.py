#!/usr/bin/env python
from gevent import monkey; monkey.patch_all()
import gevent
import puka
import time

def producer():
    client = puka.Client("amqp://guest:guest@33.33.33.10:5672", True)
    promise = client.connect()
    time.sleep(.1)
    client.wait(promise)

    count = 0
    while True:
        promise = client.basic_publish(exchange='events',
                                       routing_key='task_queue',
                                       body="Hello World! {0}".format(count))
        client.wait(promise)
        time.sleep(.01)
        count += 1
        print count

    client.close()

greenlet_1 = gevent.spawn(producer)
greenlet_1.join()
