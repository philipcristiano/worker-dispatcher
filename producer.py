#!/usr/bin/env python
import puka
import time

client = puka.Client("amqp://guest:guest@33.33.33.10:5672", True)
promise = client.connect()
time.sleep(.1)
client.wait(promise)

promise = client.queue_declare(queue='task_queue', durable=True)
client.wait(promise)

count = 0
while True:
    promise = client.basic_publish(exchange='',
                                   routing_key='task_queue',
                                   body="Hello World!")
    client.wait(promise)
    time.sleep(1)
    count += 1
    print count

client.close()
