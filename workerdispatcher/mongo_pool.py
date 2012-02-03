import gevent.queue
import pymongo

QUEUE = gevent.queue.Queue()

def put(item,result):
    QUEUE.put((item, result))

def get():
    return QUEUE.get()

def worker():
    conn = pymongo.connection.Connection('33.33.33.10:27017')
    while True:
        item, result = get()
        item(conn)
        result.set('200')

size = 2
pool = gevent.pool.Pool(size)
for i in range(size):
    pool.spawn(worker)



