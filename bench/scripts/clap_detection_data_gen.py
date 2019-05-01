import time
import pika
from datetime import datetime
from defs import *

creds = pika.PlainCredentials('sf-admin', 'buzzword')
conn = pika.BlockingConnection(
    pika.ConnectionParameters(host=EDGE_DNS, credentials=creds))
channel = conn.channel()

channel.exchange_declare(exchange='sf.topic', exchange_type='topic', durable=True)

times = []

routing_key = 'edge.%s.%s' % (TOKEN, SEND_SUFFIX)
scaling_factor = 1
for i in range(30):
    if 10 <= i and i <= 20:
        message = str(scaling_factor *(5 - abs(i - 15)))
    else:
        message = str(0.0)
    times.append(datetime.utcnow())
    channel.basic_publish(exchange='sf.topic',
                          routing_key=routing_key,
                          body=message)
    print("Sent %r:%r" % (routing_key, message))
    time.sleep(0.25)

conn.close()

with open('input.txt', 'w') as f:
    for time in times:
        f.write(time.isoformat())
        f.write('\n')
