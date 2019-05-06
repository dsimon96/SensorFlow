import time
import pika
from datetime import datetime
from defs import *

creds = pika.PlainCredentials('sf-admin', 'buzzword')
conn = pika.BlockingConnection(
    pika.ConnectionParameters(host=INGRESS_HOST, credentials=creds))
channel = conn.channel()

channel.exchange_declare(exchange='sf.topic', exchange_type='topic', durable=True)

times = []
wait_time = float(1)/TPUT

routing_key = '%s.%s.%s' % (SEND_PREFIX, TOKEN, SEND_SUFFIX)
for i, message in enumerate(MESSAGES):
    times.append(datetime.utcnow())
    channel.basic_publish(exchange='sf.topic',
                          routing_key=routing_key,
                          body=str(message))
    print("Sent %r:%r" % (routing_key, message))
    time.sleep(wait_time)

conn.close()

with open('input.txt', 'w') as f:
    for time in times:
        f.write(time.isoformat())
        f.write('\n')
