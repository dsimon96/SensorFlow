import pika
from defs import *
from datetime import datetime

creds = pika.PlainCredentials('sf-admin', 'buzzword')
conn = pika.BlockingConnection(
    pika.ConnectionParameters(host=EDGE_DNS, credentials=creds))
channel = conn.channel()

channel.exchange_declare(exchange='sf.topic', exchange_type='topic', durable=True)

result = channel.queue_declare('', exclusive=True)
queue_name = result.method.queue

binding_key = 'edge.%s.%s' % (TOKEN, RECV_SUFFIX)
channel.queue_bind(exchange='sf.topic', queue=queue_name, routing_key=binding_key)

times = []

def callback(ch, method, properties, body):
    global times
    times.append(datetime.utcnow())
    print(" Received %r:%r" % (method.routing_key, body))

print(' Listening to %r. To exit press Ctrl-c' % binding_key)
channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

try:
    channel.start_consuming()
finally:
    conn.close()

    with open('output.txt', 'w') as f:
        for time in times:
            f.write(time.isoformat())
            f.write('\n')
