import time
import pika

creds = pika.PlainCredentials('sf-admin', 'buzzword')
conn = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost', virtual_host='edge', credentials=creds))
channel = conn.channel()

channel.exchange_declare(exchange='sf.topic', exchange_type='topic', durable=True)

routing_key = 'edge.fake-token.info'
scaling_factor = 1
for i in range(30):
    if 10 <= i and i <= 20:
        message = str(scaling_factor *(5 - abs(i - 15)))
    else:
        message = str(0.0)
    channel.basic_publish(exchange='sf.topic',
                          routing_key=routing_key,
                          body=message)
    print("Sent %r:%r" % (routing_key, message))
    time.sleep(0.25)

conn.close()
