import pika

creds = pika.PlainCredentials('sf-admin', 'buzzword')
conn = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost', virtual_host='cloud', credentials=creds))
channel = conn.channel()

channel.exchange_declare(exchange='sf.topic', exchange_type='topic', durable=True)

result = channel.queue_declare('', exclusive=True)
queue_name = result.method.queue

binding_key = 'cloud.fake-token.info'
channel.queue_bind(exchange='sf.topic', queue=queue_name, routing_key=binding_key)

def callback(ch, method, properties, body):
    print(" Received %r:%r" % (method.routing_key, body))

print(' Listening to %r. To exit press Ctrl-c' % binding_key)
channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()
conn.close()
