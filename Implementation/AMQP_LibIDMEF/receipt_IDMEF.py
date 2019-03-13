import pika
import time
import logging
import ssl
import idmef
import json

queue='test'

#Set TLS Options
logging.basicConfig(level=logging.INFO)
cp= pika.ConnectionParameters(ssl=True, ssl_options = dict(
ssl_version=ssl.PROTOCOL_TLSv1, 
ca_certs="/etc/rabbitmq/certs/ca/rmqCA.pem",
keyfile="/root/mqtt-client/certs/client1.key",
certfile="/root/mqtt-client/certs/client1.pem",
cert_reqs=ssl.CERT_REQUIRED))

#Connection
connection = pika.BlockingConnection(cp)
channel=connection.channel()

channel.queue_declare(queue=queue)

def callback(ch, method, properties, body):
	print("Received %r" % body)
	print("[x] Done")

channel.basic_consume(callback, queue=queue)
print('Waiting for messages')
channel.start_consuming()







