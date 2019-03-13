import pika
import time
import logging
import ssl

# Functions for the reception of the message
# Be careful !! The different paths for the certificates have to be changed
def reception(queue_conso):
	# Connection Options
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
	channel.queue_declare(queue=queue_conso)
	#Once a message is received, call a callback
	channel.basic_consume(callback, queue=queue_conso)
	print('Waiting for messages')
	channel.start_consuming()
	return 0

# Callback once a message is received
def callback(ch, method, properties, body):
	print("Received %r" % body)
	print("[x] Done")
	return

