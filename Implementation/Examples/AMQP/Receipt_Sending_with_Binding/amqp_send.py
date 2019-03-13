import pika
import time
import logging
import ssl


#Send a message with AMQP and TLS
def sending(message,queue_consumer, routing_key):
	# Connection Options
	logging.basicConfig(level=logging.INFO)
	cp=pika.ConnectionParameters(
	ssl=True,
	ssl_options = dict(
	ssl_version=ssl.PROTOCOL_TLSv1,
	ca_certs="/etc/rabbitmq/certs/ca/rmqCA.pem",
	keyfile="/root/mqtt-client/certs/client1.key",
	certfile="/root/mqtt-client/certs/client1.pem",
	cert_reqs=ssl.CERT_REQUIRED))
	#Connection
	connection = pika.BlockingConnection(cp)
	channel = connection.channel()
	channel.queue_declare(queue=queue_consumer)
	#Sending
	message =message
	channel.publish(exchange='', routing_key=routing_key, body=message)
	print(" [x] Sent %r" % message)
	connection.close()
	return 0


