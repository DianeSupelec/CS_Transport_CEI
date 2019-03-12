import pika
import time
import logging
import ssl


#Fonction d'un message via TLS par le serveru RabbitMQ
def envoie(message_a_envoyer,queue_conso, cle_routage):
	logging.basicConfig(level=logging.INFO)
	cp=pika.ConnectionParameters(
	ssl=True,
	ssl_options = dict(
	ssl_version=ssl.PROTOCOL_TLSv1,
	ca_certs="/etc/rabbitmq/certs/ca/rmqCA.pem",
	keyfile="/root/mqtt-client/certs/client1.key",
	certfile="/root/mqtt-client/certs/client1.pem",
	cert_reqs=ssl.CERT_REQUIRED))
	connection = pika.BlockingConnection(cp)
	channel = connection.channel()
	channel.queue_declare(queue=queue_conso)
	message =message_a_envoyer
	channel.publish(exchange='', routing_key=cle_routage, body=message)
	print(" [x] Sent %r" % message)
	connection.close()
	return 0


