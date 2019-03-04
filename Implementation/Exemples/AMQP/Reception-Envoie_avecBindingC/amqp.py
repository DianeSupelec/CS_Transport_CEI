import pika
import time
import logging
import ssl

# fonction de reception d'un message en utilisant TLS sur le serveur RabbitMQ
# Attention les chemins de certificats et clé sont écrits en dur dans le code ici
def reception(queue_conso):
	logging.basicConfig(level=logging.INFO)
	cp= pika.ConnectionParameters(ssl=True, ssl_options = dict(
	ssl_version=ssl.PROTOCOL_TLSv1, 
	ca_certs="/etc/rabbitmq/certs/ca/rmqCA.pem",
	keyfile="/root/mqtt-client/certs/client1.key",
	certfile="/root/mqtt-client/certs/client1.pem",
	cert_reqs=ssl.CERT_REQUIRED))
	connection = pika.BlockingConnection(cp)
	channel=connection.channel()
	channel.queue_declare(queue=queue_conso)
	channel.basic_consume(callback, queue=queue_conso)
	print('Waiting for messages')
	channel.start_consuming()
	return 0

# Action a effectuer lors de la recepetion d'un message
def callback(ch, method, properties, body):
	print("Received %r" % body)
	print("[x] Done")
	return


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


