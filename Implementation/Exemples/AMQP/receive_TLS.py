import pika
import time
import logging
import ssl

logging.basicConfig(level=logging.INFO)

cp= pika.ConnectionParameters(ssl=True, ssl_options = dict(
ssl_version=ssl.PROTOCOL_TLSv1, ca_certs="/home/diane/tls-gen/basic/result/ca_certificate.pem",
keyfile="/home/diane/tls-gen/basic/result/client_key.pem",
certfile="/home/diane/tls-gen/basic/result/client_certificate.pem",
cert_reqs=ssl.CERT_REQUIRED))



connection = pika.BlockingConnection(cp)
channel=connection.channel()

channel.queue_declare(queue='hello')

def callback(ch, method, properties, body):
	print("Received %r" % body)
	time.sleep(body.count(b'.'))
	print("[x] Done")

channel.basic_consume(callback, queue='hello')

print('Waiting for messages')
channel.start_consuming()
