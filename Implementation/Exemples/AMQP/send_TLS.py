import pika
import sys
import logging
import ssl

logging.basicConfig(level=logging.INFO)

cp=pika.ConnectionParameters(
ssl=True,
ssl_options = dict(
ssl_version=ssl.PROTOCOL_TLSv1,
ca_certs="/home/diane/tls-gen/basic/result/ca_certificate.pem",
keyfile="/home/diane/tls-gen/basic/result/client_key.pem",
certfile="/home/diane/tls-gen/basic/result/client_certificate.pem",
cert_reqs=ssl.CERT_REQUIRED))


connection = pika.BlockingConnection(cp)
channel = connection.channel()

channel.queue_declare(queue='hello')
message ="Hello World"

channel.publish(exchange='', routing_key='hello', body=message)
print(" [x] Sent %r" % message)
connection.close()
