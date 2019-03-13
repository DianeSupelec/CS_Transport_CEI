import pika
import sys
import logging
import ssl
import idmef
import json
import IDMEF_message as msg

#Creation of the message
message = msg.create_message_JSON()
queue = "test"
routing_key = "test"

#Connection with TLS Options
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

channel.queue_declare(queue=queue)

channel.publish(exchange='', routing_key=routing_key, body=message)
print(" [x] Sent %r" % message)
connection.close()

