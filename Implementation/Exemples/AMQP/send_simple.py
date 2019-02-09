import pika
import sys

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')

channel.confirm_delivery()

message ="Hello World"

if channel.basic_publish(exchange='', routing_key='hello', body=message):
        print(" [x] Sent message: "+message)
        print("message envoye")
else:
        print("Probleme")

connection.close()
