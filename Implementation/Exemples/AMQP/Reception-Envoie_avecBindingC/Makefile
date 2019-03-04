#attention, il est possible qu'il faille modifier le chemin de Python.h dans main.c
all: amqp_receive amqp_send

amqp_receive: amqp_receive.c
	gcc -Wall amqp_receive.c -o amqp_receive -lpython3.5m

amqp_send: amqp_send.c
	gcc -Wall amqp_send.c -o amqp_send -lpython3.5m

