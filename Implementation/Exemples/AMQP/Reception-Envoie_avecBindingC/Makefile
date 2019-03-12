#attention, il est possible qu'il faille modifier le chemin de Python.h dans main.c
all: consumer producer

consumer: consumer.c
	gcc -Wall consumer.c -o consumer -lpython3.5m

producer: producer.c
	gcc -Wall producer.c -o producer -lpython3.5m

