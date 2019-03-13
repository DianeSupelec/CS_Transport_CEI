CC=gcc
CFLAGS=-Wall


all: mqtt-trans.o sender_test.out receiver_test.out

mqtt-trans.o: mqtt-trans.c mqtt-trans.h
	${CC} ${CFLAGS} -c mqtt-trans.c -o mqtt-trans.o

sender_test.out: sender_test.c mqtt-trans.o mqtt-trans.h
	${CC} ${CFLAGS} sender_test.c mqtt-trans.o -lpaho-mqtt3as -o sender_test.out

receiver_test.out: receiver_test.c mqtt-trans.o mqtt-trans.h
	${CC} ${CFLAGS} receiver_test.c mqtt-trans.o -lpaho-mqtt3as -o receiver_test.out

clean: 
	rm -f *.o
