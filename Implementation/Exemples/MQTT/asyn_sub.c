#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "MQTTClient.h"

#define ADDRESS "ssl://localhost:8883"
#define CLIENTID "Paho subscriber"
#define TOPIC "MQTT Examples"
#define TIMEOUT 10000L
#define QOS 1
#define USERNAME "mqtt_u2"
#define PASSWORD "mqtt_p2"
#define TRUSTED_CERT_PATH "/root/mqtt-client/trustedCA.pem"
#define CLIENT_CERT_PATH  "/root/mqtt-client/certs/client2.pem"
#define CLIENT_KEY_PATH   "/root/mqtt-client/certs/client2.key"

volatile MQTTClient_deliveryToken deliveredtoken;

/* Callback when a message delivery confirmation has been sent */
void delivered(void *context, MQTTClient_deliveryToken dt)
{
	printf("Message with token value %d delivery confirmed\n", dt);
	deliveredtoken = dt;
}

/* Callback when a message has arrived */
int msgarrvd(void *context, char *topicName, int topicLen, MQTTClient_message *message)
{
	int i;
	char *payloadptr;
	printf("Message arrived\n");
	printf("  topic: %s\n", topicName);
	printf("  message: ");
	
	payloadptr = message->payload;
	for (i=0; i<message->payloadlen; ++i)
	{
		putchar(*payloadptr++);
	}
	putchar('\n');
	
	MQTTClient_freeMessage(&message);
	MQTTClient_free(topicName);
	return 1;
}

/* Callback on connection lost */
void connlost(void *context, char *cause)
{
	printf("\nConnection lost\n");
	printf("  cause: %s\n", cause);
}

int main(int argc, char *argv[])
{
	MQTTClient client;
	MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
	MQTTClient_SSLOptions ssl_opts = MQTTClient_SSLOptions_initializer;

	int rc, ch;

	MQTTClient_create(&client, ADDRESS, CLIENTID, MQTTCLIENT_PERSISTENCE_NONE, NULL);

	/* Setting connection options */
	conn_opts.keepAliveInterval = 20;
	conn_opts.cleansession = 1;
	conn_opts.username = USERNAME;
	conn_opts.password = PASSWORD;

	/* Setting SSL Options */
	ssl_opts.trustStore = TRUSTED_CERT_PATH ;
	ssl_opts.keyStore = CLIENT_CERT_PATH ;
	ssl_opts.privateKey = CLIENT_KEY_PATH ;
	ssl_opts.privateKeyPassword = "";
	conn_opts.ssl = &ssl_opts;

	MQTTClient_setCallbacks(client, NULL, connlost, msgarrvd, delivered);

	if ( (rc = MQTTClient_connect(client, &conn_opts)) != MQTTCLIENT_SUCCESS){
		printf("Failed to connect, return code %d\n", rc);
		exit(EXIT_FAILURE);
	}

	printf("Subscribing to topic %s\nfor client %s using QoS%d\n\n"
	       "Press Q<Enter> to quit\n\n", TOPIC, CLIENTID, QOS);
	rc = MQTTClient_subscribe(client, TOPIC, QOS);
	if ( rc != MQTTCLIENT_SUCCESS ) {
		printf("Failed to subscribe, return code %d\n", rc);
		exit(EXIT_FAILURE);	
	}	
	/* Loop until quitting */
	do{
		ch = getchar();
	}while (ch !='Q' && ch != 'q');

	/* Disconnect and clean-up */
	MQTTClient_disconnect(client, 10000);
	MQTTClient_destroy(&client);
	return rc;
}

