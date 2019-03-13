#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "MQTTClient.h"

#define ADDRESS "ssl://localhost:8883"
#define CLIENTID "Paho Producer"
#define TOPIC "MQTT Examples"
#define PAYLOAD "Hello World"
#define QOS 1
#define TIMEOUT 10000L
#define USERNAME "mqtt_u1"
#define PASSWORD "mqtt_p1"
#define TRUSTED_CERT_PATH "/root/mqtt-client/trustedCA.pem"
#define CLIENT_CERT_PATH  "/root/mqtt-client/certs/client1.pem"
#define CLIENT_KEY_PATH   "/root/mqtt-client/certs/client1.key"


int main(int argc, char *argv[])
{
	MQTTClient client;
	MQTTClient_deliveryToken token;
	MQTTClient_message pubmsg = MQTTClient_message_initializer;
	MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
	MQTTClient_SSLOptions ssl_opts = MQTTClient_SSLOptions_initializer;

	/* return code */
	int rc;

	MQTTClient_create(&client, ADDRESS, CLIENTID, MQTTCLIENT_PERSISTENCE_NONE, NULL);

	/* Setting connection options */
	conn_opts.keepAliveInterval = 20;
	conn_opts.cleansession = 1;

	conn_opts.username = USERNAME;
	conn_opts.password = PASSWORD;

	/* Settings SSL Options */
	ssl_opts.trustStore = TRUSTED_CERT_PATH ;	
	ssl_opts.keyStore = CLIENT_CERT_PATH ;
	ssl_opts.privateKey = CLIENT_KEY_PATH ;
	ssl_opts.privateKeyPassword = "";
	conn_opts.ssl = &ssl_opts;

	if ( (rc = MQTTClient_connect(client, &conn_opts)) != MQTTCLIENT_SUCCESS){
		printf("Failed to connect, return code %d\n", rc);
		exit(EXIT_FAILURE);
	}else 
		puts("Connected !");
	
	/* Preparing Message */
	pubmsg.payload = PAYLOAD ;
	pubmsg.payloadlen = strlen(PAYLOAD);
	pubmsg.qos = QOS;
	pubmsg.retained = 0;
	
	MQTTClient_publishMessage(client, TOPIC, &pubmsg, &token);
	printf("Waiting for up to %d seconds for publication of %s\n"
	       "on topic %s for client with ClientID : %s\n",
	       (int) (TIMEOUT/1000), PAYLOAD, TOPIC, CLIENTID);
	rc = MQTTClient_waitForCompletion(client, token, TIMEOUT);
	printf("Message with delivry token %d delivered\n", token);

	/* Disconnect and Clean-up */
	MQTTClient_disconnect(client, 10000);
	MQTTClient_destroy(&client);
	return rc;
}

