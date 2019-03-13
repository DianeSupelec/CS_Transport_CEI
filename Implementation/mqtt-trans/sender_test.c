/* This is an example of sending messages
 * through the mqtt-trans transport
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mqtt-trans.h"

#define ADDRESS "localhost"
#define PORT 8883
#define CA "/root/mqtt-client/trustedCA.pem"
#define PUBCERT "/root/mqtt-client/certs/client1.pem"
#define PRIVKEY "/root/mqtt-client/certs/client1.key"

#define TOPIC1 "t1"

#define MSG "Hello there"

int main(void)
{
	MQTT_transporter_t *mt;
	int infd;
	int ret;
	int i, size;
	char *msg;

	ret = MQTT_transporter_new(&mt, &infd, ADDRESS, PORT);
	if ( ret < 0 )
		return -1;
	
	ret = MQTT_transporter_set_pkicredentials(mt, PUBCERT, PRIVKEY, CA);
	if ( ret < 0 )
		return -1;

	ret = MQTT_transporter_set_credentials(mt, "mqtt_u1", "mqtt_p1");
	if ( ret < 0 )
		return -1;
	
	ret = MQTT_transporter_add_pub_topic(mt, TOPIC1);
	if ( ret < 0 )
		return -1;

	ret = MQTT_transporter_connect(mt);
	if ( ret < 0 )	
		return -1;
	puts("Connected !");
	
	msg = NULL;
	for ( i = 0; i < 5; ++i ){
		size = snprintf(msg, 0, "%s %d\n", MSG, i);
		msg = realloc(msg, ++size * sizeof(*msg));
		snprintf(msg, size, "%s %d\n", MSG, i);	
		printf("Sending message #%d : %s", i, msg);
		MQTT_transporter_send(mt, msg , size);
	}
	ret = MQTT_transporter_disconnect(mt);
	puts("Disconnected !");
	
	MQTT_transporter_destroy(&mt);
		
	return 0;
}
