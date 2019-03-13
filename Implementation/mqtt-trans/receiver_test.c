/* This is an example of receiving messages
 * using the mqtt-trans transport
 * Inbound messages are made available through
 * the file descriptor infd
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/time.h>
#include <unistd.h>

#include "mqtt-trans.h"

#define ADDRESS "localhost"
#define PORT 8883
#define CA "/root/mqtt-client/trustedCA.pem"
#define PUBCERT "/root/mqtt-client/certs/client2.pem"
#define PRIVKEY "/root/mqtt-client/certs/client2.key"

#define TOPIC1 "t1"

int enter_key_pressed(void)
{
	fd_set set;
	struct timeval tv;

	tv.tv_sec = 0;
	tv.tv_usec = 100;

	FD_ZERO(&set);
	FD_SET(0, &set);
	
	return select(1, &set, NULL, NULL, &tv) > 0 ? 1 : 0;
}

void flush_stdin(void)
{
	int c;
	while ( (c = getchar()) != '\n' && c != EOF );
}

int main(void)
{
	MQTT_transporter_t *mt;
	int infd;
	int ret;
	fd_set rfds;
	struct timeval tv;
	unsigned char buf[512];
	int count, i;
	
	ret = MQTT_transporter_new(&mt, &infd, ADDRESS, PORT);
	if ( ret < 0 )
		return -1;

	ret = MQTT_transporter_set_pkicredentials(mt, PUBCERT, PRIVKEY, CA);
	if ( ret < 0 )
		return -1;

	ret = MQTT_transporter_set_credentials(mt, "mqtt_u1", "mqtt_p1");
	if ( ret < 0 )
		return -1;
	
	ret = MQTT_transporter_add_sub_topic(mt, TOPIC1);
	if ( ret < 0 )
		return -1;

	ret = MQTT_transporter_connect(mt);
	if ( ret < 0 )	
		return -1;
	puts("Connected !");
	
	ret = MQTT_transporter_subscribe(mt);
	if ( ret < 0 )
		return -1;
	puts("Awaiting messages... Press Enter to quit");	
	while ( !enter_key_pressed() ){
		FD_ZERO(&rfds);
		FD_SET(infd, &rfds);
		tv.tv_sec = 2;
		tv.tv_usec = 0;
		ret = select(infd+1, &rfds, NULL, NULL, &tv);
		if ( ret < 0 )
			perror("select: ");
		else if (ret > 0){
			count = 0;
			if ( (count = read(infd, buf, 64)) < 0 )
				fputs("failed to read from pipe\n", stderr);
			for (i = 0; i < count; ++i)
				putchar( buf[i] );
		}	
	} 
	flush_stdin();
	puts("Disconnecting ...");
	ret = MQTT_transporter_disconnect(mt);
	
	MQTT_transporter_destroy(&mt);
		
	return 0;
}
