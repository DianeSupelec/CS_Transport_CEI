import idmef
import json

def create_message_JSON():
	msg = idmef.IDMEF()
	msg.set("alert.classification.text", "Test alert")
	msg.set("alert.source(0).node.address(0).address", "10.0.0.1")
	msg2 = msg.toJSON()
	return msg2

#def create_message_Binary():
#	msg = idmef.IDMEF()
#	msg.set("alert.classification.text", "Test alert")
#	msg.set("alert.source(0).node.address(0).address", "10.0.0.1")
#	msg2 = msg.toBinary()
#	return msg2
