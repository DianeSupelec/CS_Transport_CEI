import logging
import pika
import ssl
import threading
import time
import os


"""
AMQPObject stub.
Handle connection and exchange declaration
Methods start_working and stop_working are
called respectively when exchange is set and
when terminating the object. Thus they shall
be overriden in child classes
"""
class AMQPClient(object):
    exchange = 'direct_prelude'
    exchange_type = 'direct'
    CHECK_FOR_STOP_INTERVAL = 2

    def __init__(self, address, port):
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._address = address
        self._port = port
        self.queues = []
        self.queues_index = 0
        self.mutex_stop = threading.Lock()
        self.has_to_stop = False
        self.sem_terminated = threading.Semaphore(0)
        

    def set_pkicredentials(self, pubcert, privkey, cacert):
        self._conn_params = pika.ConnectionParameters(self._address, self._port, '/', 
                                                      ssl = True, ssl_options = dict(
                                                      ca_certs = cacert,
                                                      cert_reqs = ssl.CERT_REQUIRED,
                                                      keyfile = privkey,
                                                      certfile = pubcert,
                                                      ssl_version = ssl.PROTOCOL_TLSv1))

    def set_exchange(self, exchange):
        self.exchange = exchange

    def add_queue(self, queue):
        self.queues.append(queue)

    def connect(self):
        return pika.SelectConnection(self._conn_params,
                                     self.on_connection_open,
                                     stop_ioloop_on_close=False)

    def on_connection_open(self, unused_connection):
        self._connection.add_on_close_callback(self.on_connection_closed)
        self.open_channel()

    def on_connection_closed(self, connection, reply_code, reply_text):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
            self.sem_terminated.release();
        else:
            self._connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        self._connection.ioloop.stop()
        if not self._closing:
            self._connection = self.connect()
            self._connection.ioloop.start()

    def open_channel(self):
        self._connection.channel(on_open_callback=self.on_channel_open)

    
    def on_channel_open(self, channel):
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        self.setup_exchange()

    def on_channel_closed(self, channel, reply_code, reply_text):
        self._connection.close()

    def setup_exchange(self):
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange=self.exchange,
                                       exchange_type=self.exchange_type)    

    def on_exchange_declareok(self, unused_frame):
        self.start_working()

    def start_working_self(self):
        return

    def stop_working_self(self):
        return

    def check_for_stop(self):
        self.mutex_stop.acquire()
        if self.has_to_stop :
            self.mutex_stop.release()
            self.stop()
        else:
            self.mutex_stop.release()            
            self._connection.add_timeout(self.CHECK_FOR_STOP_INTERVAL, self.check_for_stop)
    
    def close_channel(self):
        self._channel.close()

    def stop(self):
        self._closing = True
        self.stop_working()
        self._connection.ioloop.start()

    def run(self):
        self._connection = self.connect()
        self._connection.ioloop.start()

"""
AMQPProducer, herits from AMQPClient
Provide methods for publishing messages
"""
class AMQPProducer(AMQPClient):
    FLUSH_INTERVAL = 1

    def __init__(self, address, port):
        AMQPClient.__init__(self, address, port)
        self.msgs = []
        self.mutex_msgs = threading.Lock()

    def send_msg(self, msg):
        self.mutex_msgs.acquire()
        self.msgs.append(msg)
        self.mutex_msgs.release()

    def add_pub_queue(self, queue):
        self.add_queue(queue)

    def flush_msgs_loop(self):
        self.flush_msgs()
        if not self._closing:
            self._connection.add_timeout(self.FLUSH_INTERVAL, self.flush_msgs_loop)

    def flush_msgs(self):
        self.mutex_msgs.acquire()
        for msg in self.msgs:
            for j in range(0, len(self.queues)): 
                print("Publishing msg %s ! exchange : %s queue %s", msg, self.exchange, self.queues[j])          
                self._channel.basic_publish(exchange=self.exchange, routing_key=self.queues[j], body=msg)
        self.msgs = []
        self.mutex_msgs.release()

    def start_working(self):
        self._connection.add_timeout(self.CHECK_FOR_STOP_INTERVAL, self.check_for_stop)
        self._connection.add_timeout(self.FLUSH_INTERVAL, self.flush_msgs_loop)

    def stop_working(self):
        if self._channel is not None:
            self.flush_msgs()
            self.close_channel()

""" 
AMQPConsumer, herits from AMQPClient
Handle the reception of messages
"""
class AMQPConsumer(AMQPClient):
    def __init__(self, address, port):
        AMQPClient.__init__(self, address, port)
        self.rfd, self.wfd = os.pipe()
        self.mutex_w_pipe = threading.Lock()

    def add_sub_queue(self, queue):
        self.add_queue(queue)

    def setup_queues(self):
        self.queues_len = len(self.queues)
        self.queues_index = 0
        for i in range(0, self.queues_len):
            self._channel.queue_declare(self.on_queue_declareok, self.queues[i])


    def on_queue_declareok(self, method_frame):        
        self._channel.queue_bind(self.on_bindok, self.queues[self.queues_index],
                                 self.exchange, self.queues[self.queues_index]) #HERE ?
        self.queues_index += 1
        if self.queues_index == self.queues_len:
            self.queues_index = 0

    def on_bindok(self, unused_frame):    
        self.queues_index += 1
        if self.queues_index == self.queues_len:
            self.queues_index = 0
            self.start_consuming()

    def on_consumer_cancelled(self, method_frame):
        if self._channel:
            self._channel.close()

    def on_message(self, unused_channel, basic_deliver, properties, body):
        print('Received message # %s from %s: %s',
                    basic_deliver.delivery_tag, properties.app_id, body.decode("utf-8"))
        self.acknowledge_message(basic_deliver.delivery_tag)
        self.mutex_w_pipe.acquire()
        
        os.write(self.wfd, body)
        self.mutex_w_pipe.release()

    def acknowledge_message(self, delivery_tag):
        self._channel.basic_ack(delivery_tag)

    def start_consuming(self):
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)
        for i in range(0, self.queues_len):
            self._consumer_tag = self._channel.basic_consume(self.on_message,
                                                         self.queues[i])
    def on_cancelok(self, unused_frame):
        self.close_channel()

    def start_working(self):
        self.setup_queues()
        self._connection.add_timeout(self.CHECK_FOR_STOP_INTERVAL, self.check_for_stop)

    def stop_working(self):
        if self._channel:
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    
"""
AMQPThread stub.
Contains an AMQPclient within a thread
to avoid blocking the main thread in
th I/O loop
Provides a function to terminate the
embedded AMQPclient, which results in
the termination of the Thread
"""
class AMQPThread(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.AMQPclient = None
    def set_pkicredentials(self, pubcert, privkey, cacert):
        self.AMQPclient.set_pkicredentials(pubcert, privkey, cacert)
    def set_exchange(self, exchange):
        self.AMQPclient.set_exchange(exchange)
    def run(self):
        self.AMQPclient.run()
    def requestStop(amqpthread):
        amqpthread.AMQPclient.mutex_stop.acquire()
        amqpthread.AMQPclient.has_to_stop = True
        amqpthread.AMQPclient.mutex_stop.release()
        amqpthread.AMQPclient.sem_terminated.acquire()

"""
AMQPProducerThread, herits from AMQPThread
Contains an AMQPProducer as AMQPclient
"""
class AMQPProducerThread(AMQPThread):
    def __init__(self, address, port):
        AMQPThread.__init__(self)
        self.AMQPclient = AMQPProducer(address, port)
    
    def add_pub_queue(self, queue):
        self.AMQPclient.add_queue(queue)
    
    def send_msg(self, msg):
        self.AMQPclient.send_msg(msg)

"""
AMQPConsumerThread, herits from AMQPThread
Contains an AMQPConsumer as AMQPclient
"""
class AMQPConsumerThread(AMQPThread):
    def __init__(self, address, port):
        AMQPThread.__init__(self)
        self.AMQPclient = AMQPConsumer(address, port)
    
    def add_sub_queue(self, queue):
        self.AMQPclient.add_queue(queue)
    
    def get_rfd(self):
        return self.AMQPclient.rfd

def AMQPProducerThread_new(address, port):
    producer = AMQPProducerThread(address, port)
    return producer

def AMQPConsumerThread_new(address, port):
    consumer = AMQPConsumerThread(address, port)
    return consumer

def AMQPThread_set_pkicredentials(amqpthread, pubcert, privkey, cacert):
    amqpthread.set_pkicredentials(pubcert, privkey, cacert)
    return 0

def AMQPProducerThread_add_queue(amqpthread, queue):
    amqpthread.add_pub_queue(queue)
    return 0

def AMQPConsumerThread_add_queue(amqpthread, queue):
    amqpthread.add_sub_queue(queue)
    return 0

def AMQPConsumerThread_get_rfd(amqpthread, queue):
    return amqpthread.get_rfd()

def AMQPProducerThread_send_msg(amqpthread, msg):
    amqpthread.send_msg(msg)
    return 0

def AMQPThread_start(amqpthread):
    amqpthread.start()
    return 0

def AMQPThread_stop(amqpthread):
    amqpthread.requestStop()
    return 0

def AMQPThread_set_exchange(amqpthread, exchange):
    amqpthread.set_exchange(exchange)
    return 0

def main():
    producer = AMQPProducerThread_new('localhost', '5671')
    AMQPThread_set_pkicredentials(producer, "client1.pem", "client1.key", "trustedCA.pem")
    
    consumer = AMQPConsumerThread_new('localhost', '5671')
    AMQPThread_set_pkicredentials(consumer, "client2.pem", "client2.key", "trustedCA.pem")

    AMQPProducerThread_add_queue(producer, "Toto")
    AMQPProducerThread_add_queue(producer, "Tata")

    AMQPConsumerThread_add_queue(consumer, "Toto")
    AMQPConsumerThread_add_queue(consumer, "Tata")

    AMQPThread_start(consumer)
    AMQPThread_start(producer)

    AMQPProducerThread_send_msg(producer, "Hello there Gen Kenobi")
    print("Sleeping")
    time.sleep(15)
    print("Waken up")

    AMQPThread_stop(consumer)
    AMQPThread_stop(producer)

if __name__ == '__main__':
    main()

