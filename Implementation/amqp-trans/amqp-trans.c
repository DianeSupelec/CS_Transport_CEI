#include </usr/include/python3.5m/Python.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>

#define PIKALIBDIR "/root/Documents/CS_Transport_CEI/Implementation/amqp-trans"
#define PYTHONFILE "pikalib"
#define AMQPPRODUCERTHREAD_NEW "AMQPProducerThread_new"
#define AMQPCONSUMERTHREAD_NEW "AMQPConsumerThread_new"
#define AMQPTHREAD_SET_PKICREDENTIALS "AMQPThread_set_pkicredentials"
#define AMQPPRODUCERTHREAD_ADD_QUEUE "AMQPProducerThread_add_queue"
#define AMQPCONSUMERTHREAD_ADD_QUEUE "AMQPConsumerThread_add_queue"
#define AMQPCONSUMERTHREAD_GET_RFD "AMQPConsumerThread_get_rfd"
#define AMQPPRODUCERTHREAD_SEND_MSG "AMQPProducerThread_send_msg"
#define AMQPTHREAD_START "AMQPThread_start"
#define AMQPTHREAD_STOP "AMQPThread_stop"
#define AMQPTHREAD_SET_EXCHANGE "AMQPThread_set_exchange"

#define return_val_if_null(obj, val) do{ if ( !obj ) return val; }while(0)

int AMQPProducerThread_new(PyObject **pProducer, char* address, unsigned int port, PyObject *pModule)
{
	int ret;
	PyObject *pFunc, *pValue, *pArgs;

	return_val_if_null(pProducer, -1);
	return_val_if_null(address, -1);
	return_val_if_null(pModule, -1);

	pFunc = PyObject_GetAttrString(pModule, AMQPPRODUCERTHREAD_NEW);
	
	if ( !pFunc )
		return -1;
	
	if ( !PyCallable_Check(pFunc) ){
		Py_DECREF(pFunc);
		return -1;
	}

	pArgs = PyTuple_New(2);
	if ( !pArgs ){
		Py_DECREF(pFunc);
		return -1;
	}
	
	pValue = PyUnicode_FromString(address);
	if ( !pValue ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}

	ret = PyTuple_SetItem(pArgs, 0, pValue);
	if ( ret != 0 ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return -1;
	}

	pValue = PyLong_FromLong(port);
	if ( !pValue ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}

	ret = PyTuple_SetItem(pArgs, 1, pValue);
	if ( ret != 0 ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return -1;
	}

	pValue = PyObject_CallObject(pFunc, pArgs);
	if ( !pValue ){
		PyErr_Print();
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}else{
		*pProducer = pValue;
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return 0;
	}
}

int AMQPConsumerThread_new(PyObject **pConsumer, char* address, unsigned int port, PyObject *pModule)
{
	int ret;
	PyObject *pFunc, *pValue, *pArgs;

	return_val_if_null(pConsumer, -1);
	return_val_if_null(address, -1);
	return_val_if_null(pModule, -1);

	pFunc = PyObject_GetAttrString(pModule, AMQPCONSUMERTHREAD_NEW);
	
	if ( !pFunc )
		return -1;
	
	if ( !PyCallable_Check(pFunc) ){
		Py_DECREF(pFunc);
		return -1;
	}

	pArgs = PyTuple_New(2);
	if ( !pArgs ){
		Py_DECREF(pFunc);
		return -1;
	}
	
	pValue = PyUnicode_FromString(address);
	if ( !pValue ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}

	ret = PyTuple_SetItem(pArgs, 0, pValue);
	if ( ret != 0 ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return -1;
	}

	pValue = PyLong_FromLong(port);
	if ( !pValue ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}

	ret = PyTuple_SetItem(pArgs, 1, pValue);
	if ( ret != 0 ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return -1;
	}

	pValue = PyObject_CallObject(pFunc, pArgs);
	if ( !pValue ){
		PyErr_Print();
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}else{
		*pConsumer = pValue;
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return 0;
	}
	return 0;
}

int AMQPThread_set_pkicredentials(PyObject *amqpthread, char *pubcert, char *privkey, char *cacert, PyObject *pModule)
{
	int ret;
	PyObject *pFunc, *pValue, *pArgs;

	return_val_if_null(amqpthread, -1);
	return_val_if_null(pubcert, -1);
	return_val_if_null(privkey, -1);
	return_val_if_null(cacert, -1);
	return_val_if_null(pModule, -1);

	pFunc = PyObject_GetAttrString(pModule, AMQPTHREAD_SET_PKICREDENTIALS);
	
	if ( !pFunc )
		return -1;
	
	if ( !PyCallable_Check(pFunc) ){
		Py_DECREF(pFunc);
		return -1;
	}

	pArgs = PyTuple_New(4);
	if ( !pArgs ){
		Py_DECREF(pFunc);
		return -1;
	}
	
	pValue = amqpthread;
	if ( !pValue ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}

	ret = PyTuple_SetItem(pArgs, 0, pValue);
	Py_INCREF(amqpthread);
	if ( ret != 0 ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return -1;
	}

	pValue = PyUnicode_FromString(pubcert);
	if ( !pValue ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}

	ret = PyTuple_SetItem(pArgs, 1, pValue);
	if ( ret != 0 ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return -1;
	}

	pValue = PyUnicode_FromString(privkey);
	if ( !pValue ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}

	ret = PyTuple_SetItem(pArgs, 2, pValue);
	if ( ret != 0 ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return -1;
	}
	
	pValue = PyUnicode_FromString(cacert);
	if ( !pValue ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}

	ret = PyTuple_SetItem(pArgs, 3, pValue);
	if ( ret != 0 ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return -1;
	}
	
	pValue = PyObject_CallObject(pFunc, pArgs);
	if ( !pValue ){
		PyErr_Print();
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}else{
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return 0;
	}
	return 0;
}

int AMQPProducerThread_add_queue(PyObject *amqpthread,char *queue, PyObject *pModule)
{
	int ret;
	PyObject *pFunc, *pValue, *pArgs;

	return_val_if_null(amqpthread, -1);
	return_val_if_null(queue, -1);
	return_val_if_null(pModule, -1);

	pFunc = PyObject_GetAttrString(pModule, AMQPPRODUCERTHREAD_ADD_QUEUE);
	
	if ( !pFunc )
		return -1;
	
	if ( !PyCallable_Check(pFunc) ){
		Py_DECREF(pFunc);
		return -1;
	}

	pArgs = PyTuple_New(2);
	if ( !pArgs ){
		Py_DECREF(pFunc);
		return -1;
	}
	
	pValue = amqpthread;
	if ( !pValue ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}

	ret = PyTuple_SetItem(pArgs, 0, pValue);
	Py_INCREF(amqpthread);
	if ( ret != 0 ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return -1;
	}

	pValue = PyUnicode_FromString(queue);
	if ( !pValue ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}

	ret = PyTuple_SetItem(pArgs, 1, pValue);
	if ( ret != 0 ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return -1;
	}

	
	pValue = PyObject_CallObject(pFunc, pArgs);
	if ( !pValue ){
		PyErr_Print();
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}else{
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return 0;
	}
	return 0;
}

int AMQPConsumerThread_add_queue(PyObject *amqpthread,char *queue, PyObject *pModule)
{
	int ret;
	PyObject *pFunc, *pValue, *pArgs;

	return_val_if_null(amqpthread, -1);
	return_val_if_null(queue, -1);
	return_val_if_null(pModule, -1);

	pFunc = PyObject_GetAttrString(pModule, AMQPCONSUMERTHREAD_ADD_QUEUE);
	
	if ( !pFunc )
		return -1;
	
	if ( !PyCallable_Check(pFunc) ){
		Py_DECREF(pFunc);
		return -1;
	}

	pArgs = PyTuple_New(2);
	if ( !pArgs ){
		Py_DECREF(pFunc);
		return -1;
	}
	
	pValue = amqpthread;
	if ( !pValue ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}

	ret = PyTuple_SetItem(pArgs, 0, pValue);
	Py_INCREF(amqpthread);
	if ( ret != 0 ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return -1;
	}

	pValue = PyUnicode_FromString(queue);
	if ( !pValue ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}

	ret = PyTuple_SetItem(pArgs, 1, pValue);
	if ( ret != 0 ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return -1;
	}

	
	pValue = PyObject_CallObject(pFunc, pArgs);
	if ( !pValue ){
		PyErr_Print();
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}else{
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return 0;
	}
	return 0;
}

int AMQPConsumerThread_get_rfd(PyObject *amqpthread,char *queue, PyObject *pModule)
{
	int ret;
	PyObject *pFunc, *pValue, *pArgs;

	return_val_if_null(amqpthread, -1);
	return_val_if_null(queue, -1);
	return_val_if_null(pModule, -1);

	pFunc = PyObject_GetAttrString(pModule, AMQPCONSUMERTHREAD_GET_RFD);
	
	if ( !pFunc )
		return -1;
	
	if ( !PyCallable_Check(pFunc) ){
		Py_DECREF(pFunc);
		return -1;
	}

	pArgs = PyTuple_New(2);
	if ( !pArgs ){
		Py_DECREF(pFunc);
		return -1;
	}
	
	pValue = amqpthread;
	if ( !pValue ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}

	ret = PyTuple_SetItem(pArgs, 0, pValue);
	Py_INCREF(amqpthread);
	if ( ret != 0 ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return -1;
	}

	pValue = PyUnicode_FromString(queue);
	if ( !pValue ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}

	ret = PyTuple_SetItem(pArgs, 1, pValue);
	if ( ret != 0 ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return -1;
	}

	
	pValue = PyObject_CallObject(pFunc, pArgs);
	if ( !pValue ){
		PyErr_Print();
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}else{
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return 0;
	}
	return 0;
}

int AMQPProducerThread_send_msg(PyObject *amqpthread,char *msg, PyObject *pModule)
{
	int ret;
	PyObject *pFunc, *pValue, *pArgs;

	return_val_if_null(amqpthread, -1);
	return_val_if_null(msg, -1);
	return_val_if_null(pModule, -1);

	pFunc = PyObject_GetAttrString(pModule, AMQPPRODUCERTHREAD_SEND_MSG);
	
	if ( !pFunc )
		return -1;
	
	if ( !PyCallable_Check(pFunc) ){
		Py_DECREF(pFunc);
		return -1;
	}

	pArgs = PyTuple_New(2);
	if ( !pArgs ){
		Py_DECREF(pFunc);
		return -1;
	}
	
	pValue = amqpthread;
	if ( !pValue ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}

	ret = PyTuple_SetItem(pArgs, 0, pValue);
	Py_INCREF(amqpthread);
	if ( ret != 0 ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return -1;
	}

	pValue = PyUnicode_FromString(msg);
	if ( !pValue ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}

	ret = PyTuple_SetItem(pArgs, 1, pValue);
	if ( ret != 0 ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return -1;
	}

	
	pValue = PyObject_CallObject(pFunc, pArgs);
	if ( !pValue ){
		PyErr_Print();
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}else{
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return 0;
	}
	return 0;
}

int AMQPThread_start(PyObject *amqpthread, PyObject *pModule)
{
	int ret;
	PyObject *pFunc, *pValue, *pArgs;

	return_val_if_null(amqpthread, -1);
	return_val_if_null(pModule, -1);

	pFunc = PyObject_GetAttrString(pModule, AMQPTHREAD_START);
	
	if ( !pFunc )
		return -1;
	
	if ( !PyCallable_Check(pFunc) ){
		Py_DECREF(pFunc);
		return -1;
	}

	pArgs = PyTuple_New(1);
	if ( !pArgs ){
		Py_DECREF(pFunc);
		return -1;
	}
	
	pValue = amqpthread;
	if ( !pValue ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}

	ret = PyTuple_SetItem(pArgs, 0, pValue);
	Py_INCREF(amqpthread);
	if ( ret != 0 ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return -1;
	}

	
	pValue = PyObject_CallObject(pFunc, pArgs);
	if ( !pValue ){
		PyErr_Print();
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}else{
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return 0;
	}
	return 0;
}

int AMQPThread_stop(PyObject *amqpthread, PyObject *pModule)
{
	int ret;
	PyObject *pFunc, *pValue, *pArgs;

	return_val_if_null(amqpthread, -1);
	return_val_if_null(pModule, -1);

	pFunc = PyObject_GetAttrString(pModule, AMQPTHREAD_STOP);
	
	if ( !pFunc )
		return -1;
	
	if ( !PyCallable_Check(pFunc) ){
		Py_DECREF(pFunc);
		return -1;
	}

	pArgs = PyTuple_New(1);
	if ( !pArgs ){
		Py_DECREF(pFunc);
		return -1;
	}
	
	pValue = amqpthread;
	if ( !pValue ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}

	ret = PyTuple_SetItem(pArgs, 0, pValue);
	Py_INCREF(amqpthread);
	if ( ret != 0 ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return -1;
	}

	pValue = PyObject_CallObject(pFunc, pArgs);
	if ( !pValue ){
		PyErr_Print();
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}else{
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		Py_DECREF(amqpthread);
		return 0;
	}
	return 0;
}

int AMQPThread_set_exchange(PyObject *amqpthread, char *exchange, PyObject *pModule)
{
	int ret;
	PyObject *pFunc, *pValue, *pArgs;

	return_val_if_null(amqpthread, -1);
	return_val_if_null(exchange, -1);
	return_val_if_null(pModule, -1);

	pFunc = PyObject_GetAttrString(pModule, AMQPTHREAD_SET_EXCHANGE);
	
	if ( !pFunc )
		return -1;
	
	if ( !PyCallable_Check(pFunc) ){
		Py_DECREF(pFunc);
		return -1;
	}

	pArgs = PyTuple_New(2);
	if ( !pArgs ){
		Py_DECREF(pFunc);
		return -1;
	}
	
	pValue = amqpthread;
	if ( !pValue ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}

	ret = PyTuple_SetItem(pArgs, 0, pValue);
	Py_INCREF(amqpthread);
	if ( ret != 0 ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return -1;
	}

	pValue = PyUnicode_FromString(exchange);
	if ( !pValue ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}

	ret = PyTuple_SetItem(pArgs, 1, pValue);
	if ( ret != 0 ){
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return -1;
	}

	
	pValue = PyObject_CallObject(pFunc, pArgs);
	if ( !pValue ){
		PyErr_Print();
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		return -1;
	}else{
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		Py_DECREF(pValue);
		return 0;
	}
	return 0;
}

int AMQP_init(PyObject **Module)
{
	PyObject *pValue, *pModule;

	Py_Initialize();

	pValue=PyUnicode_FromString(PYTHONFILE);
	if ( !pValue ){
		Py_Finalize();
		return -1;
	}

    PyRun_SimpleString("import sys; sys.path.insert(0, '" PIKALIBDIR "')");
	pModule = PyImport_Import(pValue);
	Py_DECREF(pValue);
	if ( pModule == NULL ){
		Py_Finalize();
		return -1;
	}
	*Module = pModule;	
	return 0;	
}
int AMQP_finalize(PyObject *Module){
	Py_DECREF(Module);
	Py_Finalize();
	return 0;
}
/*
int main (){
	PyObject *pModule;
	PyObject *producer, *consumer;
	int res;

	if (AMQP_init(&pModule) < 0){
		fputs("Failed to init Python\n", stderr);
		return -1;
    };
	
	res= AMQPProducerThread_new(&producer, "localhost", 5671, pModule);
	printf("creation producer : %d\n", res);
	
	res = AMQPThread_set_pkicredentials(producer, "client1.pem", "client1.key", "trustedCA.pem",pModule);
    printf("set pki credential producer : %d\n", res);

	res = AMQPConsumerThread_new(&consumer,"localhost", 5671 ,pModule);
    printf("creation consumer : %d\n", res);
    
    res= AMQPThread_set_pkicredentials(consumer, "client2.pem", "client2.key", "trustedCA.pem",pModule);
	printf("pki consumer : %d\n", res);

	res = AMQPProducerThread_add_queue(producer, "Test",pModule);
    printf("queue producer 1 : %d\n", res);
    res = AMQPProducerThread_add_queue(producer, "Blop",pModule);
	printf("queue producer 2 : %d\n", res);

	res=AMQPConsumerThread_add_queue(consumer, "Test",pModule);
    printf("queue consumer 1 : %d\n", res);
    res=AMQPConsumerThread_add_queue(consumer, "Blop",pModule);
	printf("queue consumer 2 : %d\n", res);

	res=AMQPThread_start(consumer, pModule);
    printf("start consumer : %d\n", res);
    
	res=AMQPThread_start(producer,pModule);
	printf("start producer: %d\n", res);

	
    res=AMQPProducerThread_send_msg(producer, "Nianiania",pModule);
    printf("envoi msg : %d\n", res);

    printf("Sleeping\n");
    sleep(5);
    printf("Waken up\n");

    res=AMQPThread_stop(consumer,pModule);
    printf("stop consumer : %d\n", res); 
    
    res=AMQPThread_stop(producer,pModule);
    printf("stop producer : %d\n", res);
      
	AMQP_finalize(pModule);
  }

*/
