#include </usr/include/python3.5m/Python.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <assert.h>


int main(int argc, char* argv[]){
	if ( argc < 4){
		puts("message, consumer_queue, routing key");
		return 1;
	}
	PyObject *pName, *pModule, *pDict, *pFunc, *pValue, *pArgs;
	
	// Initialization of the Python Interpreter
	Py_Initialize();
	PyObject *sys = PyImport_ImportModule("sys");
	PyObject *path = PyObject_GetAttrString(sys, "path");
	PyList_Append(path, PyUnicode_FromString("."));

	// Name of the object
	pName = PyUnicode_FromString("amqp_send");

	// Load the Module
	pModule = PyImport_Import(pName);
	if (!pModule)
	{
		PyErr_Print();
		printf("ERROR in pModule \n");
		return 1;
	}

	pDict = PyModule_GetDict(pModule);
	pFunc = PyDict_GetItemString(pDict, "sending");

	if (PyCallable_Check(pFunc))
	{
	// Creation of the arguments list for the Python Function
		pArgs = PyTuple_New(argc -1);
		for (int i=0; i<argc-1; i++)
		{
			pValue=PyUnicode_FromString(argv[i+1]);
			if (!pValue)
			{
				PyErr_Print();
				return 1;
			}
			PyTuple_SetItem(pArgs, i, pValue);
		}
		// call python function
		pValue = PyObject_CallObject(pFunc, pArgs);
		if (pArgs != NULL)
		{
			Py_DECREF(pArgs);
		}
		else{
			pValue = PyObject_CallObject(pFunc, NULL);
		}
		if (pValue != NULL)
		{
			printf("Return of call : %ld\n", PyLong_AsLong(pValue));
			Py_DECREF(pValue);
		}
	} else
	{
		PyErr_Print();
	}
	
    // Clean up
    Py_DECREF(pModule);
    Py_DECREF(pName);

    // Finish the Python Interpreter
    Py_Finalize();

    return 0;
}	
