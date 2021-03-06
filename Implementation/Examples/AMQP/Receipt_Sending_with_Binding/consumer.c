#include </usr/include/python3.5m/Python.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <assert.h>


int main(int argc, char* argv[]){
	if ( argc < 2){
		puts("you have to put the consumer queue);
		return 1;
	}
	PyObject *pName, *pModule, *pDict, *pFunc, *pValue, *pArgs;
	
	// Initialization of Python Interpreter
	Py_Initialize();
	PyObject *sys = PyImport_ImportModule("sys");
	PyObject *path = PyObject_GetAttrString(sys, "path");
	PyList_Append(path, PyUnicode_FromString("."));

	// Name of the Python object
	pName = PyUnicode_FromString("amqp_receive");

	// Load the module
	pModule = PyImport_Import(pName);
	if (!pModule)
	{
		PyErr_Print();
		printf("ERROR in pModule \n");
		return 1;
	}

	pDict = PyModule_GetDict(pModule);
	pFunc = PyDict_GetItemString(pDict, "reception");

	if (PyCallable_Check(pFunc))
	{
	// creation of the arguments for the python function
		pArgs = PyTuple_New(argc -1);
		pValue=PyUnicode_FromString(argv[1]);
		if (!pValue)
		{
			PyErr_Print();
			return 1;
		}
		PyTuple_SetItem(pArgs, 0, pValue);
		//call the function
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
