#include </usr/include/python3.5m/Python.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <assert.h>


int main(int argc, char* argv[]){
	if ( argc < 6){
		puts("python_file, function_name, message, queue_consommateur, clé de routage");
		return 1;
	}
	PyObject *pName, *pModule, *pDict, *pFunc, *pValue, *pArgs;
	
	// Initialisation de l'interpreteur python
	Py_Initialize();
	PyObject *sys = PyImport_ImportModule("sys");
	PyObject *path = PyObject_GetAttrString(sys, "path");
	PyList_Append(path, PyUnicode_FromString("."));

	// Nom de l'object
	pName = PyUnicode_FromString(argv[1]);

	// Chargement du module
	pModule = PyImport_Import(pName);
	if (!pModule)
	{
		PyErr_Print();
		printf("ERROR in pModule \n");
		return 1;
	}
	//pDict a regarder
	pDict = PyModule_GetDict(pModule);

	//pFunc: fonction a appeler
	pFunc = PyDict_GetItemString(pDict, argv[2]);

	if (PyCallable_Check(pFunc))
	{
	// creation de la liste des arguments appelés
		pArgs = PyTuple_New(argc -3);
		for (int i=0; i<argc-3; i++)
		{
			pValue=PyUnicode_FromString(argv[i+3]);
			if (!pValue)
			{
				PyErr_Print();
				return 1;
			}
			PyTuple_SetItem(pArgs, i, pValue);
		}
		// appel de la fonction python
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
