#pragma once

#include <Python.h>

namespace utils {

struct PyRefWrapper {
    PyObject* obj;

    PyRefWrapper(PyObject* o): obj(o) { }
    ~PyRefWrapper() {
        if (obj) {
            Py_DECREF(obj);
        }
        obj = NULL;
    }
};

inline PyRefWrapper* pywrap(PyObject* o) {
    if (o) {
        return new PyRefWrapper(o);
    }
    return NULL;
}

}   // END NS utils
