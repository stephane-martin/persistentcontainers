#pragma once
#include <string>
#include <Python.h>

namespace quiet {
    using std::string;

    inline void PyErr_SetCppString(PyObject *type, const string& message) {
        PyErr_SetString(type, message.c_str());
    }

    void custom_exception_handler();
}
