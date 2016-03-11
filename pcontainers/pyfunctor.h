#pragma once

#include <string>
#include <stdexcept>
#include <utility>
#include <Python.h>
#include "utils.h"

namespace quiet {

using std::string;
using std::runtime_error;
using namespace utils;


class PyPredicate {
public:
    PyPredicate(): callback(NULL) { }

    static inline unary_predicate make_unary_predicate(PyObject* obj) {
        return PyPredicate(obj);
    }

    static inline binary_predicate make_binary_predicate(PyObject* obj) {
        return PyPredicate(obj);
    }


    PyPredicate(PyObject* obj) {
        if (obj == NULL) {
            callback = NULL;
            return;
        }
        PyGILState_STATE __pyx_gilstate_save = PyGILState_Ensure();
        if (!PyCallable_Check(obj)) {
            PyGILState_Release(__pyx_gilstate_save);
            throw std::runtime_error("PyPredicate: obj is not a python callable");
        }
        callback = obj;
        Py_INCREF(obj);
        PyGILState_Release(__pyx_gilstate_save);
    }

    ~PyPredicate() {
        if (callback != NULL) {
            PyGILState_STATE __pyx_gilstate_save = PyGILState_Ensure();
            Py_DECREF(callback);
            PyGILState_Release(__pyx_gilstate_save);
            callback = NULL;
        }
    }

    PyPredicate(const PyPredicate& other) {
        if (other.callback == NULL) {
            callback = NULL;
            return;
        }
        callback = other.callback;
        PyGILState_STATE __pyx_gilstate_save = PyGILState_Ensure();
        Py_INCREF(callback);
        PyGILState_Release(__pyx_gilstate_save);
    }

    PyPredicate& operator=(PyPredicate other) {
        std::swap(callback, other.callback);
        return *this;
    }

    bool operator()(const string& key) {
        if (callback == NULL) {
            throw std::runtime_error("This predicate is not initialized");
        }
        PyGILState_STATE __pyx_gilstate_save = PyGILState_Ensure();
        // convert the two strings into python 'bytes' objects (they are new references)
        PyObject* py_key = PyBytes_FromStringAndSize(key.c_str(), (Py_ssize_t) key.size());
        if (py_key == NULL) {
            PyGILState_Release(__pyx_gilstate_save);
            throw std::runtime_error("PyPredicate: PyString_FromStringAndSize failed :(");
        }

        PyObject* obj = PyObject_CallFunctionObjArgs(callback, py_key, NULL);
        Py_DECREF(py_key);

        if (obj == NULL) {
            PyGILState_Release(__pyx_gilstate_save);
            throw std::runtime_error("PyPredicate: Calling python callback failed");
        }

        // we got a Python object as a result; now let's convert it into a C++ bool
        int res = PyObject_IsTrue(obj);
        Py_DECREF(obj);
        PyGILState_Release(__pyx_gilstate_save);
        if (res == -1) {
            throw std::runtime_error("PyPredicate: Converting to bool failed");
        }
        return (bool) res;
    }

    bool operator()(const string& key, const string& value) {       // for binary predicates
        if (callback == NULL) {
            throw std::runtime_error("This predicate is not initialized");
        }
        PyGILState_STATE __pyx_gilstate_save = PyGILState_Ensure();
        // convert the two strings into python 'bytes' objects (they are new references)
        PyObject* py_key = PyBytes_FromStringAndSize(key.c_str(), (Py_ssize_t) key.size());
        if (py_key == NULL) {
            PyGILState_Release(__pyx_gilstate_save);
            throw std::runtime_error("PyPredicate: PyString_FromStringAndSize failed :(");
        }
        PyObject* py_value = PyBytes_FromStringAndSize(value.c_str(), (Py_ssize_t) value.size());
        if (py_value == NULL) {
            Py_DECREF(py_key);
            PyGILState_Release(__pyx_gilstate_save);
            throw std::runtime_error("PyPredicate: PyString_FromStringAndSize failed :(");
        }

        PyObject* obj = PyObject_CallFunctionObjArgs(callback, py_key, py_value, NULL);
        Py_DECREF(py_key);
        Py_DECREF(py_value);

        if (obj == NULL) {
            PyGILState_Release(__pyx_gilstate_save);
            throw std::runtime_error("PyPredicate: Calling python callback failed");
        }

        // we got a Python object as a result; now let's convert it into a C++ bool
        int res = PyObject_IsTrue(obj);
        Py_DECREF(obj);
        PyGILState_Release(__pyx_gilstate_save);
        if (res == -1) {
            throw std::runtime_error("PyPredicate: Converting to bool failed");
        }
        return (bool) res;
    }

private:
    PyObject* callback;
};

class PyFunctor {
public:

    static inline unary_functor make_unary_functor(PyObject* obj) {
        return PyFunctor(obj);
    }

    static inline binary_functor make_binary_functor(PyObject* obj) {
        return PyFunctor(obj);
    }


    PyFunctor(): callback(NULL) { }

    PyFunctor(PyObject* obj) {
        if (obj == NULL) {
            callback = NULL;
            return;
        }
        PyGILState_STATE __pyx_gilstate_save = PyGILState_Ensure();
        if (!PyCallable_Check(obj)) {
            PyGILState_Release(__pyx_gilstate_save);
            throw std::runtime_error("PyFunctor: obj is not a python callable");
        }
        callback = obj;
        Py_INCREF(obj);
        PyGILState_Release(__pyx_gilstate_save);
    }

    ~PyFunctor() {
        if (callback != NULL) {
            PyGILState_STATE __pyx_gilstate_save = PyGILState_Ensure();
            Py_DECREF(callback);
            PyGILState_Release(__pyx_gilstate_save);
            callback = NULL;
        }
    }

    PyFunctor(const PyFunctor& other) {
        if (other.callback == NULL) {
            callback = NULL;
            return;
        }
        callback = other.callback;
        PyGILState_STATE __pyx_gilstate_save = PyGILState_Ensure();
        Py_INCREF(callback);
        PyGILState_Release(__pyx_gilstate_save);
    }

    PyFunctor& operator=(PyFunctor other) {
        std::swap(callback, other.callback);
        return *this;
    }

    string operator()(const string& key) {
        if (callback == NULL) {
            throw std::runtime_error("The PyFunctor is not initialized");
        }
        PyGILState_STATE __pyx_gilstate_save = PyGILState_Ensure();
        // convert the two strings into python 'bytes' objects (they are new references)
        PyObject* py_key = PyBytes_FromStringAndSize(key.c_str(), (Py_ssize_t) key.size());
        if (py_key == NULL) {
            PyGILState_Release(__pyx_gilstate_save);
            throw std::runtime_error("PyFunctor: PyString_FromStringAndSize failed :(");
        }

        PyObject* obj = PyObject_CallFunctionObjArgs(callback, py_key, NULL);
        Py_DECREF(py_key);

        if (obj == NULL) {
            PyGILState_Release(__pyx_gilstate_save);
            throw std::runtime_error("PyFunctor: Calling python callback failed");
        }

        // we got a Python object as a result; now let's convert it into a C++ string

        if (PyUnicode_Check(obj)) {
            // ooops, the callback returned a unicode object... lets encode it in utf8 first
            PyObject* u_res = PyUnicode_AsUTF8String(obj);
            Py_DECREF(obj);
            if (u_res == NULL) {
                // should not happen
                PyGILState_Release(__pyx_gilstate_save);
                throw std::runtime_error("PyFunctor: converting result to UTF-8 failed :(");
            }
            obj = u_res;
        }
        if (!PyBytes_Check(obj)) {
            // res in not a bytes object... let's try to call bytes(obj)
            PyObject* u_res = PyObject_Bytes(obj);
            Py_DECREF(obj);
            if (u_res == NULL) {
                PyGILState_Release(__pyx_gilstate_save);
                throw std::runtime_error("PyFunctor: converting result to bytes failed :(");
            }
            obj = u_res;
        }
        char* buffer = NULL;
        Py_ssize_t l = 0;
        if (PyBytes_AsStringAndSize(obj, &buffer, &l) == -1) {
            Py_DECREF(obj);
            PyGILState_Release(__pyx_gilstate_save);
            throw std::runtime_error("PyFunctor: converting result to C char* failed :(");
        }
        string s(buffer, l);
        Py_DECREF(obj);
        PyGILState_Release(__pyx_gilstate_save);
        return s;
    }

    string operator()(const string& key, const string& value) {
        if (callback == NULL) {
            throw std::runtime_error("The PyFunctor is not initialized");
        }
        PyGILState_STATE __pyx_gilstate_save = PyGILState_Ensure();
        // convert the two strings into python 'bytes' objects (they are new references)
        PyObject* py_key = PyBytes_FromStringAndSize(key.c_str(), (Py_ssize_t) key.size());
        if (py_key == NULL) {
            PyGILState_Release(__pyx_gilstate_save);
            throw std::runtime_error("PyFunctor: PyString_FromStringAndSize failed :(");
        }
        PyObject* py_value = PyBytes_FromStringAndSize(value.c_str(), (Py_ssize_t) value.size());
        if (py_value == NULL) {
            Py_DECREF(py_key);
            PyGILState_Release(__pyx_gilstate_save);
            throw std::runtime_error("PyFunctor: PyString_FromStringAndSize failed :(");
        }

        PyObject* obj = PyObject_CallFunctionObjArgs(callback, py_key, py_value, NULL);
        Py_DECREF(py_key);
        Py_DECREF(py_value);

        if (obj == NULL) {
            PyGILState_Release(__pyx_gilstate_save);
            throw std::runtime_error("PyFunctor: Calling python callback failed");
        }

        // we got a Python object as a result; now let's convert it into a C++ string

        if (PyUnicode_Check(obj)) {
            // ooops, the callback returned a unicode object... lets encode it in utf8 first
            PyObject* u_res = PyUnicode_AsUTF8String(obj);
            Py_DECREF(obj);
            if (u_res == NULL) {
                // should not happen
                PyGILState_Release(__pyx_gilstate_save);
                throw std::runtime_error("PyFunctor: converting result to UTF-8 failed :(");
            }
            obj = u_res;
        }
        if (!PyBytes_Check(obj)) {
            // res in not a bytes object... let's try to call bytes(obj)
            PyObject* u_res = PyObject_Bytes(obj);
            Py_DECREF(obj);
            if (u_res == NULL) {
                PyGILState_Release(__pyx_gilstate_save);
                throw std::runtime_error("PyFunctor: converting result to bytes failed :(");
            }
            obj = u_res;
        }
        char* buffer = NULL;
        Py_ssize_t l = 0;
        if (PyBytes_AsStringAndSize(obj, &buffer, &l) == -1) {
            Py_DECREF(obj);
            PyGILState_Release(__pyx_gilstate_save);
            throw std::runtime_error("PyFunctor: converting result to C char* failed :(");
        }
        string s(buffer, l);
        Py_DECREF(obj);
        PyGILState_Release(__pyx_gilstate_save);
        return s;
    }

private:
    PyObject* callback;
};

class PyStringInputIterator {
public:
    PyStringInputIterator(): iterator(NULL), empty(true), current_value("") { }

    PyStringInputIterator(PyObject* obj): iterator(NULL), empty(true), current_value("") {
        if (obj == NULL) {
            return;
        }

        PyGILState_STATE __pyx_gilstate_save = PyGILState_Ensure();
        int res = PyIter_Check(obj);
        if (!res) {
            PyGILState_Release(__pyx_gilstate_save);
            throw std::runtime_error("PyStringInputIterator: obj is not an iterator");
        }
        iterator = obj;
        Py_INCREF(obj);
        PyGILState_Release(__pyx_gilstate_save);
        empty = false;
        _next_value();
    }

    ~PyStringInputIterator() {
        if (iterator != NULL) {
            PyGILState_STATE __pyx_gilstate_save = PyGILState_Ensure();
            Py_DECREF(iterator);
            PyGILState_Release(__pyx_gilstate_save);
            iterator = NULL;
        }
    }

    PyStringInputIterator(const PyStringInputIterator& other) {
        if (other.iterator == NULL) {
            iterator = NULL;
            empty = true;
            current_value = "";
            return;
        }
        iterator = other.iterator;
        PyGILState_STATE __pyx_gilstate_save = PyGILState_Ensure();
        Py_INCREF(iterator);
        PyGILState_Release(__pyx_gilstate_save);
        empty = other.empty;
        current_value = other.current_value;
    }

    PyStringInputIterator& operator=(PyStringInputIterator other) {
        std::swap(iterator, other.iterator);
        std::swap(empty, other.empty);
        std::swap(current_value, other.current_value);
        return *this;
    }

    friend bool operator==(const PyStringInputIterator& one, const PyStringInputIterator& other) {
        if (one.iterator == NULL && other.iterator == NULL) {
            return true;
        }
        if (one.empty && other.empty) {
            return true;
        }
        return false;
    }

    friend bool operator!=(const PyStringInputIterator& one, const PyStringInputIterator& other) {
        return !(one==other);
    }

    PyStringInputIterator& operator++() {
        _next_value();
        return *this;
    }

    PyStringInputIterator operator++(int) {
        PyStringInputIterator clone(*this);      // make a copy of iterator
        this->operator++();
        return clone;
    }

    string operator*() {
        return current_value;
    }

protected:
    void _next_value() {
        if (empty) {
            return;
        }
        PyGILState_STATE __pyx_gilstate_save = PyGILState_Ensure();
        PyObject* next_obj = Py_TYPE(iterator)->tp_iternext(iterator);      // new ref
        if (next_obj == NULL) {
            PyObject* exc_type = PyErr_Occurred();
            if (exc_type != NULL) {
                if (exc_type == PyExc_StopIteration || PyErr_GivenExceptionMatches(exc_type, PyExc_StopIteration)) {
                    PyErr_Clear();
                } else {
                    // a python exception occurred, but it's not a StopIteration. let it flow to python code.
                    empty = true;
                    current_value = "";
                    PyGILState_Release(__pyx_gilstate_save);
                    throw runtime_error("python exception happened");
                }
            }
            empty = true;
            current_value = "";
            PyGILState_Release(__pyx_gilstate_save);
            return;
        }

        if (PyUnicode_Check(next_obj)) {
            // ooops, the callback returned a unicode object... lets encode it in utf8 first
            PyObject* next_obj_str = PyUnicode_AsUTF8String(next_obj);
            Py_DECREF(next_obj);
            if (next_obj_str == NULL) {
                // should not happen
                empty = true;
                current_value = "";
                PyGILState_Release(__pyx_gilstate_save);
                throw std::runtime_error("PyIterator: converting result to UTF-8 failed :(");
            }
            next_obj = next_obj_str;
        }

        if (!PyBytes_Check(next_obj)) {
            PyObject* next_obj_str = PyObject_Bytes(next_obj);
            Py_DECREF(next_obj);
            if (next_obj_str == NULL) {
                // conversion to bytes failed
                empty = true;
                current_value = "";
                PyGILState_Release(__pyx_gilstate_save);
                throw runtime_error("python exception happened");
            }
            next_obj = next_obj_str;
        }

        char* buffer = NULL;
        Py_ssize_t length = 0;
        int res = PyBytes_AsStringAndSize(next_obj, &buffer, &length);
        if (res == -1) {
            // should not happen...
            empty = true;
            current_value = "";
            PyGILState_Release(__pyx_gilstate_save);
            throw runtime_error("python exception happened");
        } else {
            current_value = string(buffer, length);
        }
        Py_DECREF(next_obj);
        PyGILState_Release(__pyx_gilstate_save);
    }

    PyObject* iterator;
    bool empty;
    string current_value;
};

} // end namespace quiet
