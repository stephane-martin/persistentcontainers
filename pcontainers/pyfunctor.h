#pragma once

#include <string>
#include <stdexcept>
#include <utility>
#include <boost/throw_exception.hpp>
#include <Python.h>
#include "lmdb_exceptions.h"
#include "utils.h"
#include "pyutils.h"
#include "logging.h"

namespace quiet {

using std::string;
using namespace lmdb;
using namespace utils;


class PyPredicate {
protected:
    PyNewRef callback;

public:
    PyPredicate(): callback(PyNewRef()) { }

    static inline unary_predicate make_unary_predicate(PyObject* obj) {
        return PyPredicate(obj);
    }

    static inline binary_predicate make_binary_predicate(PyObject* obj) {
        return PyPredicate(obj);
    }

    PyPredicate(PyObject* obj) {
        if (obj == NULL) {
            callback = PyNewRef();
            _LOG_DEBUG << "new trivial PyPredicate object";
            return;
        }
        GilWrapper gil;
        {
            if (!PyCallable_Check(obj)) {
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyPredicate: obj is not a python callable") );
            }
            Py_INCREF(obj);
            callback = PyNewRef(obj);
        }
        if (!callback) {
            _LOG_WARNING << "'callback' has not been correctly initialized";
        }
    }

    ~PyPredicate() {
        if (callback) {
            GilWrapper gil;
            {
                callback.reset();
            }
        }
    }

    PyPredicate(const PyPredicate& other) {
        if (!other.callback) {
            callback = PyNewRef();
            return;
        }

        GilWrapper gil;
        {
            callback = PyNewRef(other.callback.get());
            if (callback) {
                Py_INCREF(callback.get());
            }
        }
    }

    PyPredicate& operator=(PyPredicate other) {
        GilWrapper gil;
        {
            callback = PyNewRef(other.callback.get());
            if (callback) {
                Py_INCREF(callback.get());
            }
            return *this;
        }
    }

    bool operator()(const string& key) {
        if (!callback) {
            BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("This predicate is not initialized") );
        }
        GilWrapper gil;
        {
            // convert the two strings into python 'bytes' objects (they are new references)
            PyNewRef py_key(PyBytes_FromStringAndSize(key.c_str(), (Py_ssize_t) key.size()));
            if (!py_key) {
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyPredicate: PyString_FromStringAndSize failed") );
            }

            if (!callback) {
                _LOG_WARNING << "callback is empty, prepare for segfault";
            }
            PyNewRef obj(PyObject_CallFunctionObjArgs(callback.get(), py_key.get(), NULL));

            if (!obj) {
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyPredicate: Calling python callback failed") );
            }

            // we got a Python object as a result; now let's convert it into a C++ bool
            int res = PyObject_IsTrue(obj.get());
            if (res == -1) {
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyPredicate: Converting to bool failed") );
            }
            return (bool) res;
        }
    }

    bool operator()(const string& key, const string& value) {       // for binary predicates
        if (!callback) {
            BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("This predicate is not initialized") );
        }
        GilWrapper gil;
        {
            // convert the two strings into python 'bytes' objects (they are new references)
            PyNewRef py_key(PyBytes_FromStringAndSize(key.c_str(), (Py_ssize_t) key.size()));
            if (!py_key) {
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyPredicate: PyString_FromStringAndSize failed :(") );
            }
            PyNewRef py_value(PyBytes_FromStringAndSize(value.c_str(), (Py_ssize_t) value.size()));
            if (!py_value) {
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyPredicate: PyString_FromStringAndSize failed :(") );
            }
            if (!callback) {
                _LOG_WARNING << "callback is empty, prepare for segfault";
            }
            //PyObject_CallFunctionObjArgs(callback.get(), py_key.get(), py_value.get(), NULL);
            PyNewRef obj(PyObject_CallFunctionObjArgs(callback.get(), py_key.get(), py_value.get(), NULL));

            if (!obj) {
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyPredicate: Calling python callback failed") );
            }

            // we got a Python object as a result; now let's convert it into a C++ bool
            int res = PyObject_IsTrue(obj.get());
            if (res == -1) {
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyPredicate: Converting to bool failed") );
            }
            return (bool) res;
        }
    }
};

class PyFunctor {
private:
    PyNewRef callback;
public:

    static inline unary_functor make_unary_functor(PyObject* obj) {
        return PyFunctor(obj);
    }

    static inline binary_functor make_binary_functor(PyObject* obj) {
        return PyFunctor(obj);
    }

    PyFunctor(): callback(PyNewRef()) { }

    PyFunctor(PyObject* obj) {
        if (!obj) {
            callback = PyNewRef();
            return;
        }
        GilWrapper gil;
        {
            if (!PyCallable_Check(obj)) {
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyFunctor: obj is not a python callable") );
            }
            callback = PyNewRef(obj);
            Py_INCREF(obj);
        }
    }

    ~PyFunctor() {
        if (callback) {
            GilWrapper gil;
            {
                callback.reset();
            }
        }
    }

    PyFunctor(const PyFunctor& other) {
        if (!other.callback) {
            callback = PyNewRef();
            return;
        }
        GilWrapper gil;
        {
            callback = PyNewRef(other.callback.get());
            if (callback) {
                Py_INCREF(callback.get());
            }
        }
    }

    PyFunctor& operator=(PyFunctor other) {
        GilWrapper gil;
        {
            callback = PyNewRef(other.callback.get());
            if (callback) {
                Py_INCREF(callback.get());
            }
            return *this;
        }
    }

    string operator()(const string& key) {
        if (!callback) {
            BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("The PyFunctor is not initialized") );
        }
        GilWrapper gil;
        {
            // convert the two strings into python 'bytes' objects (they are new references)
            PyNewRef py_key(PyBytes_FromStringAndSize(key.c_str(), (Py_ssize_t) key.size()));
            if (!py_key) {
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyFunctor: PyString_FromStringAndSize failed :(") );
            }

            PyNewRef obj(PyObject_CallFunctionObjArgs(callback.get(), py_key.get(), NULL));

            if (!obj) {
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyFunctor: Calling python callback failed") );
            }

            // we got a Python object as a result; now let's convert it into a C++ string

            if (PyUnicode_Check(obj.get())) {
                // ooops, the callback returned a unicode object... lets encode it in utf8 first
                obj = PyNewRef(PyUnicode_AsUTF8String(obj.get()));
                if (!obj) {
                    // should not happen
                    BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyFunctor: converting result to UTF-8 failed :(") );
                }
            }
            if (!PyBytes_Check(obj.get())) {
                // res in not a bytes object... let's try to call bytes(obj)
                obj = PyNewRef(PyObject_Bytes(obj.get()));
                if (!obj) {
                    BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyFunctor: converting result to bytes failed :(") );
                }
            }
            char* buffer = NULL;
            Py_ssize_t l = 0;
            if (PyBytes_AsStringAndSize(obj.get(), &buffer, &l) == -1) {
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyFunctor: converting result to C char* failed :(") );
            }
            string s(buffer, l);
            return s;
        }
    }

    string operator()(const string& key, const string& value) {
        if (!callback) {
            BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("The PyFunctor is not initialized") );
        }
        GilWrapper gil;
        {
            // convert the two strings into python 'bytes' objects (they are new references)
            PyNewRef py_key(PyBytes_FromStringAndSize(key.c_str(), (Py_ssize_t) key.size()));
            if (!py_key) {
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyFunctor: PyString_FromStringAndSize failed :(") );
            }
            PyNewRef py_value(PyBytes_FromStringAndSize(value.c_str(), (Py_ssize_t) value.size()));
            if (!py_value) {
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyFunctor: PyString_FromStringAndSize failed :(") );
            }

            PyNewRef obj(PyObject_CallFunctionObjArgs(callback.get(), py_key.get(), py_value.get(), NULL));

            if (!obj) {
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyFunctor: Calling python callback failed") );
            }

            // we got a Python object as a result; now let's convert it into a C++ string

            if (PyUnicode_Check(obj.get())) {
                // ooops, the callback returned a unicode object... lets encode it in utf8 first
                obj = PyNewRef(PyUnicode_AsUTF8String(obj.get()));
                if (!obj) {
                    // should not happen
                    BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyFunctor: converting result to UTF-8 failed :(") );
                }
            }
            if (!PyBytes_Check(obj.get())) {
                // res in not a bytes object... let's try to call bytes(obj)
                obj = PyNewRef(PyObject_Bytes(obj.get()));
                if (!obj) {
                    BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyFunctor: converting result to bytes failed :(") );
                }
            }
            char* buffer = NULL;
            Py_ssize_t l = 0;
            if (PyBytes_AsStringAndSize(obj.get(), &buffer, &l) == -1) {
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyFunctor: converting result to C char* failed :(") );
            }
            string s(buffer, l);
            return s;
        }
    }

};

class PyStringInputIterator {
public:
    PyStringInputIterator(): iterator(PyNewRef()), empty(true), current_value("") { }

    PyStringInputIterator(PyObject* obj): empty(true), current_value("") {
        if (!obj) {
            iterator = PyNewRef();
            return;
        }
        GilWrapper gil;
        {
            int res = PyIter_Check(obj);
            if (!res) {
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyStringInputIterator: obj is not an iterator") );
            }
            iterator = PyNewRef(obj);
            Py_INCREF(obj);
            empty = false;
            _next_value();
        }
    }

    ~PyStringInputIterator() {
        if (iterator) {
            GilWrapper gil;
            {
                iterator.reset();
            }
        }
    }

    PyStringInputIterator(const PyStringInputIterator& other) {
        GilWrapper gil;
        {
            iterator = PyNewRef(other.iterator.get());
            if (iterator) {
                Py_INCREF(iterator.get());
            }
            empty = other.empty;
            current_value = other.current_value;
        }
    }

    PyStringInputIterator& operator=(PyStringInputIterator other) {
        GilWrapper gil;
        {
            iterator = PyNewRef(other.iterator.get());
            if (iterator) {
                Py_INCREF(iterator.get());
            }
            empty = other.empty;
            current_value = other.current_value;
            return *this;
        }
    }

    friend bool operator==(const PyStringInputIterator& one, const PyStringInputIterator& other) {
        if (!(one.iterator) && !(other.iterator)) {
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
        GilWrapper gil;
        {
            PyNewRef next_obj(Py_TYPE(iterator.get())->tp_iternext(iterator.get()));      // new ref
            if (!next_obj) {
                PyObject* exc_type = PyErr_Occurred();
                if (exc_type != NULL) {
                    if (exc_type == PyExc_StopIteration || PyErr_GivenExceptionMatches(exc_type, PyExc_StopIteration)) {
                        PyErr_Clear();
                    } else {
                        // a python exception occurred, but it's not a StopIteration. let it flow to python code.
                        empty = true;
                        current_value = "";
                        BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("python exception happened") );
                    }
                }
                empty = true;
                current_value = "";
                return;
            }

            if (PyUnicode_Check(next_obj.get())) {
                // ooops, the callback returned a unicode object... lets encode it in utf8 first
                next_obj = PyNewRef(PyUnicode_AsUTF8String(next_obj.get()));
                if (!next_obj) {
                    // should not happen
                    empty = true;
                    current_value = "";
                    BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyIterator: converting result to UTF-8 failed :(") );
                }
            }

            if (!PyBytes_Check(next_obj.get())) {
                next_obj = PyNewRef(PyObject_Bytes(next_obj.get()));
                if (!next_obj) {
                    // conversion to bytes failed
                    empty = true;
                    current_value = "";
                    BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("python exception happened") );
                }
            }

            char* buffer = NULL;
            Py_ssize_t length = 0;
            int res = PyBytes_AsStringAndSize(next_obj.get(), &buffer, &length);
            if (res == -1) {
                // should not happen...
                empty = true;
                current_value = "";
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("python exception happened") );
            } else {
                current_value = string(buffer, length);
            }
        }
    }

    PyNewRef iterator;
    bool empty;
    string current_value;
};

} // end namespace quiet
