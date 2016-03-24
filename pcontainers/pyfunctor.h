#pragma once

#include <string>
#include <stdexcept>
#include <utility>
#include <boost/move/move.hpp>
#include <boost/throw_exception.hpp>
#include <boost/core/explicit_operator_bool.hpp>
#include <boost/smart_ptr/detail/spinlock.hpp>
#include <bstrlib/bstrwrap.h>
#include <Python.h>
#include "lmdb_exceptions.h"
#include "utils.h"
#include "pyutils.h"
#include "logging.h"

namespace quiet {

using std::string;
using namespace lmdb;
using namespace utils;
using Bstrlib::CBString;


class PyPredicate {
protected:
    PyNewRef callback;

public:
    BOOST_EXPLICIT_OPERATOR_BOOL()
    bool operator!() const { return !bool(callback); }

    PyPredicate(): callback() { }

    static inline unary_predicate make_unary_predicate(PyObject* obj) {
        return unary_predicate(PyPredicate(obj));
    }

    static inline binary_predicate make_binary_predicate(PyObject* obj) {
        return binary_predicate(PyPredicate(obj));
    }

    explicit PyPredicate(PyObject* obj): callback() {
        if (obj) {
            GilWrapper gil;
            {
                if (!PyCallable_Check(obj)) {
                    BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyPredicate: obj is not a python callable") );
                }
                callback = PyNewRef(obj);
                ++callback;
            }
        } else {
            _LOG_DEBUG << "New trivial PyPredicate object";
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

    friend bool operator==(const PyPredicate& one, const PyPredicate& other) {
        return one.callback == other.callback;
    }

    friend bool operator!=(const PyPredicate& one, const PyPredicate& other) {
        return one.callback != other.callback;
    }


    PyPredicate(const PyPredicate& other): callback() {
        GilWrapper gil;
        {
            callback = other.callback;
        }
    }

    PyPredicate& operator=(const PyPredicate& other) {
        if (*this != other) {
            GilWrapper gil;
            {
                callback = other.callback;
            }
        }
        return *this;
    }

    bool operator()(const CBString& key) {
        if (!*this) {
            BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("This predicate is not initialized") );
        }
        GilWrapper gil;
        {
            // convert the string into a python 'bytes' object (new reference)
            PyNewRef py_key(PyBytes_FromStringAndSize(key, key.length()));
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

    bool operator()(const CBString& key, const CBString& value) {       // for binary predicates
        if (!*this) {
            BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("This predicate is not initialized") );
        }
        GilWrapper gil;
        {
            // convert the two strings into python 'bytes' objects (they are new references)
            PyNewRef py_key(PyBytes_FromStringAndSize(key, key.length()));
            if (!py_key) {
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyPredicate: PyString_FromStringAndSize failed :(") );
            }
            PyNewRef py_value(PyBytes_FromStringAndSize(value, value.length()));
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
    BOOST_EXPLICIT_OPERATOR_BOOL()
    bool operator!() const { return !bool(callback); }

    static inline unary_functor make_unary_functor(PyObject* obj) {
        return unary_functor(PyFunctor(obj));
    }

    static inline binary_functor make_binary_functor(PyObject* obj) {
        return binary_functor(PyFunctor(obj));
    }

    PyFunctor(): callback() { }

    explicit PyFunctor(PyObject* obj): callback() {
        if (obj) {
            GilWrapper gil;
            {
                if (!PyCallable_Check(obj)) {
                    BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyFunctor: obj is not a python callable") );
                }
                callback = PyNewRef(obj);
                ++callback;
            }
        } else {
            _LOG_DEBUG << "New trivial PyFunctor object";
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

    PyFunctor(const PyFunctor& other): callback() {
        GilWrapper gil;
        {
            callback = other.callback;
        }
    }

    friend bool operator==(const PyFunctor& one, const PyFunctor& other) {
        return one.callback == other.callback;
    }

    friend bool operator!=(const PyFunctor& one, const PyFunctor& other) {
        return one.callback != other.callback;
    }


    PyFunctor& operator=(const PyFunctor& other) {
        if (*this != other) {
            GilWrapper gil;
            {
                callback = other.callback;
            }
        }
        return *this;
    }

    CBString operator()(const CBString& key) {
        if (!*this) {
            BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("The PyFunctor is not initialized") );
        }
        char* buffer = NULL;
        Py_ssize_t l = 0;
        GilWrapper gil;
        {
            // convert the two strings into python 'bytes' objects (they are new references)
            PyNewRef py_key(PyBytes_FromStringAndSize(key, key.length()));
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

            if (PyBytes_AsStringAndSize(obj.get(), &buffer, &l) == -1) {
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyFunctor: converting result to C char* failed :(") );
            }
        }
        return CBString(buffer, l);
    }

    CBString operator()(const CBString& key, const CBString& value) {
        if (!*this) {
            BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("The PyFunctor is not initialized") );
        }
        char* buffer = NULL;
        Py_ssize_t l = 0;
        GilWrapper gil;
        {
            // convert the two strings into python 'bytes' objects (they are new references)
            PyNewRef py_key(PyBytes_FromStringAndSize(key, key.length()));
            if (!py_key) {
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyFunctor: PyString_FromStringAndSize failed :(") );
            }
            PyNewRef py_value(PyBytes_FromStringAndSize(value, value.length()));
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
            if (PyBytes_AsStringAndSize(obj.get(), &buffer, &l) == -1) {
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyFunctor: converting result to C char* failed :(") );
            }
        }
        return CBString(buffer, l);
    }

};

class PyStringInputIterator {
private:
    BOOST_MOVABLE_BUT_NOT_COPYABLE(PyStringInputIterator)
public:
    BOOST_EXPLICIT_OPERATOR_BOOL()
    bool operator!() const { return !bool(iterator); }

    PyStringInputIterator(): iterator(), empty(true), current_value("") { }

    explicit PyStringInputIterator(PyObject* obj): iterator(), empty(true), current_value("") {
        if (obj) {
            GilWrapper gil;
            {
                int res = PyIter_Check(obj);
                if (!res) {
                    BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyStringInputIterator: obj is not an iterator") );
                }
                iterator = PyNewRef(obj);
                ++iterator;
                empty = false;
                _next_value();
            }
        } else {
            _LOG_DEBUG << "New trivial PyStringInputIterator object";
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

    PyStringInputIterator(BOOST_RV_REF(PyStringInputIterator) other): iterator(boost::move(other.iterator)), empty(other.empty), current_value(other.current_value) {
        // move constructor (doesnt need the GIL, as iterator(boost::move(other.iterator)) is just a swap of pointers)
        other.empty = true;
        other.current_value = "";
    }

    PyStringInputIterator& operator=(BOOST_RV_REF(PyStringInputIterator) other) {   // move assignment
        current_value = "";
        empty = true;
        GilWrapper gil;
        {
            iterator = boost::move(other.iterator);
            std::swap(empty, other.empty);
            std::swap(current_value, other.current_value);
        }
        return *this;
    }

    friend bool operator==(const PyStringInputIterator& one, const PyStringInputIterator& other) {
        if (!one && !other) {
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
        return this->operator++();
    }

    CBString operator*() {
        return current_value;
    }

protected:
    PyNewRef iterator;
    bool empty;
    CBString current_value;
    boost::detail::spinlock next_value_lock;

    void _next_value() {
        boost::detail::spinlock::scoped_lock slock(next_value_lock);
        if (empty) {
            return;
        }
        GilWrapper gil;
        {
            PyNewRef next_obj(Py_TYPE(iterator.get())->tp_iternext(iterator.get()));      // new ref
            if (!next_obj) {
                PyObject* exc_type = PyErr_Occurred();
                if (exc_type) {
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
                current_value = CBString(buffer, length);
            }
        }
    }


}; // END CLASS PyStringInputIterator

} // end namespace quiet
