#include <stdexcept>
#include <boost/throw_exception.hpp>
#include "../lmdb_exceptions/lmdb_exceptions.h"
#include "pyfunctor.h"
#include "../logging/logging.h"

namespace quiet {
using namespace lmdb;

PyPredicate::PyPredicate(PyObject* obj): callback() {
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


bool PyPredicate::operator()(const CBString& key) {
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

bool PyPredicate::operator()(const CBString& key, const CBString& value) {       // for binary predicates
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

PyFunctor::PyFunctor(PyObject* obj): callback() {
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

CBString PyFunctor::operator()(const CBString& key) {
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

CBString PyFunctor::operator()(const CBString& key, const CBString& value) {
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

PyStringInputIterator::PyStringInputIterator(PyObject* obj): iterator(obj), finished(false), current_value("") {
    if (obj) {
        {
            GilWrapper gil;
            ++iterator;
            if (!PyIter_Check(obj)) {
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyStringInputIterator: obj is not an iterator") );
            }
            _next_value();
        }
    } else {
        finished.store(true);
        _LOG_DEBUG << "New trivial PyStringInputIterator object";
    }
}

void PyStringInputIterator::_next_value() {        // not protected by GilWrapper, have to be careful when calling _next_value

    if (!iterator) {
        return;
    }

    if (finished.load()) {
        return;
    }

    PyNewRef next_obj(Py_TYPE(iterator.get())->tp_iternext(iterator.get()));      // new ref
    if (!next_obj) {
        PyObject* exc_type = PyErr_Occurred();
        if (exc_type) {
            if (exc_type == PyExc_StopIteration || PyErr_GivenExceptionMatches(exc_type, PyExc_StopIteration)) {
                PyErr_Clear();
            } else {
                // a python exception occurred, but it's not a StopIteration. let it flow to python code.
                finished.store(true);
                current_value = "";
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("python exception happened") );
            }
        }
        finished.store(true);
        current_value = "";
        return;
    }

    if (PyUnicode_Check(next_obj.get())) {
        // ooops, the callback returned a unicode object... lets encode it in utf8 first
        next_obj = PyNewRef(PyUnicode_AsUTF8String(next_obj.get()));
        if (!next_obj) {
            // should not happen
            finished.store(true);
            current_value = "";
            BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyIterator: converting result to UTF-8 failed :(") );
        }
    }

    if (!PyBytes_Check(next_obj.get())) {
        next_obj = PyNewRef(PyObject_Bytes(next_obj.get()));
        if (!next_obj) {
            // conversion to bytes failed
            finished.store(true);
            current_value = "";
            BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("python exception happened") );
        }
    }

    char* buffer = NULL;
    Py_ssize_t length = 0;
    int res = PyBytes_AsStringAndSize(next_obj.get(), &buffer, &length);
    if (res == -1) {
        // should not happen...
        finished.store(true);
        current_value = "";
        BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("python exception happened") );
    } else {
        current_value = CBString(buffer, length);
    }
}


PyFunctionOutputIterator::PyFunctionOutputIterator(PyObject* obj): pyfunction(obj) {
    if (obj) {
        GilWrapper gil;
        {
            ++pyfunction;
            int res = PyCallable_Check(obj);
            if (!res) {
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyFunctionOutputIterator: obj is not callable") );
            }
        }
    } else {
        _LOG_DEBUG << "New trivial PyStringInputIterator object";
    }
}

PyFunctionOutputIterator& PyFunctionOutputIterator::operator=(const CBString& s) {
    GilWrapper gil;
    {
        PyNewRef py_s(PyBytes_FromStringAndSize(s, s.length()));
        if (!py_s) {
            BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyFunctionOutputIterator: PyString_FromStringAndSize failed") );
        }
        PyNewRef obj(PyObject_CallFunctionObjArgs(pyfunction.get(), py_s.get(), NULL));
        if (!obj) {
            BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyFunctionOutputIterator: Calling python callback failed") );
        }
    }
    return *this;
}

PyFunctionOutputIterator& PyFunctionOutputIterator::operator=(const pair<const CBString, CBString>& p) {
    GilWrapper gil;
    {
        PyNewRef py_first(PyBytes_FromStringAndSize(p.first, p.first.length()));
        if (!py_first) {
            BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyFunctionOutputIterator: PyString_FromStringAndSize failed") );
        }
        PyNewRef py_second(PyBytes_FromStringAndSize(p.second, p.second.length()));
        if (!py_second) {
            BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyFunctionOutputIterator: PyString_FromStringAndSize failed") );
        }
        PyNewRef obj(PyObject_CallFunctionObjArgs(pyfunction.get(), py_first.get(), py_second.get(), NULL));
        if (!obj) {
            BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("PyFunctionOutputIterator: Calling python callback failed") );
        }


    }
    return *this;
}


} // end namespace quiet
