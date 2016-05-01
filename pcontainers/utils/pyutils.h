#pragma once

#include <stdexcept>
#include <boost/chrono/chrono.hpp>
#include <boost/move/move.hpp>
#include <boost/throw_exception.hpp>
#include <boost/core/noncopyable.hpp>
#include <boost/core/explicit_operator_bool.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <Python.h>
#include "../logging/logging.h"
#include "lmdb.h"
#include "threadsafe_queue.h"

#if PY_MAJOR_VERSION >= 3
#define MyPyInt_Check PyLong_Check
#else
#define MyPyInt_Check PyInt_Check
#endif


namespace utils {

class GilWrapper: private boost::noncopyable {
protected:
    PyGILState_STATE save;
public:
    GilWrapper() BOOST_NOEXCEPT_OR_NOTHROW: save(PyGILState_Ensure()) { }
    ~GilWrapper() { PyGILState_Release(save); }
};

class NoGilWrapper: private boost::noncopyable {
protected:
    PyThreadState* save_ptr;
public:
    NoGilWrapper() BOOST_NOEXCEPT_OR_NOTHROW: save_ptr(PyEval_SaveThread()) { }
    ~NoGilWrapper() { PyEval_RestoreThread(save_ptr); }
};



class PyNewRef {
private:
    BOOST_COPYABLE_AND_MOVABLE(PyNewRef)
protected:
    PyObject* obj;
public:
    BOOST_EXPLICIT_OPERATOR_BOOL_NOEXCEPT()
    bool operator!() const BOOST_NOEXCEPT_OR_NOTHROW { return !obj; }

    PyNewRef() BOOST_NOEXCEPT_OR_NOTHROW: obj(NULL) { }
    explicit PyNewRef(PyObject* o) BOOST_NOEXCEPT_OR_NOTHROW: obj(o) { }
    ~PyNewRef() { reset(); }

    PyObject* get() const BOOST_NOEXCEPT_OR_NOTHROW { return obj; }

    PyNewRef(const PyNewRef& other) BOOST_NOEXCEPT_OR_NOTHROW: obj(other.obj) {                   // Copy constructor
        this->operator++();
    }

    PyNewRef& operator=(BOOST_COPY_ASSIGN_REF(PyNewRef) other) BOOST_NOEXCEPT_OR_NOTHROW {        // Copy assignment
        if (*this != other) {
            reset();
            obj = other.obj;
            this->operator++();
        }
        return *this;
    }

    PyNewRef(BOOST_RV_REF(PyNewRef) other) BOOST_NOEXCEPT_OR_NOTHROW: obj(NULL) {                 // Move constructor
        std::swap(obj, other.obj);
    }

    PyNewRef& operator=(BOOST_RV_REF(PyNewRef) other) BOOST_NOEXCEPT_OR_NOTHROW {                 // Move assignment
        reset();
        std::swap(obj, other.obj);
        return *this;
    }

    void swap(PyNewRef& other) BOOST_NOEXCEPT_OR_NOTHROW {
        std::swap(obj, other.obj);
    }

    friend bool operator==(const PyNewRef& one, const PyNewRef& other) BOOST_NOEXCEPT_OR_NOTHROW {
        return one.obj == other.obj;
    }

    friend bool operator!=(const PyNewRef& one, const PyNewRef& other) BOOST_NOEXCEPT_OR_NOTHROW {
        return one.obj != other.obj;
    }

    PyNewRef& operator++() BOOST_NOEXCEPT_OR_NOTHROW {
        if (obj) {
            Py_INCREF(obj);
        }
        return *this;
    }

    PyNewRef& operator++(int) BOOST_NOEXCEPT_OR_NOTHROW {
        if (obj) {
            Py_INCREF(obj);
        }
        return *this;
    }

    void reset() BOOST_NOEXCEPT_OR_NOTHROW {
        if (obj) {
            Py_DECREF(obj);
        }
        obj = NULL;
    }

};

class PyBufferWrap {        // probably not thread safe
private:
    BOOST_MOVABLE_BUT_NOT_COPYABLE(PyBufferWrap)

    void _set_view() {      // can throw
        if (obj) {
            if (PyUnicode_Check(obj.get())) {
                obj = PyNewRef(PyUnicode_AsUTF8String(obj.get()));
            }
            if (MyPyInt_Check(obj.get()) || PyLong_Check(obj.get()) || PyFloat_Check(obj.get())) {
                obj = PyNewRef(PyObject_Bytes(obj.get()));
            }
            obj_view = (Py_buffer*) PyMem_Malloc(sizeof(Py_buffer));
            if (!obj_view) {
                BOOST_THROW_EXCEPTION(std::bad_alloc());
            }
            if (PyObject_GetBuffer(obj.get(), obj_view, PyBUF_SIMPLE) == -1) {
                PyMem_Free(obj_view);
                obj_view = NULL;
                BOOST_THROW_EXCEPTION(std::runtime_error("PyObject_GetBuffer failed"));
            }

        }
    }

protected:
    PyNewRef obj;
    Py_buffer* obj_view;

    void close() BOOST_NOEXCEPT_OR_NOTHROW {
        if (obj_view) {
            PyBuffer_Release(obj_view);
            PyMem_Free(obj_view);
            obj_view = NULL;
        }
        obj.reset();
    }

public:
    BOOST_EXPLICIT_OPERATOR_BOOL_NOEXCEPT()
    bool operator!() const BOOST_NOEXCEPT_OR_NOTHROW { return !obj_view; }

    PyBufferWrap() BOOST_NOEXCEPT_OR_NOTHROW: obj(), obj_view(NULL) {
        // _LOG_DEBUG << "New trivial PyBufferWrap object";
    }

    explicit PyBufferWrap(PyObject* o): obj(o), obj_view(NULL) {
        // _LOG_DEBUG << "New PyBufferWrap from PyObject*";
        ++obj;          // Py_INCREF
        _set_view();
    }

    PyBufferWrap(const PyNewRef& o): obj(o), obj_view(NULL) {   // PyNewRef copy constructor calls Py_INCREF
        // _LOG_DEBUG << "New PyBufferWrap from PyNewRef";
        _set_view();

    }


    PyBufferWrap(BOOST_RV_REF(PyBufferWrap) other) BOOST_NOEXCEPT_OR_NOTHROW: obj(boost::move(other.obj)), obj_view(other.obj_view) {   // Move constructor
        // _LOG_DEBUG << "PyBufferWrap move constructor";
        other.obj_view = NULL;
    }

    PyBufferWrap& operator=(BOOST_RV_REF(PyBufferWrap) other) BOOST_NOEXCEPT_OR_NOTHROW {  // Move assignment
        // _LOG_DEBUG << "PyBufferWrap move assignment";
        close();    // sets obj and obj_view to NULL
        obj.swap(other.obj);
        std::swap(obj_view, other.obj_view);    // sets other.obj_view to NULL

        return *this;
    }

    friend bool operator==(const PyBufferWrap& one, const PyBufferWrap& other) BOOST_NOEXCEPT_OR_NOTHROW {
        return one.obj == other.obj;
    }

    friend bool operator!=(const PyBufferWrap& one, const PyBufferWrap& other) BOOST_NOEXCEPT_OR_NOTHROW {
        return one.obj != other.obj;
    }

    ~PyBufferWrap() { close(); }

    Py_buffer* get() const BOOST_NOEXCEPT_OR_NOTHROW { return obj_view; }

    MDB_val get_mdb_val() const BOOST_NOEXCEPT_OR_NOTHROW {
        MDB_val v;
        if (obj_view) {
            v.mv_data = obj_view->buf;
            v.mv_size = obj_view->len;
        } else {
            v.mv_data = NULL;
            v.mv_size = 0;
        }
        return v;
    }

    Py_ssize_t length() const BOOST_NOEXCEPT_OR_NOTHROW {
        if (obj_view) {
            return obj_view->len;
        }
        return 0;
    }

    void* buf() const BOOST_NOEXCEPT_OR_NOTHROW {
        if (obj_view) {
            return obj_view->buf;
        }
        return NULL;
    }

};  // END CLASS PyBufferWrap


class py_threadsafe_queue: public threadsafe_queue<PyNewRef> {
private:
    BOOST_MOVABLE_BUT_NOT_COPYABLE(py_threadsafe_queue)

public:
    py_threadsafe_queue() BOOST_NOEXCEPT_OR_NOTHROW { }

    // default destructor is enough
    // on destruction, threadsafe_queue<PyNewRef>'s destructor will be called
    // the deque < shared_ptr < PyNewRef > > will be called
    // the shared_ptr's destructors will be called
    // the PyNewRef objects's destructors will be called
    // and finally DECREF will be called for each PyObject*

    py_threadsafe_queue(BOOST_RV_REF(py_threadsafe_queue) x): threadsafe_queue<PyNewRef>(BOOST_MOVE_BASE(threadsafe_queue<PyNewRef>, x)) { }

    py_threadsafe_queue& operator=(BOOST_RV_REF(py_threadsafe_queue) x) {
        threadsafe_queue<PyNewRef>::operator=(BOOST_MOVE_BASE(threadsafe_queue<PyNewRef>, x));
        return *this;
    }

    virtual PyObject* py_wait_and_pop() {
        boost::shared_ptr<PyNewRef> ptr;
        {
            NoGilWrapper w;
            ptr = threadsafe_queue<PyNewRef>::wait_and_pop();
        }
        if (ptr) {
            ptr->operator++();
            return ptr->get();
        }
        return NULL;
    }

    virtual PyObject* py_wait_and_pop(long long ms) {
        boost::shared_ptr<PyNewRef> ptr;
        {
            NoGilWrapper w;
            ptr = threadsafe_queue<PyNewRef>::wait_and_pop(boost::chrono::milliseconds(ms));
        }
        if (ptr) {
            ptr->operator++();
            return ptr->get();
        }
        return NULL;
    }

    virtual PyObject* py_try_pop() {
        boost::shared_ptr<PyNewRef> ptr(threadsafe_queue<PyNewRef>::try_pop());
        if (ptr) {
            // after the end of py_try_pop, the shared_ptr's reference count will be 0, so
            // PyNewRef's destructor will be called, and DECREF will occur, possibly destroying the Python object
            // so we need to call INCREF to prevent that
            ptr->operator++();  // INCREF
            return ptr->get();
        }
        return NULL;
    }

    virtual void py_push(PyObject* new_value) {
        if (new_value) {
            PyNewRef value(new_value);
            ++value;    // INCREF
            threadsafe_queue<PyNewRef>::push(boost::move(value));
            // value is NULL after 'push', so the destruction of value will not trigger DECREF
        }
    }


};  // END CLASS py_threadsafe_queue

}   // END NS utils
