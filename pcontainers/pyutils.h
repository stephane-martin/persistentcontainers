#pragma once

#include <algorithm>
#include <utility>
#include <stdexcept>
#include <boost/move/move.hpp>
#include <boost/throw_exception.hpp>
#include <boost/core/noncopyable.hpp>
#include <boost/core/explicit_operator_bool.hpp>
#include <Python.h>
#include "logging.h"
#include "lmdb.h"


namespace utils {

class GilWrapper: private boost::noncopyable {
protected:
    PyGILState_STATE save;
public:
    GilWrapper(): save(PyGILState_Ensure()) { }
    ~GilWrapper() { PyGILState_Release(save); }
};


class PyNewRef {
private:
    BOOST_COPYABLE_AND_MOVABLE(PyNewRef)
protected:
    PyObject* obj;
public:
    BOOST_EXPLICIT_OPERATOR_BOOL()
    bool operator!() const { return !obj; }

    PyNewRef(): obj(NULL) { }
    explicit PyNewRef(PyObject* o): obj(o) { }
    ~PyNewRef() { reset(); }

    PyObject* get() const { return obj; }

    PyNewRef(const PyNewRef& other): obj(other.obj) {                   // Copy constructor
        this->operator++();
    }

    PyNewRef& operator=(BOOST_COPY_ASSIGN_REF(PyNewRef) other) {        // Copy assignment
        if (*this != other) {
            reset();
            obj = other.obj;
            this->operator++();
        }
        return *this;
    }

    PyNewRef(BOOST_RV_REF(PyNewRef) other): obj(NULL) {                 // Move constructor
        std::swap(obj, other.obj);
    }

    PyNewRef& operator=(BOOST_RV_REF(PyNewRef) other) {                 // Move assignment
        reset();
        std::swap(obj, other.obj);
        return *this;
    }

    friend bool operator==(const PyNewRef& one, const PyNewRef& other) {
        return one.obj == other.obj;
    }

    friend bool operator!=(const PyNewRef& one, const PyNewRef& other) {
        return one.obj != other.obj;
    }

    PyNewRef& operator++() {
        if (obj) {
            Py_INCREF(obj);
        }
        return *this;
    }

    PyNewRef& operator++(int) {
        if (obj) {
            Py_INCREF(obj);
        }
        return *this;
    }

    void reset() {
        if (obj) {
            Py_DECREF(obj);
        }
        obj = NULL;
    }

};

class PyBufferWrap {
private:
    //BOOST_COPYABLE_AND_MOVABLE(PyBufferWrap)
    BOOST_MOVABLE_BUT_NOT_COPYABLE(PyBufferWrap)

    void _set_view() {
        if (obj) {
            obj_view = (Py_buffer*) PyMem_Malloc(sizeof(Py_buffer));
            if (!obj_view) {
                BOOST_THROW_EXCEPTION(std::bad_alloc());
            }
            // _LOG_DEBUG << "PyBufferWrap new view";
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

public:
    BOOST_EXPLICIT_OPERATOR_BOOL()
    bool operator!() const { return !obj_view; }

    PyBufferWrap(): obj(), obj_view(NULL) {
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


    PyBufferWrap(BOOST_RV_REF(PyBufferWrap) other): obj(boost::move(other.obj)), obj_view(other.obj_view) {   // Move constructor
        // _LOG_DEBUG << "PyBufferWrap move constructor";
        other.obj_view = NULL;
    }

    PyBufferWrap& operator=(BOOST_RV_REF(PyBufferWrap) other) {                     // Move assignment
        // _LOG_DEBUG << "PyBufferWrap move assignment";
        close();    // sets obj and obj_view to NULL
        obj = boost::move(other.obj);   // sets other.obj to NULL
        std::swap(obj_view, other.obj_view);    // sets other.obj_view to NULL

        return *this;
    }

    friend bool operator==(const PyBufferWrap& one, const PyBufferWrap& other) {
        return one.obj == other.obj;
    }

    friend bool operator!=(const PyBufferWrap& one, const PyBufferWrap& other) {
        return one.obj != other.obj;
    }

    ~PyBufferWrap() { close(); }

    Py_buffer* get() const { return obj_view; }

    MDB_val get_mdb_val() const {
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



    Py_ssize_t length() const {
        if (obj_view) {
            return obj_view->len;
        }
        return 0;
    }

    void* buf() const {
        if (obj_view) {
            return obj_view->buf;
        }
        return NULL;
    }

    void close() {
        if (obj_view) {
            // _LOG_DEBUG << "PyBufferWrap releasing view";
            PyBuffer_Release(obj_view);
            PyMem_Free(obj_view);
            obj_view = NULL;
        }
        obj.reset();
    }

};  // END CLASS PyBufferWrap

}   // END NS utils
