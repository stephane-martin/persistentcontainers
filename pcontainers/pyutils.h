#pragma once

#include <algorithm>
#include <utility>
#include <boost/move/move.hpp>
#include <Python.h>

namespace utils {


class GilWrapper {
private:
    GilWrapper(const GilWrapper& other);
    GilWrapper& operator=(const GilWrapper& other);
protected:
    PyGILState_STATE save;
public:
    GilWrapper(): save(PyGILState_Ensure()) { }
    ~GilWrapper() { PyGILState_Release(save); }
};

class PyNewRef {
private:
    BOOST_MOVABLE_BUT_NOT_COPYABLE(PyNewRef)
protected:
    PyObject* obj;
public:
    PyNewRef(): obj(NULL) { }

    PyNewRef(PyObject* o): obj(o) { }

    PyObject* get() const { return obj; }

    PyNewRef(BOOST_RV_REF(PyNewRef) other) {
        obj = other.obj;
        other.obj = NULL;
    }

    PyNewRef& operator=(BOOST_RV_REF(PyNewRef) other) {
        reset();
        std::swap(obj, other.obj);
        return *this;
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

    ~PyNewRef() {
        reset();
    }

    operator bool() const { return obj != NULL; }

};

}   // END NS utils
