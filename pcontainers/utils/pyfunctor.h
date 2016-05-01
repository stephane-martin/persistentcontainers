#pragma once


#include <utility>
#include <boost/move/move.hpp>
#include <boost/thread/future.hpp>
#include <boost/core/explicit_operator_bool.hpp>
#include <boost/atomic.hpp>
#include <bstrlib/bstrwrap.h>
#include <Python.h>

#include "utils.h"
#include "pyutils.h"


namespace quiet {

using namespace utils;
using Bstrlib::CBString;
using std::pair;


class PyPredicate {
protected:
    PyNewRef callback;

public:
    BOOST_EXPLICIT_OPERATOR_BOOL_NOEXCEPT()
    bool operator!() const BOOST_NOEXCEPT_OR_NOTHROW { return !bool(callback); }

    PyPredicate() BOOST_NOEXCEPT_OR_NOTHROW : callback() { }

    static inline unary_predicate make_unary_predicate(PyObject* obj) {     // can throw
        return unary_predicate(PyPredicate(obj));
    }

    static inline binary_predicate make_binary_predicate(PyObject* obj) {   // can throw
        return binary_predicate(PyPredicate(obj));
    }

    explicit PyPredicate(PyObject* obj);    // can throw

    ~PyPredicate() {
        if (callback) {
            GilWrapper gil;
            {
                callback.reset();
            }
        }
    }

    friend bool operator==(const PyPredicate& one, const PyPredicate& other) BOOST_NOEXCEPT_OR_NOTHROW {
        return one.callback == other.callback;
    }

    friend bool operator!=(const PyPredicate& one, const PyPredicate& other) BOOST_NOEXCEPT_OR_NOTHROW {
        return one.callback != other.callback;
    }


    PyPredicate(const PyPredicate& other) BOOST_NOEXCEPT_OR_NOTHROW: callback()  {
        GilWrapper gil;
        {
            callback = other.callback;
        }
    }

    PyPredicate& operator=(const PyPredicate& other) BOOST_NOEXCEPT_OR_NOTHROW {
        if (*this != other) {
            GilWrapper gil;
            {
                callback = other.callback;
            }
        }
        return *this;
    }

    bool operator()(const CBString& key);                               // can throw
    bool operator()(const CBString& key, const CBString& value);        // can throw
};

class PyFunctor {
private:
    PyNewRef callback;
public:
    BOOST_EXPLICIT_OPERATOR_BOOL_NOEXCEPT()
    bool operator!() const BOOST_NOEXCEPT_OR_NOTHROW { return !bool(callback); }

    static inline unary_functor make_unary_functor(PyObject* obj) {     // can throw
        return unary_functor(PyFunctor(obj));
    }

    static inline binary_scalar_functor make_binary_scalar_functor(PyObject* obj) {     // can throw
        return binary_scalar_functor(PyFunctor(obj));
    }

    PyFunctor() BOOST_NOEXCEPT_OR_NOTHROW: callback() { }

    explicit PyFunctor(PyObject* obj);      // can throw

    ~PyFunctor() {
        if (callback) {
            GilWrapper gil;
            {
                callback.reset();
            }
        }
    }

    PyFunctor(const PyFunctor& other) BOOST_NOEXCEPT_OR_NOTHROW: callback() {
        GilWrapper gil;
        {
            callback = other.callback;
        }
    }

    friend bool operator==(const PyFunctor& one, const PyFunctor& other) BOOST_NOEXCEPT_OR_NOTHROW {
        return one.callback == other.callback;
    }

    friend bool operator!=(const PyFunctor& one, const PyFunctor& other) BOOST_NOEXCEPT_OR_NOTHROW {
        return one.callback != other.callback;
    }


    PyFunctor& operator=(const PyFunctor& other) BOOST_NOEXCEPT_OR_NOTHROW {
        if (*this != other) {
            GilWrapper gil;
            {
                callback = other.callback;
            }
        }
        return *this;
    }

    CBString operator()(const CBString& key);                               // can throw
    CBString operator()(const CBString& key, const CBString& value);        // can throw

};

class PyStringInputIterator {
private:
    BOOST_MOVABLE_BUT_NOT_COPYABLE(PyStringInputIterator)
protected:
    PyNewRef iterator;
    boost::atomic_bool finished;
    CBString current_value;
    void _next_value();             // can throw
public:
    BOOST_EXPLICIT_OPERATOR_BOOL_NOEXCEPT()
    bool operator!() const BOOST_NOEXCEPT_OR_NOTHROW { return !bool(iterator); }
    PyStringInputIterator() BOOST_NOEXCEPT_OR_NOTHROW : iterator(), finished(true), current_value("") { }

    explicit PyStringInputIterator(PyObject* obj);      // can throw

    ~PyStringInputIterator() {
        if (iterator) {
            {
                GilWrapper gil;
                iterator.reset();
            }
        }
    }

    // move constructor
    PyStringInputIterator(BOOST_RV_REF(PyStringInputIterator) other) BOOST_NOEXCEPT_OR_NOTHROW: iterator(), finished(true), current_value() {
        if (other) {
            bool other_is_finished = other.finished.exchange(true);
            iterator = boost::move(other.iterator);
            std::swap(current_value, other.current_value);
            finished.store(other_is_finished);
        }
    }

    PyStringInputIterator& operator=(BOOST_RV_REF(PyStringInputIterator) other) BOOST_NOEXCEPT_OR_NOTHROW {   // move assignment
        finished.store(true);
        current_value = "";
        iterator.reset();
        if (other) {
            bool other_is_finished = other.finished.exchange(true);
            iterator = boost::move(other.iterator);
            std::swap(current_value, other.current_value);
            finished.store(other_is_finished);
        }
        return *this;
    }

    friend bool operator==(const PyStringInputIterator& one, const PyStringInputIterator& other) BOOST_NOEXCEPT_OR_NOTHROW {
        if (!one && !other) {
            return true;
        }
        if (one.finished.load() && other.finished.load()) {
            return true;
        }
        return false;
    }

    friend bool operator!=(const PyStringInputIterator& one, const PyStringInputIterator& other) BOOST_NOEXCEPT_OR_NOTHROW {
        return !(one==other);
    }

    PyStringInputIterator& operator++() {       // can throw
        GilWrapper gil;
        _next_value();
        return *this;
    }

    PyStringInputIterator operator++(int) {     // can throw
        return this->operator++();
    }

    CBString operator*() BOOST_NOEXCEPT_OR_NOTHROW {
        if (finished.load()) {
            return "";
        }
        return current_value;
    }

}; // END CLASS PyStringInputIterator


class PyFunctionOutputIterator {
protected:
    PyNewRef pyfunction;
public:
    BOOST_EXPLICIT_OPERATOR_BOOL_NOEXCEPT()
    bool operator!() const BOOST_NOEXCEPT_OR_NOTHROW { return !bool(pyfunction); }

    PyFunctionOutputIterator() BOOST_NOEXCEPT_OR_NOTHROW: pyfunction() { }

    explicit PyFunctionOutputIterator(PyObject* obj);   // can throw

    ~PyFunctionOutputIterator() {
        if (pyfunction) {
            GilWrapper gil;
            {
                pyfunction.reset();
            }
        }
    }

    PyFunctionOutputIterator(const PyFunctionOutputIterator& other) BOOST_NOEXCEPT_OR_NOTHROW {
        GilWrapper gil;
        {
            pyfunction = other.pyfunction;
        }
    }

    PyFunctionOutputIterator& operator=(const PyFunctionOutputIterator& other) BOOST_NOEXCEPT_OR_NOTHROW {
        GilWrapper gil;
        {
            pyfunction = other.pyfunction;
        }
        return *this;
    }

    PyFunctionOutputIterator& operator*() BOOST_NOEXCEPT_OR_NOTHROW { return *this; }
    PyFunctionOutputIterator& operator=(const CBString& s);                             // can throw
    PyFunctionOutputIterator& operator=(const pair<const CBString, CBString>& p);       // can throw

};

template <typename T>
class PyFutureCBCB {
protected:
    PyNewRef pyfunction;
public:
    BOOST_EXPLICIT_OPERATOR_BOOL_NOEXCEPT()
    bool operator!() const BOOST_NOEXCEPT_OR_NOTHROW { return !bool(pyfunction); }

    PyFutureCBCB() BOOST_NOEXCEPT_OR_NOTHROW: pyfunction() { }

    // todo: check that obj is a Python callable
    explicit PyFutureCBCB(PyObject* obj): pyfunction(obj) {
        if (obj) {
            GilWrapper gil;
            {
                ++pyfunction;
            }
        }
    }

    ~PyFutureCBCB() {
        if (pyfunction) {
            GilWrapper gil;
            {
                pyfunction.reset();
            }
        }
    }

    PyFutureCBCB(const PyFutureCBCB& other) BOOST_NOEXCEPT_OR_NOTHROW {
        GilWrapper gil;
        {
            pyfunction = other.pyfunction;
        }
    }

    PyFutureCBCB& operator=(const PyFutureCBCB& other) BOOST_NOEXCEPT_OR_NOTHROW {
        GilWrapper gil;
        {
            pyfunction = other.pyfunction;
        }
        return *this;
    }

    T operator()(boost::shared_future<T>& f) {
        _LOG_DEBUG << "PyFutureCBCB: calling the continuation";
        GilWrapper gil;
        {
            // todo: check for error codes
            PyNewRef obj(PyObject_CallFunctionObjArgs(pyfunction.get(), NULL));
        }
        return T();
    }

};

template <typename T>
class then_callback {
public:
    typedef boost::function < T (boost::shared_future < T > &) > typ;
};

template <typename T>
typename then_callback<T>::typ make_then_callback(PyObject* obj) {
    return PyFutureCBCB<T>(obj);
}

template <typename T>
boost::shared_future<T> then_(boost::shared_future<T>& fut, PyObject* obj) {        // can throw
    return fut.then(make_then_callback<T>(obj)).share();
}

} // end namespace quiet
