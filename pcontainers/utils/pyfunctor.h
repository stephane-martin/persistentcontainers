#pragma once


#include <utility>
#include <boost/move/move.hpp>

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
    BOOST_EXPLICIT_OPERATOR_BOOL()
    bool operator!() const { return !bool(callback); }

    PyPredicate(): callback() { }

    static inline unary_predicate make_unary_predicate(PyObject* obj) {
        return unary_predicate(PyPredicate(obj));
    }

    static inline binary_predicate make_binary_predicate(PyObject* obj) {
        return binary_predicate(PyPredicate(obj));
    }

    explicit PyPredicate(PyObject* obj);

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

    bool operator()(const CBString& key);
    bool operator()(const CBString& key, const CBString& value);
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

    static inline binary_scalar_functor make_binary_scalar_functor(PyObject* obj) {
        return binary_scalar_functor(PyFunctor(obj));
    }

    PyFunctor(): callback() { }

    explicit PyFunctor(PyObject* obj);

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

    CBString operator()(const CBString& key);
    CBString operator()(const CBString& key, const CBString& value);

};

class PyStringInputIterator {
private:
    BOOST_MOVABLE_BUT_NOT_COPYABLE(PyStringInputIterator)
protected:
    PyNewRef iterator;
    boost::atomic_bool finished;
    CBString current_value;
public:
    BOOST_EXPLICIT_OPERATOR_BOOL()
    bool operator!() const { return !bool(iterator); }
    PyStringInputIterator(): iterator(), finished(true), current_value("") { }

    explicit PyStringInputIterator(PyObject* obj);

    ~PyStringInputIterator() {
        if (iterator) {
            {
                GilWrapper gil;
                iterator.reset();
            }
        }
    }

    // move constructor
    PyStringInputIterator(BOOST_RV_REF(PyStringInputIterator) other): iterator(), finished(true), current_value() {
        if (other) {
            bool other_is_finished = other.finished.exchange(true);
            iterator = boost::move(other.iterator);
            std::swap(current_value, other.current_value);
            finished.store(other_is_finished);
        }
    }

    PyStringInputIterator& operator=(BOOST_RV_REF(PyStringInputIterator) other) {   // move assignment
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

    friend bool operator==(const PyStringInputIterator& one, const PyStringInputIterator& other) {
        if (!one && !other) {
            return true;
        }
        if (one.finished.load() && other.finished.load()) {
            return true;
        }
        return false;
    }

    friend bool operator!=(const PyStringInputIterator& one, const PyStringInputIterator& other) {
        return !(one==other);
    }

    PyStringInputIterator& operator++() {
        GilWrapper gil;
        _next_value();
        return *this;
    }

    PyStringInputIterator operator++(int) {
        return this->operator++();
    }

    CBString operator*() {
        if (finished.load()) {
            return "";
        }
        return current_value;
    }

protected:
    void _next_value();

}; // END CLASS PyStringInputIterator


class PyFunctionOutputIterator {
protected:
    PyNewRef pyfunction;
public:
    BOOST_EXPLICIT_OPERATOR_BOOL()
    bool operator!() const { return !bool(pyfunction); }

    PyFunctionOutputIterator(): pyfunction() { }

    explicit PyFunctionOutputIterator(PyObject* obj);

    ~PyFunctionOutputIterator() {
        if (pyfunction) {
            GilWrapper gil;
            {
                pyfunction.reset();
            }
        }
    }

    PyFunctionOutputIterator(const PyFunctionOutputIterator& other) {
        GilWrapper gil;
        {
            pyfunction = other.pyfunction;
        }
    }

    PyFunctionOutputIterator& operator=(const PyFunctionOutputIterator& other) {   // move assignment
        GilWrapper gil;
        {
            pyfunction = other.pyfunction;
        }
        return *this;
    }

    PyFunctionOutputIterator& operator*() { return *this; }
    PyFunctionOutputIterator& operator=(const CBString& s);
    PyFunctionOutputIterator& operator=(const pair<const CBString, CBString>& p);



};


} // end namespace quiet
