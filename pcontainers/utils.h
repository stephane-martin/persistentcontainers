#pragma once

#include <sstream>
#include <iostream>
#include <iomanip>
#include <string>
#include <utility>
#include <boost/function.hpp>
#include <string.h>
#include <Python.h>
#include "lmdb.h"

namespace utils {

using std::string;
using std::setfill;
using std::setw;
using std::ostringstream;

typedef boost::function<bool (const string& x)> unary_predicate;
typedef boost::function<bool (const string& x, const string& y)> binary_predicate;
typedef boost::function<string (const string& x)> unary_functor;
typedef boost::function<string (const string& x, const string& y)> binary_functor;

inline bool binary_true_pred(const string& s1, const string& s2) {
    return true;
}

inline MDB_val make_mdb_val() {
    MDB_val m;
    m.mv_data = NULL;
    m.mv_size = 0;
    return m;
}

inline MDB_val make_mdb_val(const string& s) {
    MDB_val m;
    m.mv_data = (void*) s.c_str();
    m.mv_size = s.length();
    return m;
}

inline string make_string(const MDB_val& m) {
    return string((char*) m.mv_data, m.mv_size);
}

template <typename T>
inline string any_tostring(T x, int n=0) {
    ostringstream ss;
    if (n > 0) {
        ss << setfill('0') << setw(n);
    }
    ss << x;
    return ss.str();
}

inline string error2string(int errnum) {
    if (errnum == 0) {
        return "";
    }
	if (errnum >= MDB_KEYEXIST && errnum <= MDB_LAST_ERRCODE) {
		return string(mdb_strerror(errnum));
	}
	char buf[1024] = "";
	strerror_r(errnum, buf, 1023);
	return string(buf);
}

inline void PyErr_SetCppString(PyObject *type, const string& message) {
    PyErr_SetString(type, message.c_str());
}


} // END NS utils

