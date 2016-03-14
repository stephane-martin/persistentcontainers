#pragma once

#include <string>
#include <stdexcept>
#include <sstream>
#include <iostream>
#include <memory>
#include <vector>
#include <map>
#include <utility>
#include <algorithm>
#include <cerrno>
#include <boost/exception/all.hpp>
#include <string.h>
#include "lmdb.h"

namespace lmdb {

using std::string;
using std::pair;
using std::vector;
using std::map;
using std::cout;
using std::endl;
using std::ostringstream;
using boost::errinfo_nested_exception;
using boost::errinfo_errno;
using boost::copy_exception;

inline string tostring(int x) {
    ostringstream ss;
    ss << x;
    return ss.str();
}

inline string tostring_error(int errnum) {
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

struct exception_base: virtual std::exception, virtual boost::exception {
    typedef boost::error_info<struct what_info, string> what;
    typedef boost::error_info<struct code_info, int> code;

    const string w() const {
        string s;
        const string* s_ptr = boost::get_error_info<exception_base::what>(*this);
        if (s_ptr) {
            s += *s_ptr + " ";
        }
        const int* c_ptr = boost::get_error_info<boost::errinfo_errno>(*this);
        if (c_ptr) {
            s += "(errno code: " + tostring(*c_ptr) + " '" + tostring_error(*c_ptr) + "'" + ")";
        }
        const int* code_ptr = boost::get_error_info<exception_base::code>(*this);
        if (code_ptr) {
            s += "(lmdb code: " + tostring(*code_ptr) + " '" + tostring_error(*code_ptr) + "'" + ")";
        }
        return s;
    }
};

struct runtime_error: virtual exception_base { };
struct system_error: virtual runtime_error { };
struct io_error: virtual system_error { };
struct access_error: virtual io_error { };

struct lmdb_nested_error;

struct lmdb_error: virtual exception_base {
    static inline lmdb_nested_error factory(int code);
};

struct mdb_keyexist: virtual lmdb_error { mdb_keyexist() { *this << lmdb_error::code(MDB_KEYEXIST); } };
struct mdb_notfound: virtual lmdb_error { mdb_notfound() { *this << lmdb_error::code(MDB_NOTFOUND); } };
struct mdb_page_notfound: virtual lmdb_error { mdb_page_notfound() { *this << lmdb_error::code(MDB_PAGE_NOTFOUND); } };
struct mdb_currupted: virtual lmdb_error { mdb_currupted() { *this << lmdb_error::code(MDB_CORRUPTED); } };
struct mdb_panic: virtual lmdb_error { mdb_panic() { *this << lmdb_error::code(MDB_PANIC); } };
struct mdb_version_mismatch: virtual lmdb_error { mdb_version_mismatch() { *this << lmdb_error::code(MDB_VERSION_MISMATCH); } };
struct mdb_invalid: virtual lmdb_error { mdb_invalid() { *this << lmdb_error::code(MDB_INVALID); } };
struct mdb_map_full: virtual lmdb_error { mdb_map_full() { *this << lmdb_error::code(MDB_MAP_FULL); } };
struct mdb_dbs_full: virtual lmdb_error { mdb_dbs_full() { *this << lmdb_error::code(MDB_DBS_FULL); } };
struct mdb_readers_full: virtual lmdb_error { mdb_readers_full() { *this << lmdb_error::code(MDB_READERS_FULL); } };
struct mdb_tls_full: virtual lmdb_error { mdb_tls_full() { *this << lmdb_error::code(MDB_TLS_FULL); } };
struct mdb_txn_full: virtual lmdb_error { mdb_txn_full() { *this << lmdb_error::code(MDB_TXN_FULL); } };
struct mdb_cursor_full: virtual lmdb_error { mdb_cursor_full() { *this << lmdb_error::code(MDB_CURSOR_FULL); } };
struct mdb_page_full: virtual lmdb_error { mdb_page_full() { *this << lmdb_error::code(MDB_PAGE_FULL); } };
struct mdb_map_resized: virtual lmdb_error { mdb_map_resized() { *this << lmdb_error::code(MDB_MAP_RESIZED); } };
struct mdb_incompatible: virtual lmdb_error { mdb_incompatible() { *this << lmdb_error::code(MDB_INCOMPATIBLE); } };
struct mdb_bad_rslot: virtual lmdb_error { mdb_bad_rslot() { *this << lmdb_error::code(MDB_BAD_RSLOT); } };
struct mdb_bad_txn: virtual lmdb_error { mdb_bad_txn() { *this << lmdb_error::code(MDB_BAD_TXN); } };
struct mdb_bad_valsize: virtual lmdb_error { mdb_bad_valsize() { *this << lmdb_error::code(MDB_BAD_VALSIZE); } };
struct mdb_bad_dbi: virtual lmdb_error { mdb_bad_dbi() { *this << lmdb_error::code(MDB_BAD_DBI); } };

struct empty_key: virtual mdb_bad_valsize { empty_key() { *this << lmdb_error::code(MDB_BAD_VALSIZE); } };
struct empty_database: virtual mdb_notfound { empty_database() { *this << lmdb_error::code(MDB_NOTFOUND); } };

struct not_initialized: virtual lmdb_error { };

struct lmdb_nested_error: virtual lmdb_error { };

lmdb_nested_error lmdb_error::factory(int code) {
    lmdb_nested_error exc;
    io_error io_ex;
    access_error access_ex;
    switch (code) {
        case MDB_KEYEXIST:
            exc << errinfo_nested_exception(copy_exception(mdb_keyexist()));
            break;
        case MDB_NOTFOUND:
            exc << errinfo_nested_exception(copy_exception(mdb_notfound()));
            break;
        case MDB_PAGE_NOTFOUND:
            exc << errinfo_nested_exception(copy_exception(mdb_page_notfound()));
            break;
        case MDB_CORRUPTED:
            exc << errinfo_nested_exception(copy_exception(mdb_currupted()));
            break;
        case MDB_PANIC:
            exc << errinfo_nested_exception(copy_exception(mdb_panic()));
            break;
        case MDB_VERSION_MISMATCH:
            exc << errinfo_nested_exception(copy_exception(mdb_version_mismatch()));
            break;
        case MDB_INVALID:
            exc << errinfo_nested_exception(copy_exception(mdb_invalid()));
            break;
        case MDB_MAP_FULL:
            exc << errinfo_nested_exception(copy_exception(mdb_map_full()));
            break;
        case MDB_DBS_FULL:
            exc << errinfo_nested_exception(copy_exception(mdb_dbs_full()));
            break;
        case MDB_READERS_FULL:
            exc << errinfo_nested_exception(copy_exception(mdb_readers_full()));
            break;
        case MDB_TLS_FULL:
            exc << errinfo_nested_exception(copy_exception(mdb_tls_full()));
            break;
        case MDB_TXN_FULL:
            exc << errinfo_nested_exception(copy_exception(mdb_txn_full()));
            break;
        case MDB_CURSOR_FULL:
            exc << errinfo_nested_exception(copy_exception(mdb_cursor_full()));
            break;
        case MDB_PAGE_FULL:
            exc << errinfo_nested_exception(copy_exception(mdb_page_full()));
            break;
        case MDB_MAP_RESIZED:
            exc << errinfo_nested_exception(copy_exception(mdb_map_resized()));
            break;
        case MDB_INCOMPATIBLE:
            exc << errinfo_nested_exception(copy_exception(mdb_incompatible()));
            break;
        case MDB_BAD_RSLOT:
            exc << errinfo_nested_exception(copy_exception(mdb_bad_rslot()));
            break;
        case MDB_BAD_TXN:
            exc << errinfo_nested_exception(copy_exception(mdb_bad_txn()));
            break;
        case MDB_BAD_VALSIZE:
            exc << errinfo_nested_exception(copy_exception(mdb_bad_valsize()));
            break;
        case MDB_BAD_DBI:
            exc << errinfo_nested_exception(copy_exception(mdb_bad_dbi()));
            break;
        case ENOMEM:
            exc << errinfo_nested_exception(copy_exception(std::bad_alloc()));
            break;
        case EINVAL:
            exc << errinfo_nested_exception(copy_exception(std::invalid_argument("invalid LMDB argument")));
            break;
        case EIO:
            io_ex << lmdb_error::what("a low-level I/O error occurred in LMDB");
            exc << errinfo_nested_exception(copy_exception(io_ex));
            break;
        case ENOSPC:
            io_ex << lmdb_error::what("no more disk space");
            exc << errinfo_nested_exception(copy_exception(io_ex));
            break;
        case ENOENT:
            io_ex << lmdb_error::what("the directory specified by the path parameter does not exist");
            exc << errinfo_nested_exception(copy_exception(io_ex));
            break;
        case EAGAIN:
            io_ex << lmdb_error::what("the environment was locked by another process");
            exc << errinfo_nested_exception(copy_exception(io_ex));
            break;
        case EACCES:
            access_ex << lmdb_error::what("access denied");
            exc << errinfo_nested_exception(copy_exception(access_ex));
            break;
        case EBUSY:
            io_ex << lmdb_error::what("EBUSY");
            exc << errinfo_nested_exception(copy_exception(io_ex));
            break;
        default:
            ;
    }   // end switch
    return exc;

}   // END lmdb_error::factory


}   // END NS lmdb
