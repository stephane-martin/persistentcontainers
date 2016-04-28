#include <sstream>
#include <iostream>
#include <string>
#include <stdexcept>
#include <boost/exception/all.hpp>

#include "lmdb_exceptions.h"

namespace lmdb {

using std::string;
using std::cout;
using std::endl;
using std::ostringstream;

using boost::errinfo_nested_exception;
using boost::errinfo_errno;
using boost::error_info;
using boost::copy_exception;
using boost::get_error_info;


string tostring(int x) {
    ostringstream ss;
    ss << x;
    return ss.str();
}

string tostring_error(int errnum) {
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

const string exception_base::w() const {
    string s;
    const string* s_ptr = get_error_info<what>(*this);
    if (s_ptr) {
        s += *s_ptr + " ";
    }
    const int* c_ptr = get_error_info<errinfo_errno>(*this);
    if (c_ptr) {
        s += "(errno code: " + tostring(*c_ptr) + " '" + tostring_error(*c_ptr) + "'" + ")";
    }
    const int* code_ptr = get_error_info<code>(*this);
    if (code_ptr) {
        s += "(lmdb code: " + tostring(*code_ptr) + " '" + tostring_error(*code_ptr) + "'" + ")";
    }
    return s;
}

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
