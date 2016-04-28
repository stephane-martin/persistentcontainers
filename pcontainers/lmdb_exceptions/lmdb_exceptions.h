#pragma once

#include <string>
#include <stdexcept>
#include <boost/exception/all.hpp>
#include "lmdb.h"

namespace lmdb {

using std::string;

using boost::errinfo_nested_exception;
using boost::errinfo_errno;
using boost::error_info;
using boost::copy_exception;
using boost::get_error_info;

string tostring(int x);
string tostring_error(int errnum);

struct exception_base: virtual std::exception, virtual boost::exception {
    typedef error_info<struct what_info, string> what;
    typedef error_info<struct code_info, int> code;
    const string w() const;
};

struct runtime_error: virtual exception_base { };
struct expired: virtual exception_base { };
struct stopping_ops: virtual exception_base { };
struct system_error: virtual runtime_error { };
struct io_error: virtual system_error { };
struct access_error: virtual io_error { };

struct lmdb_nested_error;

struct lmdb_error: virtual exception_base {
    static lmdb_nested_error factory(int code);
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




}   // END NS lmdb
