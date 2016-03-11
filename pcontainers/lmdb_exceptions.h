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
#include "lmdb.h"

namespace lmdb {

using std::string;
using std::pair;
using std::vector;
using std::map;
using std::cout;
using std::endl;
using std::exception;
using std::runtime_error;



class system_error: public runtime_error {
public:
    system_error(const string& w=""): runtime_error(w) { }
};

class io_error: public system_error {
public:
    io_error(const string& w=""): system_error(w) { }
};

class lmdb_error: public runtime_error {
public:
    string reason;
    virtual int code() const { return 0; }

    lmdb_error(const char* w): runtime_error(string(w)) {
        reason = string(w);
    }

    lmdb_error(const string& w=""): runtime_error(w) {
        reason = w;
    }

    ~lmdb_error() throw (){}

    virtual const char* what() const throw () {
        if (!reason.empty()) {
            return reason.c_str();
        }
        if (code() != 0) {
            return (const char*) mdb_strerror(code());
        }
        return "";
    }

    static inline exception factory(int code);
};

class access_error: public lmdb_error {
public:
    int code() const { return EACCES; }
    access_error(const string& w=""): lmdb_error(w) {}
};

class mdb_keyexist: public lmdb_error {
public:
    int code() const { return MDB_KEYEXIST; }
    mdb_keyexist(const string& w=""): lmdb_error(w) { }
};

class mdb_notfound: public lmdb_error {
public:
    int code() const { return MDB_NOTFOUND; }
    mdb_notfound(const string& w=""): lmdb_error(w) { }
};

class key_not_found: public mdb_notfound {
public:
    int code() const { return MDB_NOTFOUND; }
    key_not_found(const string& w=""): mdb_notfound(w) { }
};

class mdb_page_notfound: public lmdb_error {
public:
    int code() const { return MDB_PAGE_NOTFOUND; }
    mdb_page_notfound(const string& w=""): lmdb_error(w) { }
};

class mdb_currupted: public lmdb_error {
public:
    int code() const { return MDB_CORRUPTED; }
    mdb_currupted(const string& w=""): lmdb_error(w) { }
};

class mdb_panic: public lmdb_error {
public:
    int code() const { return MDB_PANIC; }
    mdb_panic(const string& w=""): lmdb_error(w) { }
};

class mdb_version_mismatch: public lmdb_error {
public:
    int code() const { return MDB_VERSION_MISMATCH; }
    mdb_version_mismatch(const string& w=""): lmdb_error(w) { }
};

class mdb_invalid: public lmdb_error {
public:
    int code() const { return MDB_INVALID; }
    mdb_invalid(const string& w=""): lmdb_error(w) { }
};

class mdb_map_full: public lmdb_error {
public:
    int code() const { return MDB_MAP_FULL; }
    mdb_map_full(const string& w=""): lmdb_error(w) { }
};

class mdb_dbs_full: public lmdb_error {
public:
    int code() const { return MDB_DBS_FULL; }
    mdb_dbs_full(const string& w=""): lmdb_error(w) { }
};

class mdb_readers_full: public lmdb_error {
public:
    int code() const { return MDB_READERS_FULL; }
    mdb_readers_full(const string& w=""): lmdb_error(w) { }
};

class mdb_tls_full: public lmdb_error {
public:
    int code() const { return MDB_TLS_FULL; }
    mdb_tls_full(const string& w=""): lmdb_error(w) { }
};

class mdb_txn_full: public lmdb_error {
public:
    int code() const { return MDB_TXN_FULL; }
    mdb_txn_full(const string& w=""): lmdb_error(w) { }
};

class mdb_cursor_full: public lmdb_error {
public:
    int code() const { return MDB_CURSOR_FULL; }
    mdb_cursor_full(const string& w=""): lmdb_error(w) { }
};

class mdb_page_full: public lmdb_error {
public:
    int code() const { return MDB_PAGE_FULL; }
    mdb_page_full(const string& w=""): lmdb_error(w) { }
};

class mdb_map_resized: public lmdb_error {
public:
    int code() const { return MDB_MAP_RESIZED; }
    mdb_map_resized(const string& w=""): lmdb_error(w) { }
};

class mdb_incompatible: public lmdb_error {
public:
    int code() const { return MDB_INCOMPATIBLE; }
    mdb_incompatible(const string& w=""): lmdb_error(w) { }
};

class mdb_bad_rslot: public lmdb_error {
public:
    int code() const { return MDB_BAD_RSLOT; }
    mdb_bad_rslot(const string& w=""): lmdb_error(w) { }
};

class mdb_bad_txn: public lmdb_error {
public:
    int code() const { return MDB_BAD_TXN; }
    mdb_bad_txn(const string& w=""): lmdb_error(w) { }
};

class mdb_bad_valsize: public lmdb_error {
public:
    int code() const { return MDB_BAD_VALSIZE; }
    mdb_bad_valsize(const string& w=""): lmdb_error(w) { }
};

class mdb_bad_dbi: public lmdb_error {
public:
    int code() const { return MDB_BAD_DBI; }
    mdb_bad_dbi(const string& w=""): lmdb_error(w) { }
};

class empty_key: public lmdb_error {
public:
    empty_key(): lmdb_error("the key should not be empty") { }
};

class empty_database: public lmdb_error {
public:
    empty_database(): lmdb_error("the database is empty") { }
};

class not_initialized: public lmdb_error {
public:
    not_initialized(): lmdb_error("PersistentDict is not initialized") { }
};

exception lmdb_error::factory(int code) {
    switch (code) {
        case MDB_KEYEXIST:
            throw mdb_keyexist();
        case MDB_NOTFOUND:
            throw mdb_notfound();
        case MDB_PAGE_NOTFOUND:
            throw mdb_page_notfound();
        case MDB_CORRUPTED:
            throw mdb_currupted();
        case MDB_PANIC:
            throw mdb_panic();
        case MDB_VERSION_MISMATCH:
            throw mdb_version_mismatch();
        case MDB_INVALID:
            throw mdb_invalid();
        case MDB_MAP_FULL:
            throw mdb_map_full();
        case MDB_DBS_FULL:
            throw mdb_dbs_full();
        case MDB_READERS_FULL:
            throw mdb_readers_full();
        case MDB_TLS_FULL:
            throw mdb_tls_full();
        case MDB_TXN_FULL:
            throw mdb_txn_full();
        case MDB_CURSOR_FULL:
            throw mdb_cursor_full();
        case MDB_PAGE_FULL:
            throw mdb_page_full();
        case MDB_MAP_RESIZED:
            throw mdb_map_resized();
        case MDB_INCOMPATIBLE:
            throw mdb_incompatible();
        case MDB_BAD_RSLOT:
            throw mdb_bad_rslot();
        case MDB_BAD_TXN:
            throw mdb_bad_txn();
        case MDB_BAD_VALSIZE:
            throw mdb_bad_valsize();
        case MDB_BAD_DBI:
            throw mdb_bad_dbi();
        case ENOMEM:
            throw std::bad_alloc();
        case EINVAL:
            throw std::invalid_argument("invalid argument");
        case EIO:
            throw io_error();
        case ENOSPC:
            throw io_error("no more disk space");
        case ENOENT:
            throw io_error("the directory specified by the path parameter doesn't exist");
        case EAGAIN:
            throw io_error("the environment was locked by another process");
        case EACCES:
            throw access_error();
        default:
            throw lmdb_error();
    }
    return std::runtime_error("should not happen");

}


}
