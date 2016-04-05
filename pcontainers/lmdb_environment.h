#pragma once

#include <string>
#include <stdexcept>
#include <sstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <vector>
#include <map>
#include <utility>
#include <algorithm>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/throw_exception.hpp>
#include <boost/core/explicit_operator_bool.hpp>
#include <boost/atomic.hpp>
#include <bstrlib/bstrwrap.h>
#include <errno.h>
#include "mutexwrap.h"
#include "lmdb_options.h"
#include "lmdb_exceptions.h"
#include "lmdb.h"
#include "utils.h"

namespace lmdb {

using std::string;
using std::pair;
using std::vector;
using std::map;
using std::cout;
using std::endl;
using std::exception;
using std::ostringstream;
using quiet::MutexWrap;
using quiet::MutexWrapLock;
using namespace utils;
using Bstrlib::CBString;


class environment {
private:
    environment(const environment& other);
    environment& operator=(const environment& other);

protected:
    static map<CBString, boost::shared_ptr<environment> > opened_environments;
    static MutexWrap lock_envs;
    static MutexWrap lock_dbis;
    map<CBString, MDB_dbi> opened_dbis;

public:
    BOOST_EXPLICIT_OPERATOR_BOOL()
    bool operator!() const { return !ptr; }    // should always be false

    class transaction;
    typedef boost::shared_ptr<environment> shared_ptr;
    typedef boost::shared_ptr<transaction> transaction_ptr;

    friend environment::shared_ptr env_factory(const CBString& directory_name, const lmdb_options& opts);

    MDB_dbi get_dbi(const CBString& dbname) {
        int res;
        MDB_dbi dbi;
        MutexWrapLock lock(lock_dbis);
        {
            if (opened_dbis.count(dbname)) {
                dbi = opened_dbis[dbname];
            } else {
                boost::shared_ptr<transaction> txn = start_transaction(false);
                if (!dbname.length()) {
                    res = mdb_dbi_open(txn->txn, NULL, MDB_CREATE, &dbi);
                } else {
                    res = mdb_dbi_open(txn->txn, dbname, MDB_CREATE, &dbi);
                }
                if (res != 0) {
                    txn->set_rollback();
                    BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
                }
                txn.reset();    // commit transaction
                opened_dbis[dbname] = dbi;
            }
            return dbi;
        }
    }

    MDB_env* ptr;

    environment(const CBString& directory_name, const lmdb_options& opts) {
        int res = mdb_env_create(&ptr);
        if (res != 0) {
            BOOST_THROW_EXCEPTION(lmdb_error() << lmdb_error::what("mdb_env_create_failed with: " + any_tostring(res)));
        }

        res = mdb_env_set_maxdbs(ptr, opts.max_dbs);
        if (res != 0) {
            mdb_env_close(ptr);
            ptr = NULL;
            BOOST_THROW_EXCEPTION(lmdb_error() << lmdb_error::what("mdb_env_set_maxdbs failed with: " + any_tostring(res)));
        }

        res = mdb_env_set_mapsize(ptr, opts.map_size);
        if (res != 0) {
            mdb_env_close(ptr);
            ptr = NULL;
            BOOST_THROW_EXCEPTION(lmdb_error() << lmdb_error::what("mdb_env_set_mapsize failed with: " + any_tostring(res)));
        }

        res = mdb_env_set_maxreaders(ptr, opts.max_readers);
        if (res != 0) {
            mdb_env_close(ptr);
            ptr = NULL;
            BOOST_THROW_EXCEPTION(lmdb_error() << lmdb_error::what("mdb_env_set_maxreaders failed with: " + any_tostring(res)));
        }

        res = mdb_env_open(ptr, directory_name, opts.get_flags(), 448);
        if (res != 0) {
            mdb_env_close(ptr);
            ptr = NULL;
            switch (res) {
                case MDB_VERSION_MISMATCH:
                    BOOST_THROW_EXCEPTION(mdb_version_mismatch());
                case MDB_INVALID:
                    BOOST_THROW_EXCEPTION(lmdb_error() << lmdb_error::what("the environment file headers are corrupted"));
                case EAGAIN:
                    BOOST_THROW_EXCEPTION(lmdb_error() << lmdb_error::what("the environment was locked by another process"));
                default:
                    BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
            }
        }

    }

    ~environment() {
        if (ptr) {
            mdb_env_close(ptr);
        }
    }

    int get_maxkeysize() const {
        return mdb_env_get_maxkeysize(ptr);
    }

    class transaction {
    private:
        transaction(const transaction& other);
        transaction& operator=(const transaction&);
    public:
        const environment& env;
        MDB_txn* txn;
        boost::atomic_bool rollback;
        const bool readonly;

        transaction(const environment& e, bool ro=true): env(e), txn(NULL), rollback(false), readonly(ro) {
            int res;
            if (readonly) {
                res = mdb_txn_begin(env.ptr, NULL, MDB_RDONLY, &txn);
            } else {
                res = mdb_txn_begin(env.ptr, NULL, 0, &txn);
            }
            if (res != 0) {
                BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
            }

        }

        ~transaction() {
            if (txn) {
                if (rollback.load()) {
                    mdb_txn_abort(txn);
                } else {
                    mdb_txn_commit(txn);
                }
            }
        }

        size_t size(MDB_dbi d) const {
            MDB_stat stat;
            int res = mdb_stat(txn, d, &stat);
            if (res != 0) {
                BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
            }
            return stat.ms_entries;
        }

        void set_rollback(bool val=true) {
            rollback.store(val);
        }

        class cursor {
        private:
            cursor(const cursor& other);
            cursor& operator=(const cursor&);
        public:
            MDB_cursor* c;
            transaction& txn;
            const MDB_dbi dbi;

            cursor(transaction& t, MDB_dbi d): txn(t), dbi(d) {
                int res = mdb_cursor_open(txn.txn, dbi, &c);
                if (res != 0) {
                    txn.set_rollback();
                    BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
                }
            }

            ~cursor() {
                if (c) {
                    mdb_cursor_close(c);
                }
            }

            int _get(MDB_val& key, MDB_val& value, MDB_cursor_op op) {
                return mdb_cursor_get(c, &key, &value, op);
            }

            int first() {
                MDB_val k = make_mdb_val();
                MDB_val v = make_mdb_val();
                int res = _get(k, v, MDB_FIRST);
                if (res != 0 and res != MDB_NOTFOUND) {
                    txn.set_rollback();
                    BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
                }
                return res;
            }

            int next() {
                MDB_val k = make_mdb_val();
                MDB_val v = make_mdb_val();
                int res = _get(k, v, MDB_NEXT);
                if (res != 0 and res != MDB_NOTFOUND) {
                    txn.set_rollback();
                    BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
                }
                return res;
            }

            int prev() {
                MDB_val k = make_mdb_val();
                MDB_val v = make_mdb_val();
                int res = _get(k, v, MDB_PREV);
                if (res != 0 and res != MDB_NOTFOUND) {
                    txn.set_rollback();
                    BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
                }
                return res;
            }

            int last() {
                MDB_val k = make_mdb_val();
                MDB_val v = make_mdb_val();
                int res = _get(k, v, MDB_LAST);
                if (res != 0 and res != MDB_NOTFOUND) {
                    txn.set_rollback();
                    BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
                }
                return res;
            }

            int position(MDB_val key) {
                MDB_val v = make_mdb_val();
                int res = _get(key, v, MDB_SET);
                if (res != 0 and res != MDB_NOTFOUND) {
                    txn.set_rollback();
                    BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
                }
                return res;
            }

            int after(MDB_val key) {
                MDB_val v = make_mdb_val();
                int res = _get(key, v, MDB_SET_RANGE);
                if (res != 0 and res != MDB_NOTFOUND) {
                    txn.set_rollback();
                    BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
                }
                return res;
            }

            size_t size() const {
                return txn.size(dbi);
            }

            void get_current_key(MDB_val& key) {
                MDB_val v = make_mdb_val();
                int res = _get(key, v, MDB_GET_CURRENT);
                if (res != 0) {
                    txn.set_rollback();
                    if (res == MDB_NOTFOUND) {
                        BOOST_THROW_EXCEPTION(mdb_notfound());
                    }
                    BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
                }
            }

            void get_current_value(MDB_val& value) {
                MDB_val k = make_mdb_val();
                int res = _get(k, value, MDB_GET_CURRENT);
                if (res != 0) {
                    txn.set_rollback();
                    if (res == MDB_NOTFOUND) {
                        BOOST_THROW_EXCEPTION(mdb_notfound());
                    }
                    BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
                }
            }

            void get_current_key_value(MDB_val& key, MDB_val& value) {
                int res = _get(key, value, MDB_GET_CURRENT);
                if (res != 0) {
                    txn.set_rollback();
                    if (res == MDB_NOTFOUND) {
                        BOOST_THROW_EXCEPTION(mdb_notfound());
                    }
                    BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
                }
            }

            void set_current_value(MDB_val value) {
                if (txn.readonly) {
                    BOOST_THROW_EXCEPTION(access_error() << lmdb_error::what("cursor::set_current_value: trying to write in a read-only transaction"));
                }
                MDB_val key = make_mdb_val();
                get_current_key(key);
                CBString s = make_string(key);    // need to copy the key, as the next mdb_cursor call invalidates the MDB_val
                key = make_mdb_val(s);
                int res = mdb_cursor_put(c, &key, &value, MDB_CURRENT);
                if (res != 0) {
                    txn.set_rollback();
                    BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
                }
            }

            void set_key_value(MDB_val key, MDB_val value) {
                if (txn.readonly) {
                    BOOST_THROW_EXCEPTION(access_error() << lmdb_error::what("cursor::set_key_value: trying to write in a read-only transaction"));
                }
                int res = mdb_cursor_put(c, &key, &value, 0);
                if (res != 0) {
                    txn.set_rollback();
                    BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
                }
            }

            void append_key_value(MDB_val key, MDB_val value) {
                if (txn.readonly) {
                    BOOST_THROW_EXCEPTION(access_error() << lmdb_error::what("cursor::append_key_value: trying to write in a read-only transaction"));
                }
                int res = mdb_cursor_put(c, &key, &value, MDB_APPEND);
                if (res != 0) {
                    txn.set_rollback();
                    BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
                }
            }

            void del() {
                if (txn.readonly) {
                    BOOST_THROW_EXCEPTION(access_error() << lmdb_error::what("cursor::del: trying to write in a read-only transaction"));
                }
                int res = mdb_cursor_del(c, 0);
                if (res != 0) {
                    txn.set_rollback();
                    BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
                }

            }

        };      // end class cursor

        boost::shared_ptr<cursor> make_cursor(MDB_dbi dbi) {
            return boost::shared_ptr<cursor>(new cursor(*this, dbi));
        }

    }; // end class transaction`

    typedef boost::shared_ptr<transaction::cursor> cursor_ptr;

    transaction_ptr start_transaction() const {
        return transaction_ptr(new transaction(*this, true));
    }

    transaction_ptr start_transaction(bool readonly=true) {
        return transaction_ptr(new transaction(*this, readonly));
    }

    cursor_ptr make_cursor(MDB_dbi dbi, bool readonly=true) {
        return start_transaction(readonly)->make_cursor(dbi);
    }


    void drop(MDB_dbi dbi) {
        boost::shared_ptr<transaction> txn = start_transaction(false);
        int res = mdb_drop(txn->txn, dbi, 0);
        if (res != 0) {
            txn->set_rollback();
            BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
        }
    }

    size_t size(MDB_dbi dbi) const {
        return start_transaction()->size(dbi);
    }



};      // end class environment

environment::shared_ptr env_factory(const CBString& directory_name, const lmdb_options& opts);

}       // end NS lmdb
