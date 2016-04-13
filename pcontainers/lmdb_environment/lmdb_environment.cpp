#include <errno.h>
#include <boost/bind.hpp>
#include <boost/throw_exception.hpp>
#include "lmdb_environment.h"
#include "../lmdb_exceptions/lmdb_exceptions.h"
#include "../utils/utils.h"

namespace lmdb {

using std::map;
using Bstrlib::CBString;
using utils::create_if_needed;
using utils::cpp_realpath;
using utils::make_mdb_val;
using utils::make_string;

mutex environment::lock_envs;
mutex environment::lock_dbis;
map<CBString, boost::weak_ptr<environment> > environment::opened_environments;


environment::environment(const CBString& directory_name, const lmdb_options& opts): dirname(directory_name), read_transactions_stack(200) {
    dirname = directory_name;
    dirname.trim();
    if (!dirname.length()) {
        BOOST_THROW_EXCEPTION(lmdb_error() << lmdb_error::what("environment: dirname is empty"));
    }
    create_if_needed(dirname);
    dirname = cpp_realpath(dirname);

    int res = mdb_env_create(&ptr);
    if (res != 0) {
        BOOST_THROW_EXCEPTION(lmdb_error() << lmdb_error::what("mdb_env_create_failed") << lmdb_error::code(res));
    }

    res = mdb_env_set_maxdbs(ptr, opts.max_dbs);
    if (res != 0) {
        mdb_env_close(ptr);
        ptr = NULL;
        BOOST_THROW_EXCEPTION(lmdb_error() << lmdb_error::what("mdb_env_set_maxdbs failed") << lmdb_error::code(res));
    }

    res = mdb_env_set_mapsize(ptr, opts.map_size);
    if (res != 0) {
        mdb_env_close(ptr);
        ptr = NULL;
        BOOST_THROW_EXCEPTION(lmdb_error() << lmdb_error::what("mdb_env_set_mapsize failed") << lmdb_error::code(res));
    }

    res = mdb_env_set_maxreaders(ptr, opts.max_readers);
    if (res != 0) {
        mdb_env_close(ptr);
        ptr = NULL;
        BOOST_THROW_EXCEPTION(lmdb_error() << lmdb_error::what("mdb_env_set_maxreaders failed") << lmdb_error::code(res));
    }

    res = mdb_env_open(ptr, dirname, opts.get_flags(), 448);
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


environment::shared_ptr environment::factory(const CBString& directory_name, const lmdb_options& opts) {
    CBString dirname(directory_name);
    dirname.trim();
    if (!dirname.length()) {
        BOOST_THROW_EXCEPTION(lmdb_error() << lmdb_error::what("environment::factory: dirname is empty"));
    }
    create_if_needed(dirname);
    dirname = cpp_realpath(dirname);
    {
        lock_guard<mutex> guard(environment::lock_envs);
        shared_ptr env;
        if (opened_environments.count(dirname)) {
            env = opened_environments[dirname].lock();
        }
        if (!env) {
            env = shared_ptr(new environment(dirname, opts), boost::bind(&environment::unfactory, _1));
            opened_environments[env->get_dirname()] = env;
        }
        return env;
    }
}

void environment::unfactory(environment* env) {
    if (env) {
        {
            lock_guard<mutex> guard(environment::lock_envs);
            opened_environments.erase(env->get_dirname());
        }
        delete env;
    }
}

MDB_dbi environment::get_dbi(const CBString& dbname) {
    int res;
    MDB_dbi dbi;
    lock_guard<mutex> guard(environment::lock_dbis);
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

environment::transaction_ptr environment::start_transaction(bool readonly) {
    if (readonly) {
        return transaction_ptr(new transaction(*this, true));
    } else {
        boost::weak_ptr<transaction>* weak_t = write_transaction_ptr.get();
        if (weak_t) {
            transaction_ptr t = weak_t->lock();
            if (t) {
                return t;
            }
        }
        transaction_ptr t(new transaction(*this, false));
        write_transaction_ptr.reset(new boost::weak_ptr<transaction>(t));
        return t;
    }
}

void environment::drop(MDB_dbi dbi) {
    boost::shared_ptr<transaction> txn = start_transaction(false);
    int res = mdb_drop(txn->txn, dbi, 0);
    if (res != 0) {
        txn->set_rollback();
        BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
    }
}


environment::transaction::transaction(const environment& e): env(e), txn(NULL), rollback(false), readonly(true) {
    int res;
    if (env.read_transactions_stack.pop(txn)) {
        res = mdb_txn_renew(txn);
    } else {
        res = mdb_txn_begin(const_cast<MDB_env*>(e.get()), NULL, MDB_RDONLY, &txn);
    }
    if (res != 0) {
        BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
    }
}

environment::transaction::transaction(environment& e, bool ro): env(e), txn(NULL), rollback(false), readonly(ro) {
    int res;
    if (readonly) {
        if (env.read_transactions_stack.pop(txn)) {
            res = mdb_txn_renew(txn);
        } else {
            res = mdb_txn_begin(e.get(), NULL, MDB_RDONLY, &txn);
        }
    } else {
        res = mdb_txn_begin(e.get(), NULL, 0, &txn);
    }
    if (res != 0) {
        BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
    }
}

environment::transaction::~transaction() {
    if (txn) {
        if (readonly) {
            mdb_txn_reset(txn);
            env.read_transactions_stack.bounded_push(txn);
        } else {
            if (rollback.load()) {
                mdb_txn_abort(txn);
            } else {
                mdb_txn_commit(txn);
            }
            env.write_transaction_ptr.reset();
        }
    }
}

size_t environment::transaction::size(MDB_dbi d) const {
    MDB_stat stat;
    int res = mdb_stat(txn, d, &stat);
    if (res != 0) {
        BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
    }
    return stat.ms_entries;
}

environment::transaction::cursor::cursor(transaction& t, MDB_dbi d): txn(t), dbi(d) {
    int res = mdb_cursor_open(txn.get(), dbi, &c);
    if (res != 0) {
        txn.set_rollback();
        BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
    }
}

int environment::transaction::cursor::first() {
    MDB_val k = make_mdb_val();
    MDB_val v = make_mdb_val();
    int res = _get(k, v, MDB_FIRST);
    if (res != 0 and res != MDB_NOTFOUND) {
        txn.set_rollback();
        BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
    }
    return res;
}

int environment::transaction::cursor::next() {
    MDB_val k = make_mdb_val();
    MDB_val v = make_mdb_val();
    int res = _get(k, v, MDB_NEXT);
    if (res != 0 and res != MDB_NOTFOUND) {
        txn.set_rollback();
        BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
    }
    return res;
}

int environment::transaction::cursor::prev() {
    MDB_val k = make_mdb_val();
    MDB_val v = make_mdb_val();
    int res = _get(k, v, MDB_PREV);
    if (res != 0 and res != MDB_NOTFOUND) {
        txn.set_rollback();
        BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
    }
    return res;
}

int environment::transaction::cursor::last() {
    MDB_val k = make_mdb_val();
    MDB_val v = make_mdb_val();
    int res = _get(k, v, MDB_LAST);
    if (res != 0 and res != MDB_NOTFOUND) {
        txn.set_rollback();
        BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
    }
    return res;
}

int environment::transaction::cursor::position(MDB_val key) {
    MDB_val v = make_mdb_val();
    int res = _get(key, v, MDB_SET);
    if (res != 0 and res != MDB_NOTFOUND) {
        txn.set_rollback();
        BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
    }
    return res;
}

int environment::transaction::cursor::after(MDB_val key) {
    MDB_val v = make_mdb_val();
    int res = _get(key, v, MDB_SET_RANGE);
    if (res != 0 and res != MDB_NOTFOUND) {
        txn.set_rollback();
        BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
    }
    return res;
}

void environment::transaction::cursor::get_current_key(MDB_val& key) {
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

void environment::transaction::cursor::get_current_value(MDB_val& value) {
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

void environment::transaction::cursor::get_current_key_value(MDB_val& key, MDB_val& value) {
    int res = _get(key, value, MDB_GET_CURRENT);
    if (res != 0) {
        txn.set_rollback();
        if (res == MDB_NOTFOUND) {
            BOOST_THROW_EXCEPTION(mdb_notfound());
        }
        BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
    }
}

void environment::transaction::cursor::set_current_value(MDB_val value) {
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

void environment::transaction::cursor::set_key_value(MDB_val key, MDB_val value) {
    if (txn.readonly) {
        BOOST_THROW_EXCEPTION(access_error() << lmdb_error::what("cursor::set_key_value: trying to write in a read-only transaction"));
    }
    int res = mdb_cursor_put(c, &key, &value, 0);
    if (res != 0) {
        txn.set_rollback();
        BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
    }
}

void environment::transaction::cursor::append_key_value(MDB_val key, MDB_val value) {
    if (txn.readonly) {
        BOOST_THROW_EXCEPTION(access_error() << lmdb_error::what("cursor::append_key_value: trying to write in a read-only transaction"));
    }
    int res = mdb_cursor_put(c, &key, &value, MDB_APPEND);
    if (res != 0) {
        txn.set_rollback();
        BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
    }
}

void environment::transaction::cursor::del() {
    if (txn.readonly) {
        BOOST_THROW_EXCEPTION(access_error() << lmdb_error::what("cursor::del: trying to write in a read-only transaction"));
    }
    int res = mdb_cursor_del(c, 0);
    if (res != 0) {
        txn.set_rollback();
        BOOST_THROW_EXCEPTION(lmdb_error::factory(res));
    }
}


}   // end NS lmdb
