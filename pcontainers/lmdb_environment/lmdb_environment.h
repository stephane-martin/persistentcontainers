#pragma once

#include <map>
#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>

#include <boost/core/explicit_operator_bool.hpp>
#include <boost/atomic.hpp>
#include <boost/thread/tss.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/lockfree/stack.hpp>
#include <bstrlib/bstrwrap.h>
#include "../utils/lmdb_options.h"
#include "lmdb.h"


namespace lmdb {

using std::map;
using boost::mutex;
using boost::lock_guard;
using Bstrlib::CBString;


class environment {
public:
    class transaction;
    typedef boost::shared_ptr<environment> shared_ptr;
    typedef boost::shared_ptr<transaction> transaction_ptr;

private:
    MDB_env* ptr;
    CBString dirname;
    environment(const environment& other);
    environment& operator=(const environment& other);

protected:
    static map<CBString, boost::weak_ptr<environment> > opened_environments;
    static mutex lock_envs;
    static mutex lock_dbis;

    map<CBString, MDB_dbi> opened_dbis;
    mutable boost::thread_specific_ptr< boost::weak_ptr<transaction> > write_transaction_ptr;
    mutable boost::lockfree::stack < MDB_txn*, boost::lockfree::fixed_sized<true> > read_transactions_stack;

    environment(const CBString& directory_name, const lmdb_options& opts);
    static void unfactory(environment* env);

public:

    static shared_ptr factory(const CBString& directory_name, const lmdb_options& opts);

    BOOST_EXPLICIT_OPERATOR_BOOL()
    bool operator!() const { return !ptr; }    // should always be false

    ~environment() {
        if (ptr) {
            // _LOG_DEBUG << "Deleting environment";
            mdb_env_close(ptr);
        }
    }

    MDB_env* get() { return ptr; }
    const MDB_env* get() const { return ptr; }
    CBString get_dirname() const { return dirname; }
    int get_maxkeysize() const { return mdb_env_get_maxkeysize(ptr); }
    MDB_dbi get_dbi(const CBString& dbname);

    class transaction {
    friend class environment;
    private:
        const environment& env;
        MDB_txn* txn;
        boost::atomic_bool rollback;
        transaction(const transaction& other);
        transaction& operator=(const transaction&);
    protected:
        transaction(const environment& e);
        transaction(environment& e, bool ro);
        MDB_txn* get() { return txn; }
    public:
        const bool readonly;

        ~transaction();
        size_t size(MDB_dbi d) const;
        void set_rollback(bool val=true) { rollback.store(val); }

        class cursor {
        private:
            MDB_cursor* c;
            transaction& txn;
            const MDB_dbi dbi;

            cursor(const cursor& other);
            cursor& operator=(const cursor&);
        protected:
            cursor(transaction& t, MDB_dbi d);

            int _get(MDB_val& key, MDB_val& value, MDB_cursor_op op) {
                return mdb_cursor_get(c, &key, &value, op);
            }
        public:
            ~cursor() {
                if (c) {
                    mdb_cursor_close(c);
                }
            }

            static boost::shared_ptr<cursor> factory(transaction& t, MDB_dbi d) {
                return boost::shared_ptr<cursor>(new cursor(t, d));
            }

            size_t size() const { return txn.size(dbi); }
            int first();
            int next();
            int prev();
            int last();
            int position(MDB_val key);
            int after(MDB_val key);
            void get_current_key(MDB_val& key);
            void get_current_value(MDB_val& value);
            void get_current_key_value(MDB_val& key, MDB_val& value);
            void set_current_value(MDB_val value);
            void set_key_value(MDB_val key, MDB_val value);
            void append_key_value(MDB_val key, MDB_val value);
            void del();

        };      // end class cursor

        boost::shared_ptr<cursor> make_cursor(MDB_dbi dbi) {
            return cursor::factory(*this, dbi);
        }

    }; // end class transaction`

    typedef boost::shared_ptr<transaction::cursor> cursor_ptr;

    transaction_ptr start_transaction() const {
        return transaction_ptr(new transaction(*this));
    }

    transaction_ptr start_transaction(bool readonly);

    void drop(MDB_dbi dbi);

    size_t size(MDB_dbi dbi) const {
        return start_transaction()->size(dbi);
    }



};      // end class environment

}       // end NS lmdb
