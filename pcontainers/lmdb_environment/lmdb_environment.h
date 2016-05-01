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
#include <boost/core/noncopyable.hpp>
#include <bstrlib/bstrwrap.h>
#include "../utils/lmdb_options.h"
#include "lmdb.h"


namespace lmdb {

using std::map;
using boost::mutex;
using boost::lock_guard;
using Bstrlib::CBString;


class environment: private boost::noncopyable {
public:
    class transaction;
    typedef boost::shared_ptr<environment> shared_ptr;  // todo: rename to something less ambiguous
    typedef boost::shared_ptr<transaction> transaction_ptr;

private:
    MDB_env* ptr;       // MDB_env is a C struct, not possible to use scoped_ptr
    CBString dirname;

protected:
    static map<CBString, boost::weak_ptr<environment> > opened_environments;
    static mutex lock_envs;
    static mutex lock_dbis;

    map<CBString, MDB_dbi> opened_dbis;
    mutable boost::thread_specific_ptr< boost::weak_ptr<transaction> > write_transaction_ptr;
    mutable boost::lockfree::stack < MDB_txn*, boost::lockfree::fixed_sized<true> > read_transactions_stack;

    environment(const CBString& directory_name, const lmdb_options& opts);  // protected constructor: use the factory instead; can throw
    static void unfactory(environment* env) BOOST_NOEXCEPT_OR_NOTHROW;

public:

    static shared_ptr factory(const CBString& directory_name, const lmdb_options& opts);    // can throw

    BOOST_EXPLICIT_OPERATOR_BOOL_NOEXCEPT()
    bool operator!() const BOOST_NOEXCEPT_OR_NOTHROW { return !ptr; }    // should always return false

    ~environment();

    MDB_env* get() BOOST_NOEXCEPT_OR_NOTHROW { return ptr; }
    const MDB_env* get() const BOOST_NOEXCEPT_OR_NOTHROW { return ptr; }
    CBString get_dirname() const BOOST_NOEXCEPT_OR_NOTHROW { return dirname; }
    int get_maxkeysize() const BOOST_NOEXCEPT_OR_NOTHROW { return mdb_env_get_maxkeysize(ptr); }
    MDB_dbi get_dbi(const CBString& dbname);    // can throw

    class transaction: private boost::noncopyable {
    friend class environment;
    private:
        const environment& env;
        MDB_txn* txn;
        boost::atomic_bool rollback;
    protected:
        transaction(const environment& e);      // use factories instead; can throw
        transaction(environment& e, bool ro);   // use factories instead; can throw
        MDB_txn* get() BOOST_NOEXCEPT_OR_NOTHROW { return txn; }
        const MDB_txn* get() const BOOST_NOEXCEPT_OR_NOTHROW { return txn; }
    public:
        const bool readonly;

        ~transaction();
        size_t size(MDB_dbi d) const;           // can throw
        void set_rollback(bool val=true) BOOST_NOEXCEPT_OR_NOTHROW { rollback.store(val); }

        class cursor: private boost::noncopyable {
        private:
            MDB_cursor* c;
            transaction& txn;
            const MDB_dbi dbi;
        protected:
            cursor(transaction& t, MDB_dbi d);  // use factory instead: can throw

            int _get(MDB_val& key, MDB_val& value, MDB_cursor_op op) BOOST_NOEXCEPT_OR_NOTHROW {
                return mdb_cursor_get(c, &key, &value, op);
            }
        public:
            ~cursor() {
                if (c) {
                    mdb_cursor_close(c);
                }
            }

            static boost::shared_ptr<cursor> factory(transaction& t, MDB_dbi d) {   // can throw
                return boost::shared_ptr<cursor>(new cursor(t, d));
            }

            size_t size() const { return txn.size(dbi); }   // can throw
            int first();    // can throw
            int next();     // can throw
            int prev();     // can throw
            int last();     // can throw
            int position(MDB_val key);                                      // can throw
            int after(MDB_val key);                                         // can throw
            void get_current_key(MDB_val& key);                             // can throw
            void get_current_value(MDB_val& value);                         // can throw
            void get_current_key_value(MDB_val& key, MDB_val& value);       // can throw
            void set_current_value(MDB_val value);                          // can throw
            void set_key_value(MDB_val key, MDB_val value);                 // can throw
            void append_key_value(MDB_val key, MDB_val value);              // can throw
            void del();     // can throw

        };      // END CLASS cursor

        boost::shared_ptr<cursor> make_cursor(MDB_dbi dbi) {    // can throw
            return cursor::factory(*this, dbi);
        }

    }; // end class transaction`

    typedef boost::shared_ptr<transaction::cursor> cursor_ptr;

    transaction_ptr start_transaction() const;          // read-only transaction; can throw
    transaction_ptr start_transaction(bool readonly);   // read-only or write transaction; can throw

    void drop(MDB_dbi dbi);     // can throw

    size_t size(MDB_dbi dbi) const {    // can throw
        return start_transaction()->size(dbi);
    }



};      // end class environment

}       // end NS lmdb
