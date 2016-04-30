#pragma once

#include <string>
#include <stdexcept>
#include <vector>
#include <map>
#include <utility>
#include <algorithm>
#include <boost/shared_ptr.hpp>
#include <boost/move/move.hpp>
#include <boost/bind.hpp>
#include <boost/utility.hpp>
#include <boost/type_traits/conditional.hpp>
#include <boost/throw_exception.hpp>
#include <boost/core/explicit_operator_bool.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/atomic.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/lockable_adapter.hpp>
#include <boost/core/noncopyable.hpp>
#include <bstrlib/bstrwrap.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>

#include "lmdb.h"
#include "../lmdb_exceptions/lmdb_exceptions.h"
#include "../utils/lmdb_options.h"
#include "../lmdb_environment/lmdb_environment.h"
#include "../utils/utils.h"
#include "../logging/logging.h"

namespace quiet {

using std::string;
using std::pair;
using std::make_pair;
using std::vector;
using std::map;
using std::exception;
using boost::conditional;
using boost::mutex;
using boost::shared_mutex;
using boost::unique_lock;
using boost::shared_lock;
using boost::upgrade_to_unique_lock;
using boost::upgrade_lock;
using boost::lock_guard;
using boost::shared_lockable_adapter;
using boost::shared_ptr;
using boost::enable_shared_from_this;
using Bstrlib::CBString;
using namespace lmdb;
using namespace utils;


class PersistentDict: public enable_shared_from_this<PersistentDict>, private boost::noncopyable {
friend class PersistentQueue;
friend class BufferedPersistentDict;

private:
    PersistentDict(const CBString& directory_name, const CBString& database_name, const lmdb_options& options):
            dirname(directory_name), dbname(database_name), env(), dbi(), opts(options) { init(); }

    void init();
    void close() { env.reset(); }

protected:
    CBString dirname;
    CBString dbname;
    shared_ptr<environment> env;
    MDB_dbi dbi;
    const lmdb_options opts;


public:
    typedef CBString key_type;
    typedef CBString mapped_type;
    typedef pair<CBString, CBString> value_type;

    BOOST_EXPLICIT_OPERATOR_BOOL()
    bool operator!() const {
        return !env;
    }

    static inline shared_ptr<PersistentDict> factory(const CBString& directory_name, const CBString& database_name=CBString(),
                                                     const lmdb_options& options=lmdb_options()) {
        return shared_ptr<PersistentDict>(new PersistentDict(directory_name, database_name, options));
    }

    ~PersistentDict() { close(); }

    bool is_initialized() const {
        return bool(*this);
    }

    int get_maxkeysize() const {
        if (*this) {
            return env->get_maxkeysize();
        }
        return 0;
    }

    lmdb_options get_options() const { return opts; }

    void copy_to(shared_ptr<PersistentDict> other, const CBString& first_key=CBString(), const CBString& last_key=CBString(), ssize_t chunk_size=-1) const;
    void move_to(shared_ptr<PersistentDict> other, const CBString& first_key=CBString(), const CBString& last_key=CBString(), ssize_t chunk_size=-1);

    bool erase(const CBString& key);
    bool erase(MDB_val key);

    vector<CBString> erase_interval(const CBString& first_key="", const CBString& last_key="", ssize_t chunk_size=-1);

    template <typename InputIterator>
    void insert(InputIterator first, InputIterator last, ssize_t chunk_size=-1) {
        if (!*this) {
            BOOST_THROW_EXCEPTION( not_initialized() );
        }
        if (chunk_size == -1) {
            chunk_size = SSIZE_MAX;
        }
        InputIterator it(first);
        while (it != last) {
            insert_iterator output(shared_from_this());
            for(ssize_t i = 0; i < chunk_size; i++) {
                if (it == last) {
                    break;
                }
                output = *it;
                ++it;
            }
        }
    }

    void insert(const map<CBString, CBString>& m, ssize_t chunk_size=-1) { insert(m.begin(), m.end(), chunk_size); }
    void insert(const CBString& key, const CBString& value) { insert(make_mdb_val(key), make_mdb_val(value)); }

    void insert(MDB_val k, MDB_val v) {
        if (!*this) {
            BOOST_THROW_EXCEPTION(not_initialized());
        }
        if (k.mv_size == 0 || k.mv_data == NULL) {
            BOOST_THROW_EXCEPTION(empty_key());
        }
        insert_iterator output(shared_from_this());
        output = make_pair(k, v);
    }

    CBString setdefault(MDB_val k, MDB_val dflt);

    void transform_values(unary_functor unary_funct, const CBString& first_key="", const CBString& last_key="", ssize_t chunk_size=-1) {
        // value = f(value)
        binary_scalar_functor binary_funct = boost::bind(unary_funct, _2);
        transform_values(binary_funct, first_key, last_key, chunk_size);
    }

    void transform_values(binary_scalar_functor binary_funct, const CBString& first_key="", const CBString& last_key="", ssize_t chunk_size=-1);

    void remove_if_pred_key(unary_predicate unary_pred, const CBString& first_key="", const CBString& last_key="", ssize_t chunk_size=-1) {
        // remove_if(predicate(keys))
        binary_predicate binary_pred = boost::bind(unary_pred, _1);
        remove_if(binary_pred, first_key, last_key, chunk_size);
    }

    void remove_if_pred_value(unary_predicate unary_pred, const CBString& first_key="", const CBString& last_key="", ssize_t chunk_size=-1) {
        // remove_if(predicate(value))
        binary_predicate binary_pred = boost::bind(unary_pred, _2);
        remove_if(binary_pred, first_key, last_key, chunk_size);
    }

    void remove_if(binary_predicate binary_pred, const CBString& first_key="", const CBString& last_key="", ssize_t chunk_size=-1);
    void remove_duplicates(const CBString& first_key="", const CBString& last_key="");
    CBString get_dirname() const { return dirname; }
    CBString get_dbname() const { return dbname; }

    bool empty() const {
        if (!*this) {
            return true;
        }
        return cbegin().has_reached_end();
    }

    bool empty_interval(const CBString& first_key="", const CBString& last_key="") const;

    size_t count(const CBString& key) const {
        if (!*this) {
            return 0;
        }
        if (contains(key)) {
            return 1;
        }
        return 0;
    }

    size_t count_interval(const CBString& first_key=CBString(), const CBString& last_key=CBString()) const {
        return count_interval_if(binary_predicate(), first_key, last_key);
    }

    size_t count_interval_if(binary_predicate predicate, const CBString& first_key=CBString(), const CBString& last_key=CBString()) const;

    void clear() {
        if (*this) {
            env->drop(dbi);
        }
    }

    size_t size() const {
        if (!*this) {
            return 0;
        }
        return env->size(dbi);
    }

    CBString operator[] (const CBString& key) const;
    CBString at(const CBString& key) const { return at(make_mdb_val(key)); }
    // todo: at, pop, contains, erase methods with iterator of keys
    CBString at(MDB_val k) const;
    CBString pop(MDB_val k);
    CBString pop(const CBString& key) { return pop(make_mdb_val(key)); }
    pair<CBString, CBString> popitem();
    bool contains(const CBString& key) const { return contains(make_mdb_val(key)); }

    bool contains(MDB_val key) const {
        if (key.mv_size == 0 || key.mv_data == NULL || (!*this)) {
            return false;
        }
        return !const_iterator(shared_from_this(), key).has_reached_end();
    }

    class insert_iterator {
    private:
        BOOST_MOVABLE_BUT_NOT_COPYABLE(insert_iterator)

    protected:
        boost::atomic_bool initialized;
        shared_ptr<PersistentDict> dict;
        shared_ptr<environment::transaction> txn;
        shared_ptr<environment::transaction::cursor> cursor;

    public:
        typedef void difference_type;
        typedef void value_type;
        typedef void pointer;
        typedef void reference;
        typedef void iterator_type;
        typedef std::output_iterator_tag iterator_category;

        BOOST_EXPLICIT_OPERATOR_BOOL()
        bool operator!() const { return !initialized.load(); }

        ~insert_iterator() {
            dict.reset();
            cursor.reset();
            txn.reset();
        }

        void set_rollback(bool val=true) {
            if (*this) {
                txn->set_rollback(val);
            }
        }

        insert_iterator(shared_ptr<PersistentDict> d): initialized(false), dict(d), txn(), cursor() {
            if (bool(dict) && bool(*dict)) {
                txn = dict->env->start_transaction(false);
                cursor = txn->make_cursor(d->dbi);
                initialized.store(true);
            }
        }

        insert_iterator(BOOST_RV_REF(insert_iterator) other): initialized(false), dict(), txn(), cursor() {
            if (other) {
                other.initialized.store(false);
                dict.swap(other.dict);
                cursor.swap(other.cursor);
                txn.swap(other.txn);
                initialized.store(true);
            }
        }

        insert_iterator& operator=(BOOST_RV_REF(insert_iterator) other) {
            initialized.store(false);
            dict.reset();
            cursor.reset();
            txn.reset();
            if (other) {
                other.initialized.store(false);
                dict.swap(other.dict);
                cursor.swap(other.cursor);
                txn.swap(other.txn);
                initialized.store(true);
            }
            return *this;
        }

        insert_iterator& operator*() { return *this; }
        insert_iterator& operator++() { return *this; }
        insert_iterator& operator++(int) { return *this; }

        insert_iterator& operator=(const pair<CBString, CBString>& p) {
            if (cursor) {
                cursor->set_key_value(make_mdb_val(p.first), make_mdb_val(p.second));
            }
            return *this;
        }

        insert_iterator& operator=(const pair<const CBString, CBString>& p) {
            if (cursor) {
                cursor->set_key_value(make_mdb_val(p.first), make_mdb_val(p.second));
            }
            return *this;
        }

        insert_iterator& operator=(pair<MDB_val, MDB_val> p) {
            if (cursor) {
                cursor->set_key_value(p.first, p.second);
            }
            return *this;
        }
    }; // end class insert_iterator

    class abstract_iterator: public shared_lockable_adapter<shared_mutex> {
    protected:
        shared_ptr<environment::transaction::cursor> cursor;
        shared_ptr<environment::transaction> txn;
        bool reached_end;
        bool reached_beginning;

    public:
        abstract_iterator(): cursor(), txn(), reached_end(false), reached_beginning(false) { }
        virtual ~abstract_iterator() {
            cursor.reset();
            txn.reset();
        }
        virtual size_t size() const = 0;
        virtual bool has_reached_end() const {
            shared_lock<shared_mutex> lock(lockable());
            return reached_end;
        }
        virtual bool has_reached_beginning() const {
            shared_lock<shared_mutex> lock(lockable());
            return reached_beginning;
        }
        virtual void set_rollback(bool val=true) { txn->set_rollback(val); }
        virtual CBString get_key() const = 0;
        virtual MDB_val get_key_buffer() const = 0;
        virtual CBString get_value() const = 0;
        virtual MDB_val get_value_buffer() const = 0;
        virtual pair<const CBString, CBString> get_item() const = 0;
        virtual pair<MDB_val, MDB_val> get_item_buffer() const = 0;

        virtual abstract_iterator& operator++() {
            if (!cursor) {
                return *this;
            }
            upgrade_lock<shared_mutex> lock(lockable());
            if (reached_end) {
                return *this;
            }
            upgrade_to_unique_lock<shared_mutex> ulock(lock);
            int res = 0;
            if (reached_beginning) {
                res = cursor->first();
            } else {
                res = cursor->next();
            }
            if (res == MDB_NOTFOUND) {
                reached_end = true;
            }
            reached_beginning = false;
            return *this;
        }

        virtual abstract_iterator& operator--() {
            if (!cursor) {
                return *this;
            }
            upgrade_lock<shared_mutex> lock(lockable());
            if (reached_beginning) {
                return *this;
            }
            upgrade_to_unique_lock<shared_mutex> ulock(lock);
            int res = 0;
            if (reached_end) {
                res = cursor->last();
            } else {
                res = cursor->prev();
            }
            if (res == MDB_NOTFOUND) {
                reached_beginning = true;
            }
            reached_end = false;
            return *this;
        }

        virtual abstract_iterator& operator++(int) { return this->operator++(); }

        virtual abstract_iterator& operator--(int) { return this->operator--(); }

        virtual void set_position(MDB_val key) {
            if (!cursor) {
                return;
            }
            unique_lock<shared_mutex> lock(lockable());
            if (key.mv_size == 0 || key.mv_data == NULL) {
                reached_end = cursor->first() == MDB_NOTFOUND;
            } else {
                reached_end = cursor->position(key) == MDB_NOTFOUND;
            }
        }

        virtual void set_range(MDB_val key) {
            if (!cursor) {
                return;
            }
            unique_lock<shared_mutex> lock(lockable());
            if (key.mv_size == 0 || key.mv_data == NULL) {
                reached_end = cursor->first() == MDB_NOTFOUND;
            } else {
                reached_end = cursor->after(key) == MDB_NOTFOUND;
            }
        }

    };

    template<bool B>
    class tmpl_iterator: public abstract_iterator {
    public:
        typedef typename boost::conditional< B, shared_ptr<const PersistentDict>, shared_ptr<PersistentDict> >::type dict_ptr_type;
        typedef pair<const CBString, CBString> value_type;
        typedef std::input_iterator_tag iterator_category;

    private:
        BOOST_MOVABLE_BUT_NOT_COPYABLE(tmpl_iterator)

    protected:
        boost::atomic_bool initialized;
        bool ro;
        dict_ptr_type dict;
        MDB_dbi dbi;

        virtual void swap(tmpl_iterator& other) {
            using std::swap;
            swap(ro, other.ro);
            swap(reached_end, other.reached_end);
            swap(reached_beginning, other.reached_beginning);
            swap(dbi, other.dbi);
            dict.swap(other.dict);
            cursor.swap(other.cursor);
            txn.swap(other.txn);
        }

    public:
        virtual ~tmpl_iterator() = 0;


        tmpl_iterator(): abstract_iterator(), initialized(false), ro(true), dict(), dbi() { }

        BOOST_EXPLICIT_OPERATOR_BOOL()
        bool operator!() const { return !initialized.load(); }

        tmpl_iterator(dict_ptr_type d, int pos): abstract_iterator(), initialized(false), ro(true), dict(d), dbi(d->dbi) {
            init(pos);
        }

        tmpl_iterator(dict_ptr_type d, const CBString& key): abstract_iterator(), initialized(false), ro(true), dict(d), dbi(d->dbi) {
            init(key);
        }

        tmpl_iterator(dict_ptr_type d, MDB_val key): abstract_iterator(), initialized(false), ro(true), dict(d), dbi(d->dbi) {
            init(key);
        }

        // move constructor
        tmpl_iterator(BOOST_RV_REF(tmpl_iterator) other):
            abstract_iterator(), initialized(false), ro(true) {
            if (other) {
                unique_lock<shared_mutex> lock(other.lockable());
                other.initialized.store(false);
                swap(other);
                initialized.store(true);
            }
        }

        // move assignment
        tmpl_iterator& operator=(BOOST_RV_REF(tmpl_iterator) other) {
            boost::lock(lockable(), other.lockable());
            unique_lock<shared_mutex> lock_self(lockable(), boost::adopt_lock);
            unique_lock<shared_mutex> lock_other(other.lockable(), boost::adopt_lock);
            initialized.store(false);
            ro = true;
            reached_end = false;
            reached_beginning = false;
            dbi = MDB_dbi();
            dict.reset();
            cursor.reset();
            txn.reset();
            if (other) {
                other.initialized.store(false);
                swap(other);
                initialized.store(true);
            }
            return *this;
        }

        virtual size_t size() const {
            if (!txn) {
                return 0;
            }
            return txn->size(dbi);
        }

        bool operator==(const tmpl_iterator& other) const {
            if (bool(*this) != bool(other)) {
                return false;
            }
            if (!*this) {
                return true;
            }
            shared_lock<shared_mutex> lock_self(lockable());
            shared_lock<shared_mutex> lock_other(other.lockable());
            if (
                (reached_end != other.reached_end) ||
                (reached_beginning != other.reached_beginning) ||
                (ro != other.ro) ||
                (dbi != other.dbi) ||
                ((*dict) != (*(other.dict)))
               )   { return false; }

            if (reached_end || reached_beginning) {
                return true;
            }
            return get_key() == other.get_key();
        }

        bool operator!=(const tmpl_iterator& other) const { return !(*this == other); }

        virtual tmpl_iterator& operator++() {
            abstract_iterator::operator++();
            return *this;
        }

        virtual tmpl_iterator& operator--() {
            abstract_iterator::operator--();
            return *this;
        }

        virtual tmpl_iterator& operator++(int) { return this->operator++(); }
        virtual tmpl_iterator& operator--(int) { return this->operator--(); }

        CBString get_key() const {
            if (!*this) {
                BOOST_THROW_EXCEPTION(not_initialized());
            }
            shared_lock<shared_mutex> lock(lockable());
            if (reached_end || reached_beginning) {
                BOOST_THROW_EXCEPTION(mdb_notfound());
            }
            MDB_val k = make_mdb_val();
            cursor->get_current_key(k);
            return make_string(k);
        }

        MDB_val get_key_buffer() const {
            if (!*this) {
                BOOST_THROW_EXCEPTION(not_initialized());
            }
            shared_lock<shared_mutex> lock(lockable());
            if (reached_end || reached_beginning) {
                BOOST_THROW_EXCEPTION(mdb_notfound());
            }
            MDB_val k = make_mdb_val();
            cursor->get_current_key(k);
            return k;
        }

        CBString get_value() const {
            if (!*this) {
                BOOST_THROW_EXCEPTION(not_initialized());
            }
            shared_lock<shared_mutex> lock(lockable());
            if (reached_end || reached_beginning) {
                BOOST_THROW_EXCEPTION(mdb_notfound());
            }
            MDB_val v = make_mdb_val();
            cursor->get_current_value(v);
            return make_string(v);
        }

        MDB_val get_value_buffer() const {
            if (!*this) {
                BOOST_THROW_EXCEPTION(not_initialized());
            }
            shared_lock<shared_mutex> lock(lockable());
            if (reached_end || reached_beginning) {
                BOOST_THROW_EXCEPTION(mdb_notfound());
            }
            MDB_val v = make_mdb_val();
            cursor->get_current_value(v);
            return v;
        }

        pair<const CBString, CBString> get_item() const {
            if (!*this) {
                BOOST_THROW_EXCEPTION(not_initialized());
            }
            shared_lock<shared_mutex> lock(lockable());
            if (reached_end || reached_beginning) {
                BOOST_THROW_EXCEPTION(mdb_notfound());
            }
            MDB_val k = make_mdb_val();
            MDB_val v = make_mdb_val();
            cursor->get_current_key_value(k, v);
            return make_pair(make_string(k), make_string(v));
        }

        pair<MDB_val, MDB_val> get_item_buffer() const {
            if (!*this) {
                BOOST_THROW_EXCEPTION(not_initialized());
            }
            shared_lock<shared_mutex> lock(lockable());
            if (reached_end || reached_beginning) {
                BOOST_THROW_EXCEPTION(mdb_notfound());
            }
            MDB_val k = make_mdb_val();
            MDB_val v = make_mdb_val();
            cursor->get_current_key_value(k, v);
            return make_pair(k, v);
        }

    protected:
        void init(int pos) {
            if (!dict || !(*dict)) {
                return;
            }
            ro = true;
            txn = dict->env->start_transaction();
            cursor = txn->make_cursor(dbi);
            if (pos > 0) {
                reached_end = true;
            } else if (pos < 0) {
                reached_beginning = true;
            } else {
                reached_end = cursor->first() == MDB_NOTFOUND;
            }
            initialized.store(true);
        }

        void init(int pos, bool readonly) {
            if (!dict || !(*dict)) {
                return;
            }
            ro = readonly;
            txn = dict->env->start_transaction(ro);
            cursor = txn->make_cursor(dbi);
            if (pos > 0) {
                reached_end = true;
            } else if (pos < 0) {
                reached_beginning = true;
            } else {
                reached_end = cursor->first() == MDB_NOTFOUND;
            }
            initialized.store(true);
        }

        void init(const CBString& key) {
            init(make_mdb_val(key));
        }

        void init(const CBString& key, bool readonly) {
            init(make_mdb_val(key), readonly);
        }

        void init(MDB_val key) {
            if (!dict || !(*dict)) {
                return;
            }
            ro = true;
            txn = dict->env->start_transaction();
            cursor = txn->make_cursor(dbi);
            set_position(key);
            initialized.store(true);
        }

        void init(MDB_val key, bool readonly) {
            if (!dict || !(*dict)) {
                return;
            }
            ro = readonly;
            txn = dict->env->start_transaction(ro);
            cursor = txn->make_cursor(dbi);
            set_position(key);
            initialized.store(true);
        }

    }; // end class tmpl_iterator

    typedef tmpl_iterator<true> _const_iterator;
    typedef tmpl_iterator<false> _iterator;

    class const_iterator: public _const_iterator {
    private:
        BOOST_MOVABLE_BUT_NOT_COPYABLE(const_iterator)
    public:
        const_iterator(): _const_iterator() { }
        const_iterator(shared_ptr<const PersistentDict> d, int pos): _const_iterator(d, pos) { }
        const_iterator(shared_ptr<const PersistentDict> d, const CBString& key): _const_iterator(d, key) { }
        const_iterator(shared_ptr<const PersistentDict> d, MDB_val key): _const_iterator(d, key) { }
        const_iterator(BOOST_RV_REF(const_iterator) other): _const_iterator(BOOST_MOVE_BASE(_const_iterator, other)) { }

        virtual ~const_iterator() { }

        virtual const_iterator& operator++() {
            _const_iterator::operator++();
            return *this;
        }
        virtual const_iterator& operator++(int) { return operator++(); }

        virtual const_iterator& operator--() {
            _const_iterator::operator--();
            return *this;
        }
        virtual const_iterator& operator--(int) { return operator--(); }

        const_iterator& operator=(BOOST_RV_REF(const_iterator) other) {
            _const_iterator::operator=(BOOST_MOVE_BASE(_const_iterator, other));
            return *this;
        }

        static inline const_iterator range(shared_ptr<const PersistentDict> d, const CBString& key) {
            if (!d || !*d) {
                BOOST_THROW_EXCEPTION( not_initialized() );
            }
            const_iterator it(d, 0);
            it.set_range(make_mdb_val(key));
            return it;
        }

        pair<const CBString, CBString> operator*() const { return get_item(); }

    }; // END CLASS const_iterator

    class iterator: public _iterator {
    private:
        BOOST_MOVABLE_BUT_NOT_COPYABLE(iterator)

    public:
        class PairProxy {      // kind of a reference-like pair<const CBString, CBString>&
        private:
            PairProxy(const PairProxy& other);
            PairProxy& operator=(const PairProxy& other);
        protected:
            const iterator& ipair;
        public:
            class FirstFieldProxy {
            private:
                FirstFieldProxy(const FirstFieldProxy& other);
                FirstFieldProxy& operator=(const FirstFieldProxy& other);
            protected:
                const iterator& ifirst;
            public:
                FirstFieldProxy(const iterator& i): ifirst(i) { }

                operator CBString() const {
                    MDB_val k;
                    (ifirst.cursor)->get_current_key(k);
                    return make_string(k);
                }

            }; // end class FirstFieldProxy

            class SecondFieldProxy {
            private:
                SecondFieldProxy(const SecondFieldProxy& other);
                SecondFieldProxy& operator=(const SecondFieldProxy& other);
            protected:
                const iterator& isecond;
            public:
                SecondFieldProxy(const iterator& i): isecond(i) { }

                operator CBString() const {
                    MDB_val v;
                    (isecond.cursor)->get_current_value(v);
                    return make_string(v);
                }

                const CBString& operator=(const CBString& new_value) {
                    (isecond.cursor)->set_current_value(make_mdb_val(new_value));
                    return new_value;
                }

                void operator=(MDB_val v) {
                    (isecond.cursor)->set_current_value(v);
                }

            }; // end class SecondFieldProxy

            FirstFieldProxy first;
            SecondFieldProxy second;

            PairProxy(const iterator& i): ipair(i), first(i), second(i) { }

            operator pair<CBString, CBString>() const {
                MDB_val k = make_mdb_val();
                MDB_val v = make_mdb_val();
                (ipair.cursor)->get_current_key_value(k, v);
                return make_pair(make_string(k), make_string(v));
            }

            PairProxy* operator->() {
                return this;
            }

        }; // end class PairProxy

        PairProxy pproxy;

        iterator(): _iterator(), pproxy(*this) { }

        iterator(shared_ptr<PersistentDict> d, int pos, bool readonly): _iterator(), pproxy(*this) {
            dict = d;
            dbi = d->dbi;
            init(pos, readonly);
        }

        iterator(shared_ptr<PersistentDict> d, const CBString& key, bool readonly): _iterator(), pproxy(*this) {
            dict = d;
            dbi = d->dbi;
            init(key, readonly);
        }

        iterator(shared_ptr<PersistentDict> d, MDB_val key, bool readonly): _iterator(), pproxy(*this) {
            dict = d;
            dbi = d->dbi;
            init(key, readonly);
        }

        iterator(BOOST_RV_REF(iterator) other): _iterator(BOOST_MOVE_BASE(_iterator, other)), pproxy(*this) { }

        virtual ~iterator() { }

        virtual iterator& operator++() {
            _iterator::operator++();
            return *this;
        }
        virtual iterator& operator++(int) { return operator++(); }

        virtual iterator& operator--() {
            _iterator::operator--();
            return *this;
        }
        virtual iterator& operator--(int) { return operator--(); }


        iterator& operator=(BOOST_RV_REF(iterator) other) {
            _iterator::operator=(BOOST_MOVE_BASE(_iterator, other));
            return *this;
        }

        static inline iterator range(shared_ptr<PersistentDict> d, const CBString& key, bool readonly=true) {
            if (!d || !*d) {
                BOOST_THROW_EXCEPTION( not_initialized() );
            }
            iterator it(d, 0, readonly);
            it.set_range(make_mdb_val(key));
            return it;
        }

        PairProxy& operator*() { return pproxy; }
        PairProxy& operator->() { return pproxy; }

        CBString pop();
        void set_value(const CBString& value);
        void set_value(MDB_val v);
        void set_key_value(MDB_val key, MDB_val value);
        void set_key_value(const CBString& key, const CBString& value) { set_key_value(make_mdb_val(key), make_mdb_val(value)); }
        void append_key_value(MDB_val key, MDB_val value);
        void append_key_value(const CBString& key, const CBString& value) { append_key_value(make_mdb_val(key), make_mdb_val(value)); }
        bool del();
        bool del(MDB_val key);
        bool del(const CBString& key) { return del(make_mdb_val(key)); }

    };

    template <typename OutputIterator>
    void map_keys(OutputIterator oit,
                    const CBString& first="", const CBString& last="",
                    unary_functor f=unary_identity_functor,
                    unary_predicate unary_pred=unary_true_pred) const {

        const_iterator iit(const_iterator::range(shared_from_this(), first));
        CBString k;
        for(; !iit.has_reached_end(); ++iit) {
            k = iit.get_key();
            if (last.length() > 0 && k >= last) {
                break;
            }
            if (unary_pred(k)) {
                *oit = f(k);
            }
        }
    }

    template <typename OutputIterator>
    void map_values(OutputIterator oit,
                    const CBString& first="", const CBString& last="",
                    unary_functor f=unary_identity_functor,
                    unary_predicate unary_pred=unary_true_pred) const {

        const_iterator iit(const_iterator::range(shared_from_this(), first));
        CBString v;
        for(; !iit.has_reached_end(); ++iit) {
            pair<const CBString, CBString> p(iit.get_item());
            if (last.length() > 0 && p.first >= last) {
                break;
            }
            if (unary_pred(p.second)) {
                *oit = f(p.second);
            }
        }
    }

    template <typename OutputIterator>
    void map_keys_values(OutputIterator oit,
                         const CBString& first="", const CBString& last="",
                         binary_functor f=binary_identity_functor,
                         binary_predicate binary_pred=binary_true_pred) const {

        const_iterator iit(const_iterator::range(shared_from_this(), first));
        CBString v;
        for(; !iit.has_reached_end(); ++iit) {
            pair<const CBString, CBString> p(iit.get_item());
            if (last.length() > 0 && p.first >= last) {
                break;
            }
            if (binary_pred(p.first, p.second)) {
                *oit = f(p.first, p.second);
            }
        }
    }

    iterator before(bool readonly=true) { return iterator(shared_from_this(), -1, readonly); }
    const_iterator cbefore() { return const_iterator(shared_from_this(), -1); }
    iterator begin(bool readonly=true) { return iterator(shared_from_this(), 0, readonly); }
    const_iterator cbegin() const { return const_iterator(shared_from_this(), 0); }

    iterator last(bool readonly=true) {
        iterator it(shared_from_this(), 1, readonly);
        --it;
        return BOOST_MOVE_RET(iterator, it);
    }

    const_iterator clast() const {
        const_iterator it(shared_from_this(), 1);
        --it;
        return BOOST_MOVE_RET(const_iterator, it);
    }

    iterator end(bool readonly=true) { return iterator(shared_from_this(), 1, readonly); }
    iterator find(const CBString& key, bool readonly=true) { return iterator(shared_from_this(), key, readonly); }
    const_iterator cend() const { return const_iterator(shared_from_this(), 1); }
    const_iterator cfind(const CBString& key) const { return const_iterator(shared_from_this(), key); }
    insert_iterator insertiterator() { return insert_iterator(shared_from_this()); }

};  // END CLASS PersistentDict




template<bool B> PersistentDict::tmpl_iterator<B>::~tmpl_iterator() { }

inline bool operator==(const PersistentDict& one, const PersistentDict& other) {
    if (!bool(one) && !bool(other)) {
        return true;
    }
    if (bool(one) != bool(other)) {
        return false;
    }
    return one.get_dirname() == other.get_dirname() && one.get_dbname() == other.get_dbname();
}

inline bool operator!=(const PersistentDict& one, const PersistentDict& other)  { return !(one==other); }


}   // end NS quiet



