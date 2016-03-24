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
#include <algorithm>
#include <boost/shared_ptr.hpp>
#include <boost/move/move.hpp>
#include <boost/bind.hpp>
#include <boost/utility.hpp>
#include <boost/type_traits/conditional.hpp>
#include <boost/throw_exception.hpp>
#include <boost/core/explicit_operator_bool.hpp>
#include <bstrlib/bstrwrap.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>

#include "lmdb.h"
#include "mutexwrap.h"
#include "lmdb_exceptions.h"
#include "lmdb_options.h"
#include "lmdb_environment.h"
#include "utils.h"
#include "logging.h"

namespace quiet {

using std::string;
using std::pair;
using std::make_pair;
using std::vector;
using std::map;
using std::cout;
using std::endl;
using std::exception;
using boost::conditional;

using Bstrlib::CBString;
using namespace lmdb;
using namespace utils;


inline bool key_is_in_interval(const CBString& key, const CBString& first, const CBString& last) {
    if (bool(first.length()) && (key < first)) {
        return false;
    }
    if (bool(last.length()) && (key >= last)) {
        return false;
    }
    return true;
}


class PersistentDict {

friend class PersistentQueue;

private:
    PersistentDict& operator=(const PersistentDict&);


protected:
    CBString dirname;
    CBString dbname;
    boost::shared_ptr<environment> env;
    MDB_dbi dbi;
    const lmdb_options opts;

    void init();

public:
    typedef CBString key_type;
    typedef CBString mapped_type;
    typedef pair<CBString, CBString> value_type;

    BOOST_EXPLICIT_OPERATOR_BOOL()
    bool operator!() const {
        return !env;
    }

    inline bool is_initialized() const {
        return bool(*this);
    }

    PersistentDict(): dirname(), dbname(), env(), dbi(), opts() {
        _LOG_DEBUG << "New trivial PersistentDict object";
    }

    PersistentDict(const CBString& directory_name, const CBString& database_name=CBString(), const lmdb_options& options=lmdb_options()):
    dirname(directory_name), dbname(database_name), env(), dbi(), opts(options) {
        init();
    }

    PersistentDict(const PersistentDict& other): dirname(other.dirname), dbname(other.dbname), env(), dbi(), opts(other.opts) {
        if (other) {
            init();
        }
    }

    int get_maxkeysize() const {
        if (*this) {
            return env->get_maxkeysize();
        }
        return 0;
    }

    void close() { env.reset(); }
    ~PersistentDict() { close(); }

    void copy_to(PersistentDict& other, const CBString& first_key=CBString(), const CBString& last_key=CBString(), ssize_t chunk_size=-1) const;
    void move_to(PersistentDict& other, const CBString& first_key=CBString(), const CBString& last_key=CBString(), ssize_t chunk_size=-1);

    void erase(const CBString& key);
    void erase(MDB_val key);

    vector<CBString> erase_interval(const CBString& first_key="", const CBString& last_key="", ssize_t chunk_size=-1);

    template <typename InputIterator>
    void insert(InputIterator first, InputIterator last, ssize_t chunk_size=-1) {
        if (!*this) {
            BOOST_THROW_EXCEPTION(not_initialized());
        }
        if (chunk_size == -1) {
            chunk_size = SSIZE_MAX;
        }
        InputIterator it(first);
        while (it != last) {
            insert_iterator output(this);
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
        insert_iterator output(this);
        output = make_pair(k, v);
    }

    CBString setdefault(MDB_val k, MDB_val dflt);

    void transform_values(unary_functor unary_funct, const CBString& first_key="", const CBString& last_key="", ssize_t chunk_size=-1) {
        // value = f(value)
        binary_functor binary_funct = boost::bind(unary_funct, _2);
        transform_values(binary_funct, first_key, last_key, chunk_size);
    }

    void transform_values(binary_functor binary_funct, const CBString& first_key="", const CBString& last_key="", ssize_t chunk_size=-1);

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
    CBString at(MDB_val k) const;
    CBString pop(MDB_val k);
    CBString pop(const CBString& key) { return pop(make_mdb_val(key)); }
    pair<CBString, CBString> popitem();
    bool contains(const CBString& key) const { return contains(make_mdb_val(key)); }

    bool contains(MDB_val key) const {
        if (key.mv_size == 0 || key.mv_data == NULL || (!*this)) {
            return false;
        }
        return !const_iterator(this, key).has_reached_end();
    }

    class insert_iterator {
    private:
        BOOST_MOVABLE_BUT_NOT_COPYABLE(insert_iterator)

    protected:
        bool initialized;
        boost::shared_ptr<environment::transaction> txn;
        boost::shared_ptr<environment::transaction::cursor> cursor;

    public:
        typedef void difference_type;
        typedef void value_type;
        typedef void pointer;
        typedef void reference;
        typedef void iterator_type;
        typedef std::output_iterator_tag iterator_category;

        BOOST_EXPLICIT_OPERATOR_BOOL()
        bool operator!() const { return !this->initialized; }

        ~insert_iterator() {
            cursor.reset();
            txn.reset();
        }

        void set_rollback(bool val=true) {
            if (initialized) {
                txn->set_rollback(val);
            }
        }

        insert_iterator(PersistentDict* d): initialized(false), txn(), cursor() {
            if (bool(d) && bool(*d)) {
                txn = d->env->start_transaction(false);
                cursor = txn->make_cursor(d->dbi);
                initialized = true;
            }
        }

        void swap(insert_iterator& other) {
            using std::swap;
            cursor.swap(other.cursor);
            txn.swap(other.txn);
            swap(initialized, other.initialized);
        }

        insert_iterator(BOOST_RV_REF(insert_iterator) other): initialized(false), txn(), cursor() {
            if (other) {
                swap(other);
            }
        }

        insert_iterator& operator=(BOOST_RV_REF(insert_iterator) other) {
            initialized = false;
            cursor.reset();
            txn.reset();
            if (other) {
                swap(other);
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

    template<bool B>
    class tmpl_iterator {
    private:
        BOOST_MOVABLE_BUT_NOT_COPYABLE(tmpl_iterator)

    public:
        virtual ~tmpl_iterator() = 0;

        typedef typename boost::conditional<B, const PersistentDict*, PersistentDict*>::type dict_ptr_type;
        typedef pair<const CBString, CBString> value_type;
        typedef std::input_iterator_tag iterator_category;      // can't really be a forward iterator as operator* does not return a ref

        tmpl_iterator(): initialized(false), ro(true), reached_end(false), reached_beginning(false), cursor(), txn() { }

        BOOST_EXPLICIT_OPERATOR_BOOL()
        bool operator!() const { return !this->initialized; }

        tmpl_iterator(dict_ptr_type d, int pos=0):
            initialized(false), ro(true), reached_end(false), reached_beginning(false), cursor(), txn() {
            init(d, pos);
        }

        tmpl_iterator(dict_ptr_type d, const CBString& key):
            initialized(false), ro(true), reached_end(false), reached_beginning(false), cursor(), txn() {
            init(d, key);
        }

        tmpl_iterator(dict_ptr_type d, MDB_val key):
            initialized(false), ro(true), reached_end(false), reached_beginning(false), cursor(), txn() {
            init(d, key);
        }

        void swap(tmpl_iterator& other) {
            using std::swap;
            swap(ro, other.ro);
            swap(reached_end, other.reached_end);
            swap(reached_beginning, other.reached_beginning);
            cursor.swap(other.cursor);
            txn.swap(other.txn);
            swap(initialized, other.initialized);
        }

        // move constructor
        tmpl_iterator(BOOST_RV_REF(tmpl_iterator) other):
            initialized(false), ro(true), reached_end(false), reached_beginning(false), cursor(), txn() {
            if (other) {
                swap(other);
            }
        }

        // move assignment
        tmpl_iterator& operator=(BOOST_RV_REF(tmpl_iterator) other) {
            initialized = false;
            ro = true;
            reached_end = false;
            reached_beginning = false;
            cursor.reset();
            txn.reset();
            if (other) {
                swap(other);
            }
            return *this;
        }

        size_t size() const {
            if (!txn) {
                return 0;
            }
            return txn->size(dbi);
        }

        bool has_reached_end() const { return reached_end; }

        bool has_reached_beginning() const { return reached_beginning; }

        void set_rollback(bool val=true) { txn->set_rollback(val); }

        bool operator==(const tmpl_iterator& other) const {
            if (bool(*this) != bool(other)) {
                return false;
            }
            if (!*this) {
                return true;
            }
            // todo: mouf
            // if ( ((*the_dict) != *(other.the_dict)) || (reached_end != other.reached_end) || (reached_beginning != other.reached_beginning) || (ro != other.ro)) {
            if ( (reached_end != other.reached_end) || (reached_beginning != other.reached_beginning) || (ro != other.ro)) {
                return false;
            }
            if (reached_end || reached_beginning) {
                return true;
            }
            return get_key() == other.get_key();
        }

        bool operator!=(const tmpl_iterator& other) const { return !(*this == other); }

        virtual tmpl_iterator& operator++() {
            int res = 0;
            if (reached_end || !initialized || !cursor) {
                return *this;
            }
            if (reached_beginning) {
                res = cursor->first();  // position on first element
            } else {
                res = cursor->next();   // position on next element
            }
            if (res == MDB_NOTFOUND) {
                reached_end = true;
            }
            reached_beginning = false;
            return *this;
        }

        virtual tmpl_iterator& operator--() {
            int res = 0;
            if (reached_beginning || !initialized || !cursor) {
                return *this;
            }
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

        virtual tmpl_iterator& operator++(int) { return this->operator++(); }
        virtual tmpl_iterator& operator--(int) { return this->operator--(); }

        CBString get_key() const {
            if (!initialized) {
                BOOST_THROW_EXCEPTION(not_initialized());
            }
            if (reached_end || reached_beginning) {
                BOOST_THROW_EXCEPTION(mdb_notfound());
            }
            MDB_val k = make_mdb_val();
            cursor->get_current_key(k);
            return make_string(k);
        }

        MDB_val get_key_buffer() const {
            if (!initialized) {
                BOOST_THROW_EXCEPTION(not_initialized());
            }
            if (reached_end || reached_beginning) {
                BOOST_THROW_EXCEPTION(mdb_notfound());
            }
            MDB_val k = make_mdb_val();
            cursor->get_current_key(k);
            return k;
        }

        CBString get_value() const {
            if (!initialized) {
                BOOST_THROW_EXCEPTION(not_initialized());
            }
            if (reached_end || reached_beginning) {
                BOOST_THROW_EXCEPTION(mdb_notfound());
            }
            MDB_val v = make_mdb_val();
            cursor->get_current_value(v);
            return make_string(v);
        }

        MDB_val get_value_buffer() const {
            if (!initialized) {
                BOOST_THROW_EXCEPTION(not_initialized());
            }
            if (reached_end || reached_beginning) {
                BOOST_THROW_EXCEPTION(mdb_notfound());
            }
            MDB_val v = make_mdb_val();
            cursor->get_current_value(v);
            return v;
        }

        pair<const CBString, CBString> get_item() const {
            if (!initialized) {
                BOOST_THROW_EXCEPTION(not_initialized());
            }
            if (reached_end || reached_beginning) {
                BOOST_THROW_EXCEPTION(mdb_notfound());
            }
            MDB_val k = make_mdb_val();
            MDB_val v = make_mdb_val();
            cursor->get_current_key_value(k, v);
            return make_pair(make_string(k), make_string(v));
        }

        pair<MDB_val, MDB_val> get_item_buffer() const {
            if (!initialized) {
                BOOST_THROW_EXCEPTION(not_initialized());
            }
            if (reached_end || reached_beginning) {
                BOOST_THROW_EXCEPTION(mdb_notfound());
            }
            MDB_val k = make_mdb_val();
            MDB_val v = make_mdb_val();
            cursor->get_current_key_value(k, v);
            return make_pair(k, v);
        }

    protected:
        bool initialized;
        bool ro;
        bool reached_end;
        bool reached_beginning;
        MDB_dbi dbi;
        boost::shared_ptr<environment::transaction::cursor> cursor;
        boost::shared_ptr<environment::transaction> txn;

        void init(dict_ptr_type d, int pos=0, bool readonly=true) {
            if (!d || !(*d)) {
                return;
            }
            dbi = d->dbi;
            ro = readonly;
            txn = d->env->start_transaction(ro);
            cursor = txn->make_cursor(dbi);
            initialized = true;
            if (pos > 0) {
                reached_end = true;
            } else if (pos < 0) {
                reached_beginning = true;
            } else {
                if (cursor->first() == MDB_NOTFOUND) {
                    reached_end = true;
                }
            }
        }

        void init(dict_ptr_type d, const CBString& key, bool readonly=true) {
            if (!d || !(*d)) {
                return;
            }
            dbi = d->dbi;
            ro = readonly;
            txn = d->env->start_transaction(ro);
            cursor = txn->make_cursor(dbi);
            initialized = true;
            init_set_position(key);
        }

        void init(dict_ptr_type d, MDB_val key, bool readonly=true) {
            if (!d || !(*d)) {
                return;
            }
            dbi = d->dbi;
            ro = readonly;
            txn = d->env->start_transaction(ro);
            cursor = txn->make_cursor(d->dbi);
            initialized = true;
            init_set_position(key);
        }

        void init_set_position(const CBString& key) { init_set_position(make_mdb_val(key)); }

        void init_set_position(MDB_val k) {
            if (k.mv_size == 0 || k.mv_data == NULL) {
                reached_end = true;
                return;
            }
            reached_end = cursor->position(k) == MDB_NOTFOUND;
        }

        void init_set_range(const CBString& key) { init_set_range(make_mdb_val(key)); }

        void init_set_range(MDB_val k) {
            if (k.mv_size == 0 || k.mv_data == NULL) {
                reached_end = true;
                return;
            }
            reached_end = cursor->after(k) == MDB_NOTFOUND;
        }

    }; // end class tmpl_iterator



    typedef tmpl_iterator<true> _const_iterator;
    typedef tmpl_iterator<false> _iterator;

    class const_iterator: public _const_iterator {
    private:
        BOOST_MOVABLE_BUT_NOT_COPYABLE(const_iterator)
    public:

        virtual ~const_iterator() {
            // careful to release pointers in the proper order
            cursor.reset();
            txn.reset();
        }

        const_iterator(): _const_iterator() { }
        const_iterator(dict_ptr_type d, int pos=0): _const_iterator(d, pos) { }
        const_iterator(dict_ptr_type d, const CBString& key): _const_iterator(d, key) { }
        const_iterator(dict_ptr_type d, MDB_val key): _const_iterator(d, key) { }
        const_iterator(BOOST_RV_REF(const_iterator) other): _const_iterator(BOOST_MOVE_BASE(_const_iterator, other)) { }

        virtual const_iterator& operator++() {
            _const_iterator::operator++();
            return *this;
        }
        virtual const_iterator& operator++(int) { return operator++(); }

        virtual const_iterator& operator--() {
            _const_iterator::operator++();
            return *this;
        }
        virtual const_iterator& operator--(int) { return operator--(); }

        const_iterator& operator=(BOOST_RV_REF(const_iterator) other) {
            _const_iterator::operator=(BOOST_MOVE_BASE(_const_iterator, other));
            return *this;
        }

        static const_iterator range(const PersistentDict* d, const CBString& key);
        pair<const CBString, CBString> operator*() const { return get_item(); }

    }; // END CLASS const_iterator

    class iterator: public _iterator {
    private:
        BOOST_MOVABLE_BUT_NOT_COPYABLE(iterator)

    public:
        virtual ~iterator() {
            // careful to release pointers in the proper order
            cursor.reset();
            txn.reset();
        }

        class PairProxy {      // kind of a reference like pair<const CBString, CBString>&
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
        iterator(PersistentDict* d, int pos, bool readonly): _iterator(), pproxy(*this) { init(d, pos, readonly); }
        iterator(PersistentDict* d, const CBString& key, bool readonly): _iterator(), pproxy(*this) { init(d, key, readonly); }
        iterator(PersistentDict* d, MDB_val key, bool readonly): _iterator(), pproxy(*this) { init(d, key, readonly); }
        iterator(BOOST_RV_REF(iterator) other): _iterator(BOOST_MOVE_BASE(_iterator, other)), pproxy(*this) { }

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

        static iterator range(PersistentDict* d, const CBString& key, bool readonly=true);

        PairProxy& operator*() { return pproxy; }
        PairProxy& operator->() { return pproxy; }

        CBString pop();
        void set_value(const CBString& value);
        void set_value(MDB_val v);
        void set_key_value(MDB_val key, MDB_val value);
        void set_key_value(const CBString& key, const CBString& value) { set_key_value(make_mdb_val(key), make_mdb_val(value)); }
        void append_key_value(MDB_val key, MDB_val value);
        void append_key_value(const CBString& key, const CBString& value) { append_key_value(make_mdb_val(key), make_mdb_val(value)); }
        void del();
        void del(MDB_val key);

    };

    vector<CBString> get_all_keys() const;
    vector<CBString> get_all_values() const;
    vector<pair<CBString, CBString> > get_all_items_if(binary_predicate binary_pred) const;
    vector<pair<CBString, CBString> > get_all_items() const { return get_all_items_if(binary_true_pred); }
    iterator before(bool readonly=true) { return iterator(this, -1, readonly); }
    const_iterator cbefore() { return const_iterator(this, -1); }
    iterator begin(bool readonly=true) { return iterator(this, 0, readonly); }
    const_iterator cbegin() const { return const_iterator(this, 0); }

    iterator last(bool readonly=true) {
        iterator it(this, 1, readonly);
        --it;
        return BOOST_MOVE_RET(iterator, it);
    }

    const_iterator clast() const {
        const_iterator it(this, 1);
        --it;
        return BOOST_MOVE_RET(const_iterator, it);
    }

    iterator end(bool readonly=true) { return iterator(this, 1, readonly); }
    iterator find(const CBString& key, bool readonly=true) { return iterator(this, key, readonly); }
    const_iterator cend() const { return const_iterator(this, 1); }
    const_iterator cfind(const CBString& key) const { return const_iterator(this, key); }
    insert_iterator insertiterator() { return insert_iterator(this); }

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

namespace std {
    inline void swap(quiet::PersistentDict::insert_iterator& one, quiet::PersistentDict::insert_iterator& other) { one.swap(other); }
    inline void swap(quiet::PersistentDict::iterator& one, quiet::PersistentDict::iterator& other) { one.swap(other); }
    inline void swap(quiet::PersistentDict::const_iterator& one, quiet::PersistentDict::const_iterator& other) { one.swap(other); }
}


