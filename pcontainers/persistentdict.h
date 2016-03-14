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
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/move/move.hpp>
#include <boost/bind.hpp>
#include <boost/utility.hpp>
#include <boost/type_traits/is_const.hpp>
#include <boost/type_traits/is_same.hpp>
#include <boost/type_traits/conditional.hpp>
#include <boost/throw_exception.hpp>

#include "guid.h"
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
using boost::enable_if;
using boost::enable_if_c;
using boost::disable_if;
using boost::disable_if_c;
using boost::is_const;
using boost::is_same;
using boost::conditional;

using namespace lmdb;
using namespace utils;

class PersistentDict {

friend class PersistentQueue;

protected:
    bool has_dbname;
    bool dirname_is_temp;
    string dirname;
    string dbname;
    boost::shared_ptr<environment> env;
    MDB_dbi dbi;
    lmdb_options opts;

    void init(const string& directory_name, const string& database_name, const lmdb_options& opts);

    void init() {
        has_dbname = false;
        dirname_is_temp = false;
        dirname = "";
        dbname = "";
        env.reset();
        dbi = MDB_dbi();
        opts = lmdb_options();
    }

public:
    typedef string key_type;
    typedef string mapped_type;
    typedef pair<string, string> value_type;

    PersistentDict() {
        init();
        _LOG_DEBUG << "New trivial PersistentDict object";
    }

    operator bool() const {
        return bool(env);
    }

    template <typename InputIterator>
    PersistentDict(InputIterator first, InputIterator last, const string& directory_name, const string& database_name="", const lmdb_options& opts=lmdb_options()) {
        init(directory_name, database_name, opts);
        insert(first, last);
    }

    PersistentDict(const string& directory_name, const string& database_name="", const lmdb_options& opts=lmdb_options()) {
        init(directory_name, database_name, opts);
    }

    PersistentDict(const PersistentDict& other) {
        if (other) {
            init(other.dirname, other.dbname, other.opts);
        } else {
            init();
        }
    }

    PersistentDict& operator=(PersistentDict other) {
        if (*this != other) {
            swap(*this, other);
        }
        return *this;
    }

    int get_maxkeysize() const {
        if (*this) {
            return env->get_maxkeysize();
        }
        return 0;
    }

    inline void close() {
        env.reset();
    }

    ~PersistentDict() {
        close();
    }

    PersistentDict copy_to(const string& directory_name, const string& database_name="", const lmdb_options& opts=lmdb_options(), long chunk_size=-1) const {
        if (dirname == directory_name && dbname == database_name) {
            return *this;
        }
        if (chunk_size == 0) {
            chunk_size = 1000;
        }
        if (chunk_size < 0) {
            chunk_size = LONG_MAX;
        }
        PersistentDict other(directory_name, database_name, opts);
        pair<MDB_val, MDB_val> p;
        long copied = 0;
        if (*this) {
            const_iterator src_it(this, 0);         // iterate over the source dict
            while (!src_it.has_reached_end()) {
                insert_iterator dest_it(&other);
                for(copied = 0; copied <= chunk_size; ++copied) {
                    if (src_it.has_reached_end()) {
                        break;
                    }
                    dest_it = src_it.get_item_buffer();
                    ++src_it;
                }
            }
        }
        return other;
    }

    friend inline void swap(PersistentDict& first, PersistentDict& second) {
        using std::swap;
        swap(first.has_dbname, second.has_dbname);
        swap(first.dirname_is_temp, second.dirname_is_temp);
        swap(first.dirname, second.dirname);
        swap(first.dbname, second.dbname);
        swap(first.env, second.env);
        swap(first.dbi, second.dbi);
        swap(first.opts, second.opts);
    }

    friend inline bool operator==(const PersistentDict& one, const PersistentDict& other) {
        if (!bool(one) && !bool(other)) {
            return true;
        }
        if (bool(one) != bool(other)) {
            return false;
        }
        return one.dirname == other.dirname && one.has_dbname == other.has_dbname && one.dbname == other.dbname;
    }

    friend inline bool operator!=(const PersistentDict& one, const PersistentDict& other)  {
        return !(one==other);
    }

    inline void erase(const string& key) {
        if (!*this) {
            BOOST_THROW_EXCEPTION(mdb_notfound());
        }
        iterator it = iterator(this, key, false);
        if (it.has_reached_end()) {
            BOOST_THROW_EXCEPTION(mdb_notfound());
        }
        it.del();
    }

    inline vector<string> erase(const string& first, const string& last) {
        vector<string> removed_keys;
        if (!*this) {
            return removed_keys;
        }
        string k;
        iterator it = iterator::range(this, first, false);
        for(; !it.has_reached_end(); ++it) {
            k = it.get_key();
            if (k >= last) {
                break;
            }
            it.del();
            removed_keys.push_back(k);
        }
        return removed_keys;
    }

    template <typename InputIterator>
    inline void insert(InputIterator first, InputIterator last) {
        if (!*this) {
            BOOST_THROW_EXCEPTION(not_initialized());
        }
        insert_iterator output(this);
        for(InputIterator it=first; it != last; ++it) {
            output = *it;
        }
    }

    inline void insert(const map<string, string>& m) {
        insert(m.begin(), m.end());
    }

    inline void insert(const string& key, const string& value) {
        pair<string, string> p = make_pair(key, value);
        insert(&p, &p + 1);
    }

    inline void insert(MDB_val k, MDB_val v) {
        if (!*this) {
            BOOST_THROW_EXCEPTION(not_initialized());
        }
        insert_iterator output(this);
        output = make_pair(k, v);
    }

    void transform_values(unary_functor unary_funct) {                  // value = f(value)
        binary_functor binary_funct = boost::bind(unary_funct, _2);
        transform_values(binary_funct);
    }

    void transform_values(binary_functor binary_funct) {                // value = f(key, value)
        if (!*this) {
            return;
        }
        iterator it(this, 0, false);
        for(; !it.has_reached_end(); ++it) {
            it->second = binary_funct(it->first, it->second);
        }
    }

    void remove_if_pred_key(unary_predicate unary_pred) {               // remove_if(predicate(keys))
        binary_predicate binary_pred = boost::bind(unary_pred, _1);
        remove_if(binary_pred);
    }

    void remove_if_pred_value(unary_predicate unary_pred) {             // remove_if(predicate(value))
        binary_predicate binary_pred = boost::bind(unary_pred, _2);
        remove_if(binary_pred);
    }

    void remove_if(binary_predicate binary_pred) {                      // remove_if(predicate(key, value))
        if (!*this) {
            return;
        }
        iterator it(this, 0, false);                                    // writeable iterator

        for(; !it.has_reached_end(); ++it) {
            if (binary_pred(it->first, it->second)) {
                it.del();
            }
        }
    }

    void remove_duplicates() {
        // todo: delete duplicates in the dict values
    }


    inline string get_dirname() const {
        return this->dirname;
    }

    inline string get_dbname() const {
        return this->dbname;
    }

    inline bool empty() const {
        if (*this) {
            return cbegin().has_reached_end();
        }
        return true;
    }

    inline size_t count(const string& key) const {
        if (bool(*this) && contains(key)) {
            return 1;
        }
        return 0;
    }

    inline void clear() {
        if (*this) {
            env->drop(dbi);
        }
    }

    inline size_t size() const {
        if (!*this) {
            return 0;
        }
        return env->size(dbi);
    }

    inline string operator[] (const string& key) const {
        if (key.empty()) {
            BOOST_THROW_EXCEPTION(empty_key());
        }
        if (!*this) {
            return "";
        }
        const_iterator it(this, key);
        if (it.has_reached_end()) {
            return "";
        }
        return it.get_value();
    }

    inline string at(const string& key) const {
        return at(make_mdb_val(key));
    }

    inline string at(MDB_val k) const {
        if (k.mv_size == 0 || k.mv_data == NULL) {
            BOOST_THROW_EXCEPTION(empty_key());
        }
        if (!*this) {
            BOOST_THROW_EXCEPTION(mdb_notfound());
        }
        const_iterator it(this, k);
        if (it.has_reached_end()) {
            BOOST_THROW_EXCEPTION(mdb_notfound());
        }
        return it.get_value();
    }

    inline string pop(MDB_val k) {
        if (k.mv_size == 0 || k.mv_data == NULL) {
            BOOST_THROW_EXCEPTION(empty_key());
        }
        if (!*this) {
            BOOST_THROW_EXCEPTION(mdb_notfound());
        }
        iterator it(this, k, false);
        if (it.has_reached_end()) {
            BOOST_THROW_EXCEPTION(mdb_notfound());
        }
        return it.pop();
    }

    inline string pop(const string& key) {
        return pop(make_mdb_val(key));
    }

    inline pair<string, string> popitem() {
        if (!*this) {
            BOOST_THROW_EXCEPTION(empty_database());
        }
        iterator it = iterator(this, 0, false);
        if (it.has_reached_end()) {
            BOOST_THROW_EXCEPTION(empty_database());
        }
        pair<string, string> p = *it;
        it.del();
        return p;
    }

    inline bool contains(const string& key) const {
        if (!bool(*this) || key.empty() ) {
            return false;
        }
        return !const_iterator(this, key).has_reached_end();
    }

    class insert_iterator {
    private:
        BOOST_MOVABLE_BUT_NOT_COPYABLE(insert_iterator)
    protected:
        bool initialized;
        PersistentDict* the_dict;
        boost::shared_ptr<environment::transaction> txn;
        boost::shared_ptr<environment::transaction::cursor> cursor;

        void init() {
            initialized = false;
            the_dict = NULL;
            cursor.reset();
            txn.reset();
        }

    public:
        typedef void difference_type;
        typedef void value_type;
        typedef void pointer;
        typedef void reference;
        typedef void iterator_type;
        typedef std::output_iterator_tag iterator_category;

        insert_iterator() {
            init();
        }

        ~insert_iterator() {
            cursor.reset();
            txn.reset();
        }

        operator bool() {
            return initialized;
        }

        insert_iterator(PersistentDict* d): the_dict(d) {
            if (the_dict == NULL || !(*the_dict)) {
                init();
            } else {
                txn = the_dict->env->start_transaction(false);
                cursor = txn->make_cursor(the_dict->dbi);
                initialized = true;
            }
        }

        insert_iterator(BOOST_RV_REF(insert_iterator) other): the_dict(other.the_dict) {
            if (other) {
                cursor.swap(other.cursor);
                txn.swap(other.txn);
                initialized = true;
            } else {
                init();
            }
        }

        insert_iterator& operator=(BOOST_RV_REF(insert_iterator) other) {
            if (other) {
                the_dict = other.the_dict;
                cursor = other.cursor;
                txn = other.txn;
                other.cursor.reset();
                other.txn.reset();
            } else {
                init();
            }
            return *this;
        }

        insert_iterator& operator*() { return *this; }
        insert_iterator& operator++() { return *this; }
        insert_iterator& operator++(int) { return *this; }

        insert_iterator& operator=(const pair<string, string>& p) {
            if (cursor) {
                cursor->set_key_value(make_mdb_val(p.first), make_mdb_val(p.second));
            }
            return *this;
        }

        insert_iterator& operator=(const pair<const string, string>& p) {
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
        typedef typename boost::conditional<B, const PersistentDict*, PersistentDict*>::type dict_ptr_type;
        typedef pair<const string, string> value_type;
        typedef std::input_iterator_tag iterator_category;      // can't really be a forward iterator as operator* does not return a ref

        tmpl_iterator() { init(); }

        operator bool() const { return this->initialized; }

        tmpl_iterator(dict_ptr_type d, int pos=0):
            initialized(false), ro(true), reached_end(false), reached_beginning(false), the_dict(d) {

            init(d, pos);
        }

        tmpl_iterator(dict_ptr_type d, const string& key):
            initialized(false), ro(true), reached_end(false), reached_beginning(false), the_dict(d) {

            init(d, key);
        }

        tmpl_iterator(dict_ptr_type d, MDB_val key):
            initialized(false), ro(true), reached_end(false), reached_beginning(false), the_dict(d) {

            init(d, key);
        }

        template <typename T>
        tmpl_iterator(T d, int pos, bool readonly,
            typename enable_if_c<!is_const<T>::value && is_same<T, dict_ptr_type>::value && !B>::type* dummy = 0):
            initialized(false), ro(readonly), reached_end(false), reached_beginning(false), the_dict(d) {

            init(d, pos, readonly);
        }

        template <typename T>
        tmpl_iterator(T d, const string& key, bool readonly,
            typename enable_if_c<!is_const<T>::value && is_same<T, dict_ptr_type>::value && !B>::type* dummy = 0):
            initialized(false), ro(readonly), reached_end(false), reached_beginning(false), the_dict(d) {

            init(d, key, readonly);
        }

        template <typename T>
        tmpl_iterator(T d, MDB_val key, bool readonly,
            typename enable_if_c<!is_const<T>::value && is_same<T, dict_ptr_type>::value && !B>::type* dummy = 0):
            initialized(false), ro(readonly), reached_end(false), reached_beginning(false), the_dict(d) {

            init(d, key, readonly);
        }

        static tmpl_iterator range(dict_ptr_type d, const string& key) {
            // make an iterator at first key greater than or equal to specified "key"

            if (d == NULL || !(*d)) {
                return tmpl_iterator();
            } else {
                tmpl_iterator it(d, 0);
                it.init_set_range(key);
                return it;
            }
        }

        // move constructor
        tmpl_iterator(BOOST_RV_REF(tmpl_iterator) other):
            initialized(false), ro(other.ro), reached_end(other.reached_end), reached_beginning(other.reached_beginning), the_dict(other.the_dict) {
            if (other) {
                cursor.swap(other.cursor);
                txn.swap(other.txn);
                initialized = true;
            } else {
                init();
            }
        }

        // move assignment
        tmpl_iterator& operator=(BOOST_RV_REF(tmpl_iterator) other) {
            if (other) {
                ro = other.ro;
                reached_beginning = other.reached_beginning;
                reached_end = other.reached_end;
                the_dict = other.the_dict;
                cursor = other.cursor;
                txn = other.txn;
                initialized = true;
                other.cursor.reset();
                other.txn.reset();
            } else {
                init();
            }
            return *this;
        }

        inline size_t size() const {
            if (!txn) {
                return 0;
            }
            return txn->size(the_dict->dbi);
        }

        inline bool has_reached_end() const { return reached_end; }

        inline bool has_reached_beginning() const { return reached_beginning; }

        ~tmpl_iterator() {
            // careful to release pointers in the proper order
            cursor.reset();
            txn.reset();
        }

        void set_rollback(bool val=true) { txn->set_rollback(val); }

        inline bool operator==(const tmpl_iterator& other) const {
            if (initialized != other.initialized) {
                return false;
            }
            if (!initialized) {
                return true;
            }
            if (the_dict == NULL && other.the_dict == NULL) {
                return true;
            }
            if (the_dict == NULL || other.the_dict == NULL) {
                return false;
            }
            if ((*the_dict) != *(other.the_dict) || reached_end != other.reached_end || reached_beginning != other.reached_beginning || ro != other.ro) {
                return false;
            }
            if (reached_end || reached_beginning) {
                return true;
            }
            return get_key() == other.get_key();
        }

        inline bool operator!=(const tmpl_iterator& other) const { return !(*this == other); }

        inline bool operator<(const tmpl_iterator& other) const {
            if (initialized != other.initialized) {
                BOOST_THROW_EXCEPTION(lmdb_error() << lmdb_error::what("Can't compare iterators that have different initialized status"));
            }
            if (!initialized) {
                // both iterators are not initialized, thus they are equal
                return false;
            }
            if (the_dict == NULL && other.the_dict == NULL) {
                return false;
            }
            if (the_dict == NULL || other.the_dict == NULL) {
                BOOST_THROW_EXCEPTION(lmdb_error() << lmdb_error::what("Can't compare iterators that are not relative to the same dict"));
            }
            if ((*the_dict) != *(other.the_dict)) {
                BOOST_THROW_EXCEPTION(lmdb_error() << lmdb_error::what("Can't compare iterators that are not relative to the same dict"));
            }
            if (reached_end && other.reached_end) {
                return false;
            }
            if (reached_beginning && other.reached_beginning) {
                return false;
            }
            if (reached_end || other.reached_beginning) {
                return false;
            }
            if (reached_beginning || other.reached_end) {
                return true;
            }
            return get_key() < other.get_key();
        }

        inline tmpl_iterator& operator++() {
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

        inline tmpl_iterator& operator--() {
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

        inline tmpl_iterator operator++(int) {
            iterator clone(*this);
            this->operator++();
            return clone;
        }

        inline tmpl_iterator operator--(int) {
            iterator clone(*this);
            this->operator--();
            return clone;
        }

        inline pair<const string, string> operator*() const {
            return get_item();
        }

        inline string get_key() const {
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

        inline MDB_val get_key_buffer() const {
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

        inline string get_value() const {
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

        inline MDB_val get_value_buffer() const {
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

        inline pair<const string, string> get_item() const {
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

        inline pair<MDB_val, MDB_val> get_item_buffer() const {
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
        dict_ptr_type the_dict;
        boost::shared_ptr<environment::transaction::cursor> cursor;
        boost::shared_ptr<environment::transaction> txn;

        void init() {
            ro = true;
            initialized = false;
            reached_end = false;
            reached_beginning = false;
            the_dict = NULL;
            cursor.reset();
            txn.reset();
        }

        void init(dict_ptr_type d, int pos=0, bool readonly=true) {
            if (the_dict == NULL || !(*the_dict)) {
                init();
                return;
            }

            txn = the_dict->env->start_transaction(ro);
            cursor = txn->make_cursor(the_dict->dbi);
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

        void init(dict_ptr_type d, const string& key, bool readonly=true) {
            if (the_dict == NULL || !(*the_dict)) {
                init();
            } else {
                txn = the_dict->env->start_transaction(ro);
                cursor = txn->make_cursor(the_dict->dbi);
                initialized = true;
                init_set_position(key);
            }
        }

        void init(dict_ptr_type d, MDB_val key, bool readonly=true) {
            if (the_dict == NULL || !(*the_dict)) {
                init();
            } else {
                txn = the_dict->env->start_transaction(ro);
                cursor = txn->make_cursor(the_dict->dbi);
                initialized = true;
                init_set_position(key);
            }
        }

        void init_set_position(const string& key) {
            init_set_position(make_mdb_val(key));
        }

        void init_set_position(MDB_val k) {
            if (k.mv_size == 0 || k.mv_data == NULL) {
                reached_end = true;
                return;
            }
            reached_end = cursor->position(k) == MDB_NOTFOUND;
        }

        void init_set_range(const string& key) {
            init_set_range(make_mdb_val(key));
        }

        void init_set_range(MDB_val k) {
            if (k.mv_size == 0 || k.mv_data == NULL) {
                reached_end = true;
                return;
            }
            reached_end = cursor->after(k) == MDB_NOTFOUND;
        }

    }; // end class tmpl_iterator

    typedef tmpl_iterator<true> const_iterator;
    typedef tmpl_iterator<false> _iterator;

    class iterator: public _iterator {
    private:
        BOOST_MOVABLE_BUT_NOT_COPYABLE(iterator)

    public:

        class PairProxy {      // kind of a reference like pair<const string, string>&
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

                operator string() const {
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

                operator string() const {
                    MDB_val v;
                    (isecond.cursor)->get_current_value(v);
                    return make_string(v);
                }

                const string& operator=(const string& new_value) {
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

            operator pair<string, string>() const {
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
        iterator(PersistentDict* d, int pos, bool readonly): _iterator(d, pos, readonly), pproxy(*this) { }
        iterator(PersistentDict* d, const string& key, bool readonly): _iterator(d, key, readonly), pproxy(*this) { }
        iterator(PersistentDict* d, MDB_val key, bool readonly): _iterator(d, key, readonly), pproxy(*this) { }

        iterator(BOOST_RV_REF(iterator) other): _iterator(BOOST_MOVE_BASE(_iterator, other)), pproxy(*this) { }
        iterator& operator=(BOOST_RV_REF(iterator) other) {
            _iterator::operator=(BOOST_MOVE_BASE(_iterator, other));
            return *this;
        }

        static iterator range(PersistentDict* d, const string& key, bool readonly=true) {
            iterator it;
            if (d != NULL && bool(*d)) {
                it = iterator(d, 0, readonly);
                it.init_set_range(key);
            }
            return it;
        }

        inline PairProxy& operator*() {
            return pproxy;
        }

        inline PairProxy& operator->() {
            return pproxy;
        }

        inline string pop() {
            if (!initialized) {
                BOOST_THROW_EXCEPTION(not_initialized());
            }
            if (reached_end || reached_beginning) {
                BOOST_THROW_EXCEPTION(mdb_notfound());
            }
            MDB_val v = make_mdb_val();
            cursor->get_current_value(v);
            string result = make_string(v);
            cursor->del();
            return result;
        }

        inline void set_value(const string& value) {
            if (!initialized) {
                BOOST_THROW_EXCEPTION(not_initialized());
            }
            if (reached_end || reached_beginning) {
                BOOST_THROW_EXCEPTION(mdb_notfound());
            }
            cursor->set_current_value(make_mdb_val(value));
        }

        inline void set_value(MDB_val v) {
            if (!initialized) {
                BOOST_THROW_EXCEPTION(not_initialized());
            }
            if (v.mv_data == NULL) {
                BOOST_THROW_EXCEPTION(std::invalid_argument("invalid value"));
            }
            if (reached_end || reached_beginning) {
                BOOST_THROW_EXCEPTION(mdb_notfound());
            }
            cursor->set_current_value(v);
        }

        inline void set_key_value(MDB_val key, MDB_val value) {
            if (!initialized) {
                BOOST_THROW_EXCEPTION(not_initialized());
            }
            if (key.mv_size == 0 || key.mv_data == NULL) {
                BOOST_THROW_EXCEPTION(empty_key());
            }
            if (value.mv_data == NULL) {
                BOOST_THROW_EXCEPTION(std::invalid_argument("invalid value"));
            }
            cursor->set_key_value(key, value);
            reached_beginning = false;
            reached_end = false;
        }

        inline void set_key_value(const string& key, const string& value) {
            set_key_value(make_mdb_val(key), make_mdb_val(value));
        }

        inline void append_key_value(MDB_val key, MDB_val value) {
            if (!initialized) {
                BOOST_THROW_EXCEPTION(not_initialized());
            }
            if (key.mv_size == 0 || key.mv_data == NULL) {
                BOOST_THROW_EXCEPTION(empty_key());
            }
            if (value.mv_data == NULL) {
                BOOST_THROW_EXCEPTION(std::invalid_argument("invalid value"));
            }
            cursor->append_key_value(key, value);
            reached_beginning = false;
            reached_end = false;
        }

        inline void append_key_value(const string& key, const string& value) {
            append_key_value(make_mdb_val(key), make_mdb_val(value));
        }

        inline void del() {
            if (!initialized) {
                BOOST_THROW_EXCEPTION(not_initialized());
            }
            if (reached_end || reached_beginning) {
                BOOST_THROW_EXCEPTION(mdb_notfound());
            }
            cursor->del();
        }

    };

    inline vector<string> get_all_keys() const {
        if (!*this) {
            return vector<string>();
        }
        int pos = 0;
        const_iterator it = cbegin();
        vector<string> v(it.size());
        for (; !it.has_reached_end(); ++it) {
            v[pos] = it.get_key();
            ++pos;
        }
        return v;
    }

    inline vector<string> get_all_values() const {
        if (!*this) {
            return vector<string>();
        }
        int pos = 0;
        const_iterator it = cbegin();
        vector<string> v(it.size());
        for (; !it.has_reached_end(); ++it) {
            v[pos] = it.get_value();
            ++pos;
        }
        return v;
    }

    inline vector<pair<string, string> > get_all_items_if(binary_predicate binary_pred) const {
        if (!*this) {
            return vector<pair<string, string> >();
        }
        const_iterator it = cbegin();
        vector< pair<string, string> > v;
        pair<string, string> p;
        for (; !it.has_reached_end(); ++it) {
            p = it.get_item();
            if (binary_pred(p.first, p.second)) {
                v.push_back(p);
            }
        }
        return v;
    }

    inline vector<pair<string, string> > get_all_items() const {
        return get_all_items_if(binary_true_pred);
    }

    inline iterator before(bool readonly=true) {
        return iterator(this, -1, readonly);
    }

    inline const_iterator cbefore() {
        return const_iterator(this, -1);
    }

    inline iterator begin(bool readonly=true) {
        // position the iterator exactly on the first element (or "after end" if db is empty)
        return iterator(this, 0, readonly);
    }

    inline const_iterator cbegin() const {
        return const_iterator(this, 0);
    }

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

    inline iterator end(bool readonly=true) {
        return iterator(this, 1, readonly);
    }

    inline const_iterator cend() const {
        return const_iterator(this, 1);
    }

    iterator find(const string& key, bool readonly=true) {
        return iterator(this, key, readonly);
    }

    const_iterator cfind(const string& key) const {
        return const_iterator(this, key);
    }



};




}   // end NS quiet

