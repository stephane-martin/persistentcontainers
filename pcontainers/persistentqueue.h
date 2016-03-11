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
#include <stdlib.h>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/function.hpp>
#include "persistentdict.h"

namespace quiet {

using std::string;
using std::pair;
using std::vector;
using std::map;
using std::cout;
using std::endl;
using std::exception;

class PersistentQueue {
protected:
    PersistentDict the_dict;
    static const long long MIDDLE = 4611686018427387903;
    static const int NDIGITS = 19;

public:
    typedef std::allocator<string> A;
    typedef A allocator_type;
    typedef A::value_type value_type;
    typedef A::reference reference;
    typedef A::const_reference const_reference;
    typedef A::difference_type difference_type;
    typedef A::size_type size_type;

    PersistentQueue() {
        the_dict = PersistentDict();
    }

    PersistentQueue(const string& directory_name, const string& database_name="", const lmdb_options& opts=lmdb_options()) {
        the_dict = PersistentDict(directory_name, database_name, opts);
    }

    void close() {
        the_dict.close();
    }

    ~PersistentQueue() {
        close();
    }

    PersistentQueue(const PersistentQueue& other) {
        the_dict = other.the_dict;
    }

    PersistentQueue& operator=(PersistentQueue other) {
        if (*this != other) {
            swap(*this, other);
        }
        return *this;
    }

    friend inline void swap(PersistentQueue& first, PersistentQueue& second) {
        using std::swap;
        swap(first.the_dict, second.the_dict);
    }

    operator bool() const {
        return bool(the_dict);
    }

    friend inline bool operator==(const PersistentQueue& one, const PersistentQueue& other) {
        return one.the_dict == other.the_dict;
    }

    inline string get_dirname() const {
        return the_dict.get_dirname();
    }

    inline string get_dbname() const {
        return the_dict.get_dbname();
    }

    inline size_t size() const {
        return the_dict.size();
    }

    inline bool empty() const {
        return the_dict.empty();
    }

    class oiterator {
    private:
        BOOST_MOVABLE_BUT_NOT_COPYABLE(oiterator)
    protected:
        bool initialized;
        PersistentQueue* the_queue;
        boost::shared_ptr<environment::transaction> txn;
        boost::shared_ptr<environment::transaction::cursor> cursor;

        void init() {
            initialized = false;
            the_queue = NULL;
            txn.reset();
            cursor.reset();
        }

    public:
        typedef A::difference_type difference_type;
        typedef A::value_type value_type;
        typedef A::reference reference;
        typedef A::pointer pointer;
        typedef std::output_iterator_tag iterator_category;

        oiterator() {
            init();
        }

        oiterator(PersistentQueue* q): the_queue(q) {
            if (q != NULL && bool(*q)) {
                txn = the_queue->the_dict.env->start_transaction(false);
                cursor = txn->make_cursor(the_queue->the_dict.dbi);
                initialized = true;
            } else {
                init();
            }
        }

        ~oiterator() {
            cursor.reset();
            txn.reset();
        }

        oiterator(BOOST_RV_REF(oiterator) other): the_queue(other.the_queue), txn(other.txn), cursor(other.cursor) {
            if (other) {
                initialized = true;
                other.cursor.reset();
                other.txn.reset();
            } else {
                init();
            }
        }

        oiterator& operator=(BOOST_RV_REF(oiterator) other) {
            if (other) {
                the_queue = other.the_queue;
                txn = other.txn;
                cursor = other.cursor;
                initialized = true;
                other.cursor.reset();
                other.txn.reset();
            } else {
                init();
            }
            return *this;
        }

        class pushproxy {
        protected:
            boost::shared_ptr<environment::transaction::cursor> cursor;
            int pos;
        public:
            pushproxy(boost::shared_ptr<environment::transaction::cursor> c, int p=1): cursor(c), pos(p) { }

            const string& operator=(const string& new_value) {
                MDB_val k, v;
                string insert_key;
                v = make_mdb_val(new_value);
                int res;
                if (pos > 0) {
                    res = cursor->last();
                } else {
                    res = cursor->first();
                }
                if (res == MDB_NOTFOUND) {
                    insert_key = any_tostring(MIDDLE);
                } else {
                    cursor->get_current_key(k);
                    insert_key = make_string(k);
                    long long int_k = strtoull((char*) insert_key.c_str(), NULL, 10);
                    if (int_k == 0) {
                        throw lmdb_error("A key in the database in not an integer key");
                    }
                    insert_key = any_tostring(int_k + pos, NDIGITS);
                }
                k = make_mdb_val(insert_key);
                if (pos > 0) {
                    cursor->append_key_value(k, v);
                } else {
                    cursor->set_key_value(k, v);
                }
                return new_value;
            }

        }; // end class pushproxy

        operator bool() const { return this->initialized; }

        void set_rollback(bool val=true) {
            if (initialized) {
                txn->set_rollback(val);
            }
        }

        inline size_t size() {
            if (initialized) {
                return cursor->size();
            }
            return 0;
        }

        pushproxy operator*() { return pushproxy(cursor, 1); }
        oiterator& operator++() { return *this; }
        oiterator& operator++(int) { return *this; }
        oiterator& operator--() { return *this; }
        oiterator& operator--(int) { return *this; }
    }; // end class oiterator

    class roiterator: public oiterator {
    private:
        BOOST_MOVABLE_BUT_NOT_COPYABLE(roiterator)
    public:
        roiterator(): oiterator() { }
        roiterator(PersistentQueue* q): oiterator(q) { }
        roiterator(BOOST_RV_REF(roiterator) other): oiterator(BOOST_MOVE_BASE(oiterator, other)) { }
        roiterator& operator=(BOOST_RV_REF(roiterator) other) {
            oiterator::operator=(BOOST_MOVE_BASE(oiterator, other));
            return *this;
        }
        pushproxy operator*() {
            return pushproxy(cursor, -1);
        }
    };


    class iiterator {
    private:
        BOOST_MOVABLE_BUT_NOT_COPYABLE(iiterator)

    protected:
        bool initialized;
        boost::scoped_ptr<string> current_value;
        PersistentQueue* the_queue;
        boost::shared_ptr<environment::transaction> txn;
        boost::shared_ptr<environment::transaction::cursor> cursor;

        void init() {
            initialized = false;
            the_queue = NULL;
            current_value.reset();
            txn.reset();
            cursor.reset();
        }

        inline bool empty() const {
            if (initialized) {
                return cursor->first() == MDB_NOTFOUND;
            }
            return true;
        }

        inline void _next_value() {
            if (initialized && cursor->first() != MDB_NOTFOUND) {
                MDB_val v;
                cursor->get_current_value(v);
                current_value.reset(new string((char*) v.mv_data, v.mv_size));
                cursor->del();
            } else {
                current_value.reset();
            }
        }

        inline void _last_value() {
            if (initialized && cursor->last() != MDB_NOTFOUND) {
                MDB_val v;
                cursor->get_current_value(v);
                current_value.reset(new string((char*) v.mv_data, v.mv_size));
                cursor->del();
            } else {
                current_value.reset();
            }
        }

    public:
        typedef A::difference_type difference_type;
        typedef A::value_type value_type;
        typedef A::reference reference;
        typedef A::pointer pointer;
        typedef std::input_iterator_tag iterator_category;

        iiterator() {
            init();
        }

        iiterator(PersistentQueue* q, int pos=0): initialized(false), the_queue(q) {
            if (q != NULL && bool(*q)) {
                txn = the_queue->the_dict.env->start_transaction(false);
                cursor = txn->make_cursor(the_queue->the_dict.dbi);
                initialized = true;
                if (pos > 0) {
                    _last_value();
                } else {
                    _next_value();
                }
            } else {
                init();
            }
        }

        ~iiterator() {
            current_value.reset();
            cursor.reset();
            txn.reset();
        }

        // move constructor
        iiterator(BOOST_RV_REF(iiterator) other): the_queue(other.the_queue), txn(other.txn), cursor(other.cursor) {
            if (other) {
                current_value.reset();
                if (other.current_value) {
                    current_value.reset(new string(*other.current_value));
                }
                initialized = true;
                other.cursor.reset();
                other.txn.reset();
            } else {
                init();
            }
        }

        // move assignment
        iiterator& operator=(BOOST_RV_REF(iiterator) other) {
            if (other) {
                the_queue = other.the_queue;
                current_value.reset();
                if (other.current_value) {
                    current_value.reset(new string(*other.current_value));
                }
                txn = other.txn;
                cursor = other.cursor;
                initialized = true;
                other.cursor.reset();
                other.txn.reset();
            } else {
                init();
            }
            return *this;
        }

        operator bool() const {
            return this->initialized;
        }

        void set_rollback(bool val=true) {
            if (initialized) {
                txn->set_rollback(val);
            }
        }

        inline size_t size() const {
            if (initialized) {
                return cursor->size();
            }
            return 0;
        }

        inline string operator*() {
            if (current_value) {
                return *current_value;
            }
            throw empty_database();
        }

        inline iiterator& operator++() {
            if (initialized) {
                _next_value();
            }
            return *this;
        }

        inline iiterator& operator--() {
            if (initialized) {
                _last_value();
            }
            return *this;
        }

        inline iiterator operator++(int) {
            iiterator clone(*this);
            this->operator++();
            return clone;
        }

        inline iiterator operator--(int) {
            iiterator clone(*this);
            this->operator--();
            return clone;
        }

        inline bool operator==(const iiterator& other) const {
            if (!initialized && !other.initialized) {
                return true;
            }
            if (initialized && other.initialized) {
                return (!current_value) && (!other.current_value) && (*the_queue == *other.the_queue);
            }
            return (!current_value) && (!other.current_value);
        }

        inline bool operator!=(const iiterator& other) const {
            return !(*this == other);
        }
    };


    void remove_if(unary_predicate unary_pred) {
        the_dict.remove_if_pred_value(unary_pred);
    }

    void transform_values(unary_functor unary_funct) {
        the_dict.transform_values(unary_funct);
    }

    PersistentQueue move_to(const string& directory_name, const string& database_name="", const lmdb_options& opts=lmdb_options(), long chunk_size=-1) {
        if (get_dirname() == directory_name && get_dbname() == database_name) {
            return *this;
        }
        // todo: complete
    }

    void push_back(size_t n, const string& val) {
        if (n <= 0) {
            return;
        }
        oiterator it(this);
        for(size_t i=0; i<n; i++) {
            *it = val;
            it++;
        }
    }

    void push_back(const string& val) {
        oiterator it(this);
        *it = val;
    }

    template <class InputIterator>
    void push_back(InputIterator first, InputIterator last) {
        oiterator it(this);
        for (InputIterator input_it=first; input_it != last; ++input_it) {
            *it = *input_it;
            it++;
        }
    }

    void push_front(const string& val) {
        roiterator it(this);
        *it = val;
    }

    template <class InputIterator>
    void push_front(InputIterator first, InputIterator last) {
        roiterator it(this);
        for (InputIterator input_it=first; input_it != last; ++input_it) {
            *it = *input_it;
            it++;
        }
    }

    void push_front(size_t n, const string& val) {
        if (n <= 0) {
            return;
        }
        roiterator it(this);
        for(size_t i=0; i<n; i++) {
            *it = val;
            it++;
        }
    }

    string pop_back() {
        return *iiterator(this, 1);
    }

    string pop_front() {
        return *iiterator(this);
    }

    vector<string> pop_all() {
        vector<string> v;
        iiterator it(this);
        iiterator end;
        for(; it != end; ++it) {
            v.push_back(*it);
        }
        return v;
    }

    void clear() {
        the_dict.clear();
    }


};



}   // end NS quiet

