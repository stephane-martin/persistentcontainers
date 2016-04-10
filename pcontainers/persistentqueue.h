#pragma once

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/throw_exception.hpp>
#include <boost/atomic.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <bstrlib/bstrwrap.h>

#include "persistentdict.h"
#include "lmdb.h"

namespace quiet {

using boost::mutex;
using boost::lock_guard;
using boost::shared_ptr;
using boost::enable_shared_from_this;
using Bstrlib::CBString;

class PersistentQueue: public enable_shared_from_this<PersistentQueue> {
private:
    PersistentQueue(const PersistentQueue& other);
    PersistentQueue& operator=(const PersistentQueue&);
    PersistentQueue(const CBString& directory_name, const CBString& database_name, const lmdb_options& options):
            the_dict(directory_name, database_name, options) { }
    void close() { the_dict.close(); }
protected:
    PersistentDict the_dict;        // PersistentDict is a member: implemented in terms of
    static const long long MIDDLE = 4611686018427387903;
    static const int NDIGITS = 19;


public:
    typedef CBString value_type;
    typedef CBString& reference;
    typedef const CBString& const_reference;

    BOOST_EXPLICIT_OPERATOR_BOOL()
    bool operator!() const { return !the_dict; }

    ~PersistentQueue() { close(); }

    static inline shared_ptr<PersistentQueue> factory(const CBString& directory_name, const CBString& database_name="", const lmdb_options& options=lmdb_options()) {
        return shared_ptr<PersistentQueue>(new PersistentQueue(directory_name, database_name, options));
    }

    bool operator==(const PersistentQueue& other) {
        return the_dict == other.the_dict;
    }

    bool operator!=(const PersistentQueue& other) {
        return the_dict != other.the_dict;
    }

    CBString get_dirname() const {
        return the_dict.get_dirname();
    }

    CBString get_dbname() const {
        return the_dict.get_dbname();
    }

    size_t size() const {
        return the_dict.size();
    }

    bool empty() const {
        return the_dict.empty();
    }

    class back_insert_iterator {
    private:
        BOOST_MOVABLE_BUT_NOT_COPYABLE(back_insert_iterator)
    protected:
        boost::atomic_bool initialized;
        int direction;
        shared_ptr<PersistentQueue> the_queue;
        shared_ptr<environment::transaction> txn;
        shared_ptr<environment::transaction::cursor> cursor;

        void swap(back_insert_iterator& other) {
            using std::swap;
            the_queue.swap(other.the_queue);
            cursor.swap(other.cursor);
            txn.swap(other.txn);
        }

    public:
        typedef CBString value_type;
        typedef CBString& reference;
        typedef CBString* pointer;
        typedef std::output_iterator_tag iterator_category;

        BOOST_EXPLICIT_OPERATOR_BOOL()
        bool operator!() const { return !initialized.load(); }

        back_insert_iterator(): initialized(false), direction(1), the_queue(), txn(), cursor() { }

        back_insert_iterator(shared_ptr<PersistentQueue> q): initialized(false), direction(1), the_queue(), txn(), cursor() {
            if (bool(q) && bool(*q)) {
                the_queue = q;
                txn = the_queue->the_dict.env->start_transaction(false);
                cursor = txn->make_cursor(the_queue->the_dict.dbi);
                initialized.store(true);
            }
        }

        ~back_insert_iterator() {
            cursor.reset();
            txn.reset();
            the_queue.reset();
        }

        // move constructor
        back_insert_iterator(BOOST_RV_REF(back_insert_iterator) other): initialized(false), direction(1), the_queue(),
                                                                        txn(), cursor() {
            if (other) {
                other.initialized.store(false);
                swap(other);
                initialized.store(true);
            }
        }

        // move assignment
        back_insert_iterator& operator=(BOOST_RV_REF(back_insert_iterator) other) {
            initialized.store(false);
            direction = 1;
            the_queue.reset();
            cursor.reset();
            txn.reset();
            if (other) {
                other.initialized.store(false);
                swap(other);
                initialized.store(true);
            }
            return *this;
        }

        back_insert_iterator& operator=(const CBString& new_value) {
            return operator=(make_mdb_val(new_value));
        }

        back_insert_iterator& operator=(MDB_val v);

        void set_rollback(bool val=true) {
            if (*this) {
                txn->set_rollback(val);
            }
        }

        size_t size() const { return bool(*this) ? cursor->size() : 0; }

        back_insert_iterator& operator*() { return *this; }
        back_insert_iterator& operator++() { return *this; }
        back_insert_iterator& operator++(int) { return *this; }
        back_insert_iterator& operator--() { return *this; }
        back_insert_iterator& operator--(int) { return *this; }
    }; // end class back_insert_iterator


    class front_insert_iterator: private back_insert_iterator {     // private: is-implemented-in-terms of
    private:
        BOOST_MOVABLE_BUT_NOT_COPYABLE(front_insert_iterator)
    protected:
        void swap(front_insert_iterator& other) {
            back_insert_iterator::swap(other);
        }
    public:
        BOOST_EXPLICIT_OPERATOR_BOOL()
        bool operator!() const { return back_insert_iterator::operator!(); }

        size_t size() const { return back_insert_iterator::size(); }
        void set_rollback(bool val=true) { back_insert_iterator::set_rollback(val); }

        front_insert_iterator(): back_insert_iterator() { direction = -1;}
        front_insert_iterator(shared_ptr<PersistentQueue> q): back_insert_iterator(q) { direction = -1; }

        ~front_insert_iterator() {
            cursor.reset();
            txn.reset();
        }

        front_insert_iterator(BOOST_RV_REF(front_insert_iterator) other): back_insert_iterator(BOOST_MOVE_BASE(back_insert_iterator, other)) {
            direction = -1;
        }
        front_insert_iterator& operator=(BOOST_RV_REF(front_insert_iterator) other) {
            back_insert_iterator::operator=(BOOST_MOVE_BASE(back_insert_iterator, other));
            direction = -1;
            return *this;
        }
        front_insert_iterator& operator=(const CBString& new_value) {
            back_insert_iterator::operator=(new_value);
            return *this;
        }
        front_insert_iterator& operator=(MDB_val new_value) {
            back_insert_iterator::operator=(new_value);
            return *this;
        }

        front_insert_iterator& operator*() { return *this; }
        front_insert_iterator& operator++() { return *this; }
        front_insert_iterator& operator++(int) { return *this; }
        front_insert_iterator& operator--() { return *this; }
        front_insert_iterator& operator--(int) { return *this; }

    };      // END CLASS front_insert_iterator


    class iiterator {
    private:
        BOOST_MOVABLE_BUT_NOT_COPYABLE(iiterator)

    protected:
        boost::atomic_bool initialized;
        mutex current_value_lock;
        boost::scoped_ptr<CBString> current_value;

        shared_ptr<PersistentQueue> the_queue;
        shared_ptr<environment::transaction> txn;
        shared_ptr<environment::transaction::cursor> cursor;

        inline bool empty() const {
            if (cursor) {
                return cursor->first() == MDB_NOTFOUND;
            }
            return true;
        }

        void _next_value();
        void _last_value();

        void swap(iiterator& other) {
            using std::swap;
            current_value.swap(other.current_value);
            the_queue.swap(other.the_queue);
            cursor.swap(other.cursor);
            txn.swap(other.txn);
        }

    public:
        typedef CBString value_type;
        typedef CBString& reference;
        typedef CBString* pointer;
        typedef std::input_iterator_tag iterator_category;

        BOOST_EXPLICIT_OPERATOR_BOOL()
        bool operator!() const { return !initialized.load(); }

        iiterator(): initialized(false), current_value(), the_queue(), txn(), cursor() { }

        iiterator(shared_ptr<PersistentQueue> q, int pos=0): initialized(false), current_value(), the_queue(), txn(), cursor() {
            if (bool(q) && bool(*q)) {
                the_queue = q;
                txn = the_queue->the_dict.env->start_transaction(false);
                cursor = txn->make_cursor(the_queue->the_dict.dbi);
                if (pos > 0) {
                    _last_value();
                } else {
                    _next_value();
                }
                initialized.store(true);
            } else {
                _LOG_DEBUG << "New trivial PersistentQueue iiterator";
            }
        }

        ~iiterator() {
            current_value.reset();
            cursor.reset();
            txn.reset();
            the_queue.reset();
        }


        // move constructor
        iiterator(BOOST_RV_REF(iiterator) other): initialized(false), current_value(), the_queue(), txn(), cursor() {
            if (other) {
                other.initialized.store(false);
                swap(other);
                initialized.store(true);
            }
        }

        // move assignment
        iiterator& operator=(BOOST_RV_REF(iiterator) other) {
            initialized.store(false);
            current_value.reset();
            the_queue.reset();
            cursor.reset();
            txn.reset();
            if (other) {
                other.initialized.store(false);
                swap(other);
                initialized.store(true);
            }
            return *this;
        }

        void set_rollback(bool val=true) {
            if (*this) {
                txn->set_rollback(val);
            }
        }

        size_t size() const {
            if (*this) {
                return cursor->size();
            }
            return 0;
        }

        const CBString& operator*() {
            if (current_value) {
                return *current_value;
            }
            BOOST_THROW_EXCEPTION ( empty_database() );
        }

        MDB_val get_value_buffer() {
            if (current_value) {
                return make_mdb_val(*current_value);
            }
            BOOST_THROW_EXCEPTION ( empty_database() );
        }


        iiterator& operator++() {
            if (*this) {
                _next_value();
            }
            return *this;
        }

        iiterator& operator--() {
            if (*this) {
                _last_value();
            }
            return *this;
        }

        iiterator operator++(int) {
            return operator++();
        }

        iiterator operator--(int) {
            return operator--();
        }

        bool operator==(const iiterator& other) const {
            if (!bool(*this) && !bool(other)) {
                return true;
            }
            if (bool(*this) && bool(other)) {
                return (!current_value) && (!other.current_value) && (*the_queue == *other.the_queue);
            }
            return (!current_value) && (!other.current_value);
        }

        bool operator!=(const iiterator& other) const {
            return !(*this == other);
        }
    };  // END CLASS iiterator

    void remove_if(unary_predicate unary_pred) { the_dict.remove_if_pred_value(unary_pred); }
    void transform_values(unary_functor unary_funct) { the_dict.transform_values(unary_funct); }
    void move_to(PersistentQueue& other, ssize_t chunk_size=-1);
    void push_back(size_t n, const CBString& val);

    back_insert_iterator back_inserter() {
        return back_insert_iterator(shared_from_this());
    }

    void push_back(const CBString& val) {
        back_insert_iterator it(shared_from_this());
        it = val;
    }



    void push_back(MDB_val val) {
        back_insert_iterator it(shared_from_this());
        it = val;
    }

    template <class InputIterator>
    void push_back(InputIterator first, InputIterator last) {
        back_insert_iterator it(shared_from_this());
        for (; first != last; ++first) {
            it = *first;
        }
    }

    void push_front(const CBString& val) {
        front_insert_iterator it(shared_from_this());
        it = val;
    }

    void push_front(MDB_val val) {
        front_insert_iterator it(shared_from_this());
        it = val;
    }

    template <class InputIterator>
    void push_front(InputIterator first, InputIterator last) {
        front_insert_iterator it(shared_from_this());
        for (; first != last; ++first) {
            it = *first;
        }
    }

    void push_front(size_t n, const CBString& val);
    CBString pop_back() { return CBString(*iiterator(shared_from_this(), 1)); }
    CBString pop_front() { return CBString(*iiterator(shared_from_this())); }

    template <class OutputIterator>
    void pop_all(OutputIterator oit) {
        iiterator it(shared_from_this());
        iiterator end;
        for(; it != end; ++it) {
            oit = *it;
        }
    }

    void clear() { the_dict.clear(); }
    void remove_duplicates() { the_dict.remove_duplicates(); }


}; // END CLASS PersistentQueue

}   // end NS quiet

