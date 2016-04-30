#pragma once

#include <string>
#include <boost/core/ref.hpp>
#include <boost/container/vector.hpp>
#include <boost/numeric/conversion/converter.hpp>
#include <boost/functional/hash.hpp>
#include <boost/bind.hpp>
#include <boost/move/move.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/exception/exception.hpp>
#include <boost/exception_ptr.hpp>
#include <boost/throw_exception.hpp>
#include <boost/atomic.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/lockable_adapter.hpp>
#include <boost/thread/future.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/interprocess/sync/named_condition.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/chrono/chrono.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/core/noncopyable.hpp>
#include <bstrlib/bstrwrap.h>

#include "persistentdict.h"
#include "../utils/promise_queue.h"
#include "../utils/packaged_task_queue.h"
#include "../logging/logging.h"
#include "lmdb.h"

namespace quiet {

// locks
using boost::mutex;
using boost::shared_mutex;
using boost::unique_lock;
using boost::shared_lock;
using boost::upgrade_to_unique_lock;
using boost::upgrade_lock;
using boost::lock_guard;
using boost::shared_lockable_adapter;

// futures
using boost::packaged_task;
using boost::future;
using boost::shared_future;
using boost::promise;
using boost::make_ready_future;
using boost::make_exceptional;

// interprocess locks
using boost::interprocess::named_mutex;
using boost::interprocess::named_condition;
using boost::interprocess::open_or_create;

// chrono
using boost::chrono::milliseconds;
using boost::posix_time::ptime;
using boost::posix_time::time_duration;
using boost::posix_time::millisec;

// smart pointers
using boost::scoped_ptr;
using boost::shared_ptr;
using boost::enable_shared_from_this;
using boost::make_shared;

// CBString
using Bstrlib::CBString;

typedef boost::numeric::converter<long, long long> LongLong2Long ;

class PersistentQueue: public enable_shared_from_this<PersistentQueue>, private boost::noncopyable {
public:
    typedef CBString value_type;
    typedef CBString& reference;
    typedef const CBString& const_reference;

    typedef shared_future < CBString > SharedFuture;
    typedef utils::ExpiryPromisePtr<CBString> PromisePtr;

private:
    mutable boost::atomic_bool starting_flag;
    mutable boost::atomic_bool stopping_flag;
    mutable boost::atomic_bool running_flag;

    mutable boost::atomic_bool dispatcher_is_running_flag;
    mutable scoped_ptr<boost::thread> dispatcher_thread_ptr;
    mutable scoped_ptr < utils::promise_queue<CBString> > waiters;

    mutable boost::atomic_bool pusher_is_running_flag;
    mutable scoped_ptr<boost::thread> pusher_thread_ptr;
    mutable scoped_ptr<utils::packaged_task_queue> push_queue;

    scoped_ptr<named_mutex> mutex;
    scoped_ptr<named_condition> queue_is_empty;

    PersistentQueue(const PersistentQueue& other);
    PersistentQueue& operator=(const PersistentQueue&);

    PersistentQueue(const CBString& directory_name, const CBString& database_name, const lmdb_options& options):
            stopping_flag(false),
            dispatcher_is_running_flag(false),
            pusher_is_running_flag(false),
            the_dict(PersistentDict::factory(directory_name, database_name, options))

        {
            start();
        }

    void create_interprocess_sync_objects() {
        boost::hash<std::string> string_hash;
        CBString cb_name(the_dict->get_dirname() + "/###/" + the_dict->get_dbname());
        CBString cb_mutex_name(cb_name + "/mutex");
        CBString cb_condition_name(cb_name + "/condition");
        std::string mutex_name((const char*)cb_mutex_name, cb_mutex_name.length());
        std::string condition_name((const char*)cb_condition_name, cb_condition_name.length());
        size_t mutex_h = string_hash(mutex_name);
        size_t condition_h = string_hash(condition_name);
        cb_mutex_name.format("%u", mutex_h);
        cb_condition_name.format("%u", condition_h);
        cb_mutex_name = "persistent_mutex" + cb_mutex_name;
        cb_condition_name = "persistent_condition" + cb_condition_name;
        mutex.reset(new named_mutex(open_or_create, cb_mutex_name));
        queue_is_empty.reset(new named_condition(open_or_create, cb_condition_name));
    }

    void start() {
        create_interprocess_sync_objects();
        start_dispatcher_thread();
        start_pusher_thread();
    }

    void stop() {
        stopping_flag.store(true);
        stop_pusher_thread();
        stop_dispatcher_thread();
    }

    void start_pusher_thread() {
        _LOG_DEBUG << "Starting pusher thread";
        push_queue.reset(new utils::packaged_task_queue());
        pusher_thread_ptr.reset(new boost::thread(boost::bind(&PersistentQueue::pusher_thread_fun, this)));
        while (!pusher_is_running_flag.load()) { }
    }

    void stop_pusher_thread() {
        _LOG_DEBUG << "Stopping pusher thread";
        if (pusher_thread_ptr && pusher_thread_ptr->joinable()) {
            _LOG_DEBUG << "Asking to interrupt pusher thread";
            pusher_thread_ptr->interrupt();
            pusher_thread_ptr->join();
            pusher_thread_ptr.reset();
            push_queue.reset();
            _LOG_DEBUG << "Stopped pusher thread";
        } else {
            _LOG_WARNING << "pusher_thread_ptr was not valid";
        }
        pusher_is_running_flag.store(false);
    }

    void pusher_thread_fun() {
        pusher_is_running_flag.store(true);
        shared_ptr < packaged_task < bool() > > task_ptr;

        try {
            while (true) {
                task_ptr = push_queue->wait_and_pop(milliseconds(2000));
                if (task_ptr) {
                    task_ptr->operator()();
                }
            }
        } catch (boost::thread_interrupted& ex) {
            _LOG_DEBUG << "pusher thread has been interrupted";
        }

        // push the remaining objects
        while ((task_ptr = push_queue->try_pop())) {
            task_ptr->operator()();
        }
    }

    void start_dispatcher_thread() {
        _LOG_DEBUG << "Starting dispatcher thread";
        waiters.reset(new utils::promise_queue<CBString>());
        dispatcher_thread_ptr.reset(new boost::thread(boost::bind(&PersistentQueue::dispatcher_thread_fun, this)));
        while (!dispatcher_is_running_flag.load()) { }
    }

    void stop_dispatcher_thread() {
        _LOG_DEBUG << "Stopping dispatcher thread";
        waiters.reset();    // the promise_queue destructor unblocks waiters->wait_and_pop() in the dispatcher
        queue_is_empty->notify_all();
        if (dispatcher_thread_ptr && dispatcher_thread_ptr->joinable()) {
            dispatcher_thread_ptr->join();
            dispatcher_thread_ptr.reset();
            _LOG_DEBUG << "Stopped dispatcher thread";
        }
        dispatcher_is_running_flag.store(false);
    }

    void dispatcher_thread_fun() {
        dispatcher_is_running_flag.store(true);
        while (!stopping_flag.load()) {
            _LOG_DEBUG << "dispatcher_thread: asking for a waiter";
            PromisePtr waiter = waiters->wait_and_pop();
            if (waiter) {   // waiter can be NULL if the promise_queue was stopped cause of end of operations
                _LOG_DEBUG << "dispatcher_thread: got a waiter";
                boost::interprocess::scoped_lock<named_mutex> queue_lock(*mutex);
                while (empty() && !stopping_flag.load() && !waiter.expired()) {
                    queue_is_empty->timed_wait(queue_lock, waiter.get_next_waiting_point());
                }
                if (stopping_flag.load()) {
                    _LOG_DEBUG << "dispatcher_thread: cancelling the waiter: stopping";
                    queue_lock.unlock();
                    waiter->set_exception(boost::copy_exception(lmdb::stopping_ops()));
                } else if (waiter.expired()) {
                    _LOG_DEBUG << "dispatcher_thread: waiter is expired";
                    queue_lock.unlock();
                    waiter->set_exception(copy_exception(lmdb::expired()));
                } else {
                    _LOG_DEBUG << "dispatcher_thread: pushing the element to the waiter";
                    try {
                        CBString value(*iiterator(shared_from_this(), 0, queue_lock));
                        waiter->set_value(value);
                    } catch (...) {
                        waiter->set_exception(boost::current_exception());
                    }
                }
            } else {
                _LOG_DEBUG << "dispatcher_thread: waiter is empty (stopping indication)";
            }
        }
        _LOG_DEBUG << "dispatcher_thread: finished";
    }

    bool cbstring_push_back(const CBString& val) {
        return push_back(val);
    }

    bool cbstring_push_front(const CBString& val) {
        return push_front(val);
    }

    bool vector_push_back(const boost::container::vector<CBString>& v) {
        return push_back(v.cbegin(), v.cend());
    }

    bool vector_push_front(const boost::container::vector<CBString>& v) {
        return push_front(v.cbegin(), v.cend());
    }


protected:
    shared_ptr<PersistentDict> the_dict;        // PersistentDict is a member: implemented in terms of
    static const long long MIDDLE = 4611686018427387903;
    static const int NDIGITS = 19;


public:
    BOOST_EXPLICIT_OPERATOR_BOOL()
    bool operator!() const { return !*the_dict; }

    ~PersistentQueue() {
        stop();
    }

    static inline shared_ptr<PersistentQueue> factory(const CBString& directory_name, const CBString& database_name="", const lmdb_options& options=lmdb_options()) {
        return shared_ptr<PersistentQueue>(new PersistentQueue(directory_name, database_name, options));
    }

    bool operator==(const PersistentQueue& other) {
        return *the_dict == *(other.the_dict);
    }

    bool operator!=(const PersistentQueue& other) {
        return *the_dict != *(other.the_dict);
    }

    CBString get_dirname() const {
        return the_dict->get_dirname();
    }

    CBString get_dbname() const {
        return the_dict->get_dbname();
    }

    size_t size() const {
        return the_dict->size();
    }

    bool empty() const {
        return the_dict->empty();
    }

    class back_insert_iterator {
    private:
        BOOST_MOVABLE_BUT_NOT_COPYABLE(back_insert_iterator)
    protected:
        boost::atomic_bool initialized;
        int direction;
        bool new_elements;
        boost::interprocess::scoped_lock<named_mutex> queue_lock;
        shared_ptr<PersistentQueue> the_queue;
        shared_ptr<environment::transaction> txn;
        shared_ptr<environment::transaction::cursor> cursor;

        void swap(back_insert_iterator& other) {
            using std::swap;
            the_queue.swap(other.the_queue);
            cursor.swap(other.cursor);
            txn.swap(other.txn);
            swap(new_elements, other.new_elements);
            queue_lock = boost::move(other.queue_lock);
        }

        void close() {
            if (initialized.load()) {
                initialized.store(false);
                cursor.reset();
                txn.reset();
                if (new_elements) {
                    the_queue->queue_is_empty->notify_all();
                }
                queue_lock.unlock();
                new_elements = false;
                the_queue.reset();
            }
        }

    public:
        typedef CBString value_type;
        typedef CBString& reference;
        typedef CBString* pointer;
        typedef std::output_iterator_tag iterator_category;

        BOOST_EXPLICIT_OPERATOR_BOOL()
        bool operator!() const { return !initialized.load(); }

        back_insert_iterator(): initialized(false), direction(1), new_elements(false), the_queue(), txn(), cursor() { }

        back_insert_iterator(shared_ptr<PersistentQueue> q):
                initialized(false), direction(1), new_elements(false), queue_lock(), the_queue(), txn(), cursor() {
            if (bool(q) && bool(*q)) {
                the_queue = q;
                queue_lock = boost::interprocess::scoped_lock<named_mutex>(*(the_queue->mutex));
                txn = the_queue->the_dict->env->start_transaction(false);
                cursor = txn->make_cursor(the_queue->the_dict->dbi);
                initialized.store(true);
            }
        }

        virtual ~back_insert_iterator() {
            close();
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
            close();
            if (other) {
                other.initialized.store(false);
                swap(other);
                initialized.store(true);
            }
            return *this;
        }

        virtual back_insert_iterator& operator=(const CBString& new_value) {
            return operator=(make_mdb_val(new_value));
        }

        virtual back_insert_iterator& operator=(MDB_val v);

        virtual void set_rollback(bool val=true) {
            if (*this) {
                txn->set_rollback(val);
            }
        }

        virtual size_t size() const { return bool(*this) ? cursor->size() : 0; }

        back_insert_iterator& operator*() { return *this; }
        virtual back_insert_iterator& operator++() { return *this; }
        virtual back_insert_iterator& operator++(int) { return *this; }
        virtual back_insert_iterator& operator--() { return *this; }
        virtual back_insert_iterator& operator--(int) { return *this; }
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

        virtual size_t size() const { return back_insert_iterator::size(); }
        virtual void set_rollback(bool val=true) { back_insert_iterator::set_rollback(val); }

        front_insert_iterator(): back_insert_iterator() { direction = -1;}
        front_insert_iterator(shared_ptr<PersistentQueue> q): back_insert_iterator(q) { direction = -1; }

        virtual ~front_insert_iterator() { }

        front_insert_iterator(BOOST_RV_REF(front_insert_iterator) other): back_insert_iterator(BOOST_MOVE_BASE(back_insert_iterator, other)) {
            direction = -1;
        }

        front_insert_iterator& operator=(BOOST_RV_REF(front_insert_iterator) other) {
            back_insert_iterator::operator=(BOOST_MOVE_BASE(back_insert_iterator, other));
            direction = -1;
            return *this;
        }

        virtual front_insert_iterator& operator=(const CBString& new_value) {
            back_insert_iterator::operator=(new_value);
            return *this;
        }

        virtual front_insert_iterator& operator=(MDB_val new_value) {
            back_insert_iterator::operator=(new_value);
            return *this;
        }

        virtual front_insert_iterator& operator*() { return *this; }
        virtual front_insert_iterator& operator++() { return *this; }
        virtual front_insert_iterator& operator++(int) { return *this; }
        virtual front_insert_iterator& operator--() { return *this; }
        virtual front_insert_iterator& operator--(int) { return *this; }

    };      // END CLASS front_insert_iterator


    class iiterator: public shared_lockable_adapter<shared_mutex> {
    private:
        BOOST_MOVABLE_BUT_NOT_COPYABLE(iiterator)

    protected:
        boost::atomic_bool initialized;
        boost::scoped_ptr<CBString> current_value;
        boost::interprocess::scoped_lock<named_mutex> queue_lock;
        shared_ptr<PersistentQueue> the_queue;
        shared_ptr<environment::transaction> txn;
        shared_ptr<environment::transaction::cursor> cursor;

        void _next_value();
        void _last_value();

        void swap(iiterator& other) {
            using std::swap;
            current_value.swap(other.current_value);
            the_queue.swap(other.the_queue);
            cursor.swap(other.cursor);
            txn.swap(other.txn);
            queue_lock = boost::move(other.queue_lock);
        }

        void close() {
            if (initialized.load()) {
                initialized.store(false);
                current_value.reset();
                cursor.reset();
                txn.reset();
                queue_lock.unlock();
                the_queue.reset();
            }
        }

    public:
        typedef CBString value_type;
        typedef CBString& reference;
        typedef CBString* pointer;
        typedef std::input_iterator_tag iterator_category;

        BOOST_EXPLICIT_OPERATOR_BOOL()
        bool operator!() const { return !initialized.load(); }

        iiterator(): initialized(false), current_value(), the_queue(), txn(), cursor() { }

        iiterator(shared_ptr<PersistentQueue> q, int pos, boost::interprocess::scoped_lock<named_mutex>& lock):
                initialized(false), current_value(), queue_lock(), the_queue(), txn(), cursor() {
            if (bool(q) && bool(*q)) {
                the_queue = q;
                queue_lock = boost::move(lock);
                txn = the_queue->the_dict->env->start_transaction(false);
                cursor = txn->make_cursor(the_queue->the_dict->dbi);
                if (pos > 0) {
                    _last_value();
                } else {
                    _next_value();
                }
                initialized.store(true);
            } else {
                BOOST_THROW_EXCEPTION ( not_initialized() );
            }
        }

        iiterator(shared_ptr<PersistentQueue> q, int pos=0):
                initialized(false), current_value(), queue_lock(), the_queue(), txn(), cursor() {

            if (bool(q) && bool(*q)) {
                the_queue = q;
                queue_lock = boost::interprocess::scoped_lock<named_mutex>(*(the_queue->mutex));
                txn = the_queue->the_dict->env->start_transaction(false);
                cursor = txn->make_cursor(the_queue->the_dict->dbi);
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
            close();
        }

        // move constructor
        iiterator(BOOST_RV_REF(iiterator) other): initialized(false), current_value(), the_queue(), txn(), cursor() {
            if (other) {
                unique_lock<shared_mutex> lock(other.lockable());
                other.initialized.store(false);
                swap(other);
                initialized.store(true);
            }
        }

        // move assignment
        iiterator& operator=(BOOST_RV_REF(iiterator) other) {
            boost::lock(lockable(), other.lockable());
            unique_lock<shared_mutex> lock_self(lockable(), boost::adopt_lock);
            unique_lock<shared_mutex> lock_other(other.lockable(), boost::adopt_lock);
            close();
            if (other) {
                other.initialized.store(false);
                swap(other);
                initialized.store(true);
            }
            return *this;
        }

        bool empty() const {
            if (cursor) {
                return cursor->first() == MDB_NOTFOUND;
            }
            return true;
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
            shared_lock<shared_mutex> lock(lockable());
            if (current_value) {
                return *current_value;
            }
            BOOST_THROW_EXCEPTION ( empty_database() );
        }

        MDB_val get_value_buffer() {
            shared_lock<shared_mutex> lock(lockable());
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
            shared_lock<shared_mutex> lock(lockable());
            shared_lock<shared_mutex> lock_other(other.lockable());
            if (bool(*this) && bool(other)) {
                return (!current_value) && (!other.current_value) && (*the_queue == *other.the_queue);
            }
            return (!current_value) && (!other.current_value);
        }

        bool operator!=(const iiterator& other) const {
            return !(*this == other);
        }
    };  // END CLASS iiterator

    void remove_if(unary_predicate unary_pred) { the_dict->remove_if_pred_value(unary_pred); }
    void transform_values(unary_functor unary_funct) { the_dict->transform_values(unary_funct); }
    void move_to(shared_ptr<PersistentQueue> other, ssize_t chunk_size=-1);



    back_insert_iterator back_inserter() {
        return back_insert_iterator(shared_from_this());
    }

    bool push_back(const CBString& val) {
        back_insert_iterator it(shared_from_this());
        it = val;
        return true;
    }

    bool push_back(MDB_val val) {
        back_insert_iterator it(shared_from_this());
        it = val;
        return true;
    }

    template <class InputIterator>
    bool push_back(InputIterator first, InputIterator last) {
        back_insert_iterator it(shared_from_this());
        for (; first != last; ++first) {
            it = *first;
        }
        return true;
    }

    template <class InputIterator>
    bool push_back(BOOST_RV_REF(InputIterator) first, BOOST_RV_REF(InputIterator) last) {
        back_insert_iterator it(shared_from_this());
        for (; first != last; ++first) {
            it = *first;
        }
        return true;
    }

    shared_future<bool> async_push_back(const CBString& value) {
        packaged_task<bool()> task(boost::bind(&PersistentQueue::cbstring_push_back, this, value));
        return push_queue->push_task(boost::move(task));
    }

    template <class InputIterator>
    shared_future<bool> async_push_back(BOOST_RV_REF(InputIterator) first, BOOST_RV_REF(InputIterator) last) {
        boost::container::vector<CBString> v;
        for (; first != last; ++first) {
            v.push_back(*first);
        }
        // v is *copied* in task, so we avoid a dangling reference
        packaged_task<bool()> task(boost::bind(&PersistentQueue::vector_push_back, this, v));
        return push_queue->push_task(boost::move(task));
    }

    shared_future<bool> async_push_back(MDB_val value) {
        return async_push_back(make_string(value));
    }

    bool push_front(const CBString& val) {
        front_insert_iterator it(shared_from_this());
        it = val;
        return true;
    }

    bool push_front(MDB_val val) {
        front_insert_iterator it(shared_from_this());
        it = val;
        return true;
    }

    template <class InputIterator>
    bool push_front(InputIterator first, InputIterator last) {
        front_insert_iterator it(shared_from_this());
        for (; first != last; ++first) {
            it = *first;
        }
        return true;
    }

    template <class InputIterator>
    bool push_front(BOOST_RV_REF(InputIterator) first, BOOST_RV_REF(InputIterator) last) {
        front_insert_iterator it(shared_from_this());
        for (; first != last; ++first) {
            it = *first;
        }
        return true;
    }

    bool push_front(size_t n, const CBString& val);
    bool push_back(size_t n, const CBString& val);

    CBString pop_back() { return CBString(*iiterator(shared_from_this(), 1)); }
    CBString pop_front() { return CBString(*iiterator(shared_from_this())); }

    SharedFuture async_wait_and_pop_front() {
        //_LOG_DEBUG << "wait_and_pop_front_in_thread";
        return waiters->create();
    }

    SharedFuture async_wait_and_pop_front(milliseconds ms) {
        //_LOG_DEBUG << "wait_and_pop_front_in_thread: " << ms.count() << "ms";
        return waiters->create(ms);
    }

    CBString wait_and_pop_front(milliseconds ms) {
        boost::interprocess::scoped_lock<named_mutex> queue_lock(*mutex);

        long duration_in_ms = LongLong2Long::convert(ms.count());
        ptime now = boost::date_time::microsec_clock<boost::posix_time::ptime>::universal_time();
        ptime stop_point = now + millisec(duration_in_ms);

        bool no_timeout = true;
        while (empty() && no_timeout) {
            no_timeout = queue_is_empty->timed_wait(queue_lock, stop_point);
        }
        if (no_timeout) {
            return CBString(*iiterator(shared_from_this(), 0, queue_lock));
        }
        BOOST_THROW_EXCEPTION ( empty_database() );
    }

    CBString wait_and_pop_front() {
        boost::interprocess::scoped_lock<named_mutex> queue_lock(*mutex);
        while (empty()) {
            queue_is_empty->wait(queue_lock);
        }
        return CBString(*iiterator(shared_from_this(), 0, queue_lock));
    }

    template <class OutputIterator>
    void pop_all(OutputIterator oit) {
        iiterator it(shared_from_this());
        iiterator end;
        for(; it != end; ++it) {
            oit = *it;
        }
    }

    void clear() { the_dict->clear(); }
    void remove_duplicates() { the_dict->remove_duplicates(); }


}; // END CLASS PersistentQueue



}   // end NS quiet

