#pragma once

#include <utility>
#include <boost/chrono/chrono.hpp>
#include <boost/core/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/container/deque.hpp>
#include <boost/thread/thread.hpp>
#include <boost/core/explicit_operator_bool.hpp>
#include <boost/atomic.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/future.hpp>
#include <boost/move/move.hpp>
#include <boost/throw_exception.hpp>
#include <boost/exception_ptr.hpp>
#include <bstrlib/bstrwrap.h>
#include "lmdb.h"

#include "../logging/logging.h"
#include "../lmdb_exceptions/lmdb_exceptions.h"
#include "../utils/threadsafe_queue.h"
#include "../utils/utils.h"
#include "persistentqueue.h"

namespace quiet {
using boost::shared_ptr;
using boost::atomic_bool;
using boost::chrono::milliseconds;
using boost::mutex;
using boost::lock_guard;
using boost::condition_variable;
using boost::promise;
using std::pair;
using std::make_pair;
using Bstrlib::CBString;

class BufferedPersistentQueue: private boost::noncopyable {
public:
    typedef promise < bool > MyPromise;
    typedef shared_ptr < MyPromise > PromisePtr;
    typedef utils::threadsafe_queue < pair < CBString, PromisePtr > > PushQueue;
    typedef boost::container::deque < shared_ptr < pair < CBString, PromisePtr > > > TempPushQueue;

private:
    shared_ptr<PersistentQueue> the_queue;
    mutable atomic_bool stopping_flag;
    mutable atomic_bool pusher_thread_is_running;
    const milliseconds flushing_interval;
    mutable scoped_ptr<boost::thread> pusher_thread_ptr;
    mutable mutex stopping_mutex;
    mutable mutex flush_mutex;
    mutable condition_variable stopping_condition;
    mutable PushQueue push_queue;

    void start() const {
        pusher_thread_ptr.reset(new boost::thread(boost::bind(&BufferedPersistentQueue::pusher_thread_fun, this)));
        while (!pusher_thread_is_running.load()) { }
    }

    void stop() const {
        // just until here push_back can still be called
        stopping_flag.store(true);
        stopping_condition.notify_one();

        if (pusher_thread_ptr && pusher_thread_ptr->joinable()) {
            pusher_thread_ptr->join();
            pusher_thread_ptr.reset();
        }
        pusher_thread_is_running.store(false);
    }

    void pusher_thread_fun() const {
        pusher_thread_is_running.store(true);
        unique_lock<mutex> stopping_lock(stopping_mutex);
        while (!stopping_flag.load()) {
            stopping_condition.wait_for(stopping_lock, flushing_interval);
            flush();
        }
        // if we get here, it means that stopping_flag was set, so no more push_back can happen
        // we make a last flush to empty the push_queue
        flush();
    }

    void flush() const {
        lock_guard<mutex> flush_lock(flush_mutex);  // to ensure that at most one "flush" is being executed
        // typedef boost::container::deque < shared_ptr < pair < CBString, PromisePtr > > > TempPushQueue;
        TempPushQueue temp_push_queue(push_queue.pop_all());
        if (!temp_push_queue.empty()) {
            PersistentQueue::back_insert_iterator pusher(the_queue->back_inserter());
            try{
                for(TempPushQueue::const_iterator it=temp_push_queue.cbegin(); it != temp_push_queue.cend(); ++it) {
                    pusher = (*it)->first;
                }
            } catch (...) {
                for(TempPushQueue::const_iterator it=temp_push_queue.cbegin(); it != temp_push_queue.cend(); ++it) {
                    (*it)->second->set_exception(boost::current_exception());
                }
                return;
            }
            for(TempPushQueue::const_iterator it=temp_push_queue.cbegin(); it != temp_push_queue.cend(); ++it) {
                (*it)->second->set_value(true);
            }
        }
    }


public:
    BufferedPersistentQueue(shared_ptr<PersistentQueue> q, uint64_t flush_interval):
        the_queue(q),
        stopping_flag(false),
        pusher_thread_is_running(false),
        flushing_interval(flush_interval)
    {
        if (!the_queue || !*the_queue) {
            BOOST_THROW_EXCEPTION(not_initialized());
        }

        start();
    }

    ~BufferedPersistentQueue() {
        stop();
    }

    BOOST_EXPLICIT_OPERATOR_BOOL()
    bool operator!() const { return !the_queue || !*the_queue; }

    shared_future<bool> push_back(const CBString& value) {
        if (stopping_flag.load()) {
            BOOST_THROW_EXCEPTION( stopping_ops() );
        }
        PromisePtr promise_ptr(new MyPromise());
        pair<CBString, PromisePtr> p(make_pair(value, promise_ptr));
        push_queue.push(boost::move(p));
        return (promise_ptr->get_future()).share();
    }

    shared_future<bool> push_back(MDB_val value) {
        if (stopping_flag.load()) {
            BOOST_THROW_EXCEPTION( stopping_ops() );
        }
        return push_back(utils::make_string(value));
    }

};   // END CLASS BufferedPersistentQueue

}   // END NS quiet
