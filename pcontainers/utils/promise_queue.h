#pragma once

#include <utility>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/atomic.hpp>
#include <boost/bind.hpp>
#include <boost/exception/exception.hpp>
#include <boost/exception_ptr.hpp>
#include <boost/chrono/chrono.hpp>
#include <boost/container/deque.hpp>
#include <boost/container/map.hpp>
#include <boost/core/noncopyable.hpp>
#include <boost/core/swap.hpp>
#include <boost/make_shared.hpp>
#include <boost/move/move.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/cv_status.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/lockable_adapter.hpp>
#include <boost/thread/future.hpp>
#include <boost/tuple/tuple.hpp>
#include "../logging/logging.h"
#include "../lmdb_exceptions/lmdb_exceptions.h"

namespace utils {
using boost::shared_future;
using boost::promise;
using boost::noncopyable;
using boost::basic_lockable_adapter;
using boost::mutex;
using boost::condition_variable;
using boost::scoped_ptr;
using boost::shared_ptr;
using boost::make_shared;
using boost::lock_guard;
using boost::unique_lock;
using boost::adopt_lock;
using boost::chrono::milliseconds;
using boost::chrono::nanoseconds;
using boost::chrono::high_resolution_clock;
using boost::copy_exception;
using boost::atomic_bool;
using std::pair;
using std::make_pair;
using boost::posix_time::ptime;
using boost::posix_time::millisec;


template<typename T> class promise_queue;

template<typename T>
class ExpiryPromisePtr: public shared_ptr < promise < T > > {
friend class promise_queue<T>;

private:
    ptime expiry_posix_point;

protected:
    high_resolution_clock::time_point expiry_point;

public:
    ExpiryPromisePtr(): shared_ptr < promise < T > >() { }
    ExpiryPromisePtr(promise<T>* ptr): shared_ptr < promise < T > >(ptr) { }

    ExpiryPromisePtr(promise<T>* ptr, milliseconds timeout): shared_ptr < promise < T > >(ptr) {
        expiry_point = high_resolution_clock::now() + timeout;
        expiry_posix_point = boost::date_time::microsec_clock<boost::posix_time::ptime>::universal_time() + millisec(timeout.count());
    }

    bool expired() {
        if (expiry_posix_point.is_not_a_date_time()) {
            return false;
        }
        milliseconds five_milliseconds(5);
        high_resolution_clock::time_point rightnow(high_resolution_clock::now());
        return ( (expiry_point <= rightnow) || ((rightnow - expiry_point) <= five_milliseconds) );
    }

    ptime get_next_waiting_point() {
        if (expiry_posix_point.is_not_a_date_time()) {
            // wait 2 secs maximum
            return boost::date_time::microsec_clock<boost::posix_time::ptime>::universal_time() + millisec(2000);
        }
        return expiry_posix_point;
    }

};



template<typename T>
class promise_queue: private noncopyable, public basic_lockable_adapter<mutex> {
public:
    typedef shared_future<T> MyFuture;
    typedef ExpiryPromisePtr<T> PromisePtr;
    typedef boost::container::multimap < high_resolution_clock::time_point, PromisePtr > MyMultimap;
    typedef boost::container::deque < PromisePtr > MyDeque;

private:
    BOOST_MOVABLE_BUT_NOT_COPYABLE(promise_queue<T>)

    boost::container::deque < PromisePtr > simple_promise_queue;
    MyMultimap expiry_promise_map;
    mutable condition_variable data_cond;
    mutable high_resolution_clock::time_point next_time_point;
    mutable scoped_ptr<boost::thread> prune_thread_ptr;
    mutable atomic_bool stopping_flag;

    void start() {
        _LOG_DEBUG << "promise_queue: starting pruning thread";
        prune_thread_ptr.reset(new boost::thread(boost::bind(&promise_queue::prune_thread_fun, this)));
    }

    void stop() {
        stopping_flag.store(true);
        _LOG_DEBUG << "promise_queue: stopping the pruning thread";
        if (prune_thread_ptr && prune_thread_ptr->joinable()) {
            data_cond.notify_all();     // interrupts wait_and_pop
            prune_thread_ptr->join();
            prune_thread_ptr.reset();
            prune_remaining_promises();
            _LOG_DEBUG << "promise_queue: stopped pruning thread";
        }
    }

    void prune_thread_fun() {
        unique_lock<mutex> lk(lockable());
        while (!stopping_flag.load()) {
            if (next_time_point > high_resolution_clock::now()) {
                data_cond.wait_until(lk, next_time_point);
            } else {
                // 5 secs maximum between each pruning
                data_cond.wait_for(lk, milliseconds(5000));
            }
            prune_expired_promises();
        }
    }

    void prune_expired_promises() {
        _LOG_DEBUG << "promise_queue: pruning expired promises";
        int p = 0;
        for(typename MyMultimap::iterator it=expiry_promise_map.begin(); it != expiry_promise_map.end(); ++it) {
            if ((it->second).expired()) {
                (it->second)->set_exception(copy_exception(lmdb::expired()));
                expiry_promise_map.erase(it);
                p += 1;
            } else {
                break;  // promises are sorted by expiration time, so the next promises can't be expired
            }
        }
        _LOG_DEBUG << "promise_queue: pruned expired promises: " << p;
    }

    void prune_remaining_promises() {
        unique_lock<mutex> lk(lockable());
        _LOG_DEBUG << "promise_queue: pruning remaining promises";
        int p = 0;
        for(typename MyMultimap::iterator it=expiry_promise_map.begin(); it != expiry_promise_map.end(); ++it) {
            (it->second)->set_exception(copy_exception(lmdb::stopping_ops()));
            p += 1;
        }
        for(typename MyDeque::iterator it=simple_promise_queue.begin(); it != simple_promise_queue.end(); ++it) {
            (*it)->set_exception(copy_exception(lmdb::stopping_ops()));
            p += 1;
        }
        _LOG_DEBUG << "promise_queue: pruned promises: " << p;
        expiry_promise_map.clear();
        simple_promise_queue.clear();
    }



public:
    promise_queue(): stopping_flag(false) {
        start();
    }
    ~promise_queue() {
        stop();
    }

    promise_queue(BOOST_RV_REF(promise_queue) other) {
        boost::lock(lockable(), other.lockable());
        unique_lock<mutex> lock_self(lockable(), adopt_lock);
        unique_lock<mutex> lock_other(other.lockable(), adopt_lock);
        simple_promise_queue.swap(other.simple_promise_queue);
        expiry_promise_map.swap(other.expiry_promise_map);
    }

    promise_queue& operator=(BOOST_RV_REF(promise_queue) other) {
        boost::lock(lockable(), other.lockable());
        unique_lock<mutex> lock_self(lockable(), adopt_lock);
        unique_lock<mutex> lock_other(other.lockable(), adopt_lock);
        simple_promise_queue.clear();
        simple_promise_queue.swap(other.simple_promise_queue);
        expiry_promise_map.clear();
        expiry_promise_map.swap(other.expiry_promise_map);
        return *this;
    }

    bool __notempty() {
        return !simple_promise_queue.empty() || !expiry_promise_map.empty();
    }

    bool __empty() {
        return simple_promise_queue.empty() && expiry_promise_map.empty();
    }

    PromisePtr wait_and_pop() {
        unique_lock<mutex> lk(lockable());
        while (__empty() && !stopping_flag.load()) {
            data_cond.wait(lk);
        }
        if (!expiry_promise_map.empty()) {
            typename MyMultimap::iterator it(expiry_promise_map.begin());
            PromisePtr res(it->second);
            expiry_promise_map.erase(it);
            _LOG_DEBUG << "promise_queue: popping a promise";
            return res;
        } else if (!simple_promise_queue.empty()) {
            PromisePtr res(simple_promise_queue.front());
            simple_promise_queue.pop_front();
            _LOG_DEBUG << "promise_queue: popping a simple promise";
            return res;
        } else {
            _LOG_DEBUG << "promise_queue: cancelling wait_and_pop: stopping";
            // stopping_flag is True
            return PromisePtr();
        }
    }

    PromisePtr try_pop() {
        lock_guard<boost::mutex> lk(lockable());
        if (!expiry_promise_map.empty()) {
            typename MyMultimap::iterator it(expiry_promise_map.begin());
            PromisePtr res(it->second);
            expiry_promise_map.erase(it);
            return res;
        } else if (!simple_promise_queue.empty()) {
            PromisePtr res(simple_promise_queue.front());
            simple_promise_queue.pop_front();
            return res;
        } else {
            return PromisePtr();
        }
    }

    MyFuture create() {
        return push(promise<T>());
    }

    MyFuture create(milliseconds timeout) {
        return push(promise<T>(), timeout);
    }

    MyFuture push(BOOST_RV_REF(promise<T>) new_value) {
        _LOG_DEBUG << "promise_queue: pushing a new promise";
        PromisePtr promise_ptr(new promise<T>(new_value));
        lock_guard<mutex> lk(lockable());
        simple_promise_queue.push_back(promise_ptr);
        data_cond.notify_all();
        return (promise_ptr->get_future()).share();
    }

    MyFuture push(BOOST_RV_REF(promise<T>) new_value, milliseconds timeout) {
        _LOG_DEBUG << "promise_queue: pushing a new expiring promise";
        PromisePtr promise_ptr(new promise<T>(new_value), timeout);

        lock_guard<mutex> lk(lockable());

        if ( (next_time_point <= high_resolution_clock::now()) || (promise_ptr.expiry_point < next_time_point) ) {
            next_time_point = promise_ptr.expiry_point;
        }

        expiry_promise_map.insert(make_pair(promise_ptr.expiry_point, promise_ptr));
        data_cond.notify_all();
        return (promise_ptr->get_future()).share();
    }

    bool empty() const {
        lock_guard<mutex> lk(lockable());
        return __empty();
    }

}; // END class promise_queue

}
