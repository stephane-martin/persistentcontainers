#pragma once

#include <utility>
#include <stdexcept>
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

namespace utils {

using boost::shared_future;
using boost::promise;
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

template<typename T>
class threadsafe_queue: public basic_lockable_adapter<mutex> {

private:
    BOOST_MOVABLE_BUT_NOT_COPYABLE(threadsafe_queue<T>)

protected:
    boost::container::deque < shared_ptr < T > > data_queue;
    condition_variable data_cond;

public:
    threadsafe_queue() { }

    virtual ~threadsafe_queue() { }

    threadsafe_queue(BOOST_RV_REF(threadsafe_queue) other) {
        boost::lock(lockable(), other.lockable());
        unique_lock<mutex> lock_self(lockable(), adopt_lock);
        unique_lock<mutex> lock_other(other.lockable(), adopt_lock);
        data_queue.swap(other.data_queue);
    }

    threadsafe_queue& operator=(BOOST_RV_REF(threadsafe_queue) other) {
        boost::lock(lockable(), other.lockable());
        unique_lock<mutex> lock_self(lockable(), adopt_lock);
        unique_lock<mutex> lock_other(other.lockable(), adopt_lock);
        data_queue.clear();
        data_queue.swap(other.data_queue);
        return *this;
    }

    bool __notempty() {
        return !data_queue.empty();
    }

    bool __empty() {
        return data_queue.empty();
    }

    virtual shared_ptr<T> wait_and_pop() {
        unique_lock<mutex> lk(lockable());
        while (__empty()) {
            data_cond.wait_for(lk, milliseconds(2000));
        }
        shared_ptr<T> res=data_queue.front();
        data_queue.pop_front();
        return res;
    }

    virtual shared_ptr<T> wait_and_pop(milliseconds ms) {
        unique_lock<mutex> lk(lockable());
        nanoseconds remaining = ms;
        high_resolution_clock::time_point start_point;
        while (__empty()) {
            start_point = high_resolution_clock::now();
            boost::cv_status st = data_cond.wait_until(lk, start_point + remaining);
            remaining -= high_resolution_clock::now() - start_point;
            if (st == boost::cv_status::timeout || remaining.count() <= 0) {
                return shared_ptr<T>();
            }
        }
        shared_ptr<T> res=data_queue.front();
        data_queue.pop_front();
        return res;
    }

    virtual boost::container::deque < shared_ptr < T > > pop_all() {
        unique_lock<mutex> lk(lockable());
        boost::container::deque < shared_ptr < T > > res;
        res.swap(data_queue);
        return res;
    }

    virtual shared_ptr<T> try_pop() {
        lock_guard<mutex> lk(lockable());
        if (__empty()) {
            return boost::shared_ptr<T>();
        }
        shared_ptr<T> res(data_queue.front());
        data_queue.pop_front();
        return res;
    }

    virtual void push(T new_value) {
        shared_ptr<T> data(new T(boost::move(new_value)));
        boost::lock_guard<mutex> lk(lockable());
        data_queue.push_back(boost::move(data));
        data_cond.notify_one();
    }

    virtual void push(BOOST_RV_REF(T) new_value) {              // move semantics T&&
        shared_ptr<T> data(new T(boost::move(new_value)));
        boost::lock_guard<mutex> lk(lockable());
        data_queue.push_back(boost::move(data));
        data_cond.notify_one();
    }

    virtual bool empty() const {
        lock_guard<mutex> lk(lockable());
        return data_queue.empty();
    }

}; // END class threadsafe_queue






} // END NS utils
