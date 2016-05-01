#pragma once

#include <boost/thread/future.hpp>
#include <boost/move/move.hpp>
#include <boost/shared_ptr.hpp>
#include "threadsafe_queue.h"

namespace utils {

using boost::packaged_task;
using boost::shared_future;
using boost::shared_ptr;

class packaged_task_queue: public threadsafe_queue < packaged_task < bool() > > {
private:
    BOOST_MOVABLE_BUT_NOT_COPYABLE(packaged_task_queue)

public:

    packaged_task_queue() BOOST_NOEXCEPT_OR_NOTHROW: threadsafe_queue < packaged_task < bool() > >() { }

    packaged_task_queue(BOOST_RV_REF(packaged_task_queue) other):
            threadsafe_queue < packaged_task < bool() > >(BOOST_MOVE_BASE(threadsafe_queue < packaged_task < bool() > >, other)) { }

    packaged_task_queue& operator=(BOOST_RV_REF(packaged_task_queue) other) {
        threadsafe_queue < packaged_task < bool() > >::operator=(BOOST_MOVE_BASE(threadsafe_queue < packaged_task < bool() > >, other));
        return *this;
    }

    shared_future<bool> push_task(BOOST_RV_REF(packaged_task<bool()>) new_value) {
        shared_ptr < packaged_task < bool() > > data(new packaged_task<bool()>(boost::move(new_value)));
        boost::lock_guard<mutex> lk(lockable());
        data_queue.push_back(boost::move(data));
        shared_future<bool> f((data_queue.back())->get_future().share());
        data_cond.notify_one();
        return f;
    }

};   // END CLASS packaged_task_queue

}   // END NS utils
