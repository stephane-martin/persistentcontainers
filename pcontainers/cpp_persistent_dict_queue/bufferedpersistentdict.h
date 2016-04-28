#pragma once

#include <map>
#include <set>
#include <utility>

#include <boost/chrono/chrono.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/throw_exception.hpp>
#include <boost/atomic.hpp>
#include <boost/thread.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/lockable_adapter.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/future.hpp>
#include <boost/container/deque.hpp>
#include <bstrlib/bstrwrap.h>

#include "persistentdict.h"

namespace quiet {

using std::map;
using std::set;
using std::pair;
using std::make_pair;

using boost::mutex;
using boost::shared_mutex;
using boost::unique_lock;
using boost::shared_lock;
using boost::upgrade_to_unique_lock;
using boost::upgrade_lock;
using boost::lock_guard;
using boost::shared_lockable_adapter;
using boost::condition_variable;
using boost::packaged_task;
using boost::future;
using boost::shared_future;
using boost::promise;
using boost::make_ready_future;
using boost::make_exceptional;

using boost::container::deque;

using boost::shared_ptr;
using boost::make_shared;

using boost::scoped_ptr;
using boost::atomic_bool;
using boost::chrono::milliseconds;

using Bstrlib::CBString;

typedef shared_future<CBString> s_future;
typedef future<CBString> _future;

class BufferedPersistentDict {

private:
    typedef packaged_task < CBString() > MyPackagedTask;
    typedef shared_future < CBString > MySharedFuture;
    typedef shared_future < bool > MySharedBoolFuture;
    typedef shared_ptr < MyPackagedTask > TaskPtr;
    typedef PersistentDict::iterator DictIterator;
    typedef PersistentDict::const_iterator DictConstIterator;
    typedef promise < bool > MyPromise;
    typedef shared_ptr < MyPromise > PromisePtr;

    BufferedPersistentDict(const BufferedPersistentDict& other);
    BufferedPersistentDict& operator=(const BufferedPersistentDict&);

    mutable deque < TaskPtr > reads_queue;

    mutable shared_mutex buffers_mutex;
    mutable shared_mutex current_buffers_mutex;
    mutable mutex flush_mutex;
    mutable mutex stopping_mutex;
    mutable mutex queue_mutex;

    shared_ptr<PersistentDict> the_dict;

    mutable map < CBString, pair < CBString, PromisePtr > > buffered_inserts;
    mutable map < CBString, PromisePtr > buffered_deletes;
    mutable map < CBString, pair < CBString, PromisePtr > > current_inserts;
    mutable map < CBString, PromisePtr > current_deletes;

    mutable condition_variable stopping_condition;
    mutable condition_variable queue_not_empty_condition;
    mutable atomic_bool starting_flag;
    mutable atomic_bool stopping_flag;
    mutable atomic_bool flush_thread_is_running;
    mutable atomic_bool reader_thread_is_running;
    const milliseconds flushing_interval;
    mutable scoped_ptr<boost::thread> flush_thread_ptr;
    mutable scoped_ptr<boost::thread> reader_thread_ptr;

    void flush_thread_fun() const {
        unique_lock<mutex> stopping_lock(stopping_mutex);
        while (!stopping_flag.load()) {
            stopping_condition.wait_for(stopping_lock, flushing_interval);
            flush();
        }
    }

    void reader_thread_fun() const {
        TaskPtr task_ptr;
        {
            while (!stopping_flag.load()) {
                unique_lock<mutex> queue_lock(queue_mutex);
                // queue_lock is locked
                if (!reads_queue.empty()) {
                    task_ptr = reads_queue.front();
                    reads_queue.pop_front();
                    queue_lock.unlock();    // queue_lock is unlocked
                    if (task_ptr) {
                        // execute the (potentially blocking) task
                        (task_ptr->operator())();
                    } else {
                        // the 'stop' method sent us an empty pointer for stopping notification
                        break;
                    }
                } else {
                    // queue_lock should be locked before 'wait'
                    queue_not_empty_condition.wait(queue_lock);
                    // queue_lock is locked after 'wait' returns
                }
            // here queue_lock will be destroyed, so the queue_mutex will be released in every case
            }
        }
        // consume any task that could remain in the queue, so that waiting futures can be resolved
        {
            unique_lock<mutex> queue_lock(queue_mutex);
            while (!reads_queue.empty()) {
                task_ptr = reads_queue.front();
                reads_queue.pop_front();
                if (task_ptr) {
                    (task_ptr->operator())();
                }
            }
        }


    }

    CBString read_value(shared_ptr<DictIterator> it_ptr) const {
        return it_ptr->get_value();
    }

public:
    BufferedPersistentDict(shared_ptr<PersistentDict> d, uint64_t flush_interval, bool start_thread=true):
        the_dict(d),
        starting_flag(false),
        stopping_flag(false),
        flush_thread_is_running(false),
        reader_thread_is_running(false),
        flushing_interval(flush_interval)

    {
        if (!the_dict || !*the_dict) {
            BOOST_THROW_EXCEPTION(not_initialized());
        }
        if (start_thread) {
            start();
        }
    }

    BOOST_EXPLICIT_OPERATOR_BOOL()
    bool operator!() const { return !the_dict || !*the_dict; }

    ~BufferedPersistentDict() {
        stop();
        flush();
    }

    bool operator==(const BufferedPersistentDict& other) const {
        return *the_dict == *(other.the_dict);
    }

    bool operator!=(const BufferedPersistentDict& other) const {
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

    void flush() const {
        _LOG_DEBUG << "BufferedPersistentDict::flush";
        lock_guard<mutex> flush_lock(flush_mutex);
        {
            // we lock buffers_lock for a small time (just copy the buffers)
            // here 'at', 'insert' and 'erase' are blocked
            unique_lock<shared_mutex> buffers_lock(buffers_mutex);
            unique_lock<shared_mutex> current_buffers_lock(current_buffers_mutex);
            current_deletes = buffered_deletes;
            current_inserts = buffered_inserts;
            buffered_deletes.clear();
            buffered_inserts.clear();
        }
        // every operation possible here
        {
            upgrade_lock<shared_mutex> current_buffers_lock(current_buffers_mutex);
            // currents buffers are in read mode: every operation is possible while flushing to LMDB
            // in particular updates can be posted to top buffers
            {
                DictIterator dict_it(the_dict->begin(false));
                // we now have a LMDB write transaction
                // if 'at' tries to read, it will not see the changes in LMDB yet (but it can see them in
                // current buffers)
                for(map < CBString, pair < CBString, PromisePtr > >::iterator it(current_inserts.begin()); it != current_inserts.end(); ++it) {
                    dict_it.set_key_value(it->first, (it->second).first);
                }
                for(map < CBString, PromisePtr >::iterator it(current_deletes.begin()); it != current_deletes.end(); ++it) {
                    dict_it.del(it->first);
                }
            }
            // end of LMDB transaction: changes were commited. let's confirm it to the client via promises
            for(map < CBString, pair < CBString, PromisePtr > >::iterator it(current_inserts.begin()); it != current_inserts.end(); ++it) {
                ((it->second).second)->set_value(true);
            }
            for(map < CBString, PromisePtr >::iterator it(current_deletes.begin()); it != current_deletes.end(); ++it) {
                (it->second)->set_value(true);
            }

            {
                // current_buffers_lock is locked for very small time
                // here 'at' is blocked
                upgrade_to_unique_lock<shared_mutex> ulock(current_buffers_lock);
                current_deletes.clear();
                current_inserts.clear();
            }
        }

    }

    void start() const {
        if (!stopping_flag.load() && !starting_flag.load()) {
            starting_flag.store(true);
            if (!flush_thread_is_running.load()) {
                flush_thread_is_running.store(true);
                flush_thread_ptr.reset(new boost::thread(boost::bind(&BufferedPersistentDict::flush_thread_fun, this)));
            }
            if (!reader_thread_is_running.load()) {
                reader_thread_is_running.store(true);
                reader_thread_ptr.reset(new boost::thread(boost::bind(&BufferedPersistentDict::reader_thread_fun, this)));
            }
            starting_flag.store(false);
        }

    }

    void stop() const {
        if (!stopping_flag.load() && !starting_flag.load()) {
            if (flush_thread_is_running.load() || reader_thread_is_running.load()) {
                // lock_guard<mutex> slock(stopping_mutex);
                stopping_flag.store(true);
                stopping_condition.notify_all();
                {
                    unique_lock<mutex> queue_lock(queue_mutex);
                    reads_queue.push_back(shared_ptr < packaged_task < CBString() > >());
                    queue_not_empty_condition.notify_all();
                }

                if (flush_thread_ptr && flush_thread_ptr->joinable()) {
                    flush_thread_ptr->join();
                    flush_thread_is_running.store(false);
                    flush_thread_ptr.reset();
                }
                if (reader_thread_ptr && reader_thread_ptr->joinable()) {
                    reader_thread_ptr->join();
                    reader_thread_is_running.store(false);
                    reader_thread_ptr.reset();
                }
                stopping_flag.store(false);
            }
        }
    }

    CBString at(MDB_val key) const {
        return at(CBString(key.mv_data, key.mv_size));
    }

    CBString at(const CBString& key) const {
        shared_lock<shared_mutex> buffers_lock(buffers_mutex);
        shared_lock<shared_mutex> current_buffers_lock(current_buffers_mutex);
        DictConstIterator it(the_dict->cfind(key));

        if (buffered_deletes.count(key)) {
            BOOST_THROW_EXCEPTION(mdb_notfound());
        }

        if (buffered_inserts.count(key)) {
            return buffered_inserts[key].first;
        }

        if (current_deletes.count(key)) {
            BOOST_THROW_EXCEPTION(mdb_notfound());
        }

        if (current_inserts.count(key)) {
            return current_inserts[key].first;
        }

        return it.get_value();  // can block
    }

    MySharedFuture async_at(MDB_val key) const {
        return async_at(CBString(key.mv_data, key.mv_size));
    }

    MySharedFuture async_at(const CBString& key) const {

        if (!reader_thread_is_running.load()) {
            BOOST_THROW_EXCEPTION(not_initialized());
        }

        shared_lock<shared_mutex> buffers_lock(buffers_mutex);
        shared_lock<shared_mutex> current_buffers_lock(current_buffers_mutex);
        shared_ptr<DictIterator> it_ptr = make_shared<DictIterator>(the_dict, key, true);

        if (buffered_deletes.count(key)) {
            return make_ready_future<CBString>(boost::copy_exception(mdb_notfound())).share();
        }

        if (buffered_inserts.count(key)) {
            return make_ready_future(CBString(buffered_inserts[key].first)).share();
        }

        if (current_deletes.count(key)) {
            return make_ready_future<CBString>(boost::copy_exception(mdb_notfound())).share();
        }

        if (current_inserts.count(key)) {
            return make_ready_future(CBString(current_inserts[key].first)).share();
        }

        TaskPtr task_ptr = make_shared < MyPackagedTask > (
            boost::bind( &BufferedPersistentDict::read_value, this, it_ptr )
        );

        {
            unique_lock<mutex> queue_lock(queue_mutex);
            reads_queue.push_back(task_ptr);
            queue_not_empty_condition.notify_one();
        }

        return (task_ptr->get_future()).share();
    }

    MySharedBoolFuture erase(MDB_val key) {
        return erase(CBString(key.mv_data, key.mv_size));
    }

    MySharedBoolFuture erase(const CBString& key) {
        unique_lock<shared_mutex> buffers_lock(buffers_mutex);
        PromisePtr prom = make_shared<MyPromise>();
        if (buffered_inserts.count(key)) {
            ((buffered_inserts[key]).second)->set_value(false);
            buffered_inserts.erase(key);
        }
        if (buffered_deletes.count(key)) {
            (buffered_deletes[key])->set_value(false);
        }
        buffered_deletes[key] = prom;
        return (prom->get_future()).share();
    }

    MySharedBoolFuture insert(MDB_val key, MDB_val value) {
        return insert(CBString(key.mv_data, key.mv_size), CBString(value.mv_data, value.mv_size));
    }

    MySharedBoolFuture insert(const CBString& key, const CBString& value) {
        unique_lock<shared_mutex> buffers_lock(buffers_mutex);
        PromisePtr prom = make_shared<MyPromise>();
        if (buffered_deletes.count(key)) {
            (buffered_deletes[key])->set_value(false);
            buffered_deletes.erase(key);
        }
        if (buffered_inserts.count(key)) {
            ((buffered_inserts[key]).second)->set_value(false);
        }
        buffered_inserts[key] = make_pair(value, prom);
        return (prom->get_future()).share();
    }

};  // END CLASS BufferedPersistentDict

}   // END NS quiet
