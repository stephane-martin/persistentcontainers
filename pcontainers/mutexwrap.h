#pragma once

#include <stdexcept>
#include <sstream>
#include <string>
#include <iomanip>
#include <pthread.h>
#include <boost/throw_exception.hpp>
#include "lmdb_exceptions.h"

namespace quiet {

using std::string;
using std::ostringstream;
using namespace lmdb;

inline string tostring(int x) {
    ostringstream ss;
    ss << x;
    return ss.str();
}

class ErrorCheckLock {
protected:
    // Prevent copying or assignment
    ErrorCheckLock(const ErrorCheckLock& arg);
    ErrorCheckLock& operator=(const ErrorCheckLock& rhs);

    pthread_mutex_t mutex;

    void destroy() {
        int res = pthread_mutex_destroy(&mutex);
        if (res == 0) {
            return;
        }
        if (res == EBUSY) {
            // oops, lets try to save the day anyway (todo: log !)
            pthread_mutex_unlock(&mutex);
            pthread_mutex_destroy(&mutex);
            return;
        }
        if (res == EINVAL) {
            // todo: log !
        }
        // todo: log !
    }

public:
    ErrorCheckLock() {
        pthread_mutexattr_t mattr;
        pthread_mutexattr_init(&mattr);
        pthread_mutexattr_settype(&mattr, PTHREAD_MUTEX_ERRORCHECK);
        pthread_mutex_init(&mutex, &mattr);
    }

    ~ErrorCheckLock() {
        destroy();
    }

    void lock() {
        int res = pthread_mutex_lock(&mutex);
        if (res != 0) {
            BOOST_THROW_EXCEPTION(runtime_error() << lmdb_error::what("pthread_mutex_lock returned with: " + tostring(res)));
        }
    }

    void unlock() {
        int res = pthread_mutex_unlock(&mutex);
        if (res != 0) {
            BOOST_THROW_EXCEPTION(runtime_error() << lmdb_error::what("pthread_mutex_unlock returned with: " + tostring(res)));
        }
    }

    bool trylock() {
        int res = pthread_mutex_trylock(&mutex);
        if (res == 0) {
            return true;
        }
        if (res == EBUSY) {
            return false;
        }
        BOOST_THROW_EXCEPTION(runtime_error() << lmdb_error::what("pthread_mutex_trylock returned with: " + tostring(res)));
    }
};

}

