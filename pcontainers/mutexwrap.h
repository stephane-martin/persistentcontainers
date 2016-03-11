#pragma once

#include <stdexcept>
#include <sstream>
#include <string>
#include <iomanip>
#include <pthread.h>


namespace quiet {

using std::runtime_error;
using std::string;
using std::ostringstream;

template <typename T>
string any_tostring(T x, int n=0) {
    ostringstream ss;
    if (n > 0) {
        ss << std::setfill ('0') << std::setw(n);
    }
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
            throw runtime_error("pthread_mutex_destroy returned with EINVAL");
        }
        throw runtime_error("pthread_mutex_destroy returned with: " + any_tostring(res));
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
            throw std::runtime_error("pthread_mutex_lock returned with " + any_tostring(res));
        }
    }

    void unlock() {
        int res = pthread_mutex_unlock(&mutex);
        if (res != 0) {
            throw std::runtime_error("pthread_mutex_unlock returned with " + any_tostring(res));
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
        throw std::runtime_error("pthread_mutex_trylock returned with " + any_tostring(res));
    }
};

}

