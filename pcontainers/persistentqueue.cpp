#include <string>
#include <stdexcept>
#include <sstream>
#include <iostream>
#include <memory>
#include <vector>
#include <map>
#include <utility>
#include <algorithm>
#include <bstrlib/bstrwrap.h>
#include "persistentqueue.h"

namespace quiet {

using std::string;
using std::pair;
using std::vector;
using std::map;
using std::cout;
using std::endl;
using std::exception;
using Bstrlib::CBString;


vector<CBString> PersistentQueue::pop_all() {
    vector<CBString> v;
    iiterator it(this);
    iiterator end;
    for(; it != end; ++it) {
        v.push_back(*it);
    }
    return v;
}

void PersistentQueue::push_front(size_t n, const CBString& val) {
    if (n <= 0) {
        return;
    }
    front_insert_iterator it(this);
    for(size_t i=0; i<n; i++) {
        it = val;
    }
}

void PersistentQueue::push_back(size_t n, const CBString& val) {
    if (n <= 0) {
        return;
    }
    back_insert_iterator it(this);
    for(size_t i=0; i<n; i++) {
        it = val;
    }
}

void PersistentQueue::move_to(PersistentQueue& other, ssize_t chunk_size) {
    if (!*this) {
        _LOG_DEBUG << "move_to: cancelled: source queue is not initialized";
        return;
    }
    if (*this == other) {
        _LOG_DEBUG << "move_to: cancelled: source and dest are the same queue";
        return;
    }
    if (!other) {
        BOOST_THROW_EXCEPTION(not_initialized() << lmdb_error::what("move_to: the other queue is not initialized"));
    }
    if (chunk_size <= 0) {
        chunk_size = SSIZE_MAX;
    }

    bool src_not_empty = true;

    iiterator end;
    while (src_not_empty) {
        back_insert_iterator dest_it(&other);
        iiterator src_it(this);
        for(ssize_t i=0; i < chunk_size; i++) {
            if (src_it == end) {
                src_not_empty = false;
                break;
            }
            dest_it = *src_it;
            ++src_it;
        }
    }
}

}
