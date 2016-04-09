#include <string>
#include <stdexcept>
#include <sstream>
#include <iostream>
#include <memory>
#include <vector>
#include <map>
#include <utility>
#include <algorithm>
#include <stdlib.h>
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


PersistentQueue::back_insert_iterator& PersistentQueue::back_insert_iterator::operator=(MDB_val v) {
    if (!*this) {
        BOOST_THROW_EXCEPTION(not_initialized());
    }
    MDB_val k = make_mdb_val();
    CBString insert_key;

    int res = (direction > 0) ? cursor->last() : cursor->first();
    if (res == MDB_NOTFOUND) {
        insert_key = longlong_to_cbstring(MIDDLE);
    } else {
        cursor->get_current_key(k);
        insert_key = make_string(k);
        long long int_k = strtoull(insert_key, NULL, 10);
        if (int_k == 0) {
            BOOST_THROW_EXCEPTION( lmdb_error() << lmdb_error::what("A key in the database in not an integer key") );
        }
        insert_key = longlong_to_cbstring(int_k + direction, NDIGITS);
    }
    k = make_mdb_val(insert_key);
    if (direction > 0) {
        cursor->append_key_value(k, v);
    } else {
        cursor->set_key_value(k, v);
    }
    return *this;
}


void PersistentQueue::iiterator::_next_value() {
    lock_guard<mutex> guard(current_value_lock);
    if (bool(cursor) && cursor->first() != MDB_NOTFOUND) {
        MDB_val v;
        cursor->get_current_value(v);
        current_value.reset(new CBString(v.mv_data, v.mv_size));
        cursor->del();
    } else {
        _LOG_DEBUG << "iiterator: no next_value";
        current_value.reset();
    }
}

void PersistentQueue::iiterator::_last_value() {
    lock_guard<mutex> guard(current_value_lock);
    if (bool(cursor) && cursor->last() != MDB_NOTFOUND) {
        MDB_val v;
        cursor->get_current_value(v);
        current_value.reset(new CBString(v.mv_data, v.mv_size));
        cursor->del();
    } else {
        _LOG_DEBUG << "iiterator: no last_value";
        current_value.reset();
    }
}



}
