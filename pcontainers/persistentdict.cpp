#include <string>
#include <utility>
#include <stdexcept>
#include <iostream>
#include <vector>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/throw_exception.hpp>
#include <boost/function.hpp>
#include "lmdb_environment.h"
#include "lmdb_exceptions.h"
#include "persistentdict.h"
#include "utils.h"


namespace quiet {

using std::cout;
using std::cerr;
using std::endl;
using std::pair;
using std::vector;
using std::make_pair;
using namespace utils;
using Bstrlib::CBString;

void PersistentDict::init() {
    env = lmdb::environment::factory(dirname, opts);
    dirname = env->get_dirname();
    dbname.trim();
    dbi = env->get_dbi(dbname);
}

void PersistentDict::copy_to(shared_ptr<PersistentDict> other, const CBString& first_key, const CBString& last_key, ssize_t chunk_size) const {
    if (!*this) {
        _LOG_DEBUG << "copy_to cancelled: source dict is not initialized";
        return;
    }
    if (!other || !*other) {
        BOOST_THROW_EXCEPTION(not_initialized() << lmdb_error::what("copy_to: the other dict is not initialized"));
    }
    if (*this == *other) {
        _LOG_DEBUG << "copy_to cancelled: source and dest are the same dict";
        return;
    }
    if (chunk_size <= 0) {
        chunk_size = SSIZE_MAX;
    }
    const_iterator src_it(const_iterator::range(shared_from_this(), first_key));
    bool key_in_range = !src_it.has_reached_end();
    while (key_in_range) {
        insert_iterator dest_it(other->insertiterator());
        for(ssize_t copied = 0; copied < chunk_size; ++copied) {
            if (src_it.has_reached_end()) {
                key_in_range = false;
                break;
            }
            pair<const CBString, CBString> p(src_it.get_item());
            if (!key_is_in_interval(p.first, first_key, last_key)) {
                key_in_range = false;
                break;
            }
            dest_it = p;
            ++src_it;
        }
    }
}

void PersistentDict::move_to(shared_ptr<PersistentDict> other, const CBString& first_key, const CBString& last_key, ssize_t chunk_size) {
    if (!*this) {
        _LOG_DEBUG << "move_to: cancelled: source dict is not initialized";
        return;
    }
    if (!other || !*other) {
        BOOST_THROW_EXCEPTION(not_initialized() << lmdb_error::what("move_to: the other dict is not initialized"));
    }
    if (*this == *other) {
        _LOG_DEBUG << "move_to: cancelled: source and dest are the same dict";
        return;
    }
    if (chunk_size <= 0) {
        chunk_size = SSIZE_MAX;
    }

    bool key_in_range = true;
    while (key_in_range) {
        iterator src_it(iterator::range(shared_from_this(), first_key, false));
        insert_iterator dest_it(other->insertiterator());
        for(ssize_t copied = 0; copied < chunk_size; ++copied) {
            if (src_it.has_reached_end()) {
                key_in_range = false;
                break;
            }
            pair<const CBString, CBString> p(src_it.get_item());
            if (!key_is_in_interval(p.first, first_key, last_key)) {
                key_in_range = false;
                break;
            }
            dest_it = p;
            src_it.del();
            ++src_it;
        }
    }
}

vector<CBString> PersistentDict::erase_interval(const CBString& first_key, const CBString& last_key, ssize_t chunk_size) {
    vector<CBString> removed_keys;
    CBString k;
    bool key_in_range = true;

    if (!*this ) {
        return removed_keys;
    }

    if (empty_interval(first_key, last_key)) {
        return removed_keys;
    }

    if (chunk_size == -1) {
        chunk_size = SSIZE_MAX;
    }

    try {
        while (key_in_range) {
            vector<CBString> tmp_removed_keys;
            {
                iterator it(iterator::range(shared_from_this(), first_key, false));
                for(ssize_t i=0; i < chunk_size; i++) {
                    if (it.has_reached_end()) {
                        key_in_range = false;
                        break;
                    }
                    k = it.get_key();
                    if (!key_is_in_interval(k, first_key, last_key)) {
                        key_in_range = false;
                        break;
                    }
                    it.del();
                    tmp_removed_keys.push_back(k);
                    ++it;
                }
            }
            removed_keys.insert(removed_keys.end(), tmp_removed_keys.begin(), tmp_removed_keys.end());
        }
        return removed_keys;
    } catch (...) {
        _LOG_ERROR << "Exception happened. So far " << removed_keys.size() << " have been removed from the dict";
        throw;
    }
}


void PersistentDict::transform_values(binary_scalar_functor binary_funct, const CBString& first_key, const CBString& last_key, ssize_t chunk_size) {
    // value = f(key, value)
    if (!*this) {
        _LOG_INFO << "transform_values: cancelled cause the dict is not initialized";
        return;
    }
    if (chunk_size <= 0) {
        chunk_size = SSIZE_MAX;
    }
    if (!binary_funct) {
        _LOG_INFO << "transform_values: cancelled cause binary_funct is empty";
        return;
    }
    bool key_in_range = true;
    while (key_in_range) {
        iterator it(iterator::range(shared_from_this(), first_key, false));
        for(ssize_t i=0; i < chunk_size; i++) {
            if (it.has_reached_end()) {
                key_in_range = false;
                break;
            }
            pair<const CBString, CBString> p(it.get_item());
            if (!key_is_in_interval(p.first, first_key, last_key)) {
                key_in_range = false;
                break;
            }
            it->second = binary_funct(p.first, p.second);
            ++it;
        }
    }
}


void PersistentDict::remove_if(binary_predicate binary_pred, const CBString& first_key, const CBString& last_key, ssize_t chunk_size) {
    // remove_if(predicate(key, value))
    _LOG_DEBUG << "remove_if(binary_predicate)";
    if (!*this) {
        _LOG_INFO << "remove_if: cancelled cause the dict is not initialized";
        return;
    }
    if (!binary_pred) {
        _LOG_INFO << "remove_if: cancelled cause binary_pred is empty";
        return;
    }
    if (chunk_size <= 0) {
        chunk_size = SSIZE_MAX;
    }
    bool key_in_range = true;
    while (key_in_range) {
        iterator it(iterator::range(shared_from_this(), first_key, false));
        for(ssize_t i=0; i < chunk_size; i++) {
            if (it.has_reached_end()) {
                key_in_range = false;
                break;
            }
            pair<const CBString, CBString> p(it.get_item());
            if (!key_is_in_interval(p.first, first_key, last_key)) {
                key_in_range = false;
                break;
            }
            if (binary_pred(p.first, p.second)) {
                it.del();
            }
            ++it;
        }
    }
}


void PersistentDict::remove_duplicates(const CBString& first_key, const CBString& last_key) {
    _LOG_DEBUG << "PersistentDict::remove_duplicates()";
    if (!*this) {
        _LOG_DEBUG << "cancelled: the dict is not initialized";
        return;
    }
    if (empty_interval(first_key, last_key)) {
        _LOG_DEBUG << "cancelled: the provided interval is empty";
        return;
    }

    TempDirectory::ptr tmpdir = TempDirectory::make();
    ssize_t chunk_size = 100;
    lmdb_options opts;
    opts.write_map = true;
    opts.map_async = true;
    {
        shared_ptr<PersistentDict> tmp_dict = PersistentDict::factory(tmpdir->get_path(), "", opts);

        {
            const_iterator it = const_iterator::range(shared_from_this(), first_key);
            bool key_in_range = true;
            while (key_in_range) {
                insert_iterator tmp_it = tmp_dict->insertiterator();
                for(ssize_t i=0; i < chunk_size; i++) {
                    if (it.has_reached_end()) {
                        key_in_range = false;
                        break;
                    }
                    pair<const CBString, CBString> current(it.get_item());
                    if (!key_is_in_interval(current.first, first_key, last_key)) {
                        key_in_range = false;
                        break;
                    }
                    tmp_it = make_pair(current.second, current.first);
                    ++it;
                }
            }
        }

        {
            iterator it = iterator::range(shared_from_this(), first_key, false);
            for(; !it.has_reached_end(); ++it) {
                pair<const CBString, CBString> current(it.get_item());
                if (!key_is_in_interval(current.first, first_key, last_key)) {
                    break;
                }
                CBString unique_key = tmp_dict->at(current.second);
                if (current.first != unique_key) {
                    it.del();
                }
            }
        }
    }
}

size_t PersistentDict::count_interval_if(binary_predicate predicate, const CBString& first_key, const CBString& last_key) const {
    if (!*this) {
        return 0;
    }
    const_iterator it(const_iterator::range(shared_from_this(), first_key));
    size_t n = 0;
    for(; !it.has_reached_end(); ++it) {
        pair<const CBString, CBString> p(it.get_item());
        if (!key_is_in_interval(p.first, first_key, last_key)) {
            break;
        }
        if (bool(predicate) && predicate(p.first, p.second)) {
            n += 1;
        }
    }
    return n;
}


CBString PersistentDict::operator[] (const CBString& key) const {
    if (!key.length()) {
        BOOST_THROW_EXCEPTION(empty_key());
    }
    if (!*this) {
        return "";
    }
    const_iterator it(shared_from_this(), key);
    if (it.has_reached_end()) {
        return "";
    }
    return it.get_value();
}


CBString PersistentDict::at(MDB_val k) const {
    if (k.mv_size == 0 || k.mv_data == NULL) {
        BOOST_THROW_EXCEPTION(empty_key());
    }
    if (!*this) {
        BOOST_THROW_EXCEPTION(mdb_notfound());
    }
    const_iterator it(shared_from_this(), k);
    if (it.has_reached_end()) {
        BOOST_THROW_EXCEPTION(mdb_notfound());
    }
    return it.get_value();
}

CBString PersistentDict::pop(MDB_val k) {
    if (k.mv_size == 0 || k.mv_data == NULL) {
        BOOST_THROW_EXCEPTION(empty_key());
    }
    if (!*this) {
        BOOST_THROW_EXCEPTION(mdb_notfound());
    }
    iterator it(shared_from_this(), k, false);
    if (it.has_reached_end()) {
        BOOST_THROW_EXCEPTION(mdb_notfound());
    }
    return it.pop();
}


CBString PersistentDict::setdefault(MDB_val k, MDB_val dflt) {
    if (k.mv_size == 0 || k.mv_data == NULL) {
        BOOST_THROW_EXCEPTION(empty_key());
    }
    if (!*this) {
        BOOST_THROW_EXCEPTION(not_initialized());
    }
    iterator it(shared_from_this(), k, 0);
    if (it.has_reached_end()) {
        it.set_key_value(k, dflt);
        return make_string(dflt);
    }
    return it.get_value();
}


pair<CBString, CBString> PersistentDict::popitem() {
    if (!*this) {
        BOOST_THROW_EXCEPTION(empty_database());
    }
    iterator it(shared_from_this(), 0, false);
    if (it.has_reached_end()) {
        BOOST_THROW_EXCEPTION(empty_database());
    }
    pair<CBString, CBString> p = *it;
    it.del();
    return p;
}


void PersistentDict::erase(const CBString& key) {
    if (!*this) {
        BOOST_THROW_EXCEPTION(mdb_notfound());
    }
    iterator it(shared_from_this(), key, false);
    if (it.has_reached_end()) {
        BOOST_THROW_EXCEPTION(mdb_notfound());
    }
    it.del();
}

void PersistentDict::erase(MDB_val key) {
    if (!*this) {
        BOOST_THROW_EXCEPTION(mdb_notfound());
    }
    iterator it(shared_from_this(), key, false);
    if (it.has_reached_end()) {
        BOOST_THROW_EXCEPTION(mdb_notfound());
    }
    it.del();
}


bool PersistentDict::empty_interval(const CBString& first_key, const CBString& last_key) const {
    if (!*this) {
        return true;
    }
    const_iterator it(const_iterator::range(shared_from_this(), first_key));
    if (it.has_reached_end()) {
        return true;
    }
    return !key_is_in_interval(it.get_key(), first_key, last_key);
}

void PersistentDict::iterator::del() {
    if (!initialized) {
        BOOST_THROW_EXCEPTION(not_initialized());
    }
    if (reached_end || reached_beginning) {
        BOOST_THROW_EXCEPTION(mdb_notfound());
    }
    cursor->del();
}

void PersistentDict::iterator::del(MDB_val key) {
    if (!initialized) {
        BOOST_THROW_EXCEPTION(not_initialized());
    }
    if (cursor->position(key) == MDB_NOTFOUND) {
        BOOST_THROW_EXCEPTION(mdb_notfound());
    }
    cursor->del();
}


void PersistentDict::iterator::append_key_value(MDB_val key, MDB_val value) {
    if (!initialized) {
        BOOST_THROW_EXCEPTION(not_initialized());
    }
    if (key.mv_size == 0 || key.mv_data == NULL) {
        BOOST_THROW_EXCEPTION(empty_key());
    }
    if (value.mv_data == NULL) {
        BOOST_THROW_EXCEPTION(std::invalid_argument("invalid value"));
    }
    cursor->append_key_value(key, value);
    reached_beginning = false;
    reached_end = false;
}

CBString PersistentDict::iterator::pop() {
    if (!initialized) {
        BOOST_THROW_EXCEPTION(not_initialized());
    }
    if (reached_end || reached_beginning) {
        BOOST_THROW_EXCEPTION(mdb_notfound());
    }
    MDB_val v = make_mdb_val();
    cursor->get_current_value(v);
    CBString result = make_string(v);
    cursor->del();
    return result;
}

void PersistentDict::iterator::set_value(const CBString& value) {
    if (!initialized) {
        BOOST_THROW_EXCEPTION(not_initialized());
    }
    if (reached_end || reached_beginning) {
        BOOST_THROW_EXCEPTION(mdb_notfound());
    }
    cursor->set_current_value(make_mdb_val(value));
}

void PersistentDict::iterator::set_value(MDB_val v) {
    if (!initialized) {
        BOOST_THROW_EXCEPTION(not_initialized());
    }
    if (v.mv_data == NULL) {
        BOOST_THROW_EXCEPTION(std::invalid_argument("invalid value"));
    }
    if (reached_end || reached_beginning) {
        BOOST_THROW_EXCEPTION(mdb_notfound());
    }
    cursor->set_current_value(v);
}

void PersistentDict::iterator::set_key_value(MDB_val key, MDB_val value) {
    if (!initialized) {
        BOOST_THROW_EXCEPTION(not_initialized());
    }
    if (key.mv_size == 0 || key.mv_data == NULL) {
        BOOST_THROW_EXCEPTION(empty_key());
    }
    if (value.mv_data == NULL) {
        BOOST_THROW_EXCEPTION(std::invalid_argument("invalid value"));
    }
    cursor->set_key_value(key, value);
    reached_beginning = false;
    reached_end = false;
}


}   // END NS quiet
