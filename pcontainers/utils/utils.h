#pragma once

#include <utility>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/future.hpp>
#include <bstrlib/bstrwrap.h>
#include <ftw.h>
#include "lmdb.h"

namespace utils {

using std::pair;
using std::make_pair;
using Bstrlib::CBString;



typedef boost::function < bool (const CBString& x) > unary_predicate;
typedef boost::function < bool (const CBString& x, const CBString& y) > binary_predicate;
typedef boost::function < CBString () > zeronary_functor;
typedef boost::function < CBString (const CBString& x) > unary_functor;
typedef boost::function < CBString (const CBString& x, const CBString& y) > binary_scalar_functor;
typedef boost::function < pair<CBString, CBString> (const CBString& x, const CBString& y) > binary_functor;
//typedef boost::function < CBString (boost::shared_future<CBString>&) > then_callback;

inline bool key_is_in_interval(const CBString& key, const CBString& first, const CBString& last) {
    if (bool(first.length()) && (key < first)) {
        return false;
    }
    if (bool(last.length()) && (key >= last)) {
        return false;
    }
    return true;
}

inline bool binary_true_pred(const CBString& s1, const CBString& s2) {
    return true;
}

inline bool unary_true_pred(const CBString& s) {
    return true;
}

inline CBString unary_identity_functor(const CBString& s) {
    return s;
}

inline pair<CBString, CBString> binary_identity_functor(const CBString& s1, const CBString& s2) {
    return make_pair(s1, s2);
}

class KeyInIntervalUnaryPredicate {
protected:
    const CBString first;
    const CBString last;
public:
    KeyInIntervalUnaryPredicate(const CBString& f, const CBString& l): first(f), last(l) { }
    bool operator()(const CBString& key) { return key_is_in_interval(key, first, last); }
};

inline MDB_val make_mdb_val() {
    MDB_val m;
    m.mv_data = NULL;
    m.mv_size = 0;
    return m;
}

inline MDB_val make_mdb_val(const CBString& s) {
    MDB_val m;
    m.mv_data = s.data;
    m.mv_size = s.slen;
    return m;
}

inline CBString make_string(const MDB_val& m) {
    return CBString(m.mv_data, m.mv_size);
}

CBString longlong_to_cbstring(long long i, int n=0);
CBString cpp_realpath(const CBString& path);
size_t my_copy(char *dst, const char *src, size_t siz);
void create_directory(const CBString& directory_name);
void create_if_needed(CBString path);
int unlink_cb(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf);
int rmrf(CBString path);

class TempDirectory {
private:
    TempDirectory(const TempDirectory&);
    TempDirectory& operator=(const TempDirectory&);
protected:
    CBString path;
    const bool _create;
    const bool _destroy;
    void do_destroy();

public:
    CBString get_path() { return path; }

    typedef boost::shared_ptr<TempDirectory> ptr;

    static inline ptr make(bool create=true, bool destroy=true) {
        return boost::shared_ptr<TempDirectory>(new TempDirectory(create, destroy));
    }

    static CBString cpp_mkdtemp(const CBString& full_tmpl, bool create=true);
    TempDirectory(bool create=true, bool destroy=true, const CBString& tmpl="persistent_XXXXXX");

    friend bool operator==(const TempDirectory& one, const TempDirectory& other) {
        return one.path == other.path;
    }

    friend bool operator!=(const TempDirectory& one, const TempDirectory& other) {
        return one.path != other.path;
    }

    ~TempDirectory() {
        do_destroy();
    }

};  // END CLASS TempDirectory


} // END NS utils

