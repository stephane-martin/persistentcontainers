#pragma once

#include <sstream>
#include <iostream>
#include <iomanip>
#include <utility>
#include <string>
#include <boost/function.hpp>
#include <boost/throw_exception.hpp>
#include <boost/move/move.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/core/explicit_operator_bool.hpp>
#include <bstrlib/bstrwrap.h>
#include <string.h>
#include <libgen.h>
#include <stdio.h>
#include <ftw.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include "lmdb.h"
#include "lmdb_exceptions.h"
#include "logging.h"

namespace utils {

using std::setfill;
using std::setw;
using std::ostringstream;
using lmdb::lmdb_error;
using lmdb::io_error;
using lmdb::errinfo_errno;
using Bstrlib::CBString;


typedef boost::function<bool (const CBString& x)> unary_predicate;
typedef boost::function<bool (const CBString& x, const CBString& y)> binary_predicate;
typedef boost::function<CBString (const CBString& x)> unary_functor;
typedef boost::function<CBString (const CBString& x, const CBString& y)> binary_functor;

inline bool binary_true_pred(const CBString& s1, const CBString& s2) {
    return true;
}

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

template <typename T>
inline std::string any_tostring(T x, int n=0) {
    ostringstream ss;
    if (n > 0) {
        ss << setfill('0') << setw(n);
    }
    ss << x;
    return ss.str();
}

template <typename T>
inline CBString any_tocbstring(T x, int n=0) {
    ostringstream ss;
    if (n > 0) {
        ss << setfill('0') << setw(n);
    }
    ss << x;
    return CBString(ss.str().c_str());
}



inline CBString cpp_realpath(const CBString& path) {
    char resolved_path[PATH_MAX + 1];
    char* res = realpath(path, resolved_path);
    if (res == NULL) {
        BOOST_THROW_EXCEPTION( lmdb::runtime_error() << lmdb::lmdb_error::what("realpath failed") << lmdb::errinfo_errno(errno) );
    } else {
        return CBString(resolved_path);
    }
}

inline size_t my_copy(char *dst, const char *src, size_t siz){
	register char *d = dst;
	register const char *s = src;
	register size_t n = siz;

	/* Copy as many bytes as will fit */
	if (n != 0 && --n != 0) {
		do {
			if ((*d++ = *s++) == 0) {
				break;
            }
		} while (--n != 0);
	}

	/* Not enough room in dst, add NUL and traverse rest of src */
	if (n == 0) {
		if (siz != 0) {
			*d = '\0';		/* NUL-terminate dst */
        }
		while (*s++) {
			;
        }
	}

	return(s - src - 1);	/* count does not include NUL */
}

inline void create_directory(const CBString& directory_name) {
    if (!(directory_name.length()) || directory_name == ".") {
        return;
    }

    struct stat s;
    if (stat(directory_name, &s) == 0) {
        // directory_name already exists
        return;
    }
    if (errno != ENOENT) {
        // stat failed, but directory_name exists
        BOOST_THROW_EXCEPTION ( lmdb::io_error() << lmdb::lmdb_error::what("stat failed") << lmdb::errinfo_errno(errno) );
    }
    CBString copy_directory_name(directory_name);
    char* c_parent = dirname((char*) copy_directory_name.data);     // dirname may modify its char* argument
    if (!c_parent) {
        BOOST_THROW_EXCEPTION ( lmdb::io_error() << lmdb::lmdb_error::what("dirname failed") << lmdb::errinfo_errno(errno) );
    }
    // create the parent directory
    create_directory(CBString(c_parent));
    // create directory_name
    if (mkdir(directory_name, 448) == -1) {
        BOOST_THROW_EXCEPTION ( lmdb::io_error() << lmdb::lmdb_error::what("mkdir failed") << lmdb::errinfo_errno(errno) );
    }
    return;
}

inline void create_if_needed(CBString path) {
    struct stat s;
    if (stat(path, &s) == 0) {
        // dirname exists
        // test if it is a directory
        if (S_ISDIR(s.st_mode)) {
            // it is a directory
            // test if it is writeable
            if (access(path, R_OK | W_OK) != 0) {
                BOOST_THROW_EXCEPTION( lmdb::access_error() << lmdb::lmdb_error::what("dirname exists but is not readable and writeable") );
            }
        } else {
            // it's not a directory
            BOOST_THROW_EXCEPTION( lmdb::io_error() << lmdb::lmdb_error::what("dirname exists but is not a directory") );
        }
    } else {
        switch(errno) {
            case ENOENT:
                // A component of path does not exist, or path is an empty string
                create_directory(path);
                break;
            case EACCES:
                BOOST_THROW_EXCEPTION( lmdb::access_error() << lmdb::lmdb_error::what("Search permission is denied for one of the directories in the path prefix of path") );
                break;
            case EFAULT:
                BOOST_THROW_EXCEPTION( std::domain_error("Bad address") );
                break;
            case ELOOP:
                BOOST_THROW_EXCEPTION( lmdb::io_error() << lmdb::lmdb_error::what("Too many symbolic links encountered while traversing the path") );
                break;
            case ENAMETOOLONG:
                BOOST_THROW_EXCEPTION( std::length_error("path is too long") );
                break;
            case ENOMEM:
                BOOST_THROW_EXCEPTION( std::bad_alloc() );
                break;
            case ENOTDIR:
                BOOST_THROW_EXCEPTION( std::invalid_argument("A component of the path prefix of path is not a directory") );
                break;
            case EOVERFLOW:
                BOOST_THROW_EXCEPTION( std::overflow_error("path or fd refers to a file whose size, inode number, or number of blocks cannot be represented") );
                break;
            default:
                BOOST_THROW_EXCEPTION( lmdb::runtime_error() << lmdb::lmdb_error::what("Unknown error when doing stat on dirname") );
        }
    }
}

inline int unlink_cb(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf) {
    int rv = remove(fpath);
    if (rv) {
        perror(fpath);  // todo: log instead
    }
    return rv;
}

inline int rmrf(CBString path) {
    return nftw(path, unlink_cb, 64, FTW_DEPTH | FTW_PHYS);
}

class TempDirectory {
private:
    TempDirectory(const TempDirectory&);
    TempDirectory& operator=(const TempDirectory&);
protected:
    CBString path;
    bool _create;
    bool _destroy;

    void do_destroy() {
        if (bool(path.length()) && _destroy) {
            rmrf(path);
            _destroy = false;
            _LOG_DEBUG << "Deleted temp directory: " << (const char*) path;
        }
    }

public:
    CBString get_path() { return path; }

    typedef boost::shared_ptr<TempDirectory> ptr;

    static inline ptr make(bool create=true, bool destroy=true) {
        return boost::shared_ptr<TempDirectory>(new TempDirectory(create, destroy));
    }

    static inline CBString cpp_mkdtemp(const CBString& full_tmpl, bool create=true) {
        CBString copy(full_tmpl);
        char* c_dirname = create ? mkdtemp((char*) copy.data) : mktemp((char*) copy.data);
        if (!c_dirname) {
            BOOST_THROW_EXCEPTION(io_error() << lmdb_error::what("mk(d)temp failed") << errinfo_errno(errno));
        }
        CBString tmp_dirname(c_dirname);
        _LOG_DEBUG << "New temp directory: " << (const char*) tmp_dirname << (create ? " (created)" : "");
        return tmp_dirname;
    }

    TempDirectory(bool create=true, bool destroy=true, const CBString& tmpl="persistent_XXXXXX"): _create(create), _destroy(destroy) {
        CBString full_tmpl(CBString(P_tmpdir) + CBString("/") + tmpl);
        path = cpp_mkdtemp(full_tmpl, create);
    }

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

