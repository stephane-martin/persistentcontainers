#pragma once

#include <sstream>
#include <iostream>
#include <iomanip>
#include <string>
#include <utility>
#include <boost/function.hpp>
#include <boost/throw_exception.hpp>
#include <string.h>
#include <libgen.h>
#include "lmdb.h"
#include "lmdb_exceptions.h"
#include "logging.h"

namespace utils {

using std::string;
using std::setfill;
using std::setw;
using std::ostringstream;

typedef boost::function<bool (const string& x)> unary_predicate;
typedef boost::function<bool (const string& x, const string& y)> binary_predicate;
typedef boost::function<string (const string& x)> unary_functor;
typedef boost::function<string (const string& x, const string& y)> binary_functor;

inline bool binary_true_pred(const string& s1, const string& s2) {
    return true;
}

inline MDB_val make_mdb_val() {
    MDB_val m;
    m.mv_data = NULL;
    m.mv_size = 0;
    return m;
}

inline MDB_val make_mdb_val(const string& s) {
    MDB_val m;
    m.mv_data = (void*) s.c_str();
    m.mv_size = s.length();
    return m;
}

inline string make_string(const MDB_val& m) {
    return string((char*) m.mv_data, m.mv_size);
}

template <typename T>
inline string any_tostring(T x, int n=0) {
    ostringstream ss;
    if (n > 0) {
        ss << setfill('0') << setw(n);
    }
    ss << x;
    return ss.str();
}

inline string cpp_realpath(const string& path) {
    char resolved_path[PATH_MAX + 1];
    char* res = realpath(path.c_str(), resolved_path);
    if (res == NULL) {
        BOOST_THROW_EXCEPTION( lmdb::runtime_error() << lmdb::lmdb_error::what("realpath failed") << lmdb::errinfo_errno(errno) );
    } else {
        return string(resolved_path);
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

inline void create_directory(string directory_name) {
    if (directory_name.empty() || directory_name == ".") {
        return;
    }
    char c_directory_name[directory_name.length() + 1];
    char c_directory_name2[directory_name.length() + 1];    // dirname may modify its argument
    my_copy(c_directory_name, directory_name.c_str(), directory_name.length() + 1);
    my_copy(c_directory_name2, directory_name.c_str(), directory_name.length() + 1);
    struct stat s;
    if (stat(c_directory_name, &s) == 0) {
        // directory_name already exists
        return;
    }
    if (errno != ENOENT) {
        // stat failed, but directory_name exists
        BOOST_THROW_EXCEPTION( lmdb::io_error() << lmdb::lmdb_error::what("stat failed") << lmdb::errinfo_errno(errno) );
    }

    char* c_parent = dirname(c_directory_name);
    if (c_parent == NULL) {
        BOOST_THROW_EXCEPTION( lmdb::io_error() << lmdb::lmdb_error::what("dirname failed") << lmdb::errinfo_errno(errno) );
    }
    // create the parent directory
    create_directory(string(c_parent));
    // create directory_name
    if (mkdir(c_directory_name2, 448) == -1) {
        BOOST_THROW_EXCEPTION( lmdb::io_error() << lmdb::lmdb_error::what("mkdir failed") << lmdb::errinfo_errno(errno) );
    }
    return;
}

inline void create_if_needed(string path) {
    struct stat s;
    if (stat(path.c_str(), &s) == 0) {
        // dirname exists
        // test if it is a directory
        if (S_ISDIR(s.st_mode)) {
            // it is a directory
            // test if it is writeable
            if (access(path.c_str(), R_OK | W_OK) != 0) {
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

} // END NS utils

