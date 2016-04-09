#include <sstream>
#include <iostream>
#include <iomanip>
#include <stdexcept>
#include <boost/throw_exception.hpp>
#include <bstrlib/bstrwrap.h>
#include <string.h>
#include <libgen.h>
#include <stdio.h>
#include <ftw.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include "lmdb_exceptions.h"
#include "utils.h"
#include "logging.h"

namespace utils {

using std::setfill;
using std::setw;
using std::ostringstream;
using Bstrlib::CBString;
using lmdb::lmdb_error;
using lmdb::io_error;
using lmdb::errinfo_errno;


CBString longlong_to_cbstring(long long i, int n) {
    ostringstream ss;
    if (n > 0) {
        ss << setfill('0') << setw(n);
    }
    ss << i;
    return CBString(ss.str().c_str());
}

CBString cpp_realpath(const CBString& path) {
    char resolved_path[PATH_MAX + 1];
    char* res = realpath(path, resolved_path);
    if (res == NULL) {
        BOOST_THROW_EXCEPTION( lmdb::runtime_error() << lmdb::lmdb_error::what("realpath failed") << lmdb::errinfo_errno(errno) );
    } else {
        return CBString(resolved_path);
    }
}

size_t my_copy(char *dst, const char *src, size_t siz) {
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

void create_directory(const CBString& directory_name) {
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


void create_if_needed(CBString path) {
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

int unlink_cb(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf) {
    int rv = remove(fpath);
    if (rv) {
        perror(fpath);  // todo: log instead
    }
    return rv;
}

int rmrf(CBString path) {
    return nftw(path, unlink_cb, 64, FTW_DEPTH | FTW_PHYS);
}

void TempDirectory::do_destroy() {
    if (bool(path.length()) && _destroy) {
        rmrf(path);
        _LOG_DEBUG << "Deleted temp directory: " << (const char*) path;
    }
}

CBString TempDirectory::cpp_mkdtemp(const CBString& full_tmpl, bool create) {
    CBString copy(full_tmpl);
    char* c_dirname = create ? mkdtemp((char*) copy.data) : mktemp((char*) copy.data);
    if (!c_dirname) {
        BOOST_THROW_EXCEPTION(io_error() << lmdb_error::what("mk(d)temp failed") << errinfo_errno(errno));
    }
    CBString tmp_dirname(c_dirname);
    _LOG_DEBUG << "New temp directory: " << (const char*) tmp_dirname << (create ? " (created)" : "");
    return tmp_dirname;
}

TempDirectory::TempDirectory(bool create, bool destroy, const CBString& tmpl): _create(create), _destroy(destroy) {
    CBString full_tmpl(CBString(P_tmpdir) + CBString("/") + tmpl);
    path = cpp_mkdtemp(full_tmpl, _create);
}


}
