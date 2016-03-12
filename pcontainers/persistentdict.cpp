#include <string>
#include <utility>
#include <stdexcept>
#include <iostream>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <libgen.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/throw_exception.hpp>
#include "lmdb_environment.h"
#include "lmdb_exceptions.h"
#include "persistentdict.h"
#include "utils.h"


namespace quiet {

using std::string;
using std::cout;
using std::cerr;
using std::endl;
using std::pair;
using std::make_pair;
using namespace utils;


string cpp_realpath(const string& path) {
    char resolved_path[PATH_MAX + 1];
    char* res = realpath(path.c_str(), resolved_path);
    if (res == NULL) {
        BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("realpath failed") << errinfo_errno(errno) );
    } else {
        return string(resolved_path);
    }
}

size_t my_copy(char *dst, const char *src, size_t siz){
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

void create_directory(string directory_name) {
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
        BOOST_THROW_EXCEPTION( io_error() << lmdb_error::what("stat failed") << errinfo_errno(errno) );
    }

    char* c_parent = dirname(c_directory_name);
    if (c_parent == NULL) {
        BOOST_THROW_EXCEPTION( io_error() << lmdb_error::what("dirname failed") << errinfo_errno(errno) );
    }
    // create the parent directory
    create_directory(string(c_parent));
    // create directory_name
    if (mkdir(c_directory_name2, 448) == -1) {
        BOOST_THROW_EXCEPTION( io_error() << lmdb_error::what("mkdir failed") << errinfo_errno(errno) );
    }
    return;
}

void create_if_needed(string path) {
    struct stat s;
    if (stat(path.c_str(), &s) == 0) {
        // dirname exists
        // test if it is a directory
        if (S_ISDIR(s.st_mode)) {
            // it is a directory
            // test if it is writeable
            if (access(path.c_str(), R_OK | W_OK) != 0) {
                BOOST_THROW_EXCEPTION( access_error() << lmdb_error::what("dirname exists but is not readable and writeable") );
            }
        } else {
            // it's not a directory
            BOOST_THROW_EXCEPTION( io_error() << lmdb_error::what("dirname exists but is not a directory") );
        }
    } else {
        switch(errno) {
            case ENOENT:
                // A component of path does not exist, or path is an empty string
                create_directory(path);
                break;
            case EACCES:
                BOOST_THROW_EXCEPTION( access_error() << lmdb_error::what("Search permission is denied for one of the directories in the path prefix of path") );
                break;
            case EFAULT:
                BOOST_THROW_EXCEPTION( std::domain_error("Bad address") );
                break;
            case ELOOP:
                BOOST_THROW_EXCEPTION( io_error() << lmdb_error::what("Too many symbolic links encountered while traversing the path") );
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
                BOOST_THROW_EXCEPTION( runtime_error() << lmdb_error::what("Unknown error when doing stat on dirname") );
        }
    }
}

void PersistentDict::init(const string& directory_name, const string& database_name, const lmdb_options& opts) {
    // explicit initialization of members
    init();

    if (directory_name.empty()) {       // not initialized dict
        return;
    }

    if (!database_name.empty()) {
        this->has_dbname = true;
        this->dbname = database_name;
    }
    create_if_needed(directory_name);
    this->dirname = cpp_realpath(directory_name);
    env = lmdb::env_factory(this->dirname, opts);
    dbi = env->get_dbi(database_name);

}


}
