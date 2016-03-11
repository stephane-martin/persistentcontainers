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
#include <boost/make_shared.hpp>
#include "lmdb_environment.h"
#include "persistentdict.h"


namespace quiet {

using std::string;
using std::runtime_error;
using std::exception;
using std::cout;
using std::cerr;
using std::endl;
using std::pair;
using std::make_pair;


string cpp_realpath(const string& path) {
    char resolved_path[PATH_MAX + 1];
    char* res = realpath(path.c_str(), resolved_path);
    if (res == NULL) {
        throw runtime_error("realpath returned with: " + any_tostring(errno));
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
    char* c_directory_name = new char[directory_name.length() + 1];
    my_copy(c_directory_name, directory_name.c_str(), directory_name.length() + 1);
    struct stat s;
    if (stat(c_directory_name, &s) == 0) {
        // directory_name already exists
        return;
    }
    if (errno != ENOENT) {
        // stat failed, but directory_name exists
        throw io_error("create_directory unexpectedly failed :(");
    }
    char* c_parent = dirname(c_directory_name);
    if (c_parent == NULL) {
        throw io_error("dirname failed :(");
    }
    string parent = string(c_parent);
    // create the parent directory
    create_directory(parent);
    // create directory_name
    if (mkdir(c_directory_name, 448) == -1) {
        throw io_error("mkdir unexpectedly failed :(");
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
                throw access_error("dirname exists but is not readable and writeable");
            }
        } else {
            // it's not a directory
            throw io_error("dirname exists but is not a directory");
        }
    } else {
        switch(errno) {
            case ENOENT:
                // A component of path does not exist, or path is an empty string
                create_directory(path);
                break;
            case EACCES:
                throw access_error("Search permission is denied for one of the directories in the path prefix of path");
                break;
            case EFAULT:
                throw std::domain_error("Bad address");
                break;
            case ELOOP:
                throw std::runtime_error("Too many symbolic links encountered while traversing the path");
                break;
            case ENAMETOOLONG:
                throw std::length_error("path is too long");
                break;
            case ENOMEM:
                throw std::bad_alloc();
                break;
            case ENOTDIR:
                throw std::invalid_argument("A component of the path prefix of path is not a directory");
                break;
            case EOVERFLOW:
                throw std::overflow_error("path or fd refers to a file whose size, inode number, or number of blocks cannot be represented");
                break;
            default:
                throw std::runtime_error("Unknown error when doing stat on dirname");
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
    create_if_needed(this->dirname);
    this->dirname = cpp_realpath(directory_name);
    env = lmdb::env_factory(this->dirname, opts);
    dbi = env->get_dbi(database_name);

}


}
