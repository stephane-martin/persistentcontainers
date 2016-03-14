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
