#include <string>
#include <stdexcept>
#include <sstream>
#include <iostream>
#include <memory>
#include <vector>
#include <map>
#include <utility>
#include <algorithm>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include "lmdb_environment.h"
#include "mutexwrap.h"

namespace lmdb {

using std::string;
using std::pair;
using std::vector;
using std::map;
using std::cout;
using std::endl;

using quiet::ErrorCheckLock;


map<string, boost::shared_ptr<environment> > environment::opened_environments = map<string, boost::shared_ptr<environment> >();
ErrorCheckLock* environment::lock_envs = new ErrorCheckLock();
ErrorCheckLock* environment::lock_dbis = new ErrorCheckLock();

environment::shared_ptr env_factory(const string& directory_name, const lmdb_options& opts) {
    environment::shared_ptr env;
    environment::lock_envs->lock();
    try {
        env = environment::opened_environments[directory_name];
        if (!env) {
            env = environment::shared_ptr(new environment(directory_name, opts));
            environment::opened_environments[directory_name] = env;
        }
    } catch (...) {
        environment::lock_envs->unlock();
        throw;
    }
    environment::lock_envs->unlock();
    return env;
}


}   // end NS lmdb
