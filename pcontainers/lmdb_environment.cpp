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
#include <boost/make_shared.hpp>
#include "lmdb_environment.h"
#include "mutexwrap.h"

namespace lmdb {

using std::string;
using std::pair;
using std::vector;
using std::map;
using std::cout;
using std::endl;
using std::exception;
using std::runtime_error;

using quiet::ErrorCheckLock;


map<string, boost::shared_ptr<environment> > environment::opened_environments = map<string, boost::shared_ptr<environment> >();
ErrorCheckLock* environment::lock_envs = new ErrorCheckLock();
ErrorCheckLock* environment::lock_dbis = new ErrorCheckLock();

boost::shared_ptr<environment> env_factory(const string& directory_name, const lmdb_options& opts) {
    environment::lock_envs->lock();
    boost::shared_ptr<environment> env = environment::opened_environments[directory_name];
    environment::lock_envs->unlock();
    if (!env) {
        env = boost::make_shared<environment>(directory_name, opts);
        environment::opened_environments[directory_name] = env;
    }
    return env;
}


}   // end NS lmdb
