#include "logging.h"

namespace utils {
    boost::scoped_ptr<ErrorCheckLock> Logger::cerr_lock(new ErrorCheckLock());
    bool Logger::default_has_console = false;
    bool Logger::default_has_been_created = false;
    MutexedConsoleAppender Logger::consoleAppender(cerr_lock.get());
}
