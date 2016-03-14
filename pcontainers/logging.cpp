#include "logging.h"

namespace utils {
    MutexWrap Logger::cerr_lock;
    bool Logger::default_has_console = false;
    bool Logger::default_has_been_created = false;
    MutexedConsoleAppender Logger::consoleAppender(cerr_lock);
}
