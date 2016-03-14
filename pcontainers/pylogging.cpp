#include "pylogging.h"

namespace utils {
    bool PyLogger::default_has_python = false;
    PyAppender PyLogger::pyAppender;
}
