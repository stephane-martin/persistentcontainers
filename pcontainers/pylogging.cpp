#include <iostream>
#include "pylogging.h"


namespace utils {
    using std::cerr;
    using std::endl;
    bool PyLogger::default_has_python = false;
    PyAppender PyLogger::pyAppender;
}
