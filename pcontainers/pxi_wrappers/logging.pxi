cdef extern from "logging/logging.h" namespace "utils" nogil:
    enum PlogSeverity "plog::Severity":
        PlogNone "plog::none"
        PlogFatal "plog::fatal"
        PlogError "plog::error"
        PlogWarning "plog::warning"
        PlogInfo "plog::info"
        PlogDebug "plog::debug"
        PlogVerbose "plog::verbose"

    void set_console_logger "utils::Logger::set_console_logger"()
    void set_console_logger "utils::Logger::set_console_logger"(PlogSeverity level)


cdef extern from "logging/pylogging.h" namespace "utils":
    void set_py_logger "utils::PyLogger::set_py_logger" (const CBString& name) except +custom_handler
    void set_py_logger "utils::PyLogger::set_py_logger" (object obj) except +custom_handler
