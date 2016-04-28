# -*- coding: utf-8 -*-

cpdef set_logger(int level=logging.DEBUG):
    cdef PlogSeverity plog_level = PlogDebug
    if level == logging.DEBUG:
        plog_level = PlogDebug
    elif level == logging.INFO:
        plog_level = PlogInfo
    elif level == logging.WARNING:
        plog_level = PlogWarning
    elif level == logging.ERROR:
        plog_level = PlogError
    elif level == logging.CRITICAL:
        plog_level = PlogFatal
    elif level == logging.NOTSET:
        plog_level = PlogNone
    set_console_logger(plog_level)


cpdef set_python_logger(name_or_logger_obj):
    if isinstance(name_or_logger_obj, unicode):
        name_or_logger_obj = make_utf8(name_or_logger_obj)
    if isinstance(name_or_logger_obj, bytes):
        set_py_logger(tocbstring(name_or_logger_obj))
    else:
        set_py_logger(name_or_logger_obj)
