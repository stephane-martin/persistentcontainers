#include <sstream>
#include <iostream>
#include <string>
#include <boost/throw_exception.hpp>
#include <boost/exception/diagnostic_information.hpp>
#include "pylogging.h"
#include "lmdb_exceptions.h"

namespace utils {

using std::string;
using std::cout;
using std::cerr;
using std::endl;
using std::flush;
using lmdb::runtime_error;

bool PyLogger::default_has_python = false;
PyAppender PyLogger::pyAppender;

void PyLogger::set_py_logger(const CBString& name) {
    if (!default_has_been_created) {
        plog::init<0>(plog::debug);
        default_has_been_created = true;
    }
    pyAppender.set_pylogger(name);
    if (!default_has_python) {
        get_default()->addAppender(&pyAppender);
        default_has_python = true;
    }
}

void PyLogger::set_py_logger(PyObject* obj) {
    if (!default_has_been_created) {
        plog::init<0>(plog::debug);
        default_has_been_created = true;
    }
    pyAppender.set_pylogger(obj);
    if (!default_has_python) {
        get_default()->addAppender(&pyAppender);
        default_has_python = true;
    }
}

PyAppender::PyAppender(): logger() {
    try {
        logging_module = PyNewRef(PyImport_ImportModule("logging"));
        if (!logging_module) {
            BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("'import logging' failed"));
        }
        logger_class = PyNewRef(PyObject_GetAttrString(logging_module.get(), "Logger"));
        if (!logger_class) {
            BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("'logging.Logger' not found"));
        }
        logrecord_class = PyNewRef(PyObject_GetAttrString(logging_module.get(), "LogRecord"));
        if (!logrecord_class) {
            BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("'logging.LogRecord' not found"));
        }
        getlogger_function = PyNewRef(PyObject_GetAttrString(logging_module.get(), "getLogger"));
        if (!getlogger_function) {
            BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("'logging.getLogger' not found"));
        }
    } catch (boost::exception & e) {
        cerr << "Exception happened when initializing PyAppender" << endl;
        cerr << boost::diagnostic_information(e);
        throw;

    } catch (...) {
        cerr << "Unkwon exception happened when initializing PyAppender" << endl;
        throw;
    }
}

void PyAppender::set_pylogger(PyObject* obj) {
    if (!obj) {
        logger = PyNewRef();
        return;
    }
    // check that obj has the appropriate type logging.Logger
    int res = PyObject_IsInstance(obj, logger_class.get());
    if (res == -1) {
        BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("'isinstance(..., logging.Logger)' failed"));
    } else if (res == 0) {
        BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("not a logging.Logger instance"));
    } else {
        logger = PyNewRef(obj);
        logger++;
        logger_name = PyNewRef(PyObject_GetAttrString(obj, "name"));     // retrieve logger.name
    }
}

void PyAppender::set_pylogger(const CBString& name) {
    // find logger with logging.getLogger(name)
    if (!name.length()) {
        BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("empty logger name"));
    }
    // convert name to Unicode
    PyNewRef unicode_name(PyUnicode_FromStringAndSize(name, name.length()));
    if (!unicode_name) {
        BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("can't convert logger name to Unicode"));
    }
    // call getLogger
    PyNewRef logger_obj(PyObject_CallFunctionObjArgs(getlogger_function.get(), unicode_name.get(), NULL));
    if (!logger_obj) {
        BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("logging.getLogger failed"));
    }
    set_pylogger(logger_obj.get());
}

void PyAppender::write(const plog::Record& record) {
    if (logger) {
        GilWrapper gil;
        {
            int severity;
            switch (record.getSeverity()) {
                case plog::verbose:
                    severity = 10;
                    break;
                case plog::debug:
                    severity = 10;
                    break;
                case plog::info:
                    severity = 20;
                    break;
                case plog::warning:
                    severity = 30;
                    break;
                case plog::error:
                    severity = 40;
                    break;
                case plog::fatal:
                    severity = 50;
                    break;
                case plog::none:
                    severity = 0;
                    break;
                default:
                    severity = 0;
            }

            CBString fname((const char*) (record.getObject()));
            // make a python LogRecord from the plog::Record
            PyNewRef logrecord(PyObject_CallFunction(logrecord_class.get(), (char*) "OisnsOOs", logger_name.get(), severity, (const char*) fname, record.getLine(), record.getMessage().c_str(), Py_None, Py_None, record.getFunc().c_str()));
            if (!logrecord) {
                BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("LogRecord creation failed"));
            }
            PyNewRef created(PyInt_FromLong(record.getTime().time));
            PyNewRef thread_id(PyInt_FromLong(record.getTime().time));
            if (PyObject_SetAttrString(logrecord.get(), (char*) "created", created.get()) == -1) {
                BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("setting LogRecord.created failed"));
            }
            if (PyObject_SetAttrString(logrecord.get(), (char*) "thread", thread_id.get()) == -1) {
                BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("setting LogRecord.thread failed"));
            }

            // call logger.handle(logrecord)
            PyNewRef result(PyObject_CallMethod(logger.get(), (char*) "handle", (char*) "O", logrecord.get()));
            if (!result) {
                BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("Logger.handle failed"));
            }
        }

        // plog::Record

        // const util::Time& getTime() const;   time_t time; unsigned short millitm;
        // Severity getSeverity() const;        none = 0, fatal = 1, error = 2, warning = 3, info = 4, debug = 5, verbose = 6
        // unsigned int getTid() const;
        // size_t getLine() const;
        // const util::nstring getMessage() const;
        // std::string getFunc() const;

        // python: LogRecord(name, level, pathname, lineno, msg, args, exc_info, func)
        // CRITICAL 50, ERROR 40, WARNING 30, INFO 20, DEBUG 10, NOTSET 0
        // LogRecord.created = time.time()
        // LogRecord.thread = int
        // exc_info = None
    }
}


}   // END NS utils
