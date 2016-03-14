#pragma once

#include <sstream>
#include <iostream>
#include <string>
#include <boost/throw_exception.hpp>
#include <plog/Log.h>
#include <plog/Appenders/IAppender.h>
#include <Python.h>
#include "lmdb_exceptions.h"
#include "logging.h"
#include "pyutils.h"

namespace utils {

using std::string;
using std::cout;
using std::endl;
using std::flush;
using lmdb::runtime_error;

// py: CRITICAL 50, ERROR 40, WARNING 30, INFO 20, DEBUG 10, NOTSET 0
// plog: none = 0, fatal = 1, error = 2, warning = 3, info = 4, debug = 5, verbose = 6

inline int severity_to_py(plog::Severity s) {
    switch (s) {
        case plog::verbose:
            return 10;
        case plog::debug:
            return 10;
        case plog::info:
            return 20;
        case plog::warning:
            return 30;
        case plog::error:
            return 40;
        case plog::fatal:
            return 50;
        case plog::none:
            return 0;
        default:
            return 0;
    }
}


class PyAppender : public plog::IAppender {
public:

    PyAppender(): logger(NULL) {
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
    }

    void set_pylogger(PyObject* obj) {
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

    void set_pylogger(const string& name) {
        // find logger with logging.getLogger(name)
        if (name.empty()) {
            BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("empty logger name"));
        }
        // convert name to Unicode
        PyNewRef unicode_name(PyUnicode_FromStringAndSize(name.c_str(), name.length()));
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

    virtual void write(const plog::Record& record) {
        if (logger) {
            GilWrapper gil;
            {
                int severity = severity_to_py(record.getSeverity());
                string fname((const char*) (record.getObject()));
                // make a python LogRecord from the plog::Record
                PyNewRef logrecord(PyObject_CallFunction(logrecord_class.get(), (char*) "OisnsOOs", logger_name.get(), severity, fname.c_str(), record.getLine(), record.getMessage().c_str(), Py_None, Py_None, record.getFunc().c_str()));
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

private:
    PyAppender(const PyAppender& appender);
    PyAppender& operator=(const PyAppender& appender);

    PyNewRef logger;
    PyNewRef logging_module;
    PyNewRef logger_class;
    PyNewRef logrecord_class;
    PyNewRef getlogger_function;
    PyNewRef logger_name;

};  // END CLASS PyAppender


class PyLogger: public Logger {
protected:
    static bool default_has_python;
    static PyAppender pyAppender;

public:
    static inline void set_logger(const string& name) {
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

    static inline void set_logger(PyObject* obj) {
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

};  // END class PyLogger
}   // END NS utils
