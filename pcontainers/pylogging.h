#pragma once

#include <sstream>
#include <iostream>
#include <string>
#include <boost/scoped_ptr.hpp>
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

    typedef boost::scoped_ptr<PyRefWrapper> py_ptr;

    PyAppender(): py_logger_obj(NULL) {
        logging_module.reset(pywrap(PyImport_ImportModule("logging")));
        if (!logging_module) {
            BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("'import logging' failed"));
        }
        logger_class.reset(pywrap(PyObject_GetAttrString(logging_module->obj, "Logger")));
        if (!logger_class) {
            BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("'logging.Logger' not found"));
        }
        logrecord_class.reset(pywrap(PyObject_GetAttrString(logging_module->obj, "LogRecord")));
        if (!logrecord_class) {
            BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("'logging.LogRecord' not found"));
        }
        getlogger_function.reset(pywrap(PyObject_GetAttrString(logging_module->obj, "getLogger")));
        if (!getlogger_function) {
            BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("'logging.getLogger' not found"));
        }

    }

    ~PyAppender() {
        if (logging_module || logger_class || getlogger_function || logrecord_class || py_logger_obj) {
            //PyGILState_STATE __pyx_gilstate_save = PyGILState_Ensure();
            getlogger_function.reset();
            logrecord_class.reset();
            logger_class.reset();
            logging_module.reset();
            py_logger_obj.reset();
            logger_name.reset();
            //PyGILState_Release(__pyx_gilstate_save);
        }
    }

    void set_pylogger(PyObject* obj) {
        if (!obj) {
            py_logger_obj.reset();
            return;
        }
        // check that obj has the appropriate type logging.Logger
        int res = PyObject_IsInstance(obj, logger_class->obj);
        if (res == -1) {
            BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("'isinstance(..., logging.Logger)' failed"));
        } else if (res == 0) {
            BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("not a logging.Logger instance"));
        } else {
            Py_INCREF(obj);
            py_logger_obj.reset(pywrap(obj));
            logger_name.reset(pywrap(PyObject_GetAttrString(obj, "name")));     // retrieve logger.name
        }
    }

    void set_pylogger(const string& name) {
        // find logger with logging.getLogger(name)
        if (name.empty()) {
            BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("empty logger name"));
        }
        // convert name to Unicode
        py_ptr unicode_name(pywrap(PyUnicode_FromStringAndSize(name.c_str(), name.length())));
        if (!unicode_name) {
            BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("can't convert logger name to Unicode"));
        }
        // call getLogger
        py_ptr logger_obj(pywrap(PyObject_CallFunctionObjArgs(getlogger_function->obj, unicode_name->obj, NULL)));
        if (!logger_obj) {
            BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("logging.getLogger failed"));
        }
        set_pylogger(logger_obj->obj);

    }

    virtual void write(const plog::Record& record) {
        if (py_logger_obj) {
            PyGILState_STATE __pyx_gilstate_save = PyGILState_Ensure();
            try {
                int severity = severity_to_py(record.getSeverity());
                string fname((const char*) (record.getObject()));
                // make a python LogRecord from the plog::Record
                py_ptr logrecord(pywrap(PyObject_CallFunction(logrecord_class->obj, (char*) "OisnsOOs", logger_name->obj, severity, fname.c_str(), record.getLine(), record.getMessage().c_str(), Py_None, Py_None, record.getFunc().c_str())));
                if (!logrecord) {
                    BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("LogRecord creation failed"));
                }
                py_ptr created(pywrap(PyInt_FromLong(record.getTime().time)));
                py_ptr thread_id(pywrap(PyInt_FromLong(record.getTime().time)));
                if (PyObject_SetAttrString(logrecord->obj, (char*) "created", created->obj) == -1) {
                    BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("setting LogRecord.created failed"));
                }
                if (PyObject_SetAttrString(logrecord->obj, (char*) "thread", thread_id->obj) == -1) {
                    BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("setting LogRecord.thread failed"));
                }

                // call logger.handle(logrecord)
                py_ptr result(pywrap(PyObject_CallMethod(py_logger_obj->obj, (char*) "handle", (char*) "O", logrecord->obj)));
                if (!result) {
                    BOOST_THROW_EXCEPTION(runtime_error() << runtime_error::what("Logger.handle failed"));
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



            } catch (...) {
                PyGILState_Release(__pyx_gilstate_save);
                throw;
            }
            PyGILState_Release(__pyx_gilstate_save);
        }
    }

private:
    PyAppender(const PyAppender& appender);
    PyAppender& operator=(const PyAppender& appender);

    py_ptr py_logger_obj;
    py_ptr logging_module;
    py_ptr logger_class;
    py_ptr logrecord_class;
    py_ptr getlogger_function;
    py_ptr logger_name;

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

