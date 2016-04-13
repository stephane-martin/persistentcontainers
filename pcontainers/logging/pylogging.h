#pragma once

#include <bstrlib/bstrwrap.h>
#include <plog/Log.h>
#include <plog/Appenders/IAppender.h>
#include <Python.h>
#include "logging.h"
#include "../utils/pyutils.h"

namespace utils {

using Bstrlib::CBString;


class PyAppender : public plog::IAppender {
private:
    PyAppender(const PyAppender& appender);
    PyAppender& operator=(const PyAppender& appender);

    PyNewRef logger;
    PyNewRef logging_module;
    PyNewRef logger_class;
    PyNewRef logrecord_class;
    PyNewRef getlogger_function;
    PyNewRef logger_name;
public:

    PyAppender();
    void set_pylogger(PyObject* obj);
    void set_pylogger(const CBString& name);
    virtual void write(const plog::Record& record);

};  // END CLASS PyAppender


class PyLogger: public Logger {
protected:
    static bool default_has_python;
    static PyAppender pyAppender;

public:
    static void set_py_logger(const CBString& name);
    static void set_py_logger(PyObject* obj);

};  // END class PyLogger
}   // END NS utils
