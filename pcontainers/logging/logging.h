#pragma once

#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <plog/Log.h>
#include <plog/Record.h>
#include <plog/Severity.h>
#include <plog/Appenders/IAppender.h>

namespace utils {
using boost::mutex;

class MutexedConsoleAppender: public plog::IAppender {
protected:
    mutex cerr_mutex;

public:
    MutexedConsoleAppender() {}
    virtual void write(const plog::Record& record);


};  // END CLASS MutexedConsoleAppender


class Logger {
protected:
    static MutexedConsoleAppender consoleAppender;
    static bool default_has_been_created;
    static bool default_has_console;

public:
    static void set_console_logger(plog::Severity level=plog::debug);
    static inline plog::Logger<0>* get_default() { return plog::get<0>(); }

};  // END class Logger

}   // END NS utils

#define _LOG_(instance, severity)   IF_LOG_(instance, severity) (*plog::get<instance>()) += plog::Record(severity, PLOG_GET_FUNC(), __LINE__, (const void*) __FILE__)
#define _LOG(severity)              _LOG_(0, severity)
#define _LOG_VERBOSE                _LOG(plog::verbose)
#define _LOG_DEBUG                  _LOG(plog::debug)
#define _LOG_INFO                   _LOG(plog::info)
#define _LOG_WARNING                _LOG(plog::warning)
#define _LOG_ERROR                  _LOG(plog::error)
#define _LOG_FATAL                  _LOG(plog::fatal)
