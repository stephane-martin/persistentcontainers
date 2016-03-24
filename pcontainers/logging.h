#pragma once

#include <iostream>
#include <sstream>
#include <iomanip>
#include <string>
#include <boost/scoped_ptr.hpp>
#include <boost/core/explicit_operator_bool.hpp>
#include <plog/Log.h>
#include <plog/Severity.h>
#include <plog/Appenders/ConsoleAppender.h>
#include "mutexwrap.h"

namespace utils {

using std::cerr;
using std::endl;
using std::flush;
using std::string;
using quiet::MutexWrap;
using quiet::MutexWrapLock;

class MutexedConsoleAppender: public plog::IAppender {
public:
    MutexedConsoleAppender(MutexWrap& mutex) : m_mutex(mutex) {}
    virtual void write(const plog::Record& record);

protected:
    MutexWrap& m_mutex;
};  // END CLASS MutexedConsoleAppender


class Logger {
protected:
    static MutexWrap cerr_lock;
    static MutexedConsoleAppender consoleAppender;
    static bool default_has_been_created;
    static bool default_has_console;

public:
    static inline void set_console_logger(plog::Severity level=plog::debug) {
        if (!default_has_been_created) {
            plog::init<0>(level);
            default_has_been_created = true;
        }
        if (!default_has_console) {
            get_default()->addAppender(&consoleAppender);
            default_has_console = true;
        }
        plog::get<0>()->setMaxSeverity(level);
    }

    static inline plog::Logger<0>* get_default() {
        return plog::get<0>();
    }



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
