#pragma once

#include <iostream>
#include <sstream>
#include <iomanip>
#include <string>
#include <boost/scoped_ptr.hpp>
#include <plog/Log.h>
#include <plog/Appenders/ConsoleAppender.h>
#include "mutexwrap.h"

namespace utils {
using std::cerr;
using std::endl;
using std::flush;
using std::string;
using quiet::ErrorCheckLock;


class MutexedConsoleAppender: public plog::IAppender {
public:
    MutexedConsoleAppender(ErrorCheckLock* mutex) : m_mutex(mutex) {}

    virtual void write(const plog::Record& record) {
        m_mutex->lock();
        try {
            tm t;
            plog::util::localtime_s(&t, &record.getTime().time);


            plog::util::nstringstream ss;
            ss << t.tm_year + 1900 << "-" << std::setfill(PLOG_NSTR('0')) << std::setw(2) << t.tm_mon + 1 << "-" << std::setfill(PLOG_NSTR('0')) << std::setw(2) << t.tm_mday << " ";
            ss << std::setfill(PLOG_NSTR('0')) << std::setw(2) << t.tm_hour << ":" << std::setfill(PLOG_NSTR('0')) << std::setw(2) << t.tm_min << ":" << std::setfill(PLOG_NSTR('0')) << std::setw(2) << t.tm_sec << "." << std::setfill(PLOG_NSTR('0')) << std::setw(3) << record.getTime().millitm << " ";
            ss << std::setfill(PLOG_NSTR(' ')) << std::setw(5) << std::left << getSeverityName(record.getSeverity()) << " ";
            ss << "[" << record.getTid() << "] ";
            ss << "[" << record.getFunc().c_str() << " @ " << ((const char*) record.getObject()) << ":" << record.getLine() << "] ";
            ss << record.getMessage().c_str() << "\n";

            cerr << ss.str() << flush;

        } catch (...) {
            m_mutex->unlock();
            throw;
        }
        m_mutex->unlock();

    }

protected:
    ErrorCheckLock* m_mutex;
};  // END CLASS MutexedConsoleAppender


class Logger {
protected:
    static boost::scoped_ptr<ErrorCheckLock> cerr_lock;
    static MutexedConsoleAppender consoleAppender;
    static bool default_has_been_created;
    static bool default_has_console;

public:
    static inline void add_console() {
        if (!default_has_been_created) {
            plog::init<0>(plog::debug);
            default_has_been_created = true;
        }
        if (!default_has_console) {
            get_default()->addAppender(&consoleAppender);
            default_has_console = true;
        }
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
