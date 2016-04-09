#include <iostream>
#include <sstream>
#include <iomanip>
#include <string>


#include "logging.h"

namespace utils {
using std::cerr;
using std::endl;
using std::flush;
using std::setfill;
using std::setw;
using std::string;
using boost::mutex;
using boost::lock_guard;


bool Logger::default_has_console = false;
bool Logger::default_has_been_created = false;
MutexedConsoleAppender Logger::consoleAppender;

void MutexedConsoleAppender::write(const plog::Record& record) {
    tm t;
    plog::util::localtime_s(&t, &record.getTime().time);

    plog::util::nstringstream ss;
    ss << t.tm_year + 1900 << "-" << setfill(PLOG_NSTR('0')) << setw(2) << t.tm_mon + 1 << "-" << setfill(PLOG_NSTR('0')) << setw(2) << t.tm_mday << " ";
    ss << setfill(PLOG_NSTR('0')) << setw(2) << t.tm_hour << ":" << setfill(PLOG_NSTR('0')) << setw(2) << t.tm_min << ":" << setfill(PLOG_NSTR('0')) << setw(2) << t.tm_sec << "." << setfill(PLOG_NSTR('0')) << setw(3) << record.getTime().millitm << " ";
    ss << setfill(PLOG_NSTR(' ')) << setw(5) << std::left << getSeverityName(record.getSeverity()) << " ";
    ss << "[" << record.getTid() << "] ";
    ss << "[" << record.getFunc().c_str() << " @ " << ((const char*) record.getObject()) << ":" << record.getLine() << "] ";
    ss << record.getMessage().c_str() << "\n";

    lock_guard<mutex> guard(cerr_mutex);
    cerr << ss.str() << flush;
}

void Logger::set_console_logger(plog::Severity level) {
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


}   // END NS utils
