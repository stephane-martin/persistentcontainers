#include "logging.h"

namespace utils {
    MutexWrap Logger::cerr_lock;
    bool Logger::default_has_console = false;
    bool Logger::default_has_been_created = false;
    MutexedConsoleAppender Logger::consoleAppender(cerr_lock);

    void MutexedConsoleAppender::write(const plog::Record& record) {
        tm t;
        plog::util::localtime_s(&t, &record.getTime().time);

        plog::util::nstringstream ss;
        ss << t.tm_year + 1900 << "-" << std::setfill(PLOG_NSTR('0')) << std::setw(2) << t.tm_mon + 1 << "-" << std::setfill(PLOG_NSTR('0')) << std::setw(2) << t.tm_mday << " ";
        ss << std::setfill(PLOG_NSTR('0')) << std::setw(2) << t.tm_hour << ":" << std::setfill(PLOG_NSTR('0')) << std::setw(2) << t.tm_min << ":" << std::setfill(PLOG_NSTR('0')) << std::setw(2) << t.tm_sec << "." << std::setfill(PLOG_NSTR('0')) << std::setw(3) << record.getTime().millitm << " ";
        ss << std::setfill(PLOG_NSTR(' ')) << std::setw(5) << std::left << getSeverityName(record.getSeverity()) << " ";
        ss << "[" << record.getTid() << "] ";
        ss << "[" << record.getFunc().c_str() << " @ " << ((const char*) record.getObject()) << ":" << record.getLine() << "] ";
        ss << record.getMessage().c_str() << "\n";

        MutexWrapLock lock(m_mutex);
        {
            cerr << ss.str() << flush;
        }
    }

}
