#include <iostream>
#include <sstream>
#include <vector>

#include "LatticeAgreement.hpp"

LatticeAgreement::LatticeAgreement(std::string logPath)
    : logPath_(logPath), running_(false)
{
    // Create or overwrite the log file
    logFile_.open(logPath_.c_str(), std::ios::out);
    if (!logFile_.is_open()) {
        std::cerr << "Failed to create log file at: " << logPath_ << std::endl;
        return;
    }
    // DEBUGLOG("Created log file: " << logPath_);


}

LatticeAgreement::~LatticeAgreement() {
    stop();
}


void LatticeAgreement::stop() {
    running_ = false;
    if (logFile_.is_open()) {
        {
            std::lock_guard<std::mutex> lock(loggingMutex_);
            logFile_.flush();
            logFile_.close();
        }
    }
}
