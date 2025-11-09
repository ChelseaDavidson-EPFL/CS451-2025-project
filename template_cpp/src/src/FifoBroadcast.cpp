#include <iostream>

#include "FifoBroadcast.hpp"

FifoBroadcast::FifoBroadcast(std::unordered_map<unsigned long, std::pair<in_addr_t, unsigned short>> hostMapById, std::string logPath)
    : hostMapById_(hostMapById), logPath_(logPath), running_(false)
{
    // Create or overwrite the log file
    logFile_.open(logPath_.c_str(), std::ios::out);
    if (!logFile_.is_open()) {
        std::cerr << "Failed to create log file at: " << logPath_ << std::endl;
        return;
    }
    // DEBUGLOG("Created log file: " << logPath_);

    // Initialise vars:
    numProcesses_ = hostMapById.size();

    // Define delivery callback
    deliverCallback_ = [this](unsigned long senderId, unsigned long messageId){
        // DEBUGLOGRECEIVE("Delivered \"" << messageId << "\" from: " << senderId);
        // logDelivery(senderId, messageId);
    };

}

FifoBroadcast::~FifoBroadcast() {
    stop();
}

void FifoBroadcast::broadcast(const std::string& message) {
    logFile_ << "TEST " << "\n";
}

void FifoBroadcast::stop() {
    running_ = false;
    if (logFile_.is_open()) {
        logFile_.flush();
        logFile_.close();
    }
}