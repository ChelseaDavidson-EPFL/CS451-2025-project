#include <iostream>

#include "FifoBroadcast.hpp"

FifoBroadcast::FifoBroadcast(unsigned long myProcessId, std::unordered_map<unsigned long, std::pair<in_addr_t, unsigned short>> hostMapById, std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort, std::string logPath)
    : myProcessId_(myProcessId), hostMapById_(hostMapById), hostMapByPort_(hostMapByPort), logPath_(logPath), running_(false)
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

    in_addr_t myIp = hostMapById_[myProcessId_].first;
    unsigned short myPort = hostMapById_[myProcessId_].second; 
    perfectLinkInstance_ = std::make_unique<PerfectLink>(myProcessId_, myIp, myPort, hostMapByPort_, hostMapById_);

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
    std::string testMessage = "TEST sent from processId: " + std::to_string(myProcessId_);
    for (const auto& entry : hostMapById_) {
        unsigned long processId = entry.first;
        if (processId == myProcessId_) { // Skip itself
            continue;
        }
        perfectLinkInstance_ -> sendMessage(testMessage, processId);
    }
}


void FifoBroadcast::sendMessageToAllProcesses(Message message) {

}


void FifoBroadcast::stop() {
    running_ = false;
    if (logFile_.is_open()) {
        logFile_.flush();
        logFile_.close();
    }
}