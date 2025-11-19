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
    msgSeqNumber_ = 0;

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
    // Add this message to the pending list for this process:
    unsigned long msgId = ++msgSeqNumber_;
    Message pendingMessage{myProcessId_, msgId, message};
    pendingDelivery_.insert(pendingMessage);
    sendMessageToAllProcesses(pendingMessage);
}


void FifoBroadcast::sendMessageToAllProcesses(const Message& message) {
    for (const auto& entry : hostMapById_) {
        unsigned long processId = entry.first;
        if (processId == myProcessId_) { // Skip itself - TODO - idk if this is correct
            continue;
        }
        perfectLinkInstance_ -> sendMessage(message, processId);
    }
}

void FifoBroadcast::receivedMessage(const Message& message, const unsigned long& senderId) {
    if (haveDelivered(message)) { // Don't want to waste time on something we've already delivered
      return; 
    }
    pendingDelivery_.insert(message); // Only inserts if it wasn't already in there
    acknowledged_[message].insert(senderId); // The sender ID has acknowledged this message by sending it to us

    if (canDeliver(message)) {
        deliverMessage(message);
    }
}

void FifoBroadcast::deliverMessage(Message message) {
    if (haveDelivered(message)) {
      return; // Don't want to deliver again
    }
    // Remove it from pending delivery
    pendingDelivery_.erase(message);
    // Rebroadcast
    sendMessageToAllProcesses(message);
}

bool FifoBroadcast::haveDelivered(Message message) {
    return (delivered_.find(message) != delivered_.end());
}

bool FifoBroadcast::canDeliver(Message message) {
    // If received acknowledgement from over half of processes we can deliver
    return acknowledged_[message].size() > numProcesses_ / 2;
}

void FifoBroadcast::stop() {
    running_ = false;
    if (logFile_.is_open()) {
        logFile_.flush();
        logFile_.close();
    }
}