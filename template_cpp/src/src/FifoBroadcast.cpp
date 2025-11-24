#include <iostream>

#include "FifoBroadcast.hpp"

// TODO - ************ TURN THIS OFF BEFORE SUBMISSION ****************
// #define DEBUG
// #define DEBUGBROADCAST
// #define DEBUGRECEIVE

// Debug logging
#ifdef DEBUGBROADCAST
    #define DEBUGLOGSEND(msg) (std::cout << msg << std::endl)
#else
    #define DEBUGLOGSEND(msg) do {} while(0) // no-op in release
#endif

#ifdef DEBUGRECEIVE
    #define DEBUGLOGRECEIVE(msg) (std::cout << msg << std::endl)
#else
    #define DEBUGLOGRECEIVE(msg) do {} while(0) // no-op in release
#endif


#ifdef DEBUG
    #define DEBUGLOG(msg) (std::cout << msg << std::endl)
#else
    #define DEBUGLOG(msg) do {} while(0) // no-op in release
#endif

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

    // Define perfect link's delivery callback
    perfectLinkInstance_->setDeliverCallback(
        [this](Message msg, unsigned long senderId) {
            this->receivedMessage(msg, senderId);
        }
    );


}

FifoBroadcast::~FifoBroadcast() {
    stop();
}

void FifoBroadcast::broadcast(const std::string& message) {
    // Add this message to the pending list for this process:
    unsigned long msgId = ++msgSeqNumber_;
    Message pendingMessage{myProcessId_, msgId, message};
    pendingDelivery_.insert(pendingMessage);
    acknowledged_[pendingMessage].insert(myProcessId_);
    logBroadcast(msgId);
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
    DEBUGLOGRECEIVE("Received message: " << message.origSenderId << ", " << message.messageId);
    if (haveDelivered(message)) { // Don't want to waste time on something we've already delivered
        DEBUGLOGRECEIVE("Already delivered message");
        return; 
    }
    acknowledged_[message].insert(senderId); // The sender ID has acknowledged this message by sending it to us

    auto [it, inserted] = pendingDelivery_.insert(message); // Only inserts if it wasn't already in there
    // Rebroadcast if it's the first time we're seeing it
    if (inserted) { // Means message wasn't in pendingDelivery_
        sendMessageToAllProcesses(message);
    }
    // This process has also now seen the message so they can acknowledge it from their perspective
    acknowledged_[message].insert(myProcessId_); // TODO - should this be here?
    printAcknowledged();

    if (canDeliver(message)) {
        deliverMessage(message);
    }
}

void FifoBroadcast::deliverMessage(Message message) {
    if (haveDelivered(message)) {
      return; // Don't want to deliver again
    }
    // Add it to our delivered:
    delivered_.insert(message);
    // Remove it from pending delivery
    pendingDelivery_.erase(message);
    // Debug
    DEBUGLOGRECEIVE("Just delivered a message so adding it to delivered. Delivered now: ");
    printDelivered();
    // Log the send:
    logDelivery(message);
   
}

bool FifoBroadcast::haveDelivered(Message message) {
    return (delivered_.find(message) != delivered_.end());
}

bool FifoBroadcast::canDeliver(Message message) {
    // If received acknowledgement from over half of processes we can deliver
    return acknowledged_[message].size() > numProcesses_ / 2;
}

void FifoBroadcast::logBroadcast(unsigned long messageId) { 
    if (!logFile_.is_open()) {
        std::cerr << "Failed to open log file: " << logPath_ << std::endl;
        return;
    }
    logFile_ << "b " << messageId << "\n";
    if (++writeCounter_ % linesInLogBatch_ == 0) logFile_.flush(); // every 1000 lines
} 

void FifoBroadcast::logDelivery(Message message) { 
    if (!logFile_.is_open()) {
        std::cerr << "Failed to open log file: " << logPath_ << std::endl;
        return;
    }
    logFile_ << "d " << message.origSenderId << " " << message.messageId << "\n";
    if (++writeCounter_ % linesInLogBatch_ == 0) logFile_.flush(); // every 1000 lines
}

void FifoBroadcast::stop() {
    running_ = false;
    if (logFile_.is_open()) {
        logFile_.flush();
        logFile_.close();
    }
}

void FifoBroadcast::printDelivered() const {
    #ifdef DEBUGRECEIVE
        DEBUGLOG("\n===== Delivered Messages =====\n");
        for (const Message& msg: delivered_) {
            DEBUGLOG(msg.origSenderId << ", " << msg.messageId);
        }
        DEBUGLOG("==============================");
    #endif
}

void FifoBroadcast::printAcknowledged() const {
    #ifdef DEBUGRECEIVE
        DEBUGLOG("\n===== Acknowledged Messages =====\n");
        for (const auto& [msg, processIds] : acknowledged_) {
            DEBUGLOG("For message " << msg.origSenderId << ", " << msg.messageId << ": ");
            for (const auto& procId : processIds) {
                DEBUGLOG("  Process " << procId);
            }
        }
        DEBUGLOG("==============================");
    #endif
}