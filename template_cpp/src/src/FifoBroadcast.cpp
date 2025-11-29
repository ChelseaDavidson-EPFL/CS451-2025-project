#include <iostream>
#include <sstream>
#include <vector>

#include "FifoBroadcast.hpp"

// TODO - ************ TURN THIS OFF BEFORE SUBMISSION ****************
// #define DEBUG
// #define DEBUGBROADCAST
// #define DEBUGRECEIVE
// #define DEBUGACK

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

#ifdef DEBUGACK
    #define DEBUGLOGACK(msg) (std::cout << msg << std::endl)
#else
    #define DEBUGLOGACK(msg) do {} while(0) // no-op in release
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
    msgSeqNumber_ = 0; // Used for broadcasting
    for (auto &[pid, _] : hostMapById_) {
        nextExpectedMsgId_[pid] = 1; // Expecting 1st message to arrive
    }

    in_addr_t myIp = hostMapById_[myProcessId_].first;
    unsigned short myPort = hostMapById_[myProcessId_].second; 
    perfectLinkInstance_ = std::make_unique<PerfectLink>(myProcessId_, myIp, myPort, hostMapByPort_, hostMapById_);

    // Define perfect link's delivery callback
    perfectLinkInstance_->setDeliverCallback(
        [this](Message msg, unsigned long senderId) {
            this->receivedMessage(msg, senderId);
        }
    );

    perfectLinkInstance_->setAckCallback(
        [this](Message msg, unsigned long senderId) {
            this->receivedAck(msg, senderId);
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

    {   // lock shared state
        std::lock_guard<std::mutex> lock(stateMutex_);
        pendingDelivery_.insert(pendingMessage);
        acknowledged_[pendingMessage].insert(myProcessId_);
    }

    logBroadcast(msgId);

    // send to other processes (no lock, avoids deadlock)
    sendMessageToAllProcesses(pendingMessage);
}


void FifoBroadcast::sendMessageToAllProcesses(const Message& message) {
    for (const auto& entry : hostMapById_) {
        unsigned long processId = entry.first;

        if (processId == myProcessId_) continue; // Skip itself - TODO - idk if this is correct

        perfectLinkInstance_ -> sendMessage(message, processId);
    }
}


void FifoBroadcast::receivedMessage(const Message& message, const unsigned long& senderId) {
    DEBUGLOGRECEIVE("Received message: " << message.origSenderId << ", " << message.messageId);

    bool newlyInserted = false;
    bool canDeliverNow = false;

    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        if (haveDelivered(message)) { // Don't want to waste time on something we've already delivered
            DEBUGLOGRECEIVE("Already delivered message");
            return; 
        }
        acknowledged_[message].insert(senderId);

        auto [it, inserted] = pendingDelivery_.insert(message);
        DEBUGLOGRECEIVE("Added received message to pending. Pending is now:");
        printPending();
        newlyInserted = inserted;

        // This process has also now seen the message so they can acknowledge it from their perspective
        acknowledged_[message].insert(myProcessId_); // TODO - should this be here?
        printAcknowledged();

        if (canDeliver(message)) canDeliverNow = true;
    }

    // Rebroadcast if it's the first time we're seeing it
    if (newlyInserted) {
        sendMessageToAllProcesses(message);
    }

    if (canDeliverNow) {
        deliverMessage(message);      // deliverMessage locks internally
        // also try to deliver other now-unblocked messages
        tryDeliverPending();          // tryDeliverPending locks internally
    }
}

void FifoBroadcast::receivedAck(const Message& message, const unsigned long& senderId) { // This means we sent a message to a process and got an ack back
    DEBUGLOGACK("Received ack for message: " << message.origSenderId << ", " << message.messageId << "from " << senderId);

    bool canDeliverNow = false;

    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        if (haveDelivered(message)) { // Don't want to waste time on something we've already delivered
            DEBUGLOGACK("Already delivered message");
            return; 
        } // Havent' delivered this one yet
        acknowledged_[message].insert(senderId);

        // Would have already been in our pending and we would have already acknowledged it since we were the ones that sent it to them
        if (canDeliver(message)) canDeliverNow = true;
    }

    if (canDeliverNow) {
        DEBUGLOGACK("Due to ack, can now deliver message");
        deliverMessage(message);      // deliverMessage locks internally
        // also try to deliver other now-unblocked messages
        tryDeliverPending();          // tryDeliverPending locks internally
    }
}


void FifoBroadcast::deliverMessage(const Message& message) {
    DEBUGLOGRECEIVE("Delivering  (" << message.origSenderId << ", " << message.messageId << "):");
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        // Add it to our delivered by updated next expected id for that sender
        nextExpectedMsgId_[message.origSenderId]++;
        DEBUGLOGRECEIVE("Next expected message ID is now: " << nextExpectedMsgId_[message.origSenderId]);

        // Debug
        DEBUGLOGRECEIVE("Just delivered a message so adding it to delivered. Delivered now: ");
        printDelivered();

        // Remove it from pending delivery
        pendingDelivery_.erase(message);
        acknowledged_.erase(message);
        DEBUGLOGRECEIVE("Just delivered a message so removing it from pending. Pending now: ");
        printPending();
        DEBUGLOGRECEIVE("Just delivered a message so removing it from acknowledged. Acknowledged now: ");
        printAcknowledged();

    }

    // Log the delivery:
    logDelivery(message);
}

bool FifoBroadcast::haveDelivered(Message message) {
    return message.messageId < nextExpectedMsgId_[message.origSenderId];
}

bool FifoBroadcast::canDeliver(const Message &m) {
    if (haveDelivered(m)) {
      return false; // Can't deliver something we've already delivered
    }
    unsigned long p = m.origSenderId;
    unsigned long id = m.messageId;

    DEBUGLOGRECEIVE("Checking if we can deliver  (" << p  << ", " << id << "):");

    // (1) URB condition - If received acknowledgement from over half of processes we can deliver
    if (acknowledged_[m].size() <= numProcesses_ / 2) {
        DEBUGLOGRECEIVE("Don't have enough acknowledgements. Have only: " << acknowledged_[m].size());
        return false;
    }

    DEBUGLOGRECEIVE("Have enough acknowledgements " << "Next expected message ID is: " << nextExpectedMsgId_[p]);
    // (2) FIFO condition: messages must be delivered in order
    return id == nextExpectedMsgId_[p];

}

void FifoBroadcast::tryDeliverPending() {
    DEBUGLOGRECEIVE("Checking if we can deliver any other messages from pending now");
    DEBUGLOGRECEIVE("Pending is currently:");
    printPending();
    bool progress = true;

    while (progress) {
        progress = false;
        Message toDeliver;

        {
            std::lock_guard<std::mutex> lock(stateMutex_);

            for (const auto& m : pendingDelivery_) {
                if (canDeliver(m)) {
                    toDeliver = m;
                    progress = true;
                    break;
                }
            }
        } // Unlock

        if (progress) {
            deliverMessage(toDeliver);
        }
    }
}

void FifoBroadcast::logBroadcast(unsigned long messageId) { 
    if (!logFile_.is_open()) {
        std::cerr << "Failed to open log file: " << logPath_ << std::endl;
        return;
    }
    {
        std::lock_guard<std::mutex> lock(loggingMutex_);
        logFile_ << "b " << messageId << "\n";
        if (++writeCounter_ % linesInLogBatch_ == 0) logFile_.flush(); // every 1000 lines
    }
} 

void FifoBroadcast::logDelivery(Message message) { 
    if (!logFile_.is_open()) {
        std::cerr << "Failed to open log file: " << logPath_ << std::endl;
        return;
    }
    {
        std::lock_guard<std::mutex> lock(loggingMutex_);
        logFile_ << "d " << message.origSenderId << " " << message.messageId << "\n";
        if (++writeCounter_ % linesInLogBatch_ == 0) logFile_.flush(); // every 1000 lines
    }
}

void FifoBroadcast::stop() {
    running_ = false;
    if (logFile_.is_open()) {
        {
            std::lock_guard<std::mutex> lock(loggingMutex_);
            logFile_.flush();
            logFile_.close();
        }
    }
}

void FifoBroadcast::printDelivered() {
    #ifdef DEBUGRECEIVE
        DEBUGLOGRECEIVE("\n===== Delivered Messages =====\n");
        for (const auto& [procId, nextExpectedId]: nextExpectedMsgId_) {
            for (unsigned long msgId = 1; msgId < nextExpectedId; ++msgId) {
                DEBUGLOGRECEIVE(procId << ", " << msgId);
            }
        }
        DEBUGLOGRECEIVE("==============================");
    #endif
}

void FifoBroadcast::printPending() {
    #ifdef DEBUGRECEIVE
        DEBUGLOGRECEIVE("\n===== Pending Messages =====\n");
        for (const Message& msg: pendingDelivery_) {
            DEBUGLOGRECEIVE(msg.origSenderId << ", " << msg.messageId);
        }
        DEBUGLOGRECEIVE("==============================");
    #endif
}

void FifoBroadcast::printAcknowledged() {
    #ifdef DEBUGRECEIVE
        DEBUGLOGRECEIVE("\n===== Acknowledged Messages =====\n");
        for (const auto& [msg, processIds] : acknowledged_) {
            DEBUGLOGRECEIVE("For message " << msg.origSenderId << ", " << msg.messageId << ": ");
            for (const auto& procId : processIds) {
                DEBUGLOGRECEIVE("  Process " << procId);
            }
        }
        DEBUGLOGRECEIVE("==============================");
    #endif
}