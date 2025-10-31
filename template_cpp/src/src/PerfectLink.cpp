#include <iostream>
#include <unistd.h>
#include <fstream>
#include <sys/stat.h>  // for mkdir
#include <string>
#include <cerrno>
#include <cstring>
#include <sys/time.h> // for struct timeval

#include "PerfectLink.hpp"

PerfectLink::PerfectLink(unsigned long processId, in_addr_t processIp, unsigned short processPort, unsigned long receiverId, in_addr_t receiverIp, unsigned short receiverPort,std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort, std::string logPath)
    : processId_(processId), processPort_(processPort), processIp_(processIp), receiverId_(receiverId), receiverIp_(receiverIp), receiverPort_(receiverPort), hostMapByPort_(hostMapByPort), logPath_(logPath), running_(false)
{
    seqNumber_ = 0;
    if (processId_ == receiverId_) {
        initReceiver();
    } else {
        initBroadcaster();
    }
    
    deliverCallback_ = [this](unsigned long senderId, const std::string& message){
        std::cout << "Delivered \"" << message << "\" from: " << senderId << std::endl;
        logDelivery(senderId, message);
    };


    // Create or overwrite the file
    std::ofstream logFile(logPath_.c_str(), std::ios::out);
    if (!logFile.is_open()) {
        std::cerr << "Failed to create log file at: " << logPath_ << std::endl;
        return;
    }
    
    logFile.close();

    std::cout << "Created log file: " << logPath_ << std::endl;

    // Save log path for later use
    logPath_;


}

PerfectLink::~PerfectLink() {
    stop();
    close(sockfd_);
}

void PerfectLink::initBroadcaster() {
    sockfd_ = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd_ < 0) { perror("socket"); }

    // Allow address reuse
    int optval = 1;
    setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));

    // Set up local address to bind to
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(processPort_);
    addr.sin_addr.s_addr = processIp_;

    if (bind(sockfd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("bind (sender)");
        close(sockfd_);
    }

    // Set a receive timeout so recvfrom() won't block indefinitely.
    // Adjust the timeout as needed (here 100 ms).
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 100000; // 100 ms
    if (setsockopt(sockfd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        perror("setsockopt SO_RCVTIMEO");
    }

    running_ = true;
    receiverThread_ = std::thread(&PerfectLink::receiverLoop, this);

    std::cout << "Initialised " << processId_ << " as a broadcaster\n";
    std::cout << "Listening for ACKs on port " << processPort_ << "\n";
    std::cout << "Receiver IP is: " << receiverIp_ << "\n";
    std::cout << "Receiver Port is: " << receiverPort_ << "\n";
}

void PerfectLink::initReceiver() {
    sockfd_ = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd_ < 0) { perror("socket");}

    // Allow address reuse
    int optval = 1;
    setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));

    // Set up local address to bind to
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(receiverPort_);
    addr.sin_addr.s_addr = receiverIp_;

    // Bind the socket
    if (bind(sockfd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("bind");
        close(sockfd_);
    }

    // Set a receive timeout so recvfrom() won't block indefinitely.
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 100000; // 100 ms
    if (setsockopt(sockfd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        perror("setsockopt SO_RCVTIMEO");
    }

    // Initialise the map storing the last missing message ID for each broadcaster process:
    for (const auto& [port, pairVal] : hostMapByPort_) {
        unsigned long processId = pairVal.first;

        // Skip the receiver’s own process
        if (port == receiverPort_)
            continue;

        // Initialize first missing message to "1"
        firstMissingMessage_[processId] = "1";
    }

    running_ = true;
    receiverThread_ = std::thread(&PerfectLink::receiverLoop, this);

    std::cout << "Listening on port " << receiverPort_ << "...\n";
    std::cout << "Initialised " << processId_ << " as a receiver \n";
}

void PerfectLink::sendMessage(const std::string& message) {
    // Store in pending map
    seqNumber_ += 1;
    std::string id = std::to_string(seqNumber_);
    mapMutex.lock();
    //TODO - add check it isn't already in pending
    pending_[id] = Message({id, message}); // TODO - add handling for 0 time
    mapMutex.unlock();
    std::string payload = id + "|" + message;

    // Start resend thread if not already running
    if (!resendThread_.joinable()) {
        resendThread_ = std::thread(&PerfectLink::sendMessageLoop, this);
        std::cout << "Starting sending thread" << std::endl;
    }
    logSend(message);
}

void PerfectLink::sendMessageLoop() {
    while (running_) {
        // std::cout << "In send message loop" << std::endl;
        // std::this_thread::sleep_for(std::chrono::milliseconds(500));
        std::this_thread::sleep_for(std::chrono::milliseconds(5)); // baseline pacing

        auto now = Clock::now();
        bool hasFound = false; // To check if we have a message to send
        Message msgToSend; 
        std::unique_lock<std::mutex> lock(mapMutex);

        for (auto& [id, msg] : pending_) {
            if (now - msg.lastSentTime > std::chrono::milliseconds(200)) {
                std::cout << "Found message to send" << std::endl;
                msgToSend = msg;
                msg.lastSentTime = now;
                hasFound = true;
                break;
            }
        }
        lock.unlock();

        if (!hasFound) { // Messages were sent too recently - nothing to do
            std::cout << "Messages were sent too recently - waiting 10ms" << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue; 
        }

        std::string payload = msgToSend.id + "|" + msgToSend.message;
        std::cout << "Sending message: " << msgToSend.message << std::endl;
        sendRaw(payload, receiverIp_, receiverPort_);

        // If backlog grows, sleep longer to avoid flooding
        if (pending_.size() > 100) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            std::cout << "Num messages to send too big - waiting 100ms" << std::endl;
        }
    }
}

void PerfectLink::sendRaw(const std::string& payload, in_addr_t ip, unsigned short port){
    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(port);
    dest.sin_addr.s_addr = ip;

    sendto(sockfd_, payload.c_str(), payload.size(), 0,
           reinterpret_cast<sockaddr*>(&dest), sizeof(dest));
}

// void PerfectLink::addSendToLog(const std::string& message) { // TODO
    
// }

void PerfectLink::receiverLoop() {
    char buffer[1024];
    sockaddr_in senderAddr{};
    socklen_t senderLen = sizeof(senderAddr);

    while (running_) {
        std::cout << "listening..." << std::endl;

        ssize_t bytes = recvfrom(sockfd_, buffer, sizeof(buffer)-1, 0,
                    reinterpret_cast<sockaddr*>(&senderAddr), &senderLen);

        if (bytes < 0) {
            // handle non-fatal errors (timeout / interrupt) by continuing the loop
            if (errno == EAGAIN || errno == EINTR) {
                // no data this iteration or interrupted, continue to let other activity proceed
                continue;
            } else {
                // real socket error
                perror("recvfrom");
                continue;
            }
        }

        if (bytes == 0) {
            // no data, continue
            continue;
        }

        buffer[bytes] = '\0';
        std::string payload(buffer);

        // Extract message type
        if (payload.rfind("ACK:", 0) == 0) {
            std::string msgId = payload.substr(4);
            handleAck(msgId);
            continue;
        }

        // Parse as normal message
        size_t sep = payload.find('|');
        if (sep == std::string::npos) {
            perror("incorrect payload format");
            continue;
        }

        // Get message info
        std::string idStr = payload.substr(0, sep);
        std::string message = payload.substr(sep + 1);
        unsigned short senderPort = ntohs(senderAddr.sin_port);
        unsigned long senderId = hostMapByPort_[senderPort].first;
        std::string& firstMissingMessage = firstMissingMessage_[senderId];
        std::cout << "Just received (ProcessID, idStr): " << senderId << ", " << idStr << std::endl;

        std::cout<<"Delivered at start of receive process";
        printDelivered();
        std::cout << "First missing message at start is: " << firstMissingMessage << std::endl;
        // Check if already delivered:
        if (std::stoi(idStr) < std::stoi(firstMissingMessage)) { // Already delivered it but it has been cleaned from delivered_
            sendAck(senderAddr.sin_addr.s_addr, senderPort, idStr); // Send ack again in case they didn't receive it
            std::cout << "Already delivered " << idStr << " from " << senderId << " so skipping" << std::endl;
            continue;
        }
        
        // Find delivered list for this processId
        auto& deliveredSet = delivered_[senderId]; // TODO - do I need a mutex lock here?
    

        if (std::stoi(idStr) == std::stoi(firstMissingMessage)) { // The one we've been waiting for so deliver it
            std::cout << "Just received firstMissingMessage so cleaning delivered" << std::endl;
            deliveredSet.insert(idStr);
            if (deliverCallback_) deliverCallback_(senderId, message);
            sendAck(senderAddr.sin_addr.s_addr, senderPort, idStr);

            // TODO -> now replace the firstMissingMessage and clean deliveredSet
            std::string prev = "0";
            bool gapFound = false;
            std::string lastValue = *deliveredSet.rbegin();
            for (std::string msgId : deliveredSet) {
                if (prev == "0") { // At the first value so skip
                    prev = msgId;
                } else { // Not at the first value
                    if (std::stoi(prev) + 1 != std::stoi(msgId)) { // Found the gap
                        std::cout << "Found the gap so removing up to gap" << std::endl;
                        deliveredSet.erase(prev);
                        firstMissingMessage = std::to_string(std::stoi(prev) + 1);
                        gapFound = true;
                        break;
                    } else { // Haven't found the gap but can keep cleaning
                        deliveredSet.erase(prev);
                        prev = msgId;
                    }
                }
            }
            if (!gapFound) {
                // No gap found: all are in order
                std::cout << "Didn't find gap so removing whole list" << std::endl;
                firstMissingMessage = std::to_string(std::stoi(lastValue) + 1);
                deliveredSet.clear();
            }

        } else { // Either in our delivered set or never been delivered
            auto it = deliveredSet.find(idStr);

            if (it != deliveredSet.end()) { // Already in our list
                std::cout << "Message was in delivered list" << std::endl;
                sendAck(senderAddr.sin_addr.s_addr, senderPort, idStr); // Send ack again in case they didn't receive it
                continue;
            } else { // Not in our list so add and deliver it
                std::cout << "Message not in delivered list and wasn't one we were waiting for so we're delivering it and adding it to our list" << std::endl;
                deliveredSet.insert(idStr);
                if (deliverCallback_) deliverCallback_(senderId, message);
                sendAck(senderAddr.sin_addr.s_addr, senderPort, idStr);
            }
        }
        std::cout << "Delivered list at end off the receiver processing" << std::endl;
        printDelivered();
        std::cout << "firstMissing at end of the receiver processing: " << firstMissingMessage_[senderId] << std::endl;
    }
}

void PerfectLink::sendAck(in_addr_t destIp, unsigned short destPort, const std::string& msgId) {
    std::string ack = "ACK:" + msgId;
    sendRaw(ack, destIp, destPort);
}

void PerfectLink::handleAck(const std::string& msgId) {
    std::lock_guard<std::mutex> lock(mapMutex);
    auto it = pending_.find(msgId);
    if (it != pending_.end()) {
        pending_.erase(it);
    }
}

void PerfectLink::logDelivery(unsigned long senderId, const std::string& message) {
    std::ofstream logFile(logPath_.c_str(), std::ios::app);
    if (!logFile.is_open()) {
        std::cerr << "Failed to open log file: " << logPath_ << std::endl;
        return;
    }

    logFile << "d " << senderId << " " << message << "\n";
    logFile.close();
}

void PerfectLink::logSend(const std::string& message) {
    std::ofstream logFile(logPath_.c_str(), std::ios::app);
    if (!logFile.is_open()) {
        std::cerr << "Failed to open log file: " << logPath_ << std::endl;
        return;
    }

    logFile << "b " << message << "\n";
    logFile.close();
}


void PerfectLink::stop() {
    running_ = false;
    if (receiverThread_.joinable()) receiverThread_.join();
    if (resendThread_.joinable()) resendThread_.join();
    printDelivered();
    // logFile.close(); // TODO
}

void PerfectLink::printDelivered() const {
    std::cout << "\n===== Delivered Messages =====\n";
    for (const auto& [senderId, messages] : delivered_) {
        std::cout << "From process " << senderId << ":\n";
        for (const auto& msgId : messages) {
            std::cout << "  ID " << msgId << '\n';
        }
    }
    std::cout << "==============================\n";
}
