#include <iostream>
#include <unistd.h>
#include <fstream>
#include <sys/stat.h>  // for mkdir
#include <string>
#include <cerrno>
#include <cstring>
#include <sys/time.h> // for struct timeval

#include "PerfectLink.hpp"

// TODO - ************ TURN THIS OFF BEFORE SUBMISSION ****************
// #define DEBUG

#ifdef DEBUG
    #define DEBUGLOG(msg) (std::cout << msg << std::endl)
#else
    #define DEBUGLOG(msg) do {} while(0) // no-op in release
#endif

PerfectLink::PerfectLink(unsigned long processId, in_addr_t processIp, unsigned short processPort, unsigned long receiverId, in_addr_t receiverIp, unsigned short receiverPort,std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort, std::string logPath)
    : processId_(processId), processPort_(processPort), processIp_(processIp), receiverId_(receiverId), receiverIp_(receiverIp), receiverPort_(receiverPort), hostMapByPort_(hostMapByPort), logPath_(logPath), running_(false)
{
    seqNumber_ = 0;
    partialPacket_ = "";

    if (processId_ == receiverId_) {
        initReceiver();
    } else {
        initBroadcaster();
    }
    
    deliverCallback_ = [this](unsigned long senderId, const std::string& message){
        DEBUGLOG("Delivered \"" << message << "\" from: " << senderId);
        logDelivery(senderId, message);
    };


    // Create or overwrite the file
    std::ofstream logFile(logPath_.c_str(), std::ios::out);
    if (!logFile.is_open()) {
        std::cerr << "Failed to create log file at: " << logPath_ << std::endl;
        return;
    }
    
    logFile.close();

    DEBUGLOG("Created log file: " << logPath_);

}

PerfectLink::~PerfectLink() {
    flushMessages();
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

    DEBUGLOG("Initialised " << processId_ << " as a broadcaster");
    DEBUGLOG("Listening for ACKs on port " << processPort_);
    DEBUGLOG("Receiver IP is: " << receiverIp_);
    DEBUGLOG("Receiver Port is: " << receiverPort_);
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

        // Initialize first missing message to 1
        firstMissingPacketId_[processId] = 1;
    }

    running_ = true;
    receiverThread_ = std::thread(&PerfectLink::receiverLoop, this);

    DEBUGLOG("Listening on port " << receiverPort_ << "...");
    DEBUGLOG("Initialised " << processId_ << " as a receiver");
}

void PerfectLink::sendMessage(const std::string& message) {
    // Adding message to current packet being built
    if (partialPacket_.size() + message.size() > maxPacketSize_ && partialPacket_.size() > 0) { // TODO - check enough time has past
        // Store in pending map before adding this message because it will be too big
        addFinishedPacketToPending();
        partialPacket_ = message;
    } else {
        if (partialPacket_.size() == 0) {
            partialPacket_ = message;
        } else {
            partialPacket_ += "|" + message;
        }
    }

    // Checking if partial packet is big enough to be sent
    if (partialPacket_.size() > maxPacketSize_) {
        addFinishedPacketToPending();
        partialPacket_ = "";
    }
    
    // Start resend thread if not already running
    if (!resendThread_.joinable()) {
        resendThread_ = std::thread(&PerfectLink::sendPacketLoop, this);
        DEBUGLOG("Starting sending thread");
    }
}

void PerfectLink::flushMessages() {
    if (partialPacket_.size() > 0) {
        addFinishedPacketToPending();
        partialPacket_ = "";
    }
}

void PerfectLink::addFinishedPacketToPending() {
    seqNumber_ += 1;
    Packet packet = Packet({seqNumber_, partialPacket_});
    logSendPacket(partialPacket_);
    std::lock_guard<std::mutex> lock(pendingMapMutex);   
    pending_[packet.id] = packet;
}

void PerfectLink::sendPacketLoop() {
    while (running_) {
        Packet packetToSend;
        if (!findPacketToSend(packetToSend)) {
            DEBUGLOG("Packets were sent too recently - waiting 10ms");
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }

        std::string payload = std::to_string(packetToSend.id) + "|" + packetToSend.messages;
        DEBUGLOG("Sending packet id=" << packetToSend.id << " payload: " << packetToSend.messages);
        sendRaw(payload, receiverIp_, receiverPort_);
    }
}

bool PerfectLink::findPacketToSend(Packet& outPacket) {
    auto now = Clock::now();
    const std::chrono::milliseconds minDelay(200);

    std::lock_guard<std::mutex> lock(pendingMapMutex);

    for (auto& entry : pending_) {
        Packet& pkt = entry.second;
        if (now - pkt.lastSentTime > minDelay) {
            pkt.lastSentTime = now;   // update while holding lock
            outPacket = pkt;          // make a safe copy
            return true;              // found one
        }
    }
    return false; // nothing ready
}

void PerfectLink::sendRaw(const std::string& payload, in_addr_t ip, unsigned short port){
    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(port);
    dest.sin_addr.s_addr = ip;

    sendto(sockfd_, payload.c_str(), payload.size(), 0,
           reinterpret_cast<sockaddr*>(&dest), sizeof(dest));
}

void PerfectLink::receiverLoop() {
    char buffer[1024];
    sockaddr_in senderAddr{};
    socklen_t senderLen = sizeof(senderAddr);

    while (running_) {
        DEBUGLOG("listening...");

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
            std::string pktIdStr = payload.substr(4);
            unsigned long pktId = parsePayloadId(pktIdStr);
            if (pktId == 0) {
                continue;
            }
            handleAck(pktId);
            continue;
        }

        // Parse as normal message
        size_t sep = payload.find('|');
        if (sep == std::string::npos) {
            std::cerr << "Incorrect payload format" << std::endl;
            continue;
        }

        // Get message info
        std::string idStr = payload.substr(0, sep);
        unsigned long id = parsePayloadId(idStr);
        if (id == 0) {
            continue;
        }
        std::string messages = payload.substr(sep + 1);
        unsigned short senderPort = ntohs(senderAddr.sin_port);
        unsigned long senderId = hostMapByPort_[senderPort].first;
        unsigned long firstMissingPacketId = firstMissingPacketId_[senderId];
        DEBUGLOG("Just received (ProcessID, idStr): " << senderId << ", " << id);

        DEBUGLOG("Delivered at start of receive process");
        printDelivered();
        DEBUGLOG("First missing packet at start is: " << firstMissingPacketId);
        // Check if already delivered:
        if (id < firstMissingPacketId) { // Already delivered it but it has been cleaned from delivered_
            sendAck(senderAddr.sin_addr.s_addr, senderPort, id); // Send ack again in case they didn't receive it
            DEBUGLOG("Already delivered " << id << " from " << senderId << " so skipping");
            continue;
        }
        
        // Find delivered list for this processId
        auto& deliveredSet = delivered_[senderId]; // TODO - do I need a mutex lock here?
    

        if (id == firstMissingPacketId) { // The one we've been waiting for so deliver it
            DEBUGLOG("Just received firstMissingMessageId so cleaning delivered");
            deliveredSet.insert(id);
            deliverMessages(senderId, messages); // TODO - do they want us to log the ID or the message?
            sendAck(senderAddr.sin_addr.s_addr, senderPort, id);

            // Now replace the firstMissingMessageId and clean deliveredSet
            unsigned long prev = 0;
            bool gapFound = false;
            unsigned long lastValue = *deliveredSet.rbegin();
            for (unsigned long msgId : deliveredSet) {
                if (prev == 0) { // At the first value so skip
                    prev = msgId;
                } else { // Not at the first value
                    if (prev + 1 != msgId) { // Found the gap
                        DEBUGLOG("Found the gap so removing up to gap");
                        deliveredSet.erase(prev);
                        firstMissingPacketId_[senderId] = prev + 1;
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
                DEBUGLOG("Didn't find gap so removing whole list");
                firstMissingPacketId_[senderId] = lastValue + 1;
                deliveredSet.clear();
            }

        } else { // Either in our delivered set or never been delivered
            auto it = deliveredSet.find(id);

            if (it != deliveredSet.end()) { // Already in our list
                DEBUGLOG("Message was in delivered list");
                sendAck(senderAddr.sin_addr.s_addr, senderPort, id); // Send ack again in case they didn't receive it
                continue;
            } else { // Not in our list so add and deliver it
                DEBUGLOG("Message not in delivered list and wasn't one we were waiting for so we're delivering it and adding it to our list");
                deliveredSet.insert(id);
                deliverMessages(senderId, messages);
                sendAck(senderAddr.sin_addr.s_addr, senderPort, id);
            }
        }
        DEBUGLOG("Delivered list at end off the receiver processing");
        printDelivered();
        DEBUGLOG("firstMissing at end of the receiver processing: " << firstMissingPacketId_[senderId]);
    }
}

unsigned long PerfectLink::parsePayloadId(const std::string& packetIdStr) {
    try {
        unsigned long pktId = std::stoul(packetIdStr);
        return pktId;
    } catch (std::invalid_argument&){
        std::cerr << "Id in packet payload was not a number" << std::endl;
        return 0;
    } catch (std::out_of_range&) {
        std::cerr << "Id in packet payload was out of range" << std::endl;
        return 0;
    }
    return 0;
}

void PerfectLink::deliverMessages(unsigned long senderId, const std::string& messages) {
    size_t start = 0;
    size_t end;

    DEBUGLOG("Delivering packet:\n" << messages);

    while ((end = messages.find('|', start)) != std::string::npos) {
        if (deliverCallback_) deliverCallback_(senderId, messages.substr(start, end - start)); // TODO - do they want us to log the ID or the message?
        start = end + 1;
    }
    // Last token after the last delimiter
    if (deliverCallback_) deliverCallback_(senderId, messages.substr(start)); // TODO - do they want us to log the ID or the message?
}

void PerfectLink::sendAck(in_addr_t destIp, unsigned short destPort, unsigned long msgId) {
    std::string ack = "ACK:" + std::to_string(msgId);
    sendRaw(ack, destIp, destPort);
}

void PerfectLink::handleAck(const unsigned long pktId) {
    std::lock_guard<std::mutex> lock(pendingMapMutex); // Destroys and lock and releases mutex when out of scope
    auto it = pending_.find(pktId);
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

void PerfectLink::logSendPacket(const std::string& packet) {
    std::ofstream logFile(logPath_.c_str(), std::ios::app);
    if (!logFile.is_open()) {
        std::cerr << "Failed to open log file: " << logPath_ << std::endl;
        return;
    }

    size_t start = 0;
    size_t end;
    while ((end = packet.find('|', start)) != std::string::npos) {
        logFile << "b " << packet.substr(start, end - start) << "\n";
        start = end + 1;
    }

    // Last token after the last delimiter
    logFile << "b " << packet.substr(start) << "\n";
    logFile.close();
}


void PerfectLink::stop() {
    running_ = false;
    if (receiverThread_.joinable()) receiverThread_.join();
    if (resendThread_.joinable()) resendThread_.join();
    printDelivered();
}

void PerfectLink::printDelivered() const {
    #ifdef DEBUG
        DEBUGLOG("\n===== Delivered Messages =====\n");
        for (const auto& [senderId, messages] : delivered_) {
            DEBUGLOG("From process " << senderId << ":");
            for (const auto& msgId : messages) {
                DEBUGLOG("  ID " << msgId);
            }
        }
        DEBUGLOG("==============================");
    #endif
}
