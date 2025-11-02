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
// #define DEBUGSEND
// #define DEBUGRECEIVE

#ifdef DEBUGSEND
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

PerfectLink::PerfectLink(unsigned long processId, in_addr_t processIp, unsigned short processPort, unsigned long receiverId, in_addr_t receiverIp, unsigned short receiverPort,std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort, std::string logPath)
    : processId_(processId), processPort_(processPort), processIp_(processIp), receiverId_(receiverId), receiverIp_(receiverIp), receiverPort_(receiverPort), hostMapByPort_(hostMapByPort), logPath_(logPath), running_(false)
{
    // Initialse messages and packets
    packetSeqNumber_ = 0;
    msgSeqNumber_ = 0;
    partialPacket_ = "";

    // Start listening on ports
    if (processId_ == receiverId_) {
        initReceiver();
    } else {
        initBroadcaster();
    }
    
    // Define a delivery callback - can change this later depending on what needs to happen on delivery
    deliverCallback_ = [this](unsigned long senderId, unsigned long messageId){
        DEBUGLOGRECEIVE("Delivered \"" << messageId << "\" from: " << senderId);
        logDelivery(senderId, messageId);
    };

    // Create or overwrite the log file
    std::ofstream logFile(logPath_.c_str(), std::ios::out);
    if (!logFile.is_open()) {
        std::cerr << "Failed to create log file at: " << logPath_ << std::endl;
        return;
    }
    logFile.close();
    DEBUGLOG("Created log file: " << logPath_);
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

    // Set a receive timeout so recvfrom() in receiverLoop won't block indefinitely.
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 100000; // 100 ms
    if (setsockopt(sockfd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        perror("setsockopt SO_RCVTIMEO");
    }

    running_ = true;
    receiverThread_ = std::thread(&PerfectLink::receiverLoop, this); // Start listening for ACKs

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

    // Set a receive timeout so recvfrom() in receiverLoop() won't block indefinitely.
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 100000; // 100 ms
    if (setsockopt(sockfd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        perror("setsockopt SO_RCVTIMEO");
    }

    // Cleaning logic - initialise the map storing the last missing packet ID for each broadcaster process:
    for (const auto& [port, hostDetails] : hostMapByPort_) {
        unsigned long processId = hostDetails.first;

        // Skip the receiver’s own process
        if (port == receiverPort_)
            continue;

        // Initialize first missing packet to 1 - waiting for first packet to arrive
        firstMissingPacketId_[processId] = 1;
    }

    running_ = true;
    receiverThread_ = std::thread(&PerfectLink::receiverLoop, this); // Start listening for messages

    DEBUGLOG("Listening on port " << receiverPort_ << "...");
    DEBUGLOG("Initialised " << processId_ << " as a receiver");
}

void PerfectLink::sendMessage(const std::string& message) {
    // Add messageId to message payload
    msgSeqNumber_++;
    std::string messagePayload = std::to_string(msgSeqNumber_) + ":" + message; // Final payload will be pktId|msgId:msg|mgId:msg ...
    addMessageToPacket(messagePayload);
    
    // Start resend thread if not already running
    if (!resendThread_.joinable()) {
        resendThread_ = std::thread(&PerfectLink::sendPacketLoop, this);
        DEBUGLOGSEND("Starting sending thread");
    }
}

void PerfectLink::addMessageToPacket(const std::string& messagePayload) { // Adding message to current packet being built
    DEBUGLOGSEND("Adding message payload: " << messagePayload << " to pending packet");
    std::string packetToMove;

    // Hold partialPacketMutex 
   {
        std::lock_guard<std::mutex> lock(partialPacketMutex_);

        // Check if adding this message would make packet too big
        if (partialPacket_.size() + messagePayload.size() > maxPacketSize_ && partialPacket_.size() > 0) {
            DEBUGLOGSEND("Partial packet will be too big if we add message so move current packet out");
            packetToMove = partialPacket_;          // copy out current partial packet
            partialPacket_ = messagePayload;       // start new partial packet with this message
        } else {
            DEBUGLOGSEND("Partial packet not too big so adding message to it");
            if (partialPacket_.empty()) {
                partialPacket_ = messagePayload;
            } else {
                partialPacket_ += "|" + messagePayload;
            }
        }
        lastPacketUpdateTime_ = Clock::now();
    } // partialPacketMutex_ released here

    // If we copied a packet out, add it to pending now without holding partialPacketMutex_
    if (!packetToMove.empty()) {
        addPacketToPending(packetToMove);
    }

}

void PerfectLink::flushMessages() {
    DEBUGLOGSEND("Flushing messages");
    std::string packetToMove;
    { // Hold partialPacketMutex_ lock
        std::lock_guard<std::mutex> lock(partialPacketMutex_);
        DEBUGLOGSEND("Was able to lock partialPacketMutex");
        if (!partialPacket_.empty()) {
            packetToMove = partialPacket_;
            partialPacket_.clear();
        } else {
            DEBUGLOGSEND("Partial packet was empty so didn't do anything");
        }
    } // partialPacketMutex_ released here

    if (!packetToMove.empty()) {
        addPacketToPending(packetToMove);
    }
}

void PerfectLink::addPacketToPending(const std::string &packetStr) {
    if (packetStr.empty()) return;

    // lock pending map and assign packet id under that lock
    std::lock_guard<std::mutex> lockPending(pendingMapMutex_);
    packetSeqNumber_ += 1;
    Packet packet = Packet({packetSeqNumber_, packetStr});
    logSendPacket(packetStr);
    pending_[packet.id] = packet;
    DEBUGLOGSEND("Added packet id=" << packet.id << " to pending. pending_ size=" << pending_.size());
}

void PerfectLink::flushPendingPacketIfReady() {
    bool shouldFlush = false;
    { // Holding partialPacketMutex_
        std::lock_guard<std::mutex> lock(partialPacketMutex_);
        auto now = Clock::now();
        DEBUGLOGSEND("Checking if pending packet is ready. Partial packet size: " << partialPacket_.size());
        if (now - lastPacketUpdateTime_ > maxPacketUpdateTimePast_ || partialPacket_.size() > maxPacketSize_) {
            DEBUGLOGSEND("Packet is ready so will flush messages");
            shouldFlush = !partialPacket_.empty();
        }
    }
    if (shouldFlush) flushMessages(); // flushMessages does copy-and-call safely
}

void PerfectLink::sendPacketLoop() {
    while (running_) {
        flushPendingPacketIfReady();
        Packet packetToSend;
        if (!findPacketToSend(packetToSend)) { // Updating packetToSend with the packet that is ready to be sent // TODO - reuse this logic when parsing ID
            DEBUGLOGSEND("Packets were sent too recently or pending_ was empty - waiting 10ms");
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }

        std::string payload = std::to_string(packetToSend.id) + "|" + packetToSend.messages;
        DEBUGLOGSEND("Sending packet id:" << packetToSend.id << " messages: " << packetToSend.messages);
        sendRaw(payload, receiverIp_, receiverPort_);
    }
}

bool PerfectLink::findPacketToSend(Packet& outPacket) { // Finds a packet in pending_ that hasn't been sent too recently
    auto now = Clock::now();
    const std::chrono::milliseconds minDelay(200);

    std::lock_guard<std::mutex> lock(pendingMapMutex_);

    for (auto& entry : pending_) {
        Packet& pkt = entry.second;
        if (now - pkt.lastSentTime > minDelay) {
            pkt.lastSentTime = now;   // update while holding lock
            outPacket = pkt;          // make a safe copy
            return true;
        }
    }
    return false; // nothing ready to be sent again
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
        // DEBUGLOG("listening...");

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
            unsigned long pktId = parsePacketPayloadId(pktIdStr);
            if (pktId == 0) { // TODO - add more meaningful error
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
        unsigned long id = parsePacketPayloadId(idStr);
        if (id == 0) { // TODO - Add more meaningful error
            continue;
        }
        std::string messages = payload.substr(sep + 1);
        unsigned short senderPort = ntohs(senderAddr.sin_port);
        unsigned long senderId = hostMapByPort_[senderPort].first;
        unsigned long firstMissingPacketId = firstMissingPacketId_[senderId];
        DEBUGLOGRECEIVE("Just received (ProcessID, idStr): " << senderId << ", " << id);

        DEBUGLOGRECEIVE("Delivered at start of receive process");
        printDelivered();
        DEBUGLOGRECEIVE("First missing packet at start is: " << firstMissingPacketId);
        // Check if already delivered:
        if (id < firstMissingPacketId) { // Already delivered it but it has been cleaned from delivered_
            sendAck(senderAddr.sin_addr.s_addr, senderPort, id); // Send ack again in case they didn't receive it
            DEBUGLOGRECEIVE("Already delivered " << id << " from " << senderId << " so skipping");
            continue;
        }
        
        // Find delivered list for this processId
        auto& deliveredSet = delivered_[senderId]; // TODO - do I need a mutex lock here?
    

        if (id == firstMissingPacketId) { // The one we've been waiting for so deliver it
            DEBUGLOGRECEIVE("Just received firstMissingMessageId so attempting to deliver and clean delivered");
            if (!deliverMessages(senderId, messages)) { // Couldn't deliver one or more of the messages, so don't acknowledge
                DEBUGLOGRECEIVE("Failed to deliver one or more of the messages so skipping this packet");
                std::cerr << "Failed to deliver one or more of the messages so skipping packet with id: " << id << std::endl;
                continue;
            } 
            deliveredSet.insert(id);
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
                        DEBUGLOGRECEIVE("Found the gap so removing up to gap");
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
                DEBUGLOGRECEIVE("Didn't find gap so removing whole list");
                firstMissingPacketId_[senderId] = lastValue + 1;
                deliveredSet.clear();
            }

        } else { // Either in our delivered set or never been delivered
            auto it = deliveredSet.find(id);

            if (it != deliveredSet.end()) { // Already in our list
                DEBUGLOGRECEIVE("Message was in delivered list");
                sendAck(senderAddr.sin_addr.s_addr, senderPort, id); // Send ack again in case they didn't receive it
                continue;
            } else { // Not in our list so add and deliver it
                DEBUGLOGRECEIVE("Message not in delivered list and wasn't one we were waiting for so we're delivering it and adding it to our list");
                if (!deliverMessages(senderId, messages)) { // Couldn't deliver one or more of the messages, so don't acknowledge
                    DEBUGLOGRECEIVE("Failed to deliver one or more of the messages so skipping this packet");
                    std::cerr << "Failed to deliver one or more of the messages so skipping packet with id: " << id << std::endl;
                    continue;
                } 
                deliveredSet.insert(id);
                sendAck(senderAddr.sin_addr.s_addr, senderPort, id);
            }
        }
        DEBUGLOGRECEIVE("Delivered list at end off the receiver processing");
        printDelivered();
        DEBUGLOGRECEIVE("firstMissing at end of the receiver processing: " << firstMissingPacketId_[senderId]);
    }
}

unsigned long PerfectLink::parsePacketPayloadId(const std::string& packetIdStr) {
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

unsigned long PerfectLink::parseMessagePayloadId(const std::string& messageIdStr) {
    try {
        unsigned long msgId = std::stoul(messageIdStr);
        return msgId;
    } catch (std::invalid_argument&){
        std::cerr << "Id in message payload was not a number" << std::endl;
        return 0;
    } catch (std::out_of_range&) {
        std::cerr << "Id in message payload was out of range" << std::endl;
        return 0;
    }
    return 0;
}

bool PerfectLink::deliverMessages(unsigned long senderId, const std::string& messages) { // returns true if it was able to deliver and false otherwise
    size_t start = 0;
    size_t end;

    DEBUGLOGRECEIVE("Delivering packet:\n" << messages);

    while ((end = messages.find('|', start)) != std::string::npos) {
        if (!deliverMessage(senderId, messages.substr(start, end - start))) {
            return false; // Something failed delivering this message so fail the whole packet
        }
        start = end + 1;
    }
    // Last token after the last delimiter
    if (!deliverMessage(senderId, messages.substr(start))) {
        return false; // Something failed delivering this message so fail the whole packet
    }
    return true; // All messages successfully delivered
}

bool PerfectLink::deliverMessage(unsigned long senderId, const std::string& messagePayload) {
    size_t sep = messagePayload.find(':');
    if (sep == std::string::npos) {
        std::cerr << "Incorrect payload format of message" << std::endl;
        return false;
    }
    std::string msgIdStr = messagePayload.substr(0, sep);
    unsigned long msgId = parseMessagePayloadId(msgIdStr);
    if (msgId == 0) {
        return false; // MsgId could not be converted into a unsigned long so message could not be delivered
    }
    if (deliverCallback_) deliverCallback_(senderId, msgId);
    return true;
}

void PerfectLink::sendAck(in_addr_t destIp, unsigned short destPort, unsigned long msgId) {
    std::string ack = "ACK:" + std::to_string(msgId);
    sendRaw(ack, destIp, destPort);
}

void PerfectLink::handleAck(const unsigned long pktId) {
    std::lock_guard<std::mutex> lock(pendingMapMutex_); // Destroys and lock and releases mutex when out of scope
    auto it = pending_.find(pktId);
    if (it != pending_.end()) {
        pending_.erase(it);
    }
}

void PerfectLink::logDelivery(unsigned long senderId, unsigned long messageId) {
    std::ofstream logFile(logPath_.c_str(), std::ios::app);
    if (!logFile.is_open()) {
        std::cerr << "Failed to open log file: " << logPath_ << std::endl;
        return;
    }

    logFile << "d " << senderId << " " << messageId << "\n";
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
        std::string messagePayload = packet.substr(start, end - start);
        size_t sep = messagePayload.find(':');
        if (sep == std::string::npos) {
            std::cerr << "Incorrect payload format of message, cannot send packet" << std::endl;
            return;
        }
        std::string msgIdStr = messagePayload.substr(0, sep);
        logFile << "b " << msgIdStr << "\n";
        start = end + 1;
    }
    // Last token after the last delimiter
    std::string messagePayload = packet.substr(start);
    size_t sep = messagePayload.find(':');
    if (sep == std::string::npos) {
        std::cerr << "Incorrect payload format of message, cannot send packet" << std::endl;
        return;
    }
    std::string msgIdStr = messagePayload.substr(0, sep);
    logFile << "b " << msgIdStr << "\n";
    logFile.close();
}


void PerfectLink::stop() {
    running_ = false;
    if (receiverThread_.joinable()) receiverThread_.join();
    if (resendThread_.joinable()) resendThread_.join();
    printDelivered();
}

void PerfectLink::printDelivered() const {
    #ifdef DEBUGRECEIVE
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
