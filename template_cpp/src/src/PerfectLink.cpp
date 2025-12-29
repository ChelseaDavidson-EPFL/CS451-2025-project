#include <iostream>
#include <unistd.h>
#include <fstream>
#include <sys/stat.h>  // for mkdir
#include <cassert>
#include <string>
#include <cerrno>
#include <cstring>
#include <sys/time.h> // for struct timeval

#include "PerfectLink.hpp"

// TODO - ************ TURN THIS OFF BEFORE SUBMISSION ****************
// #define DEBUG
// #define DEBUGSEND
// #define DEBUGRECEIVE

// Debug logging
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

PerfectLink::PerfectLink(unsigned long myProcessId, in_addr_t myProcessIp, unsigned short myProcessPort, std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort, std::unordered_map<unsigned long, std::pair<in_addr_t, unsigned short>> hostMapById, std::string logPath)
    : myProcessId_(myProcessId), myProcessPort_(myProcessPort), myProcessIp_(myProcessIp), hostMapByPort_(hostMapByPort), hostMapById_(hostMapById), logPath_(logPath), running_(false)
{
    // Create or overwrite the log file
    if (logPath_ == "") {
        std::cout << "Not logging to file" << std::endl;
        loggingToFile_ = false;
    }
    if (loggingToFile_) {
        logFile_.open(logPath_.c_str(), std::ios::out);
        if (!logFile_.is_open()) {
            std::cerr << "Failed to create log file at: " << logPath_ << std::endl;
            loggingToFile_ = false;
        }
        DEBUGLOG("Created log file: " << logPath_);
    }

    // Initialse messages and packets
    for (const auto& [processId, _] : hostMapById_) {
        // Skip it's own process  // TODO - is this correct?
        if (processId == myProcessId_) {
            continue;
        }

        // Sender logic - this process behaving as a sender
        packetSeqNumber_[processId] = 0;
        numMessagesInPacket_[processId] = 0;
        partialPacket_[processId] = {};
        lastPacketUpdateTime_[processId] = Clock::now();

        // Receiver logic - this process behaving as a receiver
        firstMissingPacketId_[processId] = 1;     // Cleaning logic - Initialize first missing packet for each sender process to 1 - waiting for first packet to arrive
    }
   

    // Start listening on ports
    initReceiverBroadcaster();
}

PerfectLink::~PerfectLink() {
    stop();
}

void PerfectLink::setDeliverCallback(std::function<void(const Message&, unsigned long)> cb) {
    deliverCallback_ = cb;
}

void PerfectLink::setDeliverCallbackToDefault() {
    // Define delivery callback - change this for later assignments
    deliverCallback_ = [this](Message message, unsigned long senderId){
        DEBUGLOG("Delivered \"" << message.messageId << "\" from: " << senderId);
        logDelivery(message.origSenderId, message.messageId);
    };

}


void PerfectLink::initReceiverBroadcaster() {
    sockfd_ = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd_ < 0) { perror("socket");}

    // Allow address reuse - prevent "Address already in use" error when run tests back to back
    int optval = 1;
    setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));

    // Set up local address to bind to
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(myProcessPort_);
    addr.sin_addr.s_addr = myProcessIp_;

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

    running_ = true;
    receiverThread_ = std::thread(&PerfectLink::receiverLoop, this); // Start listening for messages

    DEBUGLOG("Listening on port " << myProcessPort_ << "...");
    DEBUGLOG("Initialised " << myProcessId_ << " as a receiver/ broadcaster");
}

std::vector<unsigned char> PerfectLink::toBytes(unsigned long value) {
    std::vector<unsigned char> bytes(sizeof(unsigned long));

    for (size_t i = 0; i < sizeof(unsigned long); ++i) {
        bytes[sizeof(unsigned long) - 1 - i] =
            static_cast<unsigned char>((value >> (i * 8)) & 0xFF);
    }

    return bytes;
}

void PerfectLink::sendMessage(const Message& msg, unsigned long receiverId) {
    addMessageToPacket(msg, receiverId);
    if (!resendThread_.joinable()) {
        resendThread_ = std::thread(&PerfectLink::sendPacketLoop, this);
    }
}

void PerfectLink::addMessageToPacket(const Message& msg, unsigned long receiverId) {
    std::vector<Message> packetToFlush;

    {
        std::lock_guard<std::mutex> lock(partialPacketMutex_);

        partialPacket_[receiverId].push_back(msg);
        numMessagesInPacket_[receiverId]++;
        lastPacketUpdateTime_[receiverId] = Clock::now();

        // Flush if packet full
        if (numMessagesInPacket_[receiverId] >= maxMessagesPerPacket_) {
            packetToFlush = partialPacket_[receiverId];
            partialPacket_[receiverId] = {};
            numMessagesInPacket_[receiverId] = 0;
        }
    }

    // Serialize & enqueue outside the lock
    if (!packetToFlush.empty()) {
        auto payload =
            serializePacket(packetSeqNumber_[receiverId], packetToFlush);

        addPacketToPending(payload, receiverId);
    }
}


void PerfectLink::flushMessages(unsigned long receiverId)
{
    std::vector<Message> packetToFlush;

    {
        std::lock_guard<std::mutex> lock(partialPacketMutex_);
        if (!partialPacket_[receiverId].empty()) {
            packetToFlush = std::move(partialPacket_[receiverId]);
            partialPacket_[receiverId].clear();
            numMessagesInPacket_[receiverId] = 0;
        }
    }

    if (!packetToFlush.empty()) {
        auto payload =
            serializePacket(packetSeqNumber_[receiverId], packetToFlush);

        addPacketToPending(payload, receiverId);
    }
}


void PerfectLink::addPacketToPending(const std::vector<uint8_t>& payload, unsigned long receiverId) {
    if (payload.empty()) return;

    packetSeqNumber_[receiverId]++;   // ONLY HERE

    Packet packet{receiverId, packetSeqNumber_[receiverId], payload};
    logSendPacket(payload);

 // lock pending map and assign packet id under that lock
    std::lock_guard<std::mutex> lockPending(pendingMapMutex_);
    pending_[receiverId][packet.id] = packet;
}

void PerfectLink::flushPendingPacketIfReady(unsigned long receiverId) {
    bool shouldFlush = false;
    { // Holding partialPacketMutex_
        std::lock_guard<std::mutex> lock(partialPacketMutex_);
        auto now = Clock::now();
        DEBUGLOGSEND("Checking if pending packet is ready. Num messages in partial packet is: " << numMessagesInPacket_[receiverId]);
        if (now - lastPacketUpdateTime_[receiverId] > maxPacketUpdateTimePast_ || numMessagesInPacket_[receiverId] >= maxMessagesPerPacket_) {
            DEBUGLOGSEND("Packet is ready so will flush messages");
            shouldFlush = !partialPacket_[receiverId].empty();
        }
    } // Releases lock
    if (shouldFlush) flushMessages(receiverId); // flushMessages does copy-and-call safely
}

void PerfectLink::flushPendingPacketsIfReady() {
     for (const auto& [processId, _] : hostMapById_) {
        // Skip it's own process  // TODO - is this correct?
        if (processId == myProcessId_) {
            continue;
        }

        flushPendingPacketIfReady(processId);
     }
}

void PerfectLink::sendPacketLoop() {
    while (running_) {
        // Flush partial packets if needed
        flushPendingPacketsIfReady();

        Packet packetToSend;
        if (!findPacketToSend(packetToSend)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }

        // Lookup destination
        auto [receiverIp, receiverPort] = hostMapById_[packetToSend.receiverId];

        // Send raw binary payload
        sendRaw(packetToSend.payload, receiverIp, receiverPort);
    }
}

bool PerfectLink::findPacketToSend(Packet& outPacket) { // Finds a packet in pending_ that hasn't been sent too recently
    auto now = Clock::now();
    const std::chrono::milliseconds minDelay(100);

    std::lock_guard<std::mutex> lock(pendingMapMutex_);

    // Loop through outer map
    for (auto& outerEntry : pending_) {
        // outerEntry.first is the outer key
        // outerEntry.second is the inner map
        auto& innerMap = outerEntry.second;

        // Loop through inner map // TODO - is this going to always prioritise the packets being sent to the lowest processIds?
        for (auto& innerEntry : innerMap) {
            Packet& pkt = innerEntry.second;

            if (now - pkt.lastSentTime > minDelay) {
                pkt.lastSentTime = now;  // update while holding lock
                outPacket = pkt;         // make a safe copy
                return true;
            }
        }
    }
    return false; // nothing ready to be sent again
}

void PerfectLink::sendRaw(const std::vector<uint8_t>& data, in_addr_t ip, unsigned short port) {
    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(port);
    dest.sin_addr.s_addr = ip;

    sendto(sockfd_,
           data.data(),
           data.size(),
           0,
           reinterpret_cast<sockaddr*>(&dest),
           sizeof(dest));
}

void PerfectLink::receiverLoop() {
    uint8_t buffer[2048];
    sockaddr_in senderAddr{};
    socklen_t senderLen = sizeof(senderAddr);

    while (running_) {
        ssize_t bytes = recvfrom(sockfd_, buffer, sizeof(buffer), 0,
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

        const uint8_t* p = buffer;
        uint8_t type = *p++;

        unsigned short senderPort = ntohs(senderAddr.sin_port);
        unsigned long senderId = hostMapByPort_[senderPort].first;

        if (type == 1) { // ACK
            uint64_t pktId = read_u64(p);
            handleAck(senderId, pktId);
        } else { // DATA
            uint64_t packetId = read_u64(p);

            if (deserializeAndDeliver(senderId, packetId, p, bytes - 1 - sizeof(uint64_t))) {
                sendAck(senderAddr.sin_addr.s_addr, senderPort, packetId);
            }
        }
    }
}

bool PerfectLink::deserializeAndDeliver(unsigned long senderId,
                                        uint64_t packetId,
                                        const uint8_t* data,
                                        size_t /*len*/)
{
    const uint8_t* p = data;

    uint32_t count = read_u32(p);

    unsigned long firstMissing = firstMissingPacketId_[senderId];
    auto& deliveredSet = delivered_[senderId];

    // ---- CASE 1: already cleaned ----
    if (packetId < firstMissing) {
        return true; // ACK again, do NOT deliver
    }

    // ---- CASE 2: exactly the one we're waiting for ----
    if (packetId == firstMissing) {
        // Deliver messages
        for (uint32_t i = 0; i < count; ++i) {
            Message m;
            m.origSenderId = read_u64(p);
            m.messageId    = read_u64(p);
            uint32_t sz    = read_u32(p);
            m.content.assign(p, p + sz);
            p += sz;

            if (deliverCallback_) {
                deliverCallback_(m, senderId);
            }
        }

        deliveredSet.insert(packetId);

        // ---- CLEAN deliveredSet (safe + linear) ----
        // Now replace the firstMissingMessageId and clean deliveredSet
        unsigned long prev = 0;
        bool gapFound = false;
        unsigned long lastValue = *deliveredSet.rbegin();
        for (unsigned long pktId : deliveredSet) {
            if (prev == 0) { // At the first value so skip
                prev = pktId;
            } else { // Not at the first value
                if (prev + 1 != pktId) { // Found the gap
                    DEBUGLOGRECEIVE("Found the gap so removing up to gap");
                    deliveredSet.erase(prev);
                    firstMissingPacketId_[senderId] = prev + 1;
                    gapFound = true;
                    break;
                } else { // Haven't found the gap but can keep cleaning
                    deliveredSet.erase(prev);
                    prev = pktId;
                }
            }
        }
        if (!gapFound) {
            // No gap found: all are in order
            DEBUGLOGRECEIVE("Didn't find gap so removing whole list");
            firstMissingPacketId_[senderId] = lastValue + 1;
            deliveredSet.clear();
        }
        return true; // sends the ack
    }

    // ---- CASE 3: packetId > firstMissing ----
    // Check if already delivered
    auto it = deliveredSet.find(packetId);

    if (it != deliveredSet.end()) { // Already in our list
        return true;
    } 
    // Not in our list so add and deliver it
    // Deliver messages
    for (uint32_t i = 0; i < count; ++i) {
        Message m;
        m.origSenderId = read_u64(p);
        m.messageId    = read_u64(p);
        uint32_t sz    = read_u32(p);
        m.content.assign(p, p + sz);
        p += sz;

        if (deliverCallback_) {
            deliverCallback_(m, senderId);
        }
    }
    deliveredSet.insert(packetId);
    return true;
}



void PerfectLink::sendAck(in_addr_t destIp, unsigned short destPort, uint64_t packetId) {
    std::vector<uint8_t> ack;
    ack.reserve(1 + sizeof(uint64_t));

    ack.push_back(1);              // ACK type
    write_u64(ack, packetId);      // Packet ID

    sendRaw(ack, destIp, destPort);
}

void PerfectLink::handleAck(const unsigned long receiverId, const unsigned long pktId) {
    std::lock_guard<std::mutex> lock(pendingMapMutex_); // Destroys and lock and releases mutex when out of scope
    auto it = pending_[receiverId].find(pktId);
    if (it != pending_[receiverId].end()) {
        pending_[receiverId].erase(it);
    }
}

void PerfectLink::logDelivery(unsigned long senderId, unsigned long messageId) { 
    if (!loggingToFile_) {
        return;
    }
    if (!logFile_.is_open()) {
        std::cerr << "Failed to open log file: " << logPath_ << std::endl;
        return;
    }
    {
        std::lock_guard<std::mutex> lock(loggingMutex_);
        logFile_ << "d " << senderId << " " << messageId << "\n";
        if (++writeCounter_ % linesInLogBatch_ == 0) logFile_.flush(); // every 1000 lines
    }
    
}

void PerfectLink::logSendPacket(const std::vector<uint8_t>& payload)
{
    if (!loggingToFile_ || !logFile_.is_open()) {
        return;
    }

    const uint8_t* p   = payload.data();
    const uint8_t* end = payload.data() + payload.size();

    // ---- Packet type ----
    if (p + 1 > end) return;
    uint8_t type = *p++;
    if (type != 0) return; // DATA packets only

    // ---- packetId ----
    if (p + sizeof(uint64_t) > end) return;
    read_u64(p);

    // ---- message count ----
    if (p + sizeof(uint32_t) > end) return;
    uint32_t numMessages = read_u32(p);

    for (uint32_t i = 0; i < numMessages; ++i) {
        // ---- origSenderId ----
        if (p + sizeof(uint64_t) > end) return;
        read_u64(p);

        // ---- messageId ----
        if (p + sizeof(uint64_t) > end) return;
        uint64_t messageId = read_u64(p);

        // ---- content ----
        if (p + sizeof(uint32_t) > end) return;
        uint32_t contentSize = read_u32(p);

        if (p + contentSize > end) return;
        p += contentSize;

        // ---- LOG ----
        {
            std::lock_guard<std::mutex> lock(loggingMutex_);
            logFile_ << "b " << messageId << "\n";
            if (++writeCounter_ % linesInLogBatch_ == 0) {
                logFile_.flush();
            }
        }
    }
}


std::vector<uint8_t>
PerfectLink::serializePacket(unsigned long packetId, const std::vector<Message>& msgs) {
    std::vector<uint8_t> b;
    b.push_back(0); // DATA type
    write_u64(b, packetId);
    assert(msgs.size() <= std::numeric_limits<uint32_t>::max());
    write_u32(b, static_cast<uint32_t>(msgs.size()));

    for (auto& m : msgs) {
        write_u64(b, m.origSenderId);
        write_u64(b, m.messageId);
        assert(m.content.size() <= std::numeric_limits<uint32_t>::max());
        write_u32(b, static_cast<uint32_t>(m.content.size()));
        b.insert(b.end(), m.content.begin(), m.content.end());
    }
    return b;
}

void PerfectLink::stop() {
    running_ = false;
    if (receiverThread_.joinable()) receiverThread_.join();
    if (resendThread_.joinable()) resendThread_.join();
    if (loggingToFile_ && logFile_.is_open()) {
        {
            std::lock_guard<std::mutex> lock(loggingMutex_);
            logFile_.flush();
            logFile_.close();
        }
    }
    close(sockfd_);
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
