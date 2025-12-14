#include <iostream>
#include <unistd.h>
#include <fstream>
#include <sys/stat.h>  // for mkdir
#include <string>
#include <sstream>
#include <cerrno>
#include <cstring>
#include <sys/time.h> // for struct timeval

#include "PerfectLink.hpp"

// TODO - ************ TURN THIS OFF BEFORE SUBMISSION ****************
// #define DEBUG
// #define DEBUGSEND
// #define DEBUGRECEIVE
// #define DEBUGACK
// #define DEBUGSENDACK

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

#ifdef DEBUGACK
    #define DEBUGLOGACK(msg) (std::cout << msg << std::endl)
#else
    #define DEBUGLOGACK(msg) do {} while(0) // no-op in release
#endif

#ifdef DEBUGSENDACK
    #define DEBUGLOGSENDACK(msg) (std::cout << msg << std::endl)
#else
    #define DEBUGLOGSENDACK(msg) do {} while(0) // no-op in release
#endif

PerfectLink::PerfectLink(unsigned long myProcessId, in_addr_t myProcessIp, unsigned short myProcessPort, std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort, std::unordered_map<unsigned long, std::pair<in_addr_t, unsigned short>> hostMapById, std::string logPath)
    : myProcessId_(myProcessId), myProcessPort_(myProcessPort), myProcessIp_(myProcessIp), hostMapByPort_(hostMapByPort), hostMapById_(hostMapById), logPath_(logPath), running_(false)
{
    // Create or overwrite the log file
    if (logPath_ == "") {
        DEBUGLOG("Not logging to file in Perfect Links");
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
    for (const auto& [processId, pairVal] : hostMapById_) {
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

        // Initialise ack batches
        numAcksInBatch_[pairVal.second] = 0;
        lastAckUpdateTime_[pairVal.second] = Clock::now();
    }

    // Start listening on ports
    initReceiverBroadcaster();
}

PerfectLink::~PerfectLink() {
    stop();
}

void PerfectLink::setDeliverCallback(std::function<void(Message, unsigned long)> cb) {
    deliverCallback_ = cb;
}

void PerfectLink::setDeliverCallbackToDefault() {
    // Define delivery callback - change this for later assignments
    deliverCallback_ = [this](Message message, unsigned long senderId){
        DEBUGLOG("Delivered \"" << message.messageId << "\" from: " << senderId);
        logDelivery(message.origSenderId, message.messageId);
    };

}

void PerfectLink::setAckCallback(std::function<void(Message, unsigned long)> cb) {
    ackCallback_ = cb;
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

    // Set a receive timeout so recvfrom() in receiverLoop() won't block indefinitely
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

void PerfectLink::sendMessage(const Message& message, unsigned long receiverId) {
    DEBUGLOG("Sending message " << message.messageId << " to process " << receiverId);
    // Add messageId to message payload
    auto msgBytes = serializeMessage(message);
    addMessageToPacket(msgBytes, message, receiverId);

    // Start resend thread if not already running
    if (!resendThread_.joinable()) {
        resendThread_ = std::thread(&PerfectLink::sendPacketLoop, this);
        DEBUGLOGSEND("Starting sending thread");
    }
}


void PerfectLink::addMessageToPacket(const std::vector<uint8_t>& msgBytes, const Message& message, unsigned long receiverId) {
    DEBUGLOGSEND("Adding message " << message.messageId << " to packet");
    Packet packetToMove;
    // Hold partialPacketMutex 
   {
        std::lock_guard<std::mutex> lock(partialPacketMutex_);
        Packet& pkt = partialPacket_[receiverId];

        // Append bytes to payload
        pkt.payload.insert(pkt.payload.end(), msgBytes.begin(), msgBytes.end());

        // Store message metadata
        pkt.messages.emplace_back(
            message.origSenderId,
            message.messageId
        );

        // Update time and number of messages in packet
        lastPacketUpdateTime_[receiverId] = Clock::now();
        numMessagesInPacket_[receiverId]++;
        // Check if we now have to send the packet
        if (numMessagesInPacket_[receiverId] == maxMessagesPerPacket_) {
            packetToMove = pkt;
            partialPacket_[receiverId] = {};
        }
    } // partialPacketMutex_ released here

    // If we copied a packet out, add it to pending now without holding partialPacketMutex_
    if (!packetToMove.payload.empty()) {
        addPacketToPending(packetToMove, receiverId);
    }
}

void PerfectLink::flushMessages(unsigned long receiverId) {
    DEBUGLOGSEND("Flushing messages");
    Packet packetToMove;
    { // Hold partialPacketMutex_ lock
        std::lock_guard<std::mutex> lock(partialPacketMutex_);
        if (!partialPacket_[receiverId].payload.empty()) {
            packetToMove = partialPacket_[receiverId];
            partialPacket_[receiverId] = {};
        } else {
            DEBUGLOGSEND("Partial packet was empty so didn't do anything");
        }
    } // partialPacketMutex_ released here

    if (!packetToMove.payload.empty()) {
        addPacketToPending(packetToMove, receiverId);
    }
}

void PerfectLink::addPacketToPending(const Packet packet, unsigned long receiverId) {
    if (packet.payload.empty()) return;

    packetSeqNumber_[receiverId]++;
    Packet newPacket{receiverId, packetSeqNumber_[receiverId], packet.payload, packet.messages}; // Clock time is set automatically
    logSendPacket(newPacket);
    numMessagesInPacket_[receiverId] = 0;
    { // lock pending map and assign packet id under that lock
        std::lock_guard<std::mutex> lockPending(pendingMapMutex_);

        // Insert into ordered set
        auto it = orderedPendingPackets_.insert(packet).first;

        // Insert the iterator into lookup map
        pendingIndex_[{receiverId, packet.id}] = it;
    }
    // Notify send thread that work is available
    pendingCv_.notify_one();
}

void PerfectLink::flushPendingPacketIfReady(unsigned long receiverId) {
    bool shouldFlush = false;
    { // Holding partialPacketMutex_
        std::lock_guard<std::mutex> lock(partialPacketMutex_);
        auto now = Clock::now();
        DEBUGLOGSEND("Checking if pending packet is ready. Num messages in partial packet is: " << numMessagesInPacket_[receiverId]);
        if (now - lastPacketUpdateTime_[receiverId] > maxPacketUpdateTimePast_ || numMessagesInPacket_[receiverId] >= maxMessagesPerPacket_) {
            DEBUGLOGSEND("Packet is ready so will flush messages");
            shouldFlush = !partialPacket_[receiverId].payload.empty();
        }
    } // Releases lock
    if (shouldFlush) flushMessages(receiverId);
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

std::vector<uint8_t> PerfectLink::serializeMessage(const Message& msg) {
    std::vector<uint8_t> out;

    MessageHeader hdr;
    hdr.origSenderId = msg.origSenderId;
    hdr.messageId    = msg.messageId;

    if (std::holds_alternative<uint64_t>(msg.content)) {
        hdr.payloadType = static_cast<uint8_t>(MessagePayloadType::INT);
        hdr.payloadSize = sizeof(uint64_t);

        appendBytes(out, hdr);
        appendBytes(out, std::get<uint64_t>(msg.content));
    }
    else {
        const auto& vec = std::get<std::vector<uint64_t>>(msg.content);
        hdr.payloadType = static_cast<uint8_t>(MessagePayloadType::INT_LIST);
        hdr.payloadSize = static_cast<uint32_t>(vec.size() * sizeof(uint64_t));

        appendBytes(out, hdr);
        for (uint64_t v : vec) {
            appendBytes(out, v);
        }
    }

    return out;
}

void PerfectLink::sendPacketLoop() { 
    const std::chrono::milliseconds shortPoll(10);

    while (running_) {
        // Try to flush partial packets
        flushPendingPacketsIfReady();

        Packet packetToSend;

        // Try to find a packet to send
        if (!findPacketToSend(packetToSend)) {
            // No packet ready - wait until notified or timeout
            std::unique_lock<std::mutex> lk(pendingCvMutex_);

            pendingCv_.wait_for(lk, shortPoll, [this]() {
                std::lock_guard<std::mutex> lock(pendingMapMutex_);
                return !orderedPendingPackets_.empty();
            });

            // Loop again and retry findPacketToSend()
            continue;
        }

        // We have a packet so send it
        std::vector<uint8_t> payload;

        DataPacketHeader hdr{
            static_cast<uint8_t>(PacketType::DATA),
            packetToSend.id,
            static_cast<uint32_t>(packetToSend.messages.size())
        };

        payload.insert(payload.end(),
            reinterpret_cast<uint8_t*>(&hdr),
            reinterpret_cast<uint8_t*>(&hdr) + sizeof(hdr));

        payload.insert(payload.end(),
            packetToSend.payload.begin(),
            packetToSend.payload.end());

        DEBUGLOGSEND("Sending packet id:" << packetToSend.id << " messages: " << packetToSend.payload);
        
        auto [receiverIp, receiverPort] = hostMapById_[packetToSend.receiverId];
        DEBUGLOGACK("Sending packet with payload: "<< payload << " to process " << packetToSend.receiverId);

        sendRaw(payload, receiverIp, receiverPort);
    }
}

bool PerfectLink::findPacketToSend(Packet& outPacket) {
    auto now = Clock::now();
    const std::chrono::milliseconds minDelay(100);

    std::lock_guard<std::mutex> lock(pendingMapMutex_);

    if (orderedPendingPackets_.empty())
        return false;

    while (!orderedPendingPackets_.empty()) {
        // Extract oldest
        auto nh = orderedPendingPackets_.extract(orderedPendingPackets_.begin());
        Packet& pkt = nh.value();
        Key key{pkt.receiverId, pkt.id};

        // Delay check
        if (now - pkt.lastSentTime < minDelay) {
            // reinsert and stop searching
            auto ret = orderedPendingPackets_.insert(std::move(nh));
            auto it = ret.position;
            pendingIndex_[key] = it;
            return false;
        }

        // Update send time
        pkt.lastSentTime = now;

        // Reinsert with updated key
        auto ret = orderedPendingPackets_.insert(std::move(nh));
        auto it = ret.position;
        pendingIndex_[key] = it;

        outPacket = pkt;
        return true;
    }

    return false;
}

void PerfectLink::sendRaw(const std::vector<uint8_t>& payload, in_addr_t ip, unsigned short port) {
    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(port);
    dest.sin_addr.s_addr = ip;

    sendto(sockfd_, payload.data(), payload.size(), 0,
           reinterpret_cast<sockaddr*>(&dest), sizeof(dest));
}

void PerfectLink::receiverLoop() {
    uint8_t buffer[4096];
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
            }
            // raise socket error
            perror("recvfrom");
            continue;
        }

        if (bytes < static_cast<ssize_t>(sizeof(uint8_t))) {
            continue;   
        }
        
        uint8_t packetType = buffer[0];

        unsigned short senderPort = ntohs(senderAddr.sin_port);
        unsigned long senderId = hostMapByPort_[senderPort].first;

        /* ===================== ACK PACKET ===================== */
        if (packetType == static_cast<uint8_t>(PacketType::ACK)) {
            if (bytes < static_cast<ssize_t>(sizeof(AckPacketHeader))) {
                continue;
            }

            auto* hdr = reinterpret_cast<const AckPacketHeader*>(buffer);
            size_t offset = sizeof(AckPacketHeader);

            for (uint32_t i = 0; i < hdr->numAcks; ++i) {
                if (offset + sizeof(uint64_t) > static_cast<size_t>(bytes))
                    break;

                uint64_t pktId;
                memcpy(&pktId, buffer + offset, sizeof(uint64_t));
                offset += sizeof(uint64_t);

                handleAck(senderId, pktId);
            }
            continue;
        }

        /* ===================== DATA PACKET ===================== */
        if (packetType != static_cast<uint8_t>(PacketType::DATA))
            continue;

        if (bytes < static_cast<ssize_t>(sizeof(DataPacketHeader))) {
            continue;
        }

        auto* hdr = reinterpret_cast<const DataPacketHeader*>(buffer);
        uint64_t id = hdr->id;

        const uint8_t* payload =
            buffer + sizeof(DataPacketHeader);
        size_t payloadLen =
            bytes - sizeof(DataPacketHeader);

        unsigned long firstMissingPacketId = firstMissingPacketId_[senderId];
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
            if (!deliverMessages(senderId, payload, payloadLen)) { // Couldn't deliver one or more of the messages, so don't acknowledge
                DEBUGLOGRECEIVE("Failed to deliver one or more of the messages so skipping this packet");
                std::cerr << "Failed to deliver one or more of the messages so skipping packet with id: " << id << std::endl;
                continue;
            } 
            deliveredSet.insert(id);
            sendAck(senderAddr.sin_addr.s_addr, senderPort, id);

            // Replace the firstMissingMessageId and clean deliveredSet
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
                // No gap found - all are in order
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
                if (!deliverMessages(senderId, payload, payloadLen)) { // Couldn't deliver one or more of the messages, so don't acknowledge
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
        flushAckBatch(senderAddr.sin_addr.s_addr, senderPort);
    }
}

bool PerfectLink::deliverMessages(unsigned long senderId, const uint8_t* data, size_t len) {
    size_t offset = 0;

    while (offset < len) {
        auto* mh = reinterpret_cast<const MessageHeader*>(data + offset);
        offset += sizeof(MessageHeader);

        Message msg;
        msg.origSenderId = mh->origSenderId;
        msg.messageId    = mh->messageId;

        if (mh->payloadType == static_cast<uint8_t>(MessagePayloadType::INT)) {
            uint64_t v;
            memcpy(&v, data + offset, sizeof(uint64_t));
            msg.content = v;
            offset += sizeof(uint64_t);
        }
        else if (mh->payloadType == static_cast<uint8_t>(MessagePayloadType::INT_LIST)) {
            size_t count = mh->payloadSize / sizeof(uint64_t);
            std::vector<uint64_t> vec(count);
            memcpy(vec.data(), data + offset, mh->payloadSize);
            msg.content = std::move(vec);
            offset += mh->payloadSize;
        }
        else {
            return false; // unknown payload
        }

        deliverCallback_(msg, senderId);
    }
    return true;
}

void PerfectLink::sendAck(in_addr_t destIp, unsigned short destPort, unsigned long packetId) {
    const auto now = Clock::now();

    {
        std::lock_guard<std::mutex> lock(ackMutex_);

        // Append the ACK to the pending vector
        pendingAcks_[destPort].push_back(packetId);

        // Increase count of ACKs in this batch
        numAcksInBatch_[destPort].fetch_add(1, std::memory_order_relaxed);

        // Record the last update time - needed for flushing
        lastAckUpdateTime_[destPort] = now;
    }
}

void PerfectLink::flushAckBatch(in_addr_t destIp, unsigned short destPort) {
    std::vector<uint64_t> acks;
    bool shouldFlush = false;

    {
        std::lock_guard<std::mutex> lock(ackMutex_);

        auto& vec = pendingAcks_[destPort];
        if (vec.empty())
            return;

        auto now = Clock::now();
        auto elapsed = now - lastAckUpdateTime_[destPort];

        if (vec.size() >= maxAcksPerBatch_ ||
            elapsed >= maxAckUpdateTimePast_) {
            acks.swap(vec);
            numAcksInBatch_[destPort].store(0);
            shouldFlush = true;
        }
    }

    if (!shouldFlush)
        return;

    /* Build binary ACK packet */
    std::vector<uint8_t> payload;

    AckPacketHeader hdr;
    hdr.type    = static_cast<uint8_t>(PacketType::ACK);
    hdr.numAcks = static_cast<uint32_t>(acks.size());

    payload.insert(
        payload.end(),
        reinterpret_cast<uint8_t*>(&hdr),
        reinterpret_cast<uint8_t*>(&hdr) + sizeof(hdr)
    );

    for (uint64_t id : acks) {
        payload.insert(
            payload.end(),
            reinterpret_cast<uint8_t*>(&id),
            reinterpret_cast<uint8_t*>(&id) + sizeof(id)
        );
    }

    DEBUGLOGSENDACK("Flushing " << acks.size() << " ACKs");

    sendRaw(payload, destIp, destPort);
}

void PerfectLink::handleAck(const unsigned long receiverId, const unsigned long pktId) {
    Packet acknowledgedPacket;
    bool hasPacket = false;
    {
        std::lock_guard<std::mutex> lock(pendingMapMutex_);

        Key key{receiverId, pktId};
        auto it = pendingIndex_.find(key);

        if (it != pendingIndex_.end()) {
            // Copy the packet before erasing it
            acknowledgedPacket = *(it->second);
            hasPacket = true;

            // Erase from set using iterator
            orderedPendingPackets_.erase(it->second);

            // Erase from index
            pendingIndex_.erase(it);
        }
    }

    // Call callback outside the lock
    if (hasPacket && ackCallback_) {
        handlePacketAck(receiverId, acknowledgedPacket);
    }
}


void PerfectLink::handlePacketAck(unsigned long receiverId, const Packet& acknowledgedPacket) {
    // receiverId = process that sent the ACK back to us

    DEBUGLOGACK(
        "Just received ACK for packetId="
        << acknowledgedPacket.packetId
        << " from process "
        << receiverId
    );

    // Each message contained in the packet is now acknowledged
    for (const auto& [senderId, messageId] : acknowledgedPacket.messages) {
        Message msg;
        msg.origSenderId = senderId;
        msg.messageId = messageId;

        DEBUGLOGACK(
            "Pushing ACK for message ("
            << senderId << ", "
            << messageId
            << ") up to FIFO"
        );

        ackCallback_(msg, receiverId);
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

void PerfectLink::logSendPacket(const Packet& packet) {
    for (const auto& [senderId, messageId] : packet.messages) {
        logSendMessage(senderId, messageId);
    }
}

void PerfectLink::logSendMessage(unsigned long senderId, unsigned long messageId) {
    (void)senderId; // senderId not used by the log format, but kept for clarity

    if (!loggingToFile_)
        return;

    if (!logFile_.is_open()) {
        std::cerr << "Failed to open log file: " << logPath_ << std::endl;
        return;
    }

    std::lock_guard<std::mutex> lock(loggingMutex_);

    // Format required by spec: b <messageId>
    logFile_ << "b " << messageId << "\n";

    if (++writeCounter_ % linesInLogBatch_ == 0)
        logFile_.flush();
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
