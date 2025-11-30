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
        partialPacket_[processId] = "";
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

void PerfectLink::sendMessage(const Message& message, unsigned long receiverId) {
    DEBUGLOG("Sending message " << message.messageId << " to process " << receiverId);
    // Add messageId to message payload
    std::string messagePayload = std::to_string(message.origSenderId) + "-" + std::to_string(message.messageId) + ":" + message.content; // Final payload will be pktId|sendId-msgId:msg|sendId-mgId:msg ...
    addMessageToPacket(messagePayload, receiverId);
    
    // Start resend thread if not already running
    if (!resendThread_.joinable()) {
        resendThread_ = std::thread(&PerfectLink::sendPacketLoop, this);
        DEBUGLOGSEND("Starting sending thread");
    }
}

void PerfectLink::addMessageToPacket(const std::string& messagePayload, unsigned long receiverId) { // Adding message to current packet being built
    DEBUGLOGSEND("Adding message payload: " << messagePayload << " to partial packet");
    std::string packetToMove;

    // Hold partialPacketMutex 
   {
        std::lock_guard<std::mutex> lock(partialPacketMutex_);
        // TODO - get direct reference to the partialPacket_ at this processId

        // Check if it's the first message
        if (partialPacket_[receiverId].empty()) {
            partialPacket_[receiverId] = messagePayload;
        } else {
            partialPacket_[receiverId] += "|" + messagePayload;
        }
        lastPacketUpdateTime_[receiverId] = Clock::now();
        numMessagesInPacket_[receiverId]++;

        DEBUGLOGSEND("Num messages in partial packet is now: " << numMessagesInPacket_[receiverId]);

        // Check if we now have to send the packet
        if (numMessagesInPacket_[receiverId] == maxMessagesPerPacket_) {
            DEBUGLOGSEND("Just added the message and partial packet now big enough so flushing");
            packetToMove = partialPacket_[receiverId];
            partialPacket_[receiverId] = "";
        }
    } // partialPacketMutex_ released here

    // If we copied a packet out, add it to pending now without holding partialPacketMutex_
    if (!packetToMove.empty()) {
        addPacketToPending(packetToMove, receiverId);
    }
}

void PerfectLink::flushMessages(unsigned long receiverId) {
    DEBUGLOGSEND("Flushing messages");
    std::string packetToMove;
    { // Hold partialPacketMutex_ lock
        std::lock_guard<std::mutex> lock(partialPacketMutex_);
        if (!partialPacket_[receiverId].empty()) {
            packetToMove = partialPacket_[receiverId];
            partialPacket_[receiverId].clear();
        } else {
            DEBUGLOGSEND("Partial packet was empty so didn't do anything");
        }
    } // partialPacketMutex_ released here

    if (!packetToMove.empty()) {
        addPacketToPending(packetToMove, receiverId);
    }
}

void PerfectLink::addPacketToPending(const std::string &packetStr, unsigned long receiverId) {
    if (packetStr.empty()) return;
    
    packetSeqNumber_[receiverId]++;
    Packet packet = Packet({receiverId, packetSeqNumber_[receiverId], packetStr});
    logSendPacket(packetStr);
    numMessagesInPacket_[receiverId] = 0;
    
    { // lock pending map and assign packet id under that lock
        std::lock_guard<std::mutex> lockPending(pendingMapMutex_);
        pending_[receiverId][packet.id] = packet;
        orderedPendingPackets_.insert(packet);
        DEBUGLOGSEND("Added packet id=" << packet.id << " to pending for receiverId: " << receiverId << ". pending_ size for this receiver id is =" << pending_[receiverId].size());
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
    const std::chrono::milliseconds idleWait(100); // max wait
    const std::chrono::milliseconds shortPoll(10); // small poll when no packets ready

    while (running_) {
        // Try to flush partial packets first (keeps old behaviour)
        flushPendingPacketsIfReady();

        Packet packetToSend;
        if (!findPacketToSend(packetToSend)) {
            // No ready packet; block for a short while or until notified
            std::unique_lock<std::mutex> lk(pendingCvMutex_);
            // Wait until new packet is available or timeout so flushPendingPacketsIfReady gets a chance again
            pendingCv_.wait_for(lk, shortPoll, [this](){
                std::lock_guard<std::mutex> lock(pendingMapMutex_);
                // quick check if there is any pending packet
                for (const auto &outer : pending_) {
                    if (!outer.second.empty()) return true;
                }
                return false;
            });
            // after wait_for, loop again to try findPacketToSend()
            continue;
        }

        std::string payload = std::to_string(packetToSend.id) + "|" + packetToSend.messages;
        DEBUGLOGSEND("Sending packet id:" << packetToSend.id << " messages: " << packetToSend.messages);
        auto [receiverIp, receiverPort] = hostMapById_[packetToSend.receiverId];
        DEBUGLOGACK("Sending packet with payload: "<< payload << " to process " << packetToSend.receiverId);
        sendRaw(payload, receiverIp, receiverPort);
    }
}

bool PerfectLink::findPacketToSend(Packet& outPacket) {  
    auto now = Clock::now();
    const std::chrono::milliseconds minDelay(100);

    std::lock_guard<std::mutex> lock(pendingMapMutex_);

    while (!orderedPendingPackets_.empty()) {

        // Extract oldest element
        auto nh = orderedPendingPackets_.extract(orderedPendingPackets_.begin());
        Packet& pkt = nh.value();

        // Check if packet was acknowledged
        auto outerIt = pending_.find(pkt.receiverId);
        if (outerIt == pending_.end()) {
            // ACK removed entire receiver bucket → drop packet permanently
            continue; // Try next packet
        }

        auto& innerMap = outerIt->second;
        auto innerIt = innerMap.find(pkt.id);
        if (innerIt == innerMap.end()) {
            // ACK removed just this packet → discard permanently
            continue; // Try next packet
        }

        // Check resend delay
        if (now - pkt.lastSentTime >= minDelay) {
            pkt.lastSentTime = now;

            // Reinsert at new position
            orderedPendingPackets_.insert(std::move(nh));

            outPacket = pkt;
            return true;
        }

        // Not ready yet - reinsert exactly where it belongs
        orderedPendingPackets_.insert(std::move(nh));

        // Since this was the oldest packet and it's not ready,
        // no later packets can be ready either (they’re all newer).
        return false;
    }

    // No packets left
    return false;
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

        unsigned short senderPort = ntohs(senderAddr.sin_port);
        unsigned long senderId = hostMapByPort_[senderPort].first;

        // Extract message type
        if (payload.rfind("ACKLIST:", 0) == 0) {
            std::string idsStr = payload.substr(8);
            std::stringstream ss(idsStr);
            std::string idTok;

            while (std::getline(ss, idTok, ',')) {
                unsigned long pktId = parsePacketPayloadId(idTok);
                if (pktId > 0) {
                    handleAck(senderId, pktId);
                }
            }
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
        flushAckBatch(senderAddr.sin_addr.s_addr, senderPort);
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
    // Getting the originalSenderId
    size_t sep = messagePayload.find('-');
    if (sep == std::string::npos) {
        std::cerr << "Incorrect payload format of message" << std::endl;
        return false;
    }
    std::string origSenderIdStr = messagePayload.substr(0, sep);
    unsigned long origSenderId = parseMessagePayloadId(origSenderIdStr);
    if (origSenderId == 0) {
        return false; // origSenderId could not be converted into a unsigned long so message could not be delivered
    }
    std::string remainingMsgPayload = messagePayload.substr(sep + 1);
    size_t sep2 = remainingMsgPayload.find(':');
    if (sep2 == std::string::npos) {
        std::cerr << "Incorrect payload format of message" << std::endl;
        return false;
    }
    std::string msgIdStr = remainingMsgPayload.substr(0, sep2);
    unsigned long msgId = parseMessagePayloadId(msgIdStr);
    if (msgId == 0) {
        return false; // MsgId could not be converted into a unsigned long so message could not be delivered
    }
    if (deliverCallback_) {
        Message messageToDeliver{origSenderId, msgId, remainingMsgPayload.substr(sep2+1)};
        deliverCallback_(messageToDeliver, senderId);
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

        // Record the last update time to support timeout-based flushing
        lastAckUpdateTime_[destPort] = now;
    }

    // NOTE: We don't flush here.
    // Flushing is controlled by the receiver loop calling flushAckBatch(), which checks batch size or timeout.
}

void PerfectLink::flushAckBatch(in_addr_t destIp, unsigned short destPort) {
    std::vector<unsigned long> list;
    bool shouldFlush = false;
    Clock::time_point lastUpdate;

    {
        std::lock_guard<std::mutex> lock(ackMutex_);

        auto &vec = pendingAcks_[destPort];
        unsigned long count =
            numAcksInBatch_.count(destPort) ? numAcksInBatch_[destPort].load() : 0;

        if (count == 0)
            return; // nothing to do

        // Check batching conditions
        lastUpdate = lastAckUpdateTime_[destPort];
        auto now = Clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - lastUpdate);

        if (count >= maxAcksPerBatch_ || elapsed >= maxAckUpdateTimePast_) {
            shouldFlush = true;

            // Move the batch out
            list.swap(vec);
            numAcksInBatch_[destPort].store(0);
        }
    }

    if (!shouldFlush)
        return;

    // Build batched ACK payload
    std::string payload = "ACKLIST:";
    for (size_t i = 0; i < list.size(); ++i) {
        payload += std::to_string(list[i]);
        if (i + 1 < list.size()) payload += ",";
    }

    DEBUGLOGSENDACK("Flushing ACK batch (" << list.size() << " acks): "
                      << payload);

    sendRaw(payload, destIp, destPort);
}

void PerfectLink::handleAck(const unsigned long receiverId, const unsigned long pktId) {
    Packet acknowledgedPacket;
    bool hasPacket = false;
    {
        std::lock_guard<std::mutex> lock(pendingMapMutex_); // Destroys and lock and releases mutex when out of scope
        auto it = pending_[receiverId].find(pktId);
        if (it != pending_[receiverId].end()) {
            acknowledgedPacket = it->second;
            hasPacket = true;
            pending_[receiverId].erase(it);
        }
    }
    if (hasPacket && ackCallback_) {
        handlePacketAck(receiverId, acknowledgedPacket);
    }
}


void PerfectLink::handlePacketAck(unsigned long receiverId, Packet acknowledgedPacket) {
    // receiverId is who received the message and has just sent back the ack
    // Packet messages payload is be pktId|sendId-msgId:msg|sendId-mgId:msg ... 
    std::string payload = acknowledgedPacket.messages;
    DEBUGLOGACK("Just received ack for packet with payload: " << payload << " from process " << receiverId);

    std::stringstream ss(payload);
    std::string part;

    // Now parse each "sendId-msgId:msg"
    while (std::getline(ss, part, '|')) {
        if (part.empty()) continue;

        // Format: sendId-msgId:content
        size_t dashPos = part.find('-');
        if (dashPos == std::string::npos) continue;

        size_t colonPos = part.find(':', dashPos + 1);
        if (colonPos == std::string::npos) continue;

        // Extract fields
        std::string senderStr  = part.substr(0, dashPos);
        std::string msgIdStr   = part.substr(dashPos + 1, colonPos - (dashPos + 1));
        std::string contentStr = part.substr(colonPos + 1);

        // Convert numeric fields
        unsigned long senderId = std::stoul(senderStr);
        unsigned long msgId = std::stoul(msgIdStr);

        // Build the Message object
        Message msg;
        msg.origSenderId = senderId;
        msg.messageId = msgId;
        msg.content = contentStr;

        // Callback for this message
        DEBUGLOGACK("Pushing ack for message (" << msg.origSenderId << ", " << msg.messageId << ") up to FIFO");
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

void PerfectLink::logSendPacket(const std::string& packet) { // TODO - probably don't want this anymore or need to adapt it to be for each receiverId
    size_t start = 0;
    size_t end;
    while ((end = packet.find('|', start)) != std::string::npos) {
        std::string messagePayload = packet.substr(start, end - start);
        size_t sep = messagePayload.find(':');
        if (sep == std::string::npos) {
            std::cerr << "Incorrect payload format of message, cannot send packet" << std::endl;
            return;
        }
        logSendMessage(messagePayload.substr(0, sep));
        start = end + 1;
    }
    // Last token after the last delimiter
    std::string messagePayload = packet.substr(start);
    size_t sep = messagePayload.find(':');
    if (sep == std::string::npos) {
        std::cerr << "Incorrect payload format of message, cannot send packet" << std::endl;
        return;
    }
    logSendMessage(messagePayload.substr(0, sep));
}

void PerfectLink::logSendMessage(const std::string& messageIds) { 
    if (!loggingToFile_) {
        return;
    }
    if (!logFile_.is_open()) {
        std::cerr << "Failed to open log file: " << logPath_ << std::endl;
        return;
    }
    size_t sep = messageIds.find('-');
    if (sep == std::string::npos) {
        std::cerr << "Incorrect payload format of message, cannot send packet" << std::endl;
        return;
    }
    std::string messageId = messageIds.substr(sep+1); // First value is the originalSenderId, second value is the msgId
    
    {
        std::lock_guard<std::mutex> lock(loggingMutex_);
        logFile_ << "b " << messageId << "\n";
        if (++writeCounter_ % linesInLogBatch_ == 0) logFile_.flush(); // every 1000 lines
    }
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
