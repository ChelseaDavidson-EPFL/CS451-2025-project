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
    for (const auto& [processId, _] : hostMapById_) {
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
        numAcksInBatch_[processId] = 0;
        lastAckUpdateTime_[processId] = Clock::now();
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
        DEBUGLOGSEND("Added packet id=" << packet.id << " to pending for receiverId: " << receiverId << ". pending_ size for this receiver id is =" << pending_[receiverId].size());
    }

    // Schedule first resend attempt (push task into heap)
    {
        std::lock_guard<std::mutex> lk(heapMutex_);
        ResendTask t;
        t.receiverId = receiverId;
        t.pktId = packet.id;
        t.nextSendTime = Clock::now(); // send immediately (or add small jitter if you like)
        resendHeap_.push(std::move(t));
    }
    heapCv_.notify_one();

    // Notify send thread that work is available (keeps compatibility with existing condition variable)
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
    // Resend thread main loop: driven by resendHeap_. We also keep compatibility with pendingCv_ notifications.
    while (running_) {
        ResendTask task;
        bool haveTask = false;

        {   // determine next task/time safely
            std::unique_lock<std::mutex> lk(heapMutex_);
            while (running_ && resendHeap_.empty()) {
                // wait until something scheduled
                heapCv_.wait(lk);
            }
            if (!running_) break;

            // peek top
            auto top = resendHeap_.top();
            auto now = Clock::now();
            if (top.nextSendTime > now) {
                // wait until the top task becomes due or a new earlier task arrives
                heapCv_.wait_until(lk, top.nextSendTime);
                // go back to top of loop to re-evaluate conditions
                continue;
            }

            // Pop the due task
            task = top;
            resendHeap_.pop();
            haveTask = true;
        }

        if (!haveTask) continue;

        // Validate if this task was cancelled (i.e., acked)
        uint64_t key = makeKey(task.receiverId, task.pktId);
        {
            std::lock_guard<std::mutex> lk(heapMutex_);
            if (cancelledPackets_.find(key) != cancelledPackets_.end()) {
                // already acked => ignore this task (and optionally remove tombstone after some time)
                continue;
            }
        }

        // Lookup the packet in pending_ (under pendingMapMutex_)
        Packet packetToSend;
        bool exists = false;
        {
            std::lock_guard<std::mutex> lock(pendingMapMutex_);
            auto oit = pending_.find(task.receiverId);
            if (oit != pending_.end()) {
                auto it = oit->second.find(task.pktId);
                if (it != oit->second.end()) {
                    packetToSend = it->second;
                    exists = true;
                }
            }
        }

        if (!exists) {
            // Somebody removed it concurrently (ack arrived), mark cancelled for safety and continue
            std::lock_guard<std::mutex> lk(heapMutex_);
            cancelledPackets_.insert(key);
            continue;
        }

        // Now actually send the packet
        std::string payload = std::to_string(packetToSend.id) + "|" + packetToSend.messages;
        DEBUGLOGSEND("Sending packet id:" << packetToSend.id << " messages: " << packetToSend.messages);
        auto [receiverIp, receiverPort] = hostMapById_[packetToSend.receiverId];
        DEBUGLOGACK("Sending packet with payload: "<< payload << " to process " << packetToSend.receiverId);
        sendRaw(payload, receiverIp, receiverPort);

        // Reschedule next resend for this packet (if still pending). Schedule at now + retransmitInterval_
        {
            std::lock_guard<std::mutex> lk(heapMutex_);
            // double-check cancelled set again quickly
            if (cancelledPackets_.find(key) == cancelledPackets_.end()) {
                ResendTask nextTask;
                nextTask.receiverId = task.receiverId;
                nextTask.pktId = task.pktId;
                nextTask.nextSendTime = Clock::now() + retransmitInterval_;
                resendHeap_.push(std::move(nextTask));
                // notify is not required here because we are running the thread already, but keep symmetric signaling
                heapCv_.notify_one();
            }
        }
    } // while running_

    DEBUGLOG("sendPacketLoop exiting");
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

        // Handle ACKLIST:
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

        // Parse normal packet
        size_t sep = payload.find('|');
        if (sep == std::string::npos) {
            std::cerr << "Incorrect payload format" << std::endl;
            continue;
        }

        // Get message info
        std::string idStr = payload.substr(0, sep);
        unsigned long id = parsePacketPayloadId(idStr);
        if (id == 0) continue;
        std::string messages = payload.substr(sep + 1);

        unsigned long firstMissingPacketId = firstMissingPacketId_[senderId];
        DEBUGLOGRECEIVE("Received packet id=" << id << " from sender=" << senderId);

        // Quick path: already delivered and cleaned
        if (id < firstMissingPacketId) {
            // send ack again in case sender missed it
            sendAck(senderAddr.sin_addr.s_addr, senderPort, id);
            DEBUGLOGRECEIVE("Already delivered " << id << " from " << senderId << " so skipping");
            continue;
        }

        // We will reference delivered_[senderId] — lock just while mutating it
        {
            // delivered_ is presumably a map<unsigned long, std::set<unsigned long>>
            auto &deliveredSet = delivered_[senderId];

            if (id == firstMissingPacketId) {
                DEBUGLOGRECEIVE("Received expected packet id " << id << " from " << senderId << ". Attempting to deliver.");
                if (!deliverMessages(senderId, messages)) {
                    DEBUGLOGRECEIVE("Failed to deliver messages from packet " << id << " — skipping ack");
                    std::cerr << "Failed to deliver one or more of the messages so skipping packet with id: " << id << std::endl;
                    continue;
                }
                // record as delivered
                deliveredSet.insert(id);
                sendAck(senderAddr.sin_addr.s_addr, senderPort, id);

                // --- SAFELY advance firstMissingPacketId_ by consuming consecutive entries ---
                // Instead of iterating and erasing while iterating (unsafe), do a simple while loop:
                while (deliveredSet.find(firstMissingPacketId) != deliveredSet.end()) {
                    // remove the entry and increment the expected counter
                    deliveredSet.erase(firstMissingPacketId);
                    firstMissingPacketId++;
                }
                // update stored firstMissing
                firstMissingPacketId_[senderId] = firstMissingPacketId;

            } else {
                // id > firstMissingPacketId: buffer it (if new) and ack
                auto it = deliveredSet.find(id);
                if (it != deliveredSet.end()) {
                    // Already buffered/delivered; re-ack in case sender needs it
                    sendAck(senderAddr.sin_addr.s_addr, senderPort, id);
                    DEBUGLOGRECEIVE("Packet " << id << " already in deliveredSet for " << senderId);
                    // no other action
                } else {
                    // Attempt to deliver messages (this will call your deliverCallback which may depend on FIFO)
                    if (!deliverMessages(senderId, messages)) {
                        DEBUGLOGRECEIVE("Failed to deliver messages from packet " << id << " — skipping ack");
                        std::cerr << "Failed to deliver one or more of the messages so skipping packet with id: " << id << std::endl;
                        continue;
                    }
                    // Insert into delivered set for future gap filling
                    deliveredSet.insert(id);
                    sendAck(senderAddr.sin_addr.s_addr, senderPort, id);
                }
            }
        } // end scope for deliveredSet mutation

        // After processing, flush ack batch to the remote peer
        flushAckBatch(senderAddr.sin_addr.s_addr, senderPort);
    } // while running_
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
        std::lock_guard<std::mutex> lock(pendingMapMutex_);
        auto itOuter = pending_.find(receiverId);
        if (itOuter != pending_.end()) {
            auto it = itOuter->second.find(pktId);
            if (it != itOuter->second.end()) {
                acknowledgedPacket = it->second;
                hasPacket = true;
                itOuter->second.erase(it);
                // if the inner map becomes empty you may optionally erase the outer entry:
                if (itOuter->second.empty()) pending_.erase(itOuter);
            }
        }
    }

    // Mark as cancelled so any heap entries for this (receiverId,pktId) are ignored when popped
    {
        std::lock_guard<std::mutex> lk(heapMutex_);
        cancelledPackets_.insert(makeKey(receiverId, pktId));
    }

    // Notify heap thread in case the top was cancelled (so it can pop quickly)
    heapCv_.notify_one();

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
