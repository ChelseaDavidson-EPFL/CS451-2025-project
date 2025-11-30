#pragma once

#include <netdb.h>
#include <atomic>
#include <thread>
#include <unordered_map>
#include <queue>        // for std::priority_queue
#include <functional>
#include <map>
#include <list>
#include <mutex>
#include <condition_variable>
#include <ctime>
#include <set>
#include <unordered_set>

struct Message {
    unsigned long origSenderId; // Id of original sender
    unsigned long messageId;
    std::string content;

    bool operator==(const Message& other) const {
        return origSenderId == other.origSenderId &&
               messageId == other.messageId &&
               content == other.content;
    }

    bool operator<(const Message& other) const {
        if (origSenderId == other.origSenderId)
            return messageId < other.messageId;
        return origSenderId < other.origSenderId;
    }
};

// Make Message hashable so it can be used as a key in a map
namespace std {
    template<>
    struct hash<Message> {
        std::size_t operator()(const Message& m) const noexcept {
            std::size_t h1 = std::hash<unsigned long>{}(m.origSenderId);
            std::size_t h2 = std::hash<unsigned long>{}(m.messageId);
            std::size_t h3 = std::hash<std::string>{}(m.content);
            return h1 ^ (h2 << 1) ^ (h3 << 2);
        }
    };
}

class PerfectLink {
public:
    PerfectLink(unsigned long myProcessId, in_addr_t myProcessIp, unsigned short myProcessPort, std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort, std::unordered_map<unsigned long, std::pair<in_addr_t, unsigned short>> hostMapById, std::string logPath = "");

    ~PerfectLink();
    void stop();

    void setDeliverCallback(std::function<void(Message, unsigned long)> cb);
    void setDeliverCallbackToDefault();
    void setAckCallback(std::function<void(Message, unsigned long)> cb);
    void sendMessage(const Message& message, unsigned long receiverId);

private:
    const unsigned long myProcessId_;
    const unsigned short myProcessPort_;
    const in_addr_t myProcessIp_;
    std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort_; // Port: (processId, ipAddress)
    std::unordered_map<unsigned long, std::pair<in_addr_t, unsigned short>> hostMapById_; // Id: (ip, port)
    std::string logPath_;
    std::ofstream logFile_;
    bool loggingToFile_ = true;
    int sockfd_;
    sockaddr_in localAddr_;
    std::atomic<bool> running_;
    std::thread receiverThread_;
    std::thread resendThread_;

    size_t writeCounter_ = 0; // To log in batches
    size_t linesInLogBatch_ = 1000;

    using Clock = std::chrono::steady_clock;

    struct Packet {
        unsigned long receiverId;
        unsigned long id;
        std::string messages;
        Clock::time_point lastSentTime = Clock::now() - std::chrono::milliseconds(100); // So that it sends the message immediately in sendMessageLoop
    };

    // resend heap entry
    struct ResendTask {
        std::chrono::steady_clock::time_point nextSendTime;
        unsigned long receiverId;
        unsigned long pktId;

        bool operator>(ResendTask const& o) const {
            return nextSendTime > o.nextSendTime;
        }
    };

    // Priority queue (min-heap) of resend tasks
    std::priority_queue<ResendTask, std::vector<ResendTask>, std::greater<ResendTask>> resendHeap_;
    std::condition_variable heapCv_;      // notifies resend thread of new tasks or earlier deadlines

    std::chrono::milliseconds retransmitInterval_{100}; // base retransmit delay (tunable)

    std::unordered_map<unsigned long, std::string> partialPacket_; // receiverId, partialPacket
    std::unordered_map<unsigned long, Clock::time_point> lastPacketUpdateTime_; // So we can finish packet after enough time has past

    const unsigned long maxMessagesPerPacket_ = 8;
    std::unordered_map<unsigned long, std::atomic<unsigned long>> numMessagesInPacket_;
    const std::chrono::milliseconds maxPacketUpdateTimePast_ = std::chrono::milliseconds(500); // 500ms
    std::unordered_map<unsigned long, std::unordered_map<unsigned long, Packet>> pending_; // [receiverId][packetId]: Packet

    std::unordered_map<unsigned long, std::vector<unsigned long>> pendingAcks_;
    const std::chrono::milliseconds maxAckUpdateTimePast_ = std::chrono::milliseconds(500); // 500ms
    std::unordered_map<unsigned long, Clock::time_point> lastAckUpdateTime_; // lastAckUpdateTime_[receiverId] = last time ack was updated
    std::unordered_map<unsigned long, std::atomic<unsigned long>> numAcksInBatch_; // numAcksInBatch_[receiverId] = number of acks we have in that batch
    const unsigned long maxAcksPerBatch_ = 8;


    std::mutex pendingMapMutex_;
    std::mutex heapMutex_;                // protects resendHeap_
    std::mutex partialPacketMutex_;
    std::mutex loggingMutex_;
    std::mutex pendingCvMutex_;
    std::condition_variable pendingCv_;
    std::mutex ackMutex_;

    std::unordered_map<unsigned long, std::atomic<unsigned long>> packetSeqNumber_; // receiverId, seqNum
    std::function<void(Message, unsigned long)> deliverCallback_;
    std::function<void(Message, unsigned long)> ackCallback_;
    std::map<unsigned long, std::set<unsigned long>> delivered_; // Outer key: senderId, Inner pair: message sequence number (id), message content
    std::map<unsigned long, unsigned long> firstMissingPacketId_; // Outer key: senderId, Inner value: firstMissingMessage_

    void initReceiverBroadcaster();
    void addMessageToPacket(const std::string& messagePayload, unsigned long receiverId) ;
    void flushMessages(unsigned long receiverId);
    void addPacketToPending(const std::string &packetStr, unsigned long receiverId);
    void flushPartialPacketIfReady(unsigned long receiverId);
    void flushPartialPacketsIfReady();
    void sendPacketLoop();
    void sendRaw(const std::string& payload, in_addr_t ip, unsigned short port);
    void receiverLoop();
    unsigned long parsePacketPayloadId(const std::string& packetIdStr);
    unsigned long parseMessagePayloadId(const std::string& messageIdStr);
    bool deliverMessages(unsigned long senderId, const std::string& messages);
    bool deliverMessage(unsigned long senderId, const std::string& messagePayload);
    void sendAck(in_addr_t destIp, unsigned short destPort, unsigned long packetId);
    void flushAckBatch(in_addr_t destIp, unsigned short destPort);
    void handleAck(const unsigned long receiverId, const unsigned long pktId);
    void handlePacketAck(unsigned long receiverId, Packet acknowledgedPacket);
    void logDelivery(unsigned long senderId, unsigned long messageId);
    void logSendPacket(const std::string& packet);
    void logSendMessage(const std::string& messageIds);
    void printDelivered() const;
};