#pragma once

#include <netdb.h>
#include <atomic>
#include <thread>
#include <unordered_map>
#include <functional>
#include <map>
#include <list>
#include <mutex>
#include <ctime>
#include <set>

class PerfectLink {
public:
    PerfectLink(unsigned long processId, in_addr_t processIp, unsigned short processPort, unsigned long receiverId, in_addr_t receiverIp, unsigned short receiverPort, std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort, std::string logPath);

    ~PerfectLink();
    void stop();

    void sendMessage(const std::string& message);

private:
    unsigned long processId_;
    unsigned short processPort_;
    in_addr_t processIp_;
    unsigned long receiverId_;
    in_addr_t receiverIp_;
    unsigned short receiverPort_;
    std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort_; // Port: (processId, ipAddress)
    std::string logPath_;
    std::ofstream logFile_;
    int sockfd_;
    sockaddr_in localAddr_;
    std::atomic<bool> running_;
    std::thread receiverThread_;
    std::thread resendThread_;

    size_t writeCounter_ = 0; // To log in batches
    size_t linesInLogBatch_ = 1000;

    using Clock = std::chrono::steady_clock;

    struct Packet {
        unsigned long id;
        std::string messages;
        Clock::time_point lastSentTime = Clock::now() - std::chrono::milliseconds(100); // So that it sends the message immediately in sendMessageLoop
    };

    std::string partialPacket_;
    Clock::time_point lastPacketUpdateTime_ = Clock::now(); // So we can finish packet after enough time has past

    const unsigned long maxMessagesPerPacket_ = 8;
    std::atomic<unsigned long> numMessagesInPacket_ = 0;
    const std::chrono::milliseconds maxPacketUpdateTimePast_ = std::chrono::milliseconds(500); // 500ms
    std::unordered_map<unsigned long, Packet> pending_;

    std::mutex pendingMapMutex_;
    std::mutex partialPacketMutex_;

    std::atomic<unsigned long> packetSeqNumber_;
    std::atomic<unsigned long> msgSeqNumber_;
    std::function<void(unsigned long, unsigned long)> deliverCallback_;
    std::map<unsigned long, std::set<unsigned long>> delivered_; // Outer key: processId, Inner pair: message sequence number (id), message content
    std::map<unsigned long, unsigned long> firstMissingPacketId_; // Outer key: processId, Inner value: firstMissingMessage_

    void initBroadcaster();
    void initReceiver();
    void addMessageToPacket(const std::string& messagePayload);
    void flushMessages();
    void addPacketToPending(const std::string &packetStr);
    void flushPendingPacketIfReady();
    void sendPacketLoop();
    bool findPacketToSend(Packet& packet);
    void sendRaw(const std::string& payload, in_addr_t ip, unsigned short port);
    void receiverLoop();
    unsigned long parsePacketPayloadId(const std::string& packetIdStr);
    unsigned long parseMessagePayloadId(const std::string& messageIdStr);
    bool deliverMessages(unsigned long senderId, const std::string& messages);
    bool deliverMessage(unsigned long senderId, const std::string& messagePayload);
    void sendAck(in_addr_t destIp, unsigned short destPort, unsigned long packetId);
    void handleAck(unsigned long msgId);
    void logDelivery(unsigned long senderId, unsigned long messageId);
    void logSendPacket(const std::string& packet);
    void logSendMessage(const std::string& messageId);
    void printDelivered() const;
};