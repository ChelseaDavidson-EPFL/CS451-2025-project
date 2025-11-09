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
    PerfectLink(unsigned long myProcessId, in_addr_t myProcessIp, unsigned short myProcessPort, std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort, std::unordered_map<unsigned long, std::pair<in_addr_t, unsigned short>> hostMapById, std::string logPath = "");

    ~PerfectLink();
    void stop();

    void sendMessage(const std::string& message, unsigned long receiverId);

private:
    unsigned long myProcessId_;
    unsigned short myProcessPort_;
    in_addr_t myProcessIp_;
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

    std::unordered_map<unsigned long, std::string> partialPacket_; // receiverId, partialPacket
    std::unordered_map<unsigned long, Clock::time_point> lastPacketUpdateTime_; // So we can finish packet after enough time has past

    const unsigned long maxMessagesPerPacket_ = 8;
    std::unordered_map<unsigned long, std::atomic<unsigned long>> numMessagesInPacket_;
    const std::chrono::milliseconds maxPacketUpdateTimePast_ = std::chrono::milliseconds(500); // 500ms
    std::unordered_map<unsigned long, Packet> pending_; // Packet has a receiverId so each packet can go to a different receiver process

    std::mutex pendingMapMutex_;
    std::mutex partialPacketMutex_;

    std::unordered_map<unsigned long, std::atomic<unsigned long>> packetSeqNumber_; // receiverId, seqNum
    std::unordered_map<unsigned long, std::atomic<unsigned long>> msgSeqNumber_; // receiverId, seqNum
    std::function<void(unsigned long, unsigned long)> deliverCallback_;
    std::map<unsigned long, std::set<unsigned long>> delivered_; // Outer key: senderId, Inner pair: message sequence number (id), message content
    std::map<unsigned long, unsigned long> firstMissingPacketId_; // Outer key: senderId, Inner value: firstMissingMessage_

    void initReceiverBroadcaster();
    void addMessageToPacket(const std::string& messagePayload, unsigned long receiverId) ;
    void flushMessages(unsigned long receiverId);
    void addPacketToPending(const std::string &packetStr, unsigned long receiverId);
    void flushPendingPacketIfReady(unsigned long receiverId);
    void flushPendingPacketsIfReady();
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