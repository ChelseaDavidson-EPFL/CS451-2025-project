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

    void sendMessage(const std::string& message);
    void flushMessages();

private:
    unsigned long processId_;
    unsigned short processPort_;
    in_addr_t processIp_;
    unsigned long receiverId_;
    in_addr_t receiverIp_;
    unsigned short receiverPort_;
    std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort_;
    std::string logPath_;
    int sockfd_;
    sockaddr_in localAddr_;
    std::atomic<bool> running_;
    std::thread receiverThread_;
    std::thread resendThread_;

    using Clock = std::chrono::steady_clock;

    struct Packet {
        unsigned long id;
        std::string messages;
        Clock::time_point lastSentTime = Clock::now() - std::chrono::milliseconds(200); // So that it sends the message immediately in sendMessageLoop
    };

    std::string partialPacket_;
    unsigned long maxPacketSize_ = 1000; // Recommend 1500 bytes packet size - left room for elements at the front
    std::unordered_map<unsigned long, Packet> pending_;

    std::mutex pendingMapMutex;

    unsigned long packetSeqNumber_;
    unsigned long msgSeqNumber_;
    std::function<void(unsigned long, unsigned long)> deliverCallback_;
    std::map<unsigned long, std::set<unsigned long>> delivered_; // Outer key: processId, Inner pair: message sequence number (id), message content
    std::map<unsigned long, unsigned long> firstMissingPacketId_; // Outer key: processId, Inner value: firstMissingMessage_

    void initBroadcaster();
    void initReceiver();
    void addFinishedPacketToPending();
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
    void stop();
    void printDelivered() const;
};