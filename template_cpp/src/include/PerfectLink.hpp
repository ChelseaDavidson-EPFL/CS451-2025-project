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

      struct Message {
        unsigned long id;
        std::string message;
        Clock::time_point lastSentTime = Clock::now() - std::chrono::milliseconds(200); // So that it sends the message immediately in sendMessageLoop
    };

    std::unordered_map<unsigned long, Message> pending_;

    std::mutex pendingMapMutex;

    unsigned long seqNumber_;
    std::function<void(unsigned long, const std::string&)> deliverCallback_;
    std::map<unsigned long, std::set<unsigned long>> delivered_; // Outer key: processId, Inner pair: message sequence number (id), message content
    std::map<unsigned long, unsigned long> firstMissingMessageId_; // Outer key: processId, Inner value: firstMissingMessage_

    void initBroadcaster();
    void initReceiver();
    void addMessageToPending(Message message);
    void sendMessageLoop();
    Message* findMessageToSend();
    void sendRaw(const std::string& payload, in_addr_t ip, unsigned short port);
    void receiverLoop();
    void sendAck(in_addr_t destIp, unsigned short destPort, unsigned long msgId);
    void handleAck(unsigned long msgId);
    void logDelivery(unsigned long senderId, const std::string& message);
    void logSend(const std::string& message);
    void stop();
    void printDelivered() const;
};