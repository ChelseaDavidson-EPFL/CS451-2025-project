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

      struct Message {
        std::string id;
        std::string message;
        clock_t lastSentTime = 0;
    };

    std::unordered_map<std::string, Message> pending_;

    std::mutex mapMutex;

    int seqNumber_;
    std::function<void(unsigned long, const std::string&)> deliverCallback_;
    std::map<unsigned long, std::list<std::pair<unsigned long, std::string>>> delivered_; // Outer key: processId, Inner pair: message sequence number (id), message content
    std::map<unsigned long, std::list<std::pair<unsigned long, std::string>>> pendingDelivery_; 

    void initBroadcaster();
    void initReceiver();
    void sendMessageLoop();
    void sendRaw(const std::string& payload, in_addr_t ip, unsigned short port);
    void receiverLoop();
    void sendAck(in_addr_t destIp, unsigned short destPort, const std::string& msgId);
    void logDelivery(unsigned long senderId, const std::string& message);
    void logSend(const std::string& message);
    void stop();
    void printDelivered() const;
    void printPendingDelivery() const;



};