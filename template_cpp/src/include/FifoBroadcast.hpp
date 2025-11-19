#pragma once
#include <string>
#include <fstream>
#include <functional>
#include <netdb.h>
#include <atomic>
#include <unordered_map>
#include <map>
#include <set>

#include "PerfectLink.hpp"

class FifoBroadcast {
public:
    FifoBroadcast(unsigned long myProcessId, std::unordered_map<unsigned long, std::pair<in_addr_t, unsigned short>> hostMapById, std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort, std::string logPath);

    ~FifoBroadcast();
    void stop();
    void broadcast(const std::string& message);

private:
    unsigned long myProcessId_;
    std::unordered_map<unsigned long, std::pair<in_addr_t, unsigned short>> hostMapById_;
    std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort_;
    std::string logPath_;
    std::atomic<bool> running_;
    std::ofstream logFile_;
    std::function<void(unsigned long, unsigned long)> deliverCallback_;
    std::atomic<unsigned long> msgSeqNumber_;
    // std::map<unsigned long, std::set<unsigned long>> delivered_; // senderId: [messageIds] // TODO - use this instead of the other datastruct for delivered_
    std::set<Message> pendingDelivery_;  // messages ordered first by processId then by messageId // TODO - this will probably have to change
    unsigned long numProcesses_; // at least NumProcesses/2 + 1 are correct
    std::unordered_map<Message, std::set<unsigned long>> acknowledged_; // Message: [processIdsOfAcks]
    std::set<Message> delivered_;
    std::unique_ptr<PerfectLink> perfectLinkInstance_; // processId: PerfectLink where processId is the receiver 

    void sendMessageToAllProcesses(const Message& message);
    void receivedMessage(const Message& message, const unsigned long& senderId);
    void deliverMessage(Message message);
    bool haveDelivered(Message message);
    bool canDeliver(Message message);

};