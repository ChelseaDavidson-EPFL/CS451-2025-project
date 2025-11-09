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

struct Message {
    unsigned long processId;
    unsigned long messageId;
    std::string message;

    bool operator==(const Message& other) const {
        return processId == other.processId &&
               messageId == other.messageId &&
               message == other.message;
    }

    bool operator<(const Message& other) const {
        if (processId == other.processId)
            return messageId < other.messageId;
        return processId < other.processId;
    }
};

// Make Message hashable so it can be used as a key in a map
namespace std {
    template<>
    struct hash<Message> {
        std::size_t operator()(const Message& m) const noexcept {
            std::size_t h1 = std::hash<unsigned long>{}(m.processId);
            std::size_t h2 = std::hash<unsigned long>{}(m.messageId);
            std::size_t h3 = std::hash<std::string>{}(m.message);
            return h1 ^ (h2 << 1) ^ (h3 << 2);
        }
    };
}


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
    std::map<unsigned long, std::set<unsigned long>> delivered_; // senderId: [messageIds]
    std::set<Message> pendingDelivery_;  // messages ordered first by processId then by messageId // TODO - this will probably have to change
    unsigned long numProcesses_; // at least NumProcesses/2 + 1 are correct
    std::unordered_map<Message, std::set<unsigned long>> acknowledged_; // Message: [processIdsOfAcks]

    std::unique_ptr<PerfectLink> perfectLinkInstance_; // processId: PerfectLink where processId is the receiver 

    void sendMessageToAllProcesses(Message message);
};