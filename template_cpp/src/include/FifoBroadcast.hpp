// #pragma once
// #include <string>
// #include <fstream>
// #include <functional>
// #include <netdb.h>
// #include <atomic>
// #include <unordered_map>
// #include <map>
// #include <set>
// #include <mutex> 

// #include "PerfectLink.hpp"

// class FifoBroadcast {
// public:
//     FifoBroadcast(unsigned long myProcessId, std::unordered_map<unsigned long, std::pair<in_addr_t, unsigned short>> hostMapById, std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort, std::string logPath);

//     ~FifoBroadcast();
//     void stop();
//     void broadcast(const MessagePayload& message);

// private:
//     unsigned long myProcessId_;
//     std::unordered_map<unsigned long, std::pair<in_addr_t, unsigned short>> hostMapById_;
//     std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort_;
//     std::string logPath_;
//     std::atomic<bool> running_;
//     std::ofstream logFile_;
//     std::function<void(unsigned long, unsigned long)> deliverCallback_;
//     std::atomic<unsigned long> msgSeqNumber_;
//     // std::map<unsigned long, std::set<unsigned long>> delivered_; // senderId: [messageIds] // TODO - use this instead of the other datastruct for delivered_
//     std::unordered_map<unsigned long, std::set<Message>> pendingDelivery_;  // messages ordered first by processId then by messageId // TODO - this will probably have to change
//     unsigned long numProcesses_; // at least NumProcesses/2 + 1 are correct
//     std::unordered_map<Message, std::set<unsigned long>> acknowledged_; // Message: [processIdsOfAcks]
//     std::unordered_map<unsigned long, unsigned long> nextExpectedMsgId_;   // nextExpectedMsgId_[p] = smallest messageId not yet delivered from process p
//     std::unique_ptr<PerfectLink> perfectLinkInstance_; // processId: PerfectLink where processId is the receiver 

//     // Logging vars
//     size_t writeCounter_ = 0; // To log in batches
//     size_t linesInLogBatch_ = 1000;
//     size_t linesInLogBatchBroadcast_ = 100;

//     // Concurrency primitives
//     std::mutex stateMutex_;   // protects pendingDelivery_, acknowledged_, delivered_, nextExpectedMsgId_
//     std::mutex loggingMutex_;     // protects logFile_ writes

//     void sendMessageToAllProcesses(const Message& message);
//     void receivedMessage(const Message& message, const unsigned long& senderId);
//     void receivedAck(const Message& message, const unsigned long& senderId);
//     void deliverMessage(const Message& m);
//     bool haveDelivered(Message message);
//     bool canDeliver(const Message &m);
//     void tryDeliverPending(unsigned long processId);

//     void logBroadcast(unsigned long messageId);
//     void logDelivery(Message message);

//     void printDelivered();
//     void printPendingForSender(unsigned long senderId);
//     void printAcknowledged();

// };