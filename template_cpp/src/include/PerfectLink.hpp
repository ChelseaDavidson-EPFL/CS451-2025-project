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
#include <vector>
#include <cstdint>
#include <fstream>

// Binary helpers:
static void write_u64(std::vector<uint8_t>& b, uint64_t v) {
    for (int i = 7; i >= 0; --i) b.push_back((v >> (i*8)) & 0xff);
}

static void write_u32(std::vector<uint8_t>& b, uint32_t v) {
    for (int i = 3; i >= 0; --i) b.push_back((v >> (i*8)) & 0xff);
}

static uint64_t read_u64(const uint8_t*& p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v = (v << 8) | *p++;
    return v;
}

static uint32_t read_u32(const uint8_t*& p) {
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i) v = (v << 8) | *p++;
    return v;
}

struct Message {
    unsigned long origSenderId;
    unsigned long messageId;
    std::vector<uint8_t> content; // Set of bytes (8 chars)

    bool operator==(const Message& other) const {
        return origSenderId == other.origSenderId &&
               messageId == other.messageId &&
               content == other.content;
    }
};

class PerfectLink {
public:
    PerfectLink(unsigned long myProcessId,
                in_addr_t myProcessIp,
                unsigned short myProcessPort,
                std::unordered_map<unsigned short,
                std::pair<unsigned long, in_addr_t>> hostMapByPort,
                std::unordered_map<unsigned long,
                std::pair<in_addr_t, unsigned short>> hostMapById,
                std::string logPath = "");

    ~PerfectLink();
    void stop();

    void setDeliverCallback(std::function<void(const Message&, unsigned long)> cb);
    void setDeliverCallbackToDefault();

    std::vector<unsigned char> toBytes(unsigned long value);

    void sendMessage(const Message& message, unsigned long receiverId);

private:
    using Clock = std::chrono::steady_clock;

    struct Packet {
        unsigned long receiverId;
        unsigned long id;
        std::vector<uint8_t> payload;
        Clock::time_point lastSentTime =
            Clock::now() - std::chrono::milliseconds(100);
    };

    const unsigned long myProcessId_;
    const unsigned short myProcessPort_;
    const in_addr_t myProcessIp_;

    std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort_;
    std::unordered_map<unsigned long, std::pair<in_addr_t, unsigned short>> hostMapById_;

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

    std::unordered_map<unsigned long, std::vector<Message>> partialPacket_;
    std::unordered_map<unsigned long, Clock::time_point> lastPacketUpdateTime_;
    std::unordered_map<unsigned long, std::atomic<unsigned long>> packetSeqNumber_;
    std::unordered_map<unsigned long, std::atomic<unsigned long>> numMessagesInPacket_;
    std::unordered_map<unsigned long,
        std::unordered_map<unsigned long, Packet>> pending_;

    std::map<unsigned long, std::set<unsigned long>> delivered_;
    std::map<unsigned long, unsigned long> firstMissingPacketId_;

    std::mutex pendingMapMutex_;
    std::mutex partialPacketMutex_;
    std::mutex loggingMutex_;

    std::function<void(const Message&, unsigned long)> deliverCallback_;

    const unsigned long maxMessagesPerPacket_ = 8;
    const std::chrono::milliseconds maxPacketUpdateTimePast_ =
        std::chrono::milliseconds(500);

    /* ---------- Internal helpers ---------- */
    void initReceiverBroadcaster();
    void receiverLoop();
    void sendPacketLoop();

    void addMessageToPacket(const Message& msg, unsigned long receiverId);
    void flushMessages(unsigned long receiverId);
    void addPacketToPending(const std::vector<uint8_t>& payload,
                            unsigned long receiverId);
    void flushPendingPacketIfReady(unsigned long receiverId);
    void flushPendingPacketsIfReady();
        

    bool findPacketToSend(Packet& out);
    void sendRaw(const std::vector<uint8_t>& data,
                 in_addr_t ip, unsigned short port);

    void sendAck(in_addr_t ip, unsigned short port, uint64_t packetId);
    void handleAck(unsigned long receiverId, unsigned long packetId);

    std::vector<uint8_t> serializePacket(unsigned long packetId,
                                         const std::vector<Message>& msgs);
    bool deserializeAndDeliver(unsigned long senderId,
                                uint64_t packetId,
                                const uint8_t* data,
                                size_t len);
    
    void logDelivery(unsigned long senderId, unsigned long messageId);
    void logSendPacket(const std::vector<uint8_t>& payload);

    void printDelivered() const;
};
