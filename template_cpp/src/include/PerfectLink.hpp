#pragma once
#include <netdb.h>
#include <atomic>
#include <thread>
#include <unordered_map>
#include <functional>
#include <map>
#include <list>
#include <mutex>
#include <condition_variable>
#include <ctime>
#include <set>
#include <variant>

enum class MessagePayloadType : uint8_t {
    INT      = 0x01,
    INT_LIST = 0x02
};

#pragma pack(push, 1)

enum class PacketType : uint8_t {
    DATA = 0,
    ACK  = 1
};

struct DataPacketHeader {
    uint8_t type;
    unsigned long id;
    uint32_t numMessages; 
};

struct AckPacketHeader {
    uint8_t  type;        // PacketType::ACK
    uint32_t numAcks;
};

struct MessageHeader {
    uint64_t origSenderId;
    uint64_t messageId;
    uint8_t  payloadType;   // MessagePayloadType
    uint32_t payloadSize;   // bytes following
};

#pragma pack(pop)

using MessagePayload = std::variant<
    uint64_t,
    std::vector<uint64_t>
>;

struct Message {
    uint64_t origSenderId;
    uint64_t messageId;
    MessagePayload content;

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

template <typename T>
void appendBytes(std::vector<uint8_t>& buf, const T& val) {
    static_assert(std::is_trivially_copyable_v<T>);
    const uint8_t* p = reinterpret_cast<const uint8_t*>(&val);
    buf.insert(buf.end(), p, p + sizeof(T));
}

// Make Message hashable so it can be used as a key in a map
namespace std {
    template<>
    struct hash<Message> {
        size_t operator()(const Message& m) const noexcept {
            size_t seed = 0;

            auto hashCombine = [&](size_t h) noexcept {
                seed ^= h + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
            };

            hashCombine(std::hash<uint64_t>{}(m.origSenderId));
            hashCombine(std::hash<uint64_t>{}(m.messageId));

            std::visit(
                [&](const auto& payload) noexcept {
                    using T = std::decay_t<decltype(payload)>;

                    if constexpr (std::is_same_v<T, uint64_t>) {
                        hashCombine(
                            std::hash<uint8_t>{}(
                                static_cast<uint8_t>(MessagePayloadType::INT)
                            )
                        );
                        hashCombine(std::hash<uint64_t>{}(payload));
                    }
                    else if constexpr (std::is_same_v<T, std::vector<uint64_t>>) {
                        hashCombine(
                            std::hash<uint8_t>{}(
                                static_cast<uint8_t>(MessagePayloadType::INT_LIST)
                            )
                        );
                        hashCombine(std::hash<size_t>{}(payload.size()));
                        for (uint64_t v : payload) {
                            hashCombine(std::hash<uint64_t>{}(v));
                        }
                    }
                },
                m.content
            );

            return seed;
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
        std::vector<uint8_t> payload;
        std::vector<std::pair<unsigned long, uint64_t>> messages; // For logging etc
        Clock::time_point lastSentTime = Clock::now() - std::chrono::milliseconds(100); // So that it sends the message immediately in sendMessageLoop
    };

    // comparator that sorts by lastSentTime (oldest first)
    struct PacketComparator {
        bool operator()(Packet const& a, Packet const& b) const {
            return a.lastSentTime < b.lastSentTime;
        }
    };

    using Key = std::pair<unsigned long, unsigned long>; // (receiverId, packetId)

    struct KeyHash {
        size_t operator()(const Key& k) const noexcept {
            return std::hash<unsigned long>()(k.first) ^ (std::hash<unsigned long>()(k.second) << 1);
        }
    };

    std::unordered_map<unsigned long, Packet> partialPacket_; // receiverId, partialPacket
    std::unordered_map<unsigned long, Clock::time_point> lastPacketUpdateTime_; // So we can finish packet after enough time has past

    const unsigned long maxMessagesPerPacket_ = 8;
    std::unordered_map<unsigned long, std::atomic<unsigned long>> numMessagesInPacket_;
    const std::chrono::milliseconds maxPacketUpdateTimePast_ = std::chrono::milliseconds(500); // 500ms
    std::set<Packet, PacketComparator> orderedPendingPackets_;
    std::unordered_map<Key, std::set<Packet>::iterator, KeyHash> pendingIndex_;

    std::unordered_map<unsigned long, std::vector<unsigned long>> pendingAcks_;
    const std::chrono::milliseconds maxAckUpdateTimePast_ = std::chrono::milliseconds(500); // 500ms
    std::unordered_map<unsigned long, Clock::time_point> lastAckUpdateTime_; // lastAckUpdateTime_[receiverId] = last time ack was updated
    std::unordered_map<unsigned long, std::atomic<unsigned long>> numAcksInBatch_; // numAcksInBatch_[receiverId] = number of acks we have in that batch
    const unsigned long maxAcksPerBatch_ = 8;


    std::mutex pendingMapMutex_;
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
    void addMessageToPacket(const std::vector<uint8_t>& msgBytes, const Message& message, unsigned long receiverId);
    void flushMessages(unsigned long receiverId);
    void addPacketToPending(const Packet packet, unsigned long receiverId);
    void flushPendingPacketIfReady(unsigned long receiverId);
    void flushPendingPacketsIfReady();
    std::vector<uint8_t> serializeMessage(const Message& msg);
    void sendPacketLoop();
    bool findPacketToSend(Packet& packet);
    void sendRaw(const std::vector<uint8_t>& payload, in_addr_t ip, unsigned short port);
    void receiverLoop();
    bool deliverMessages(unsigned long senderId, const uint8_t* data, size_t len) ;
    void sendAck(in_addr_t destIp, unsigned short destPort, unsigned long packetId);
    void flushAckBatch(in_addr_t destIp, unsigned short destPort);
    void handleAck(const unsigned long receiverId, const unsigned long pktId);
    void handlePacketAck(unsigned long receiverId, const Packet& acknowledgedPacket);
    void logDelivery(unsigned long senderId, unsigned long messageId);
    void logSendPacket(const Packet& packet);
    void logSendMessage(unsigned long senderId, unsigned long messageId);
    void printDelivered() const;
};