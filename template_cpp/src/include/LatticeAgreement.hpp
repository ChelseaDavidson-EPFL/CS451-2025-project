#include <atomic>
#include <mutex> 
#include <fstream>
#include <set>
#include <unordered_map>
#include <unistd.h>
#include <netdb.h>
#include <thread>
#include <functional>
#include <map>
#include <list>
#include <ctime>
#include <condition_variable>


#include "PerfectLink.hpp"


struct Nack {
    unsigned long proposalNumber;
    std::set<unsigned long> proposedValue;
};

struct Proposal {
    unsigned long proposalNumber;
    std::set<unsigned long> proposedValue;
};


class LatticeAgreement {
public:
    LatticeAgreement(unsigned long myProcessId, in_addr_t myProcessIp, unsigned short myProcessPort, std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort, std::unordered_map<unsigned long, std::pair<in_addr_t, unsigned short>> hostMapById, std::string logPath = "");

    ~LatticeAgreement();

    void propose(std::set<unsigned long> proposal);

    // Application-layer wait
    void waitForDecision();

    void stop();

private:
    const unsigned long myProcessId_;
    const in_addr_t myProcessIp_;
    const unsigned short myProcessPort_;
    std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort_; // Port: (processId, ipAddress)
    std::unordered_map<unsigned long, std::pair<in_addr_t, unsigned short>> hostMapById_; // Id: (ip, port)
    std::string logPath_;
    std::ofstream logFile_;
    std::atomic<bool> running_;
    unsigned long majority_;

    std::unique_ptr<PerfectLink> perfectLinkInstance_;

    // Message vars
    unsigned long msgSeqNumber_;

    // Application level vars
    std::mutex decisionMutex_;
    std::condition_variable decisionCv_;
    bool decisionReady_ = false;
    
    // Logging vars
    size_t writeCounter_ = 0; // To log in batches
    size_t linesInLogBatch_ = 1000;
    size_t linesInLogBatchBroadcast_ = 100;

    // Concurrency primitives
    std::mutex stateMutex_;   // protects pendingDelivery_, acknowledged_, delivered_, nextExpectedMsgId_
    std::mutex loggingMutex_;     // protects logFile_ writes

    // Proposer vars
    bool active_;
    unsigned long ackCount_;
    unsigned long nackCount_;
    unsigned long activeProposalNumber_;
    std::set<unsigned long> proposedValue_;

    // Acceptor vars
    std::set<unsigned long> acceptedValue_;

    // Functions
    void broadcastProposal(Proposal proposal);
    void handleProposal(Proposal proposal, unsigned long senderId);
    void handleAck(unsigned long proposalNumber);
    void handleNack(Nack nack);
    void checkIfNeedNewProposal();
    void tryDecide();
    void decide(std::set<unsigned long> proposedValue);
    void logDecision(std::set<unsigned long> proposedValue);
    void sendAckMsg(unsigned long proposalNumber, unsigned long receiverId);
    void sendNackMsg(Nack nack, unsigned long receiverId);
    void sendProposalMsg(Proposal proposal, unsigned long receiverId);
    void receivedMessage(const Message& message, const unsigned long& senderId);

    // Helper functions
    unsigned long parseNumberInParens(const std::string& content);
    std::set<unsigned long> parseValueSet(const std::string& content, size_t startPos);
};