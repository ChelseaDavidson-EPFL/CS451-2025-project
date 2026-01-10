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

struct Ack {
    unsigned long shotNumber;
    unsigned long proposalNumber;
};

struct Nack {
    unsigned long shotNumber;
    unsigned long proposalNumber;
    std::set<unsigned long> proposedValue;
};

struct Proposal {
    unsigned long shotNumber;
    unsigned long proposalNumber;
    std::set<unsigned long> proposedValue;
};


class LatticeAgreement {
public:
    LatticeAgreement(unsigned long myProcessId, in_addr_t myProcessIp, unsigned short myProcessPort, std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort, std::unordered_map<unsigned long, std::pair<in_addr_t, unsigned short>> hostMapById, unsigned long maxDistinctElements, std::string logPath = "");

    ~LatticeAgreement();

    void propose(std::set<unsigned long> proposal, unsigned long shotNumber);

    // Application-layer wait
    void waitForDecision();

    void stop();

private:
    const unsigned long myProcessId_;
    const in_addr_t myProcessIp_;
    const unsigned short myProcessPort_;
    std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort_; // Port: (processId, ipAddress)
    std::unordered_map<unsigned long, std::pair<in_addr_t, unsigned short>> hostMapById_; // Id: (ip, port)
    const unsigned long maxDistinctElements_;
    std::string logPath_;
    std::ofstream logFile_;
    std::atomic<bool> running_;
    unsigned long majority_;

    std::unique_ptr<PerfectLink> perfectLinkInstance_;

    // Message vars
    std::atomic<unsigned long> msgSeqNumber_;

    // Application level vars
    std::mutex decisionMutex_;
    std::condition_variable decisionCv_;
    bool decisionReady_ = false;
    
    // Logging vars
    size_t writeCounter_ = 0; // To log in batches
    size_t linesInLogBatch_ = 100; // TODO - change this back to 1000 after debugging
    std::map<unsigned long, std::set<unsigned long>> decidedValues_; // Only stores max of p elements
    unsigned long nextShotToLog_ = 1;
    std::mutex decisionOrderMutex_;

    // Concurrency primitives
    std::mutex shotsMutex_;
    std::mutex loggingMutex_;     // protects logFile_ writes

    struct ShotState {
        // Proposer
        bool active = false;
        unsigned long proposalNumber = 0;
        unsigned long ackCount = 0;
        unsigned long nackCount = 0;
        std::set<unsigned long> proposedValue;

        // Acceptor
        std::set<unsigned long> acceptedValue;

        // Decision
        bool decided = false;
    };

    std::unordered_map<unsigned long, ShotState> shots_; // Only stores max of p elements

    // Functions
    void broadcastProposal(Proposal proposal);
    void handleProposal(Proposal proposal, unsigned long senderId);
    void handleAck(Ack ack);
    void handleNack(Nack nack);
    void checkIfNeedNewProposal(unsigned long shotNumber);
    void tryDecide(unsigned long shotNumber);
    void decide(unsigned long shotNumber, std::set<unsigned long> proposedValue);
    void logDecision(std::set<unsigned long> proposedValue);
    void sendAckMsg(Ack ack, unsigned long receiverId);
    void sendNackMsg(Nack nack, unsigned long receiverId);
    Message createProposalMsg(Proposal proposal);
    void sendProposalMsg(Message proposalMessage, unsigned long receiverId);
    void receivedMessage(const Message& message, const unsigned long& senderId);

    // Helper functions
    unsigned long parseNumberInParens(const std::string& content);
    unsigned long parseNumberInBrackets(const std::string& content);
    std::set<unsigned long> parseValueSet(const std::string& content, size_t startPos);
};