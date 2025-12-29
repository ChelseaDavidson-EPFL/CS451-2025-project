#include <atomic>
#include <mutex> 
#include <fstream>
#include <set>

struct Nack {
    unsigned long proposalNumber;
    std::set<unsigned long> proposedValue;
};

struct Proposal {
    unsigned long proposorId;
    unsigned long proposalNumber;
    std::set<unsigned long> proposedValue;
};


class LatticeAgreement {
public:
    LatticeAgreement(std::string logPath);

    ~LatticeAgreement();

    void propose(std::set<unsigned long> proposal);

    void stop();

private:
    std::string logPath_;
    std::atomic<bool> running_;
    std::ofstream logFile_;
    unsigned long majority_;
    
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
    void handleProposal(Proposal proposal);
    void handleAck(unsigned long proposalNumber);
    void handleNack(Nack nack);
    void triggerNewProposal();
    void tryDecide();
    void decide(std::set<unsigned long> proposedValue);
};