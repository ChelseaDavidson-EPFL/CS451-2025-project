#include <atomic>
#include <mutex> 
#include <fstream>


class LatticeAgreement {
public:
    LatticeAgreement(std::string logPath);

    ~LatticeAgreement();
    void stop();

private:
    std::string logPath_;
    std::atomic<bool> running_;
    std::ofstream logFile_;
    
    // Logging vars
    size_t writeCounter_ = 0; // To log in batches
    size_t linesInLogBatch_ = 1000;
    size_t linesInLogBatchBroadcast_ = 100;

    // Concurrency primitives
    std::mutex stateMutex_;   // protects pendingDelivery_, acknowledged_, delivered_, nextExpectedMsgId_
    std::mutex loggingMutex_;     // protects logFile_ writes

};