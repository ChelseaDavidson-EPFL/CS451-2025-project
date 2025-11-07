#include <string>
#include <fstream>
#include <functional>
#include <netdb.h>
#include <atomic>

class FifoBroadcast {
public:
    FifoBroadcast(std::string logPath);

    ~FifoBroadcast();
    void stop();
    void broadcast(const std::string& message);

private:
    std::string logPath_;
    std::atomic<bool> running_;
    std::ofstream logFile_;
    std::function<void(unsigned long, unsigned long)> deliverCallback_;
};