#include <iostream>
#include <unistd.h>
#include <fstream>
#include <sys/stat.h>  // for mkdir
#include <string>

#include "PerfectLink.hpp"

PerfectLink::PerfectLink(unsigned long processId, in_addr_t processIp, unsigned short processPort, unsigned long receiverId, in_addr_t receiverIp, unsigned short receiverPort,std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort, std::string logPath)
    : processId_(processId), processPort_(processPort), processIp_(processIp), receiverId_(receiverId), receiverIp_(receiverIp), receiverPort_(receiverPort), hostMapByPort_(hostMapByPort), logPath_(logPath), running_(false)
{
    seqNumber_ = 0;
    if (processId_ == receiverId_) {
        initReceiver();
    } else {
        initBroadcaster();
    }
    
    deliverCallback_ = [this](unsigned long senderId, const std::string& message){
        std::cout << "Delivered \"" << message << "\" from: " << senderId << std::endl;
        logDelivery(senderId, message);
    };


    // Create or overwrite the file
    std::ofstream logFile(logPath_.c_str(), std::ios::out);
    if (!logFile.is_open()) {
        std::cerr << "Failed to create log file at: " << logPath_ << std::endl;
        return;
    }
    
    logFile.close();

    std::cout << "Created log file: " << logPath_ << std::endl;

    // Save log path for later use
    logPath_;


}

PerfectLink::~PerfectLink() {
    stop();
    close(sockfd_);
}

void PerfectLink::initBroadcaster() {
    sockfd_ = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd_ < 0) { perror("socket");}

    // Allow address reuse
    int optval = 1;
    setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));

    // Set up local address to bind to
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(processPort_);        
    addr.sin_addr.s_addr = processIp_;

    if (bind(sockfd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("bind (sender)");
        close(sockfd_);
    }

    running_ = true;
    receiverThread_ = std::thread(&PerfectLink::receiverLoop, this);

    std::cout << "Initialised " << processId_ << " as a broadcaster\n";
    std::cout << "Listening for ACKs on port " << processPort_ << "\n";
    std::cout << "Receiver IP is: " << receiverIp_ << "\n";
    std::cout << "Receiver Port is: " << receiverPort_ << "\n";
}

void PerfectLink::initReceiver() {
    sockfd_ = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd_ < 0) { perror("socket");}

    // Allow address reuse
    int optval = 1;
    setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));

    // Set up local address to bind to
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(receiverPort_);        
    addr.sin_addr.s_addr = receiverIp_;

    // Bind the socket
    if (bind(sockfd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("bind");
        close(sockfd_);
    }

    running_ = true;
    receiverThread_ = std::thread(&PerfectLink::receiverLoop, this);

    std::cout << "Listening on port " << receiverPort_ << "...\n";
  std::cout << "Initialised " << processId_ << " as a receiver \n";

}

void PerfectLink::sendMessage(const std::string& message) {
    // Store in pending map
    seqNumber_ += 1;
    std::string id = std::to_string(seqNumber_);
    mapMutex.lock();
    //TODO - add check it isn't already in pending
    pending_[id] = Message({id, message}); // TODO - add handling for 0 time
    mapMutex.unlock();
    std::string payload = id + "|" + message;

    // Start resend thread if not already running
    if (!resendThread_.joinable()) {
        resendThread_ = std::thread(&PerfectLink::sendMessageLoop, this);
    }
}

void PerfectLink::sendMessageLoop() {
    while (running_) {
        // std::this_thread::sleep_for(std::chrono::milliseconds(500));

        Message message;
        bool hasFound = false; // To check if we have a message to send
        mapMutex.lock();
        for (const auto &pair: pending_) { // Only find first message that is ready to send
            if (clock() - pair.second.lastSentTime > CLOCKS_PER_SEC/4){ // every 250ms
                message = pair.second;
                hasFound = true;
                break;
            } 
        }
        mapMutex.unlock();

        if (!hasFound) { // Messages were sent too recently - nothing to do
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            continue; 
        }

        std::string payload = message.id + "|" + message.message;

        sendRaw(payload, receiverIp_, receiverPort_);
        std::cout << "Resending message: " << message.message << std::endl;
    }
}

void PerfectLink::sendRaw(const std::string& payload, in_addr_t ip, unsigned short port){
    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(port);
    dest.sin_addr.s_addr = ip;

    sendto(sockfd_, payload.c_str(), payload.size(), 0,
           reinterpret_cast<sockaddr*>(&dest), sizeof(dest));
}

// void PerfectLink::addSendToLog(const std::string& message) { // TODO
    
// }

void PerfectLink::receiverLoop() {
    char buffer[1024];
    sockaddr_in senderAddr{};
    socklen_t senderLen = sizeof(senderAddr);

    while (running_) {
        std::cout << "listening..." << std::endl;
        ssize_t bytes = recvfrom(sockfd_, buffer, sizeof(buffer)-1, 0,
                    reinterpret_cast<sockaddr*>(&senderAddr), &senderLen);
        if (bytes <= 0) continue;

        buffer[bytes] = '\0';
        std::string payload(buffer);

        // Extract message type
        if (payload.rfind("ACK:", 0) == 0) {
            std::string msgId = payload.substr(4);

            mapMutex.lock();
            auto it = pending_.find(msgId);
            if (it == pending_.end()) { // Message Id not in our list TODO - could this throw an error when it might not be an error?
                perror("Received ACK for message that was not pending");
                continue;
            }
            pending_.erase(msgId);
            logSend(it->second.message);
            std::cout << "Received ACK for message id: " << msgId << std::endl;
            std::cout << "Size of pending_ now " << pending_.size() << std::endl;
            mapMutex.unlock();     
            continue;
        }

        // Parse as normal message
        size_t sep = payload.find('|');
        if (sep == std::string::npos) {
            perror("incorrect payload format");
            continue;
        }
        
        // Get message info
        std::string idStr = payload.substr(0, sep);
        std::string message = payload.substr(sep + 1);
        unsigned short senderPort = ntohs(senderAddr.sin_port);
        unsigned long senderId = hostMapByPort_[senderPort].first;
        unsigned long seqNbr = std::stoul(idStr);
         std::cout << "Just received (ProcessID, seqNbrID): " << senderId << ", " << seqNbr << std::endl;


        // Check if already delivered:
        auto& deliveredList = delivered_[senderId]; // TODO - do I need a mutex lock here?
        auto& pendingList = pendingDelivery_[senderId];

        bool alreadyDelivered = false;
        if (!deliveredList.empty()) {
            if (seqNbr <= deliveredList.front().first) {
                alreadyDelivered = true;
            } else {
                // Not older than the front; still might be present somewhere in the list.
                // We check the list only if necessary.
                alreadyDelivered = std::any_of(deliveredList.begin(), deliveredList.end(),
                                            [&](const auto& p) { return p.first == seqNbr; });
            }
        }

        if (alreadyDelivered) {
            // See if we can clean:
            if (deliveredList.size() > 1) {
                deliveredList.pop_front();
            }
            std::cout << "Cleaning delivered because message sent was already delivered: " << std::endl;
            printDelivered(); 
            continue;
        }

        // Find the last delivered sequence number (0 if none)
        unsigned long lastDeliveredId = deliveredList.empty() ? 0 : deliveredList.back().first;

        unsigned int deliveredCount = 0; // track how many we deliver this round

        // CASE 1: This message is the next in sequence and can therefore deliver it
        if (seqNbr == lastDeliveredId + 1) {
            deliveredList.emplace_back(seqNbr, message);
            deliveredCount++;

            std::cout << "Deliver | p=" << senderId << ", m=\"" << message << "\"⟩\n";
            if (deliverCallback_) deliverCallback_(senderId, message);

            // Try to deliver pending messages that can now be delivered in order
            bool deliveredNew = true;
            while (deliveredNew && !pendingList.empty()) {
                deliveredNew = false;
                for (auto it = pendingList.begin(); it != pendingList.end();) {
                    if (it->first == deliveredList.back().first + 1) {
                        deliveredList.push_back(*it);
                        deliveredCount++;
                        if (deliverCallback_) deliverCallback_(senderId, it->second);
                        std::cout << "From pending: Deliver | p=" << senderId << ", m=\"" << it->second << "\"⟩\n";
                        it = pendingList.erase(it);
                        deliveredNew = true;
                    } else {
                        ++it;
                    }
                }
            }
        }
        // CASE 2: Out of order so store it in pendingDelivery_
        else {
            // Insert message in order
            std::cout << "Added '" << message << "' from process " << senderId << " to pending list" << std::endl;
            auto pos = std::find_if(pendingList.begin(), pendingList.end(),
                                    [&](const auto& p) { return seqNbr < p.first; });
            pendingList.insert(pos, {seqNbr, message});
            printPendingDelivery();
        }

        // Clean delivered list - remove as many as we added
        if (deliveredList.size() > 1) {
            std::cout << "Cleaning delivered because we just delivered stuff: " << std::endl;

            for (unsigned int i = 0; i < deliveredCount && deliveredList.size() > 1; ++i) {
                deliveredList.pop_front();
            }
        }
        printDelivered();

        // Send ACK regardless of delivery status
        sendAck(senderAddr.sin_addr.s_addr, senderPort, idStr);
    }
}

void PerfectLink::sendAck(in_addr_t destIp, unsigned short destPort, const std::string& msgId) {
    std::string ack = "ACK:" + msgId;
    sendRaw(ack, destIp, destPort);
}

void PerfectLink::logDelivery(unsigned long senderId, const std::string& message) {
    std::ofstream logFile(logPath_.c_str(), std::ios::app);
    if (!logFile.is_open()) {
        std::cerr << "Failed to open log file: " << logPath_ << std::endl;
        return;
    }

    logFile << "d " << senderId << " " << message << "\n";
    logFile.close();
}

void PerfectLink::logSend(const std::string& message) {
    std::ofstream logFile(logPath_.c_str(), std::ios::app);
    if (!logFile.is_open()) {
        std::cerr << "Failed to open log file: " << logPath_ << std::endl;
        return;
    }

    logFile << "b " << message << "\n";
    logFile.close();
}


void PerfectLink::stop() {
    running_ = false;
    if (receiverThread_.joinable()) receiverThread_.join();
    if (resendThread_.joinable()) resendThread_.join();
    printDelivered();
    // logFile.close(); // TODO
}

void PerfectLink::printDelivered() const {
    std::cout << "\n===== Delivered Messages =====\n";
    for (const auto& [senderId, messages] : delivered_) {
        std::cout << "From process " << senderId << ":\n";
        for (const auto& [msgId, msg] : messages) {
            std::cout << "  ID " << msgId << " → " << msg << '\n';
        }
    }
    std::cout << "==============================\n";
}

void PerfectLink::printPendingDelivery() const {
    std::cout << "\n===== Pending Delivery Messages =====\n";
    for (const auto& [senderId, messages] : pendingDelivery_) {
        std::cout << "From process " << senderId << ":\n";
        for (const auto& [msgId, msg] : messages) {
            std::cout << "  ID " << msgId << " → " << msg << '\n';
        }
    }
    std::cout << "==============================\n";
}
