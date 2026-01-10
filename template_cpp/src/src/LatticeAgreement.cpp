#include <iostream>
#include <sstream>
#include <vector>
#include <algorithm>
#include <unistd.h>
#include <fstream>
#include <sys/stat.h>  // for mkdir
#include <string>
#include <cerrno>
#include <cstring>
#include <sys/time.h> // for struct timeval

#include "LatticeAgreement.hpp"

// #define DEBUG

#ifdef DEBUG
    #define DEBUGLOG(msg) (std::cout << msg << std::endl)
#else
    #define DEBUGLOG(msg) do {} while(0) // no-op in release
#endif

LatticeAgreement::LatticeAgreement(unsigned long myProcessId, in_addr_t myProcessIp, unsigned short myProcessPort, std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort, std::unordered_map<unsigned long, std::pair<in_addr_t, unsigned short>> hostMapById, unsigned long maxDistinctElements, std::string logPath)
    : myProcessId_(myProcessId), myProcessIp_(myProcessIp), myProcessPort_(myProcessPort),  hostMapByPort_(hostMapByPort), hostMapById_(hostMapById), maxDistinctElements_(maxDistinctElements), logPath_(logPath), running_(false)
{
    // Create or overwrite the log file
    logFile_.open(logPath_.c_str(), std::ios::out);
    if (!logFile_.is_open()) {
        std::cerr << "Failed to create log file at: " << logPath_ << std::endl;
        return;
    }

    majority_ = hostMapByPort.size()/2 + 1;

    // Message vars
    msgSeqNumber_ = 0;

    // Make Perfect Link instance
    perfectLinkInstance_ = std::make_unique<PerfectLink>(myProcessId_, myProcessIp_, myProcessPort_, hostMapByPort_, hostMapById_);

    // Define perfect link's delivery callback
    perfectLinkInstance_->setDeliverCallback(
        [this](Message msg, unsigned long senderId) {
            this->receivedMessage(msg, senderId);
        }
    );
}

LatticeAgreement::~LatticeAgreement() {
    stop();
}

void LatticeAgreement::propose(std::set<unsigned long> proposal, unsigned long shotNumber) {
    unsigned long proposalNumber;
    {
        std::lock_guard<std::mutex> lk(shotsMutex_);
        auto &s = shots_[shotNumber];

        if (s.decided) return; 

        s.active = true;
        s.proposalNumber++;
        s.ackCount = 0;
        s.nackCount = 0;
        s.proposedValue = proposal;
        // s.acceptedValue = {}; // Might have already acted as an acceptor for another process

        proposalNumber = s.proposalNumber;
    }

    Proposal prop{shotNumber, proposalNumber, proposal};

    // Trigger beb.broadcast of the proposal
    broadcastProposal(prop); // Locks when needed 

    // Optimisation: Check if we can decide immediately after broadcast
    tryDecide(shotNumber); // Locks
}

void LatticeAgreement::receivedMessage(const Message& message, const unsigned long& senderId) {
    const std::string& content = message.content;

    std::string type = content.substr(0, 3);
    unsigned long shotNumber = parseNumberInBrackets(content);

    if (type == "PRP") {
        Proposal proposal;
        proposal.shotNumber = shotNumber;
        proposal.proposalNumber = parseNumberInParens(content);

        size_t valuesStart = content.find(')') + 1;
        proposal.proposedValue = parseValueSet(content, valuesStart);

        handleProposal(proposal, senderId);
    }
    else if (type == "NAC") {
        Nack nack;
        nack.shotNumber = shotNumber;
        nack.proposalNumber = parseNumberInParens(content);

        size_t valuesStart = content.find(')') + 1;
        nack.proposedValue = parseValueSet(content, valuesStart);

        handleNack(nack);
    }
    else if (type == "ACK") {
        Ack ack;
        ack.shotNumber = shotNumber;
        ack.proposalNumber = parseNumberInParens(content);

        handleAck(ack);
    }
    else {
        throw std::runtime_error("Unknown message type: " + type);
    }

    tryDecide(shotNumber);
    checkIfNeedNewProposal(shotNumber);
}

void LatticeAgreement::handleProposal(Proposal proposal, unsigned long senderId) {
    bool sendAck = false;
    bool sendNack = false;
    Ack ack;
    Nack nack;

    {
        std::lock_guard<std::mutex> lk(shotsMutex_);
        auto &s = shots_[proposal.shotNumber];

        if (std::includes(proposal.proposedValue.begin(), proposal.proposedValue.end(), s.acceptedValue.begin(), s.acceptedValue.end())) {
            // accepted_value ⊆ proposed_value
            s.acceptedValue = proposal.proposedValue;
            // ONLY send ACK if sender is remote
            if (senderId != myProcessId_) {
                ack = Ack{proposal.shotNumber, proposal.proposalNumber};
                sendAck = true;
            }
        } else {
            // accepted_value !⊆ proposed_value
            // Update accepted with union of the two
            s.acceptedValue.insert(proposal.proposedValue.begin(), proposal.proposedValue.end());

            if (senderId != myProcessId_) {
                nack = Nack{proposal.shotNumber, proposal.proposalNumber, s.acceptedValue};
                sendNack = true;
            }
        }
    } // shotsMutex_ released here

    // Send messages after releasing the lock
    if (sendAck) {
        sendAckMsg(ack, senderId);
    } else if (sendNack) {
        sendNackMsg(nack, senderId);
    }
}

void LatticeAgreement::handleAck(Ack ack) {
    std::lock_guard<std::mutex> lk(shotsMutex_);
    auto &s = shots_[ack.shotNumber];
    if (s.decided) return;

    if (ack.proposalNumber == s.proposalNumber) {
        s.ackCount++;
    }
}

void LatticeAgreement::handleNack(Nack nack) {
    std::lock_guard<std::mutex> lk(shotsMutex_);
    auto &s = shots_[nack.shotNumber];
    if (s.decided) return;

    if (nack.proposalNumber == s.proposalNumber) {
        s.proposedValue.insert(nack.proposedValue.begin(), nack.proposedValue.end());
        s.nackCount++;
    }
}

void LatticeAgreement::checkIfNeedNewProposal(unsigned long shotNumber){
    unsigned long proposalNumber;
    std::set<unsigned long> proposedValue;
    bool shouldBroadcast = false;

    {
        std::lock_guard<std::mutex> lk(shotsMutex_);
        auto &s = shots_[shotNumber];

        if (s.decided) return;
        
        if (s.nackCount > 0 && s.ackCount + s.nackCount >= majority_ && s.active == true) {
            s.proposalNumber++;
            s.ackCount= 0;
            s.nackCount = 0;
            // Trigger beb.broadcast of the proposal
            shouldBroadcast = true;
            proposalNumber = s.proposalNumber;
            proposedValue = s.proposedValue;
        }
    }
    if (shouldBroadcast) {
        Proposal proposal{shotNumber, proposalNumber, proposedValue};
        broadcastProposal(proposal);
    }
}

void LatticeAgreement::broadcastProposal(Proposal proposal) {
    // Create proposal message
    Message msgToSend = createProposalMsg(proposal);
    
    // Send to others
    for (const auto& entry : hostMapById_) {
        unsigned long processId = entry.first;
        if (processId == myProcessId_) continue;
        sendProposalMsg(msgToSend, processId);
    }

    // === SELF AS ACCEPTOR ===
    handleProposal(proposal, myProcessId_);

    // === SELF AS PROPOSER (ACK) ===
    {
        std::lock_guard<std::mutex> lk(shotsMutex_);
        auto &s = shots_[proposal.shotNumber];
        if (!s.decided && proposal.proposalNumber == s.proposalNumber) {
            s.ackCount++;
        }
    }
}

void LatticeAgreement::tryDecide(unsigned long shotNumber){
    bool doDecide = false;
    std::set<unsigned long> value;
    {
        std::lock_guard<std::mutex> lk(shotsMutex_);
        auto &s = shots_[shotNumber];

        if (s.decided) return;

        // Optimisation: If we have all possible distinct elements, we are the top of the lattice - we can therefore decide immediately
        if (!s.decided && s.proposedValue.size() >= maxDistinctElements_) {
            doDecide = true;
            value = s.proposedValue;
            s.decided = true;
            s.active = false;
        }

        else if (!s.decided && s.ackCount >= majority_) {
            doDecide = true;
            value = s.proposedValue;
            s.decided = true;
            s.active = false;
        }
    }
    if (doDecide) {
        decide(shotNumber, value);
    }  
}

void LatticeAgreement::decide(unsigned long shotNumber, std::set<unsigned long> proposedValue) {
    {
        DEBUGLOG("Decided for shot " << shotNumber);
        std::lock_guard<std::mutex> lock(decisionOrderMutex_);
        decidedValues_[shotNumber] = proposedValue;

        // Try to flush in order
        while (decidedValues_.count(nextShotToLog_)) {
            logDecision(decidedValues_[nextShotToLog_]);
            decidedValues_.erase(nextShotToLog_);
            nextShotToLog_++;
        }
    }

    // Wake application if needed
    {
        std::lock_guard<std::mutex> lock(decisionMutex_);
        decisionReady_ = true;
    }
    decisionCv_.notify_one();
}


void LatticeAgreement::logDecision(std::set<unsigned long> proposedValue) {
    if (!logFile_.is_open()) {
        std::cerr << "Failed to open log file: " << logPath_ << std::endl;
        return;
    }

    {
        std::lock_guard<std::mutex> lock(loggingMutex_);

        bool first = true;
        for (unsigned long value : proposedValue) {
            if (!first) {
                logFile_ << " ";
            }
            logFile_ << value;
            first = false;
        }

        logFile_ << "\n";

        if (++writeCounter_ % linesInLogBatch_ == 0) logFile_.flush(); // every 1000 lines
    }
}

void LatticeAgreement::waitForDecision() {
    std::unique_lock<std::mutex> lock(decisionMutex_);
    decisionCv_.wait(lock, [this]() { return decisionReady_; });

    // Reset for next proposal
    decisionReady_ = false;
}

void LatticeAgreement::sendAckMsg(Ack ack, unsigned long receiverId) {
    std::string content = "ACK{" + std::to_string(ack.shotNumber) + "}(" + std::to_string(ack.proposalNumber) + ")";
    unsigned long msgId = ++msgSeqNumber_;
    Message message{myProcessId_, msgId, content};
    perfectLinkInstance_-> sendMessage(message, receiverId);
}

void LatticeAgreement::sendNackMsg(Nack nack, unsigned long receiverId) {
    std::string content = "NAC{" + std::to_string(nack.shotNumber) + "}(" + std::to_string(nack.proposalNumber) + ")";
    bool first = true;
    for (unsigned long value : nack.proposedValue) {
        if (!first) {
            content += ",";
        }
        content += std::to_string(value);
        first = false;
    }
    unsigned long msgId = ++msgSeqNumber_;
    Message message{myProcessId_, msgId, content};
    perfectLinkInstance_-> sendMessage(message, receiverId);
}

Message LatticeAgreement::createProposalMsg(Proposal proposal) {
    std::string content = "PRP{" + std::to_string(proposal.shotNumber) + "}(" + std::to_string(proposal.proposalNumber) + ")";
    bool first = true;
    for (unsigned long value : proposal.proposedValue) {
        if (!first) {
            content += ",";
        }
        content += std::to_string(value);
        first = false;
    }
    unsigned long msgId = ++msgSeqNumber_;
    Message message{myProcessId_, msgId, content};
    return message;
}

void LatticeAgreement::sendProposalMsg(Message proposalMessage, unsigned long receiverId) {
    perfectLinkInstance_-> sendMessage(proposalMessage, receiverId);
}

unsigned long LatticeAgreement::parseNumberInParens(const std::string& content) {
    size_t open = content.find('(');
    size_t close = content.find(')');

    if (open == std::string::npos || close == std::string::npos || close <= open) {
        throw std::runtime_error("Malformed message header");
    }

    return std::stoul(content.substr(open + 1, close - open - 1));
}

unsigned long LatticeAgreement::parseNumberInBrackets(const std::string& content) {
    size_t open = content.find('{');
    size_t close = content.find('}');

    if (open == std::string::npos || close == std::string::npos || close <= open) {
        throw std::runtime_error("Malformed message header");
    }

    return std::stoul(content.substr(open + 1, close - open - 1));
}


std::set<unsigned long> LatticeAgreement::parseValueSet(const std::string& content, size_t startPos) {
    std::set<unsigned long> values;

    if (startPos >= content.size()) {
        return values; // empty set
    }

    std::stringstream ss(content.substr(startPos));
    std::string token;

    while (std::getline(ss, token, ',')) {
        if (!token.empty()) {
            values.insert(std::stoul(token));
        }
    }

    return values;
}


void LatticeAgreement::stop() {
    running_ = false;
    if (logFile_.is_open()) {
        {
            std::lock_guard<std::mutex> lock(loggingMutex_);
            logFile_.flush();
            logFile_.close();
        }
    }
}
