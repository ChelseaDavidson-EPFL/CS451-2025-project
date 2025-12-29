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

LatticeAgreement::LatticeAgreement(unsigned long myProcessId, in_addr_t myProcessIp, unsigned short myProcessPort, std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort, std::unordered_map<unsigned long, std::pair<in_addr_t, unsigned short>> hostMapById, std::string logPath)
    : myProcessId_(myProcessId), myProcessIp_(myProcessIp), myProcessPort_(myProcessPort),  hostMapByPort_(hostMapByPort), hostMapById_(hostMapById), logPath_(logPath), running_(false)
{
    // Create or overwrite the log file
    logFile_.open(logPath_.c_str(), std::ios::out);
    if (!logFile_.is_open()) {
        std::cerr << "Failed to create log file at: " << logPath_ << std::endl;
        return;
    }
    
    // Proposer initialisation
    active_ = false;
    ackCount_ = 0;
    nackCount_ = 0;
    activeProposalNumber_ = 0;
    proposedValue_ = {};
    majority_ = hostMapByPort.size()/2 + 1;

    // Accepter initialisation
    acceptedValue_ = {};

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

void LatticeAgreement::propose(std::set<unsigned long> proposal) {
    proposedValue_ = proposal;
    active_ = true;
    activeProposalNumber_++;
    ackCount_ = 0;
    nackCount_ = 0;

    Proposal prop{activeProposalNumber_, proposedValue_};

    // Trigger beb.broadcast of the proposal
    broadcastProposal(prop);
}

void LatticeAgreement::receivedMessage(const Message& message, const unsigned long& senderId) {
    const std::string& content = message.content;

    std::string type = content.substr(0, 3);

    if (type == "PRP") {
        Proposal proposal;
        proposal.proposalNumber = parseNumberInParens(content);

        size_t valuesStart = content.find(')') + 1;
        proposal.proposedValue = parseValueSet(content, valuesStart);

        handleProposal(proposal, senderId);
    }
    else if (type == "NAC") {
        Nack nack;
        nack.proposalNumber = parseNumberInParens(content);

        size_t valuesStart = content.find(')') + 1;
        nack.proposedValue = parseValueSet(content, valuesStart);

        handleNack(nack);
    }
    else if (type == "ACK") {
        unsigned long proposalNumber = parseNumberInParens(content);

        handleAck(proposalNumber);
    }
    else {
        throw std::runtime_error("Unknown message type: " + type);
    }

    tryDecide();
    checkIfNeedNewProposal();
}

void LatticeAgreement::handleProposal(Proposal proposal, unsigned long senderId) {
    if (std::includes(proposal.proposedValue.begin(), proposal.proposedValue.end(), acceptedValue_.begin(), acceptedValue_.end())) {
        // accepted_value ⊆ proposed_value
        acceptedValue_ = proposal.proposedValue; 
        sendAckMsg(proposal.proposalNumber, senderId);
        return;
    } 
    
    // accepted_value !⊆ proposed_value
    // Update accepted with union of the two
    auto result = acceptedValue_;
    result.insert(proposal.proposedValue.begin(), proposal.proposedValue.end());
    acceptedValue_ = result;

    // Send Nack for this proposal number
    Nack nack{proposal.proposalNumber, acceptedValue_};
    sendNackMsg(nack, senderId);
}

void LatticeAgreement::handleAck(unsigned long proposalNumber) {
    if (proposalNumber == activeProposalNumber_){
        ackCount_++;
    }
}

void LatticeAgreement::handleNack(Nack nack) {
    if (nack.proposalNumber == activeProposalNumber_){
        // Do the union of the nack proposed value and your current proposed value
        auto result = proposedValue_;
        result.insert(nack.proposedValue.begin(), nack.proposedValue.end());
        proposedValue_ = result;
        
        nackCount_++;
    }
}

void LatticeAgreement::checkIfNeedNewProposal(){
    if (nackCount_ > 0 && ackCount_ + nackCount_ >= majority_ && active_ == true) {
        activeProposalNumber_++;
        ackCount_= 0;
        nackCount_ = 0;
        // Trigger beb.broadcast of the proposal
        Proposal proposal{activeProposalNumber_, proposedValue_};
        broadcastProposal(proposal);
    }
}

void LatticeAgreement::broadcastProposal(Proposal proposal) {
    for (const auto& entry : hostMapById_) {
        unsigned long processId = entry.first;

        if (processId == myProcessId_) continue; // Skip itself - TODO - idk if this is correct

        sendProposalMsg(proposal, processId);
    }
}

void LatticeAgreement::tryDecide(){
    if (ackCount_ >= majority_ && active_ == true){
        decide(proposedValue_);
        active_ = false;
    }
}

void LatticeAgreement::decide(std::set<unsigned long> proposedValue) {
    // Log the decision
    logDecision(proposedValue);
    
    // Signal application layer
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

void LatticeAgreement::sendAckMsg(unsigned long proposalNumber, unsigned long receiverId) {
    std::string content = "ACK(" + std::to_string(proposalNumber) + ")";
    unsigned long msgId = ++msgSeqNumber_;
    Message message{myProcessId_, msgId, content};
    perfectLinkInstance_-> sendMessage(message, receiverId);
}

void LatticeAgreement::sendNackMsg(Nack nack, unsigned long receiverId) {
    std::string content = "NAC(" + std::to_string(nack.proposalNumber) + ")";
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

void LatticeAgreement::sendProposalMsg(Proposal proposal, unsigned long receiverId) {
    std::string content = "PRP(" + std::to_string(proposal.proposalNumber) + ")";
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
    perfectLinkInstance_-> sendMessage(message, receiverId);
}

unsigned long LatticeAgreement::parseNumberInParens(const std::string& content) {
    size_t open = content.find('(');
    size_t close = content.find(')');

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
