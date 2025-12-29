#include <iostream>
#include <sstream>
#include <vector>
#include <algorithm>

#include "LatticeAgreement.hpp"

LatticeAgreement::LatticeAgreement(std::string logPath)
    : logPath_(logPath), running_(false)
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
    majority_ = 0; //TODO - find when store things required for PL -> numProcesses/2 + 1

    // Accepter initialisation
    acceptedValue_ = {};
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

    // TODO - trigger beb.broadcast of the proposal
}

void LatticeAgreement::handleProposal(Proposal proposal) {
    if (std::includes(proposal.proposedValue.begin(), proposal.proposedValue.end(), acceptedValue_.begin(), acceptedValue_.end())) {
        // accepted_value ⊆ proposed_value
        acceptedValue_ = proposal.proposedValue; 
        // TODO - Send ack with proposal number to proposal.proposorId 
        return;
    } 
    
    // accepted_value !⊆ proposed_value
    // Update accepted with union of the two
    auto result = acceptedValue_;
    result.insert(proposal.proposedValue.begin(), proposal.proposedValue.end());
    acceptedValue_ = result;

    // Send Nack for this proposal number
    Nack{proposal.proposalNumber, acceptedValue_};
    // TODO - Send nack to proposal.proposorId
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

void LatticeAgreement::triggerNewProposal(){
    if (nackCount_ > 0 && ackCount_ + nackCount_ >= majority_ && active_ == true) {
        activeProposalNumber_++;
        ackCount_= 0;
        nackCount_ = 0;
        // TODO - trigger beb.broadcast of the proposal
    }
}

void LatticeAgreement::tryDecide(){
    if (ackCount_ >= majority_ && active_ == true){
        decide(proposedValue_);
        active_ = false;
    }
}

void LatticeAgreement::decide(std::set<unsigned long> proposedValue) {
    // TODO - do we log here?
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
