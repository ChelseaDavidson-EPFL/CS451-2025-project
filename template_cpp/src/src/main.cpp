#include <chrono>
#include <iostream>
#include <thread>
#include <signal.h>
#include <set>

#include "parser.hpp"
#include "hello.h"
#include "PerfectLink.hpp"
#include "LatticeAgreement.hpp"


PerfectLink* g_pl = nullptr;
LatticeAgreement* g_la = nullptr;


static void stop(int) {
  // reset signal handlers to default
  signal(SIGTERM, SIG_DFL);
  signal(SIGINT, SIG_DFL);

  std::cout << "Received terminating signal" << std::endl;

  // immediately stop network packet processing
  std::cout << "Immediately stopping network packet processing.\n";

  // write/flush output file if necessary
  std::cout << "Writing output.\n";
  if (g_pl) {
    std::cout << "Stopping PerfectLink and flushing logs...\n";
    g_pl->stop();
  }
  if (g_la) {
    std::cout << "Stopping LatticeAgreement and flushing logs...\n";
    g_la->stop();
  }

  // exit directly from signal handler
  exit(0);
}

int main(int argc, char **argv) {
  signal(SIGTERM, stop);
  signal(SIGINT, stop);

  // `true` means that a config file is required.
  // Call with `false` if no config file is necessary.
  bool requireConfig = true;

  Parser parser(argc, argv);
  parser.parse();

  hello();

  // Get lattice config details
  unsigned long numProposals, maxElementsInProposal, maxDistinctElements;
  if(!parser.configDetailsLattice(numProposals, maxElementsInProposal, maxDistinctElements)) {
    std::cerr << "Could not parse lattice agreement config" << std::endl;
  }
  std::cout << "My ID: " << parser.id() << "\n\n";

  // Get host details
  auto hosts = parser.hosts();
  std::unordered_map<unsigned long, std::pair<in_addr_t, unsigned short>> hostMapById; // ID -> Ip, Port
  std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort; // Port -> ID, Ip
  
  for (auto &host : hosts) {
    hostMapById[host.id] = {host.ip, host.port};
    hostMapByPort[host.port] = {host.id, host.ip};
  }

  bool idInHosts = hostMapById.count(parser.id()) > 0;
  
  if (!idInHosts) {
    std::ostringstream os;
    os << parser.id() << "is not a host process";
    throw std::invalid_argument(os.str());
  }

  in_addr_t processIp = hostMapById[parser.id()].first;
  unsigned short processPort = hostMapById[parser.id()].second;
  LatticeAgreement la = LatticeAgreement(parser.id(), processIp, processPort, hostMapByPort, hostMapById, parser.outputPath());
  g_la = &la; // Have global reference to lattice agreement so that you can call stop() when terminate signals are called

  for (unsigned long n = 1; n <= numProposals; ++n) {
    std::set<unsigned long> prop = parser.getProposal(n);

    la.propose(prop, n);

    // block until decide() is called
    la.waitForDecision();
  }

  
  // After a process finishes broadcasting,
  // it waits forever for the delivery of messages.
  while (true) {
    std::this_thread::sleep_for(std::chrono::hours(1));
  }

  return 0;
}
