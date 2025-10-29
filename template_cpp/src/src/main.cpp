#include <chrono>
#include <iostream>
#include <thread>
#include <signal.h>

#include "parser.hpp"
#include "hello.h"
#include "PerfectLink.hpp"


static void stop(int) {
  // reset signal handlers to default
  signal(SIGTERM, SIG_DFL);
  signal(SIGINT, SIG_DFL);

  std::cout << "Received terminating signal" << std::endl;

  // immediately stop network packet processing
  std::cout << "Immediately stopping network packet processing.\n";
  // TODO

  // write/flush output file if necessary
  std::cout << "Writing output.\n";
  // TODO

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
  std::cout << std::endl;

  auto configDetails = parser.configDetails();
  unsigned long receiverId = configDetails.second;
  // in_addr_t receiverIp = 0;
  // unsigned short receiverPort = 0;

  std::cout << "My PID: " << getpid() << "\n";
  std::cout << "From a new terminal type `kill -SIGINT " << getpid() << "` or `kill -SIGTERM "
            << getpid() << "` to stop processing packets\n\n";

  std::cout << "My ID: " << parser.id() << "\n\n";

  std::cout << "List of resolved hosts is:\n";
  std::cout << "==========================\n";
 
  auto hosts = parser.hosts();
  std::unordered_map<unsigned long, std::pair<in_addr_t, unsigned short>> hostMapById; // ID -> Ip, Port
  std::unordered_map<unsigned short, std::pair<unsigned long, in_addr_t>> hostMapByPort; // Port -> ID, Ip
  

  for (auto &host : hosts) {
    std::cout << host.id << "\n";
    hostMapById[host.id] = {host.ip, host.port};
    hostMapByPort[host.port] = {host.id, host.ip};

    // if(host.id == receiverId) {
    //   receiverIp = host.ip;
    //   receiverPort = host.port;
    // }
    std::cout << "Human-readable IP: " << host.ipReadable() << "\n";
    std::cout << "Machine-readable IP: " << host.ip << "\n";
    std::cout << "Human-readbale Port: " << host.portReadable() << "\n";
    std::cout << "Machine-readbale Port: " << host.port << "\n";
    std::cout << "\n";
  }
  in_addr_t receiverIp = hostMapById[receiverId].first;
  unsigned short receiverPort = hostMapById[receiverId].second;
  std::cout << "\n";

  std::cout << "Path to output:\n";
  std::cout << "===============\n";
  std::cout << parser.outputPath() << "\n\n";

  std::cout << "Path to config:\n";
  std::cout << "===============\n";
  std::cout << parser.configPath() << "\n";
  
  std::cout << "Number of messages to be sent: " << configDetails.first << "\n";
  std::cout << "ID of receiver: " << configDetails.second << "\n\n";

  std::cout << "Doing some initialization...\n\n";

  // bool idInHosts = std::find(hostIds.begin(), hostIds.end(), parser.id()) != hostIds.end();
  bool idInHosts = hostMapById.count(parser.id()) > 0;
  
  if (!idInHosts) {
    std::ostringstream os;
    os << parser.id() << "is not a host process";
    throw std::invalid_argument(os.str());
  }
  in_addr_t processIp = hostMapById[parser.id()].first;
  unsigned short processPort = hostMapById[parser.id()].second;
  PerfectLink pl = PerfectLink(parser.id(), processIp, processPort, configDetails.second, receiverIp, receiverPort, hostMapByPort, parser.outputPath());

  std::cout << "Broadcasting and delivering messages...\n\n";

  int numMessages = configDetails.first;

  if(parser.id() != configDetails.second) {
    for (int i = 1; i <= numMessages; ++i) {
      std::string message = std::to_string(i);
      pl.sendMessage(message);
    }
   
  }
  
  // After a process finishes broadcasting,
  // it waits forever for the delivery of messages.
  while (true) {
    std::this_thread::sleep_for(std::chrono::hours(1));
  }

  return 0;
}
