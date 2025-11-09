#include <chrono>
#include <iostream>
#include <thread>
#include <signal.h>

#include "parser.hpp"
#include "hello.h"
#include "PerfectLink.hpp"
#include "FifoBroadcast.hpp"

FifoBroadcast* g_fb = nullptr;


static void stop(int) {
  // reset signal handlers to default
  signal(SIGTERM, SIG_DFL);
  signal(SIGINT, SIG_DFL);

  std::cout << "Received terminating signal" << std::endl;

  // immediately stop network packet processing
  std::cout << "Immediately stopping network packet processing.\n";

  // write/flush output file if necessary
  std::cout << "Writing output.\n";
  if (g_fb) {
    std::cout << "Stopping FifoBroadcast and flushing logs...\n";
    g_fb->stop();
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
  FifoBroadcast fb = FifoBroadcast(hostMapById, parser.outputPath());
  g_fb = &fb; // Have global reference to fifo broadcast so that you can call stop() when terminate signals are called

  std::cout << "Broadcasting and delivering messages...\n\n";

  int numMessages = parser.configDetailsFifo();

  for (int i = 1; i <= numMessages; ++i) {
    std::string message = std::to_string(i);
    fb.broadcast(message);
  }
  
  
  // After a process finishes broadcasting,
  // it waits forever for the delivery of messages.
  while (true) {
    std::this_thread::sleep_for(std::chrono::hours(1));
  }

  return 0;
}
