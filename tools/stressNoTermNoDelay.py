#!/usr/bin/env python3
import argparse
import os
import sys
import subprocess
import itertools
import random


PROCESSES_BASE_IP = 11000


def positive_int(value):
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError(f"{value} is not a positive integer")
    return ivalue


class Validation:
    def __init__(self, procs, msgs):
        self.processes = procs
        self.messages = msgs

    def generatePerfectLinksConfig(self, directory):
        hostsfile = os.path.join(directory, "hosts")
        configfile = os.path.join(directory, "config")

        with open(hostsfile, "w") as hosts:
            for i in range(1, self.processes + 1):
                hosts.write(f"{i} localhost {PROCESSES_BASE_IP + i}\n")

        with open(configfile, "w") as config:
            config.write(f"{self.messages} 1\n")

        return hostsfile, configfile

    def generateFifoConfig(self, directory):
        hostsfile = os.path.join(directory, "hosts")
        configfile = os.path.join(directory, "config")

        with open(hostsfile, "w") as hosts:
            for i in range(1, self.processes + 1):
                hosts.write(f"{i} localhost {PROCESSES_BASE_IP + i}\n")

        with open(configfile, "w") as config:
            config.write(f"{self.messages}\n")

        return hostsfile, configfile


class LatticeAgreementValidation:
    def __init__(self, processes, proposals, max_proposal_size, distinct_values):
        self.procs = processes
        self.props = proposals
        self.mps = max_proposal_size
        self.dval = distinct_values

    def generate(self, directory):
        hostsfile = os.path.join(directory, "hosts")
        with open(hostsfile, "w") as hosts:
            for i in range(1, self.procs + 1):
                hosts.write(f"{i} localhost {PROCESSES_BASE_IP + i}\n")

        maxint = 2**31 - 1
        seeded_rand = random.Random(42)
        try:
            values = seeded_rand.sample(range(0, maxint + 1), self.dval)
        except ValueError:
            print("Cannot have too many distinct values")
            sys.exit(1)

        configfiles = []
        for pid in range(1, self.procs + 1):
            configfile = os.path.join(directory, f"proc{pid:02d}.config")
            configfiles.append(configfile)

            with open(configfile, "w") as config:
                config.write(f"{self.props} {self.mps} {self.dval}\n")
                for _ in range(self.props):
                    proposal = seeded_rand.sample(
                        values, seeded_rand.randint(1, self.mps)
                    )
                    config.write(" ".join(map(str, proposal)) + "\n")

        return hostsfile, configfiles


def startProcesses(processes, runscript, hostsFilePath, configFilePaths, outputDir):
    runscriptPath = os.path.abspath(runscript)
    if not os.path.isfile(runscriptPath):
        raise Exception(f"`{runscriptPath}` is not a file")

    if os.path.basename(runscriptPath) != "run.sh":
        raise Exception(f"`{runscriptPath}` is not a runscript")

    outputDirPath = os.path.abspath(outputDir)
    if not os.path.isdir(outputDirPath):
        raise Exception(f"`{outputDirPath}` is not a directory")

    baseDir, _ = os.path.split(runscriptPath)
    bin_cpp = os.path.join(baseDir, "bin", "da_proc")
    bin_java = os.path.join(baseDir, "bin", "da_proc.jar")

    if os.path.exists(bin_cpp):
        cmd = [bin_cpp]
    elif os.path.exists(bin_java):
        cmd = ["java", "-jar", bin_java]
    else:
        raise Exception("No binary found. Build before validating.")

    procs = []
    for pid, config_path in zip(range(1, processes + 1), itertools.cycle(configFilePaths)):
        cmd_ext = [
            "--id", str(pid),
            "--hosts", hostsFilePath,
            "--output", os.path.join(outputDirPath, f"proc{pid:02d}.output"),
            config_path,
        ]

        stdoutFd = open(os.path.join(outputDirPath, f"proc{pid:02d}.stdout"), "w")
        stderrFd = open(os.path.join(outputDirPath, f"proc{pid:02d}.stderr"), "w")

        process = subprocess.Popen(cmd + cmd_ext, stdout=stdoutFd, stderr=stderrFd)
        procs.append((pid, process))
        print(f"Started process {pid} (PID {process.pid})")

    return procs


def main(parser_results):
    cmd = parser_results.command
    runscript = parser_results.runscript
    logsDir = parser_results.logsDir
    processes = parser_results.processes

    if not os.path.isdir(logsDir):
        raise ValueError(f"Directory `{logsDir}` does not exist")

    if cmd == "perfect":
        validation = Validation(processes, parser_results.messages)
        hostsFile, configFile = validation.generatePerfectLinksConfig(logsDir)
        configFiles = [configFile]
    elif cmd == "fifo":
        validation = Validation(processes, parser_results.messages)
        hostsFile, configFile = validation.generateFifoConfig(logsDir)
        configFiles = [configFile]
    elif cmd == "agreement":
        proposals = parser_results.proposals
        pmv = parser_results.proposal_max_values
        pdv = parser_results.proposals_distinct_values

        if pmv > pdv:
            print("Distinct proposal values must ≥ maximum values per proposal")
            sys.exit(1)

        validation = LatticeAgreementValidation(processes, proposals, pmv, pdv)
        hostsFile, configFiles = validation.generate(logsDir)
    else:
        raise ValueError("Unrecognized command")

    procs = startProcesses(processes, runscript, hostsFile, configFiles, logsDir)

    print("\n✅ All processes started successfully.")
    input("Press Enter to stop all processes...\n")

    print("Terminating processes...")
    still_running = []

    for pid, proc in procs:
        proc.terminate()  # SIGTERM
        still_running.append((pid, proc))

    for pid, proc in still_running:
        try:
            proc.wait(timeout=2)
            print(f"Process {pid} terminated with code {proc.returncode}")
        except subprocess.TimeoutExpired:
            print(f"Process {pid} did not respond, killing...")
            proc.kill()
            proc.wait()
            print(f"Process {pid} killed (SIGKILL).")

    print("\n✅ All processes cleaned up.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    sub_parsers = parser.add_subparsers(dest="command")

    parser_perfect = sub_parsers.add_parser("perfect")
    parser_fifo = sub_parsers.add_parser("fifo")
    parser_agreement = sub_parsers.add_parser("agreement")

    for sp in [parser_perfect, parser_fifo, parser_agreement]:
        sp.add_argument("-r", "--runscript", required=True)
        sp.add_argument("-l", "--logs", required=True, dest="logsDir")
        sp.add_argument("-p", "--processes", required=True, type=positive_int)

    for sp in [parser_perfect, parser_fifo]:
        sp.add_argument("-m", "--messages", required=True, type=positive_int)

    parser_agreement.add_argument("-n", "--proposals", required=True, type=positive_int)
    parser_agreement.add_argument("-v", "--proposal-values", required=True, type=positive_int, dest="proposal_max_values")
    parser_agreement.add_argument("-d", "--distinct-values", required=True, type=positive_int, dest="proposals_distinct_values")

    results = parser.parse_args()

    if results.command is None:
        parser.print_help(sys.stderr)
        sys.exit(1)

    main(results)
