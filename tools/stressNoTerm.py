#!/usr/bin/env python3

import argparse
import os, sys
import time
import threading, subprocess
import itertools
import signal
import random
from enum import Enum

PROCESSES_BASE_IP = 11000


def positive_int(value):
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError(f"{value} is not positive integer")
    return ivalue


class ProcessState(Enum):
    RUNNING = 1
    STOPPED = 2


class ProcessInfo:
    def __init__(self, handle):
        self.lock = threading.Lock()
        self.handle = handle
        self.state = ProcessState.RUNNING

    @staticmethod
    def stateToSignal(state):
        if state == ProcessState.RUNNING:
            return signal.SIGCONT
        if state == ProcessState.STOPPED:
            return signal.SIGSTOP
        raise ValueError("Invalid state")

    @staticmethod
    def stateToSignalStr(state):
        if state == ProcessState.RUNNING:
            return "SIGCONT"
        if state == ProcessState.STOPPED:
            return "SIGSTOP"
        raise ValueError("Invalid state")

    @staticmethod
    def validStateTransition(current, desired):
        if current == ProcessState.RUNNING:
            return desired == ProcessState.STOPPED
        if current == ProcessState.STOPPED:
            return desired == ProcessState.RUNNING
        return False


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

        return (hostsfile, configfile)

    def generateFifoConfig(self, directory):
        hostsfile = os.path.join(directory, "hosts")
        configfile = os.path.join(directory, "config")

        with open(hostsfile, "w") as hosts:
            for i in range(1, self.processes + 1):
                hosts.write(f"{i} localhost {PROCESSES_BASE_IP + i}\n")

        with open(configfile, "w") as config:
            config.write(f"{self.messages}\n")

        return (hostsfile, configfile)


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

        return (hostsfile, configfiles)


class StressTest:
    def __init__(self, procs, concurrency, attempts, attemptsRatio):
        self.processes = len(procs)
        self.processesInfo = {pid: ProcessInfo(handle) for (pid, handle) in procs}
        self.concurrency = concurrency
        self.attempts = attempts
        self.attemptsRatio = attemptsRatio

    def stress(self):
        selectProc = list(range(1, self.processes + 1))
        random.shuffle(selectProc)

        selectOp = (
            [ProcessState.STOPPED] * int(1000 * self.attemptsRatio["STOP"])
            + [ProcessState.RUNNING] * int(1000 * self.attemptsRatio["CONT"])
        )
        random.shuffle(selectOp)

        successfulAttempts = 0
        while successfulAttempts < self.attempts:
            proc = random.choice(selectProc)
            op = random.choice(selectOp)
            info = self.processesInfo[proc]

            with info.lock:
                if ProcessInfo.validStateTransition(info.state, op):
                    time.sleep(float(random.randint(50, 500)) / 1000.0)
                    info.handle.send_signal(ProcessInfo.stateToSignal(op))
                    info.state = op
                    successfulAttempts += 1
                    print(f"Sending {ProcessInfo.stateToSignalStr(op)} to process {proc}")

    def continueStoppedProcesses(self):
        for _, info in self.processesInfo.items():
            with info.lock:
                if info.state == ProcessState.STOPPED:
                    info.handle.send_signal(ProcessInfo.stateToSignal(ProcessState.RUNNING))
                    info.state = ProcessState.RUNNING

    def run(self):
        if self.concurrency > 1:
            threads = [
                threading.Thread(target=self.stress) for _ in range(self.concurrency)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        else:
            self.stress()


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

        procs.append((pid, subprocess.Popen(cmd + cmd_ext, stdout=stdoutFd, stderr=stderrFd)))

    return procs


def main(parser_results, testConfig):
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
    st = StressTest(procs, testConfig["concurrency"], testConfig["attempts"], testConfig["attemptsDistribution"])

    for (logicalPID, procHandle) in procs:
        print(f"Process {logicalPID} started with PID {procHandle.pid}")

    st.run()
    print("StressTest complete. Resuming stopped processes...")
    st.continueStoppedProcesses()

    input("Press Enter when all processes have finished...")

    print("Resuming any stopped processes before final wait...")
    st.continueStoppedProcesses()

    print("Waiting for processes to exit naturally (1s timeout each)...")
    still_running = []
    for pid, proc in procs:
        try:
            proc.wait(timeout=1)
            print(f"Process {pid} exited with code {proc.returncode}")
        except subprocess.TimeoutExpired:
            print(f"Process {pid} is still running (did not exit naturally).")
            still_running.append((pid, proc))

    if still_running:
        print("\nTerminating remaining processes gracefully...")
        for pid, proc in still_running:
            print(f"Sending terminate() to process {pid} (PID {proc.pid})")
            proc.terminate()  # sends SIGTERM, only at the very end

        for pid, proc in still_running:
            try:
                proc.wait(timeout=1)
                print(f"Process {pid} terminated with code {proc.returncode}")
            except subprocess.TimeoutExpired:
                print(f"Process {pid} did not respond to terminate(), killing forcefully.")
                proc.kill()
                proc.wait()
                print(f"Process {pid} killed (SIGKILL).")

    print("\n✅ All processes cleaned up.")



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    sub_parsers = parser.add_subparsers(dest="command")  # no 'required=True' (for Python 3.6)
    
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

    # manual enforcement for Python 3.6
    if results.command is None:
        parser.print_help(sys.stderr)
        sys.exit(1)

    testConfig = {
        "concurrency": 8,
        "attempts": 8,
        "attemptsDistribution": {"STOP": 0.5, "CONT": 0.5},
    }

    main(results, testConfig)

