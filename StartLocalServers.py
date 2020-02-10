from __future__ import print_function
import sys
from subprocess import Popen, PIPE
import os

DEVNULL = open(os.devnull, 'wb')

START_PORT = 4321
START_CLIENT_PORT = 5432
MIN_RPS = 10
MAX_RPS = 40000


def RunNodes(numNodes):
    cmd = [sys.executable, 'Kvtestobj.py']
    processes = []
    allAddrs = []
    for i in range(numNodes):
        allAddrs.append('localhost:%d' % (START_PORT + 2 * i))
    for i in range(numNodes):
        addrs = list(allAddrs)
        selfAddr = addrs.pop(i)
        port = str(START_PORT + 2 * i + 1)
        Popen(cmd + [port] + [selfAddr] + addrs)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Please enter the number of required nodes")
        sys.exit(0)
    numNodes = int(sys.argv[1])
    RunNodes(numNodes)
