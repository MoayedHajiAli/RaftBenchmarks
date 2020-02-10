from subprocess import Popen, PIPE
import subprocess
import sys
import logging
import matplotlib.pyplot as plot
import time
# Global variables
mn_packet_sz = 1
mx_packet_sz = 10000
num_nodes = 3
rps = 500
TEST_GAP = 500
logger = logging.getLogger(__name__)
X_axis = []
Y_axis = []
success_X_axis = []
success_Y_axis = []


def DoTests():
    subprocess.Popen(['python3 ' + 'StartLocalServers.py ' + str(num_nodes)], shell=True)
    time.sleep(2.0)
    cmd = [sys.executable, "Single_delay_benchmark.py"]
    test_gap = (mx_packet_sz - mn_packet_sz) / (num_tests-1)
    i = mn_packet_sz
    while i <= mx_packet_sz:
        p = Popen(cmd + [str(rps)] + [str(int(i / 2))] + [str(int(i / 2))], stdin=PIPE, stdout=PIPE)
        res = p.communicate()[0].decode()
        p.wait()
        print(res)
        try:
            res = float(res)
        except ValueError:
            res = -1
        if res != -1:
            X_axis.append(i)
            Y_axis.append(float(res))
        i += test_gap


def drawGraph():
    for i in range(0, len(X_axis)):
        print(X_axis[i], Y_axis[i])
    plot.xlabel('Packet Size')
    plot.ylabel('Average latency')
    plot.xlim(mn_packet_sz-10, mx_packet_sz+10)
    plot.ylim(0, max(Y_axis)+2)
    plot.title("Average latency for %s rps over %s nodes" % (str(rps), str(num_nodes)))
    plot.plot(X_axis, Y_axis, color='orange', linestyle='dashed', linewidth=1,
             marker='o', markerfacecolor='blue', markersize=4)
    plot.show()

def setLogger():
    with open("client_side.log", 'w') as w:
        pass
    with open("server_side.log", 'w') as w:
        pass

def main():
    setLogger()
    DoTests()
    drawGraph()


if __name__ == '__main__':
    if len(sys.argv) != 6:
        print("""Please enter the number of nodes - rps - starting packet size - ending packet size - number of tests""")
        sys.exit(0)
    num_nodes, rps, mn_packet_sz, mx_packet_sz, num_tests = map(int, sys.argv[1:])
    main()
