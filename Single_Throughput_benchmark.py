import sys
import random
import socket
import time
import _thread
from threading import Lock
import selectors
from subprocess import Popen, PIPE
inp = []
ADDR_FAMILY = socket.AF_INET
SOCK_TYPE = socket.SOCK_STREAM
SERVERS_IP = ['localhost:4322', 'localhost:4324', 'localhost:4326']
BUF_SIZE = 8000
responsed = 0
err_cnt = 0
delay_analysis = []
error_analysis = []
delay = []
sum_delay = 0.0
change_in_leader = False
sel = selectors.DefaultSelector()
connection_id = {}
ret = {}
change_in_leader = False
VALID_ERROR_PERCENTAGE = 0.1
MAX_RPS = 1000
vis = [0] * 100
lock = Lock()

def generateString(Size):
    f = ""
    for _ in range(0, Size):
        f += chr(random.randrange(0, 26) + ord('a'))
    return f


def reset(inputSize):
    global commited, inp
    inp = []
    connect()
    for _ in range(0, inputSize):
        inp.append(generateString(keySize) + ":" + generateString(valueSize))
    commited = 0

def find_throughput(rps):
    global err_cnt, commited, throughput_analysis
    err_cnt = 0
    reset(25 * rps)
    iterations = 0
    startTime = time.time()
    while time.time() - startTime < 25.0:
        st = time.time()
        i = 0
        while i < rps:
            packet = ''
            while i < rps and len(packet) + len(inp[(iterations * rps + i)]) < BUF_SIZE:
                packet += str(inp[(iterations * rps + i)] + '#' + str(iterations) + '$')
                i += 1
            try:
                mySocket.sendall(packet.encode())
            except socket.error:
                print("ERROR IN SENDING")
                pass
        dif = time.time() - st
        if dif < 1.0:
            time.sleep(1.0 - dif)
        iterations += 1
    time.sleep(16.0)


def connect(IP = None):
    print("new Connection")
    global mySocket, change_in_leader
    try:
        mySocket.close()
    except:
        pass
    mySocket = socket.socket(ADDR_FAMILY, SOCK_TYPE)
    i = 0
    while i < len(SERVERS_IP):
        is_changed = True
        targetAddr, port = SERVERS_IP[i].split(':')
        port = int(port)
        if IP:
            targetAddr, port = IP.split(':')
            port = int(port)
            port += 1
            IP = None
            i -= 1
        try:
            print("Connect to", targetAddr, port)
            mySocket.connect((targetAddr, port))
            leader = str(mySocket.recv(BUF_SIZE).decode())
            print(leader)
            while leader != str(targetAddr)+":"+str(port-1):
                targetAddr, port = leader.split(':')
                port = int(port)
                port += 1
                mySocket.close()
                print("closed")
                try:
                    mySocket = socket.socket(ADDR_FAMILY, SOCK_TYPE)
                    mySocket.connect((targetAddr, port))
                    print("connected to the leader")
                except socket.error:
                    is_changed = False
                    break
                print("Waiting for the leader response")
                leader = str(mySocket.recv(BUF_SIZE).decode())
                print(leader)
            if is_changed:
                print("Connected")
                change_in_leader = True
                return
        except socket.error:
            print("Socket error when connecting")
            continue
        i += 1
    connect()

def revc_respone():
    global mySocket, commited, delay_analysis, error_analysis, err_cnt
    mySocket.settimeout(200.0)
    ret = ''
    while True:
        try:
            ret += mySocket.recv(BUF_SIZE).decode()
            # print(ret)
            ind, last = ret.find('$'), 0
            while ind != -1:
                sep = ret.find('#', last)
                err = ret[last:sep]
                res = ret[sep + 1:ind]
                err = int(err)
                if err == -1:
                    connect(res)
                    ret = ''
                elif err == 0:
                    res = int(res)
                    if res and vis[res-1] == 0:
                        print(throughput_analysis[int(res)])
                        vis[res-1] = 1
                    throughput_analysis[res] += 1
                else:
                    error_analysis.append((err, res, time.time()))
                    err_cnt += 1
                last = ind + 1
                ind = ret.find('$', last)
            ret = ret[last:]
        except socket.timeout:
            print("socket timeout in receiving from the server")
            continue
        except socket.error as e:
            pass

def main():
    global inp, conn, keySize, valueSize, iterations, throughput_analysis
    if(len(sys.argv) != 5):
        print("Iteration - RPS - the size of the key - the size of the value")
        sys.exit(0)
    iterations = int(sys.argv[1])
    rps = int(sys.argv[2])
    keySize = int(sys.argv[3])
    valueSize = int(sys.argv[4])
    print("All the data in delay_client_analysis and error_client_analysis will be delated y|n ?")
    res = input()
    if(res != 'y'):
        sys.exit(0)
    throughput_analysis = [0] * iterations
    connect()
    print("connected")
    _thread.start_new_thread(revc_respone, ())
    find_throughput(rps)
    with open("throughput_client_analysis.txt", 'w+') as f:
        for i in range(0, len(throughput_analysis)):
            f.write(str(throughput_analysis[i]) + '\n')
    with open("error_client_analysis.txt", 'w+') as f:
        for i in range(0, len(error_analysis)):
            f.write(str(error_analysis[i][0]) + " " + str(error_analysis[i][1]) +
                    " " + str(error_analysis[i][2]) + '\n')


if __name__ == '__main__':
    main()