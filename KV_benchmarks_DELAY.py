import sys
import random
import socket
import time
import _thread
from subprocess import Popen, PIPE
inp = []
ADDR_FAMILY = socket.AF_INET
SOCK_TYPE = socket.SOCK_STREAM
SERVERS_IP = ['172.16.112.12']
PORT = 65432
CLIENT_PORT = 65430
BUF_SIZE = 1024
commited = 0
err_cnt = 0
delay_analysis = []
error_analysis = []
change_in_leader = False

def generateString(Size):
    f = ""
    for _ in range(0, Size):
        f += chr(random.randrange(0, 26) + ord('a'))
    return f

def send_packets():
    global change_in_leader, commited
    i = 0
    while True:
        if commited == len(inp):
            return
        if change_in_leader:
            i = commited
        while i < len(inp):
            if change_in_leader:
                i = commited
                change_in_leader = False
            try:
                mySocket.sendall((inp[i]+'#'+str(time.time())+':').encode())
                #print((inp[i]+'#'+str(time.time())+':'))
                i += 1
            except socket.error:
                connect()
                i = commited


def connect(IP = None):
    print("new Connection")
    global mySocket, change_in_leader
    try:
        mySocket.close()
    except:
        pass
    mySocket = socket.socket(ADDR_FAMILY, SOCK_TYPE)
    is_changed = True
    if IP:
        try:
            mySocket.connect((IP, CLIENT_PORT))
            LeaderIP = mySocket.recv(BUF_SIZE).decode()
            while LeaderIP != str(IP):
                mySocket.close()
                IP = LeaderIP
                try:
                    mySocket = socket.socket(ADDR_FAMILY, SOCK_TYPE)
                    mySocket.connect((LeaderIP, CLIENT_PORT))
                except socket.error:
                    is_changed = False
                    break
                LeaderIP = str(mySocket.recv(BUF_SIZE).decode())
            if is_changed:
                change_in_leader = True
                return
        except socket.error:
            pass
    is_changed = True
    for i in range(0, len(SERVERS_IP)):
        targetAddr = SERVERS_IP[i]
        try:
            mySocket.connect((targetAddr, CLIENT_PORT))
            LeaderIP = str(mySocket.recv(BUF_SIZE).decode())
            while LeaderIP != str(targetAddr):
                mySocket.close()
                print("closed")
                targetAddr = LeaderIP
                try:
                    mySocket = socket.socket(ADDR_FAMILY, SOCK_TYPE)
                    mySocket.connect((LeaderIP, CLIENT_PORT))
                    print("connected to the leader")
                except socket.error:
                    is_changed = False
                    break
                LeaderIP = str(mySocket.recv(BUF_SIZE).decode())
            if is_changed:
                change_in_leader = True
                return
        except socket.error:
            continue
    print("Could not connect to any server")
    exit(0)


def revc_respone():
    global mySocket, commited, delay_analysis, error_analysis, err_cnt
    mySocket.settimeout(4.0)
    ret = ''
    while True:
        try:
            ret += mySocket.recv(BUF_SIZE).decode()
            ind1, ind2 = ret.find('#', 1), -1
            while len(ret) and ind1 != -1:
                #print(ret)
                err = int(ret[ind2 + 1:ind1])
                tem = ret.find('#', ind1 + 1)
                if tem == -1:
                    break
                ind2 = tem
                res = ret[ind1 + 1:ind2]
                #print("ret", err, res)
                if err == -1:
                    connect(res)
                elif err == 0:
                    delay_analysis.append((res, time.time()))
                    commited += 1
                else:
                    error_analysis.append((err, res, time.time()))
                    err_cnt += 1
                ind1 = ret.find('#', ind2 + 1)
            if ind2 != 0:
                ret = ret[ind2+1:]
        except socket.timeout:
            print("socket timeout in receiving from the server" + str(commited))
            if commited == len(inp):
                return
            else:
                connect()
        except socket.error as e:
            print("Error in connection", e, commited)
            connect()

def main():
    global inp, conn
    if(len(sys.argv) < 4):
        print("the number of packets - the size of the key - the size of the value")
        sys.exit(0)
    cmdSize = int(sys.argv[1])
    keySize = int(sys.argv[2])
    valueSize = int(sys.argv[3])
    print("All the data in delay_client_analysis and error_client_analysis will be delated y|n ?")
    res = input()
    if(res != 'y'):
        sys.exit(0)
    for _ in range(0, cmdSize):
        inp.append(generateString(keySize) + ":" + generateString(valueSize))
    connect()
    _thread.start_new_thread(send_packets, ())
    _thread.start_new_thread(revc_respone, ())
    while commited != len(inp):
        time.sleep(0.1)
    print(len(delay_analysis))
    with open("dealy_client_analysis.txt", 'w+') as f:
        for i in range(0, len(delay_analysis)):
            f.write(str(delay_analysis[i][0]) + " " + str(delay_analysis[i][1]) + '\n')
    with open("error_client_analysis.txt", 'w+') as f:
        for i in range(0, len(error_analysis)):
            f.write(str(error_analysis[i][0]) + " " + str(error_analysis[i][1]) +
                    " " + str(error_analysis[i][2]) + '\n')


if __name__ == '__main__':
    main()