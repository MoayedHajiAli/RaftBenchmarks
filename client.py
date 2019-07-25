import socket
import random
import time

#  CONSTRAINS:
ADDR_FAMILY = socket.AF_INET
SOCK_TYPE = socket.SOCK_STREAM
SERVERS_IP = ['172.16.112.12']
PORT = 65432
CLIENT_PORT = 65430
BUF_SIZE = 1024




def main():
    with socket.socket(ADDR_FAMILY, SOCK_TYPE) as mySocket:
        targetAddr = SERVERS_IP[random.randrange(0, len(SERVERS_IP))]
        mySocket.connect((targetAddr, CLIENT_PORT))
        error_cnt = 0.0
        request_cnt = 0.0
        while True:
            try:
                key, value = input().split()
            except EOFError as e:
                if request_cnt > 0:
                    time.sleep(4.0)
                    data = mySocket.recv(BUF_SIZE).decode()
                    e = time.time()
                    if data == '0':
                        with open("dealy_client_analysis.txt", "a+") as f:
                            f.write(targetAddr + " " + str(s) + " " + str(e) + '\n')
                    else:
                        error_cnt += 1
                        with open("error_client_analysis.txt", "a+") as f:
                            f.write(targetAddr + " " + data + '\n')
                    print(error_cnt/request_cnt)
                else:
                    print(0)
                break
            request_cnt += 1
            data = str(key + ":" + value)
            s = time.time()
            mySocket.sendall(data.encode())
            data = mySocket.recv(BUF_SIZE).decode()
            e = time.time()
            if data == '0':
                with open("dealy_client_analysis.txt", "a+") as f:
                    f.write(targetAddr + " " + str(s) + " " + str(e) + '\n')
            else:
                error_cnt += 1
                with open("error_client_analysis.txt", "a+") as f:
                    f.write(targetAddr + " " + data + '\n')
            #print(error_cnt/request_cnt)

if __name__ == '__main__':
    main()