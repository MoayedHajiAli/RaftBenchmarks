import sys
import random
import time
from subprocess import Popen, PIPE

# CONSTRAINS
MAX_RPS = 50
MIN_PACKET_SIZE = 5
MAX_PACKET_SIZE = 1024
PACKET_SIZE_DIFFERENCE = 50
ITERATION_TEST = 10
VALID_ERROR_PERCENTAGE = 0.1

def generateString(Size):
    f = ""
    for _ in range(0, Size):
        f += chr(random.randrange(0, 26) + ord('a'))
    return f

def checkErr(packetSize, rps):
    err_rate = 0.0
    for _ in range(ITERATION_TEST):
        st = time.time()
        cmd = [sys.executable, 'client.py']
        p = Popen(cmd, stdin=PIPE, stdout=PIPE)
        inp = []
        for __ in range(0, int(rps)):
            inp.append(generateString(int(packetSize/2)) + " " + generateString(int(packetSize/2)))
        s = '\n'.join(inp)
        #print(out.decode())
        err_rate += float(p.communicate(input=s.encode())[0].decode())
        if time.time()-st > 1.0:
            return 1
        time.sleep(1.0 - (time.time()-st))
    print(err_rate/ITERATION_TEST)
    return err_rate/ITERATION_TEST

def main():

    print("All the data in RPS_client_analysis.txt will be deleted y|n ?")
    res = input()
    if(res == 'n'):
        sys.exit(0)
    cmd = [sys.executable, 'client.py']
    with open("RPS_client_analysis.txt", 'w+') as f:
        for packetSize in range(MIN_PACKET_SIZE, MAX_PACKET_SIZE, PACKET_SIZE_DIFFERENCE):
            lo, hi = 1, MAX_RPS
            while lo < hi:
                if lo == hi-1:
                    if checkErr(packetSize, hi) <= VALID_ERROR_PERCENTAGE:
                        lo = hi
                    break
                mid = int((lo + hi)/2)
                if checkErr(packetSize, mid) <= VALID_ERROR_PERCENTAGE:
                    lo = mid
                else:
                    hi = mid-1
                print (lo, hi)
            f.write(str(packetSize) + " " + str(lo) + '\n')


if __name__ == '__main__':
    main()