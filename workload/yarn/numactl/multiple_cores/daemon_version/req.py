#!/usr/bin/env python

import threading
import os
import shutil
import sys
import time
import random
import subprocess
import socket
import struct
import re

def main(argList):
    local_port = 10101
    req_count = argList[0]
    container_id = argList[1]
    socklocal = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socklocal.connect(("localhost", int(local_port)))
    message = struct.pack("I39s", int(req_count), container_id + "\n")
    socklocal.send(message)
    if ((int(req_count) != 0) and (int(req_count) != 9999)):
        result = socklocal.recv(1024)
        print result
    socklocal.close()

if __name__ == '__main__':
    main(sys.argv[1:])
