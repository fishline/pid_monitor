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
    socklocal = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socklocal.connect(("localhost", int(local_port)))
    message = struct.pack("I8s", 9999, "test123\n")
    socklocal.send(message)
    socklocal.close()

if __name__ == '__main__':
    main(sys.argv[1:])
