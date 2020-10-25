#! /bin/python3

import subprocess
from functools import partial
import multiprocessing as mp
from os import sys

import deploy as dp

def executeUM(address):
    try:
        #commande=
        process= subprocess.run([f'ssh mgardette@{address} \'/tmp/mgardette/slave.py 0 $(find /tmp/mgardette/splits/ -type f -print)\''],encoding="UTF-8",timeout=15,capture_output=True,shell=True)
        sys.stdout.write(f'{address}: {process.stdout}\n')
    except subprocess.TimeoutExpired:
        sys.stderr.write(f'TimeoutError : {address}\n')
    #except

#print(dp.getValidIps('src/available_ips'))

ipSent=dp.copySplits(['src/S0','src/S1','src/S2'],'src/available_ips')
dp.copySlaves('src/slave.py',ipSent)

with mp.Pool(5) as p:
    p.map(executeUM,ipSent)