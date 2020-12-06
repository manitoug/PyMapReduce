#! /bin/python3

import subprocess
from functools import partial
import multiprocessing as mp
from os import sys

import deploy as dp

def executeMap(address):
    try:
        #commande=
        process= subprocess.run([f'ssh mgardette@{address} \'/tmp/mgardette/slave.py 0 $(find /tmp/mgardette/splits/ -type f -print)\''],encoding="UTF-8",timeout=15,capture_output=True,shell=True)
        sys.stdout.write(f'{address}: {process.stdout}\n')
    except subprocess.TimeoutExpired:
        sys.stderr.write(f'TimeoutError : {address}\n')
    except:
        sys.stderr.write(f'Erreur {address}: {process.stderr}\n')
            #except

def executeShuffle(address):
    try:
        process= subprocess.run([f'ssh mgardette@{address} \'/tmp/mgardette/slave.py 1 $(find /tmp/mgardette/maps/ -type f -print)\''],encoding="UTF-8",timeout=15,capture_output=True,shell=True)
        sys.stdout.write(f'{address}: {process.stdout}\n')
    except:
        sys.stderr.write(f'Erreur {address}: {process.stderr}\n')
            #except

def executeReduce(address):
    try:
        process= subprocess.run([f'ssh mgardette@{address} \'/tmp/mgardette/slave.py 2 .\''],encoding="UTF-8",timeout=15,capture_output=True,shell=True)
        sys.stdout.write(f'{address}: {process.stdout}\n')
    except:
        sys.stderr.write(f'Erreur {address}: {process.stderr}\n')
            #except

def shuffle_async(ipSent):
    with mp.Pool(5) as p:
        shuffling=p.map_async(executeShuffle,ipSent,callback=reduce_async(ipSent))
        print(shuffling.get())

def reduce_async(ipSent):
    with mp.Pool(5) as p:
        reducing=p.map_async(executeReduce,ipSent)
        print(reducing.get())

def main():
    #print(dp.getValidIps('src/available_ips'))
    try:
        process= subprocess.run([f'./clean.py machines'],encoding="UTF-8",timeout=15,capture_output=True,shell=True)
        sys.stdout.write(f'{process.stdout}\n')
    except:
        sys.stderr.write(f'{process.stderr}\n')
        raise

    machines=open('machines','w+')
    ipSent=dp.copySplits(['S0','S1','S2'],'available_ips')
    for ip in ipSent:
        machines.write(f'{ip}\n')
    machines.close()
    dp.copyFiles('slave.py',ipSent)
    dp.copyFiles('machines',ipSent)

    with mp.Pool(5) as p:
        mapping=p.map_async(executeMap,ipSent,callback=shuffle_async(ipSent))
        print(mapping.get())
        

if __name__ == "__main__":
    main()