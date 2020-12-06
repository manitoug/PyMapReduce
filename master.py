#! /bin/python3

import subprocess
from functools import partial
import multiprocessing as mp
from os import sys
import os
from datetime import datetime
import shutil
import time

import deploy as dp

TIMOUTEXECS=100
SPLITNBR=10

SPLITDIR='./splits'
ENTRYDATA='./data/code_domaine_fluvial'

def splitting(nbMachines, srcFile):
    with open(srcFile) as file:
        toSplit=file.read()

    filesSize=int(len(toSplit)/nbMachines)

    if os.path.isdir(SPLITDIR):
        for file in os.listdir(SPLITDIR):
            os.remove(f'{SPLITDIR}/{file}')
    else:
        os.mkdir(SPLITDIR)

    for i in range (nbMachines):
        buffer = toSplit[i*filesSize:(i+1)*filesSize]
        with open (f'{SPLITDIR}/S{i}','w+') as split :
            split.write(buffer)
        sys.stdout.write(f"S{i}: OK\n")
        split.close()
    sys.stdout.write('Splitting Done\n')

def executeMap(address):   
    try:
        #commande=
        process= subprocess.run([f'ssh mgardette@{address} \'/tmp/mgardette/slave.py 0 $(find /tmp/mgardette/splits/ -type f -print)\''],encoding="UTF-8",timeout=TIMOUTEXECS,capture_output=True,shell=True)
        sys.stdout.write(f'{address}: {process.stdout}\n')
    except subprocess.TimeoutExpired:
        sys.stderr.write(f'TimeoutError : {address}\n')
    except:
        sys.stderr.write(f'Erreur {address}: {process.stderr}\n')
            #except

def executeShuffle(address):
    try:
        process= subprocess.run([f'ssh mgardette@{address} \'/tmp/mgardette/slave.py 1 $(find /tmp/mgardette/maps/ -type f -print)\''],encoding="UTF-8",timeout=TIMOUTEXECS,capture_output=True,shell=True)
        sys.stdout.write(f'{address}: {process.stdout}\n')
    except:
        sys.stderr.write(f'Erreur {address}: {process.stderr}\n')
            #except

def executeReduce(address):
    try:
        process= subprocess.run([f'ssh mgardette@{address} \'/tmp/mgardette/slave.py 2 .\''],encoding="UTF-8",timeout=TIMOUTEXECS,capture_output=True,shell=True)
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
    with open('./valid_ips', 'w+') as valid:
        [valid.write(f'{line}\n') for line in dp.getValidIps('./available_ips')]
    splitting(SPLITNBR,ENTRYDATA)
    
    try:
        process= subprocess.run([f'./clean.py machines'],encoding="UTF-8",timeout=TIMOUTEXECS,capture_output=True,shell=True)
        sys.stdout.write(f'{process.stdout}\n')
    except:
        sys.stderr.write(f'{process.stderr}\n')
        raise

    machines=open('machines','w+')

    start = datetime.now()
    ipSent=dp.copySplits([f'{SPLITDIR}/{split}' for split in os.listdir('./splits')],'./valid_ips')
    print(ipSent)
    for ip in ipSent:
        machines.write(f'{ip}\n')
    machines.close()
    dp.copyFiles('slave.py',ipSent)
    dp.copyFiles('machines',ipSent)
    print(f'Sending took {datetime.now()-start}----------------------\n')
    time.sleep(5)

    with mp.Pool(5) as p:
        p.map(executeMap,ipSent)
        p.map(executeShuffle,ipSent)
        p.map(executeReduce,ipSent)
        #mapping=p.map_async(executeMap,ipSent,callback=shuffle_async(ipSent))
        #print(mapping.get())

if __name__ == "__main__":
    main()