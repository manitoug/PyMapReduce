#! /bin/python3

import subprocess
import asyncio
import platform as pt
import sys
import os
import multiprocessing as mp
import zlib as z
from datetime import datetime
from functools import partial

def unsortedMap(filename):
    start=datetime.now()
    lines=[]
    allWords=[]
    words={}
    with open(filename, '+r') as file:
        lines=file.read().splitlines()
        for line in lines:
            for word in line.split(' '):
                allWords.append(word.lower())
        ''' On réserve pour le reduce
       print(list(allWords))
            for word in allWords:
                words.update({word:words.setdefault(word,1)})
        '''
    print(f'Mapping took {datetime.now()-start}----------------------\n')
    return list(filter(None,allWords))

def hashUM(filename):
    try: 
        mkdir = subprocess.run([f'mkdir -p /tmp/mgardette/shuffles/'],encoding="UTF-8",timeout=3,capture_output=True,shell=True)
    except:
        sys.stderr.write(f'{mkdir.stderr}')
        raise
    try:
        start=datetime.now()
        with open(filename, 'r') as file:
            lines=file.read().splitlines()
            hashes=[]
            for line in lines:
                hashed=z.adler32(bytearray(line, "utf8"))
                with open(f'/tmp/mgardette/shuffles/{hashed}-{pt.uname().node}', 'a+') as hashFile:
                    hashFile.write(f'{line}\n')
                    hashFile.close()
                hashes.append(hashed)
        file.close()
        print(f'Hashing Maps took {datetime.now()-start}----------------------\n')
        return hashes
    except:
        sys.stderr.write("Open failed")

def __sendShuffle(address,toDeploy):
    try: 
        #print(f'Address= {address}; Shuffle= {toDeploy}')
        yourName="mgardette"
        mkdir = subprocess.run([f'ssh {yourName}@{address.strip()} \"mkdir -p /tmp/{yourName}/shufflesreceived/\"'],encoding="UTF-8",timeout=100,capture_output=True,shell=True)
    except:
        sys.stderr.write(f'{mkdir.stderr}') 
        raise 
    try:
        process= subprocess.run([f'scp -o StrictHostKeyChecking=no -r -p {toDeploy} {yourName}@{address}:/tmp/{yourName}/shufflesreceived'],encoding="UTF-8",timeout=1000,capture_output=True,shell=True)
        sys.stdout.write(process.stdout)#sys.stdout.write(f'{process.stdout} sent to {address}\n')
    except:
        sys.stderr.write(f'Could not send to {process.stderr}\n')
        raise

def sendShuffles(machineFile):
    with open(machineFile, 'r') as machines:
        start=datetime.now()
        lines=machines.read().splitlines()
        try:
            files = os.listdir('/tmp/mgardette/shuffles')
        except:
            sys.stderr.write('Could not open shuffle folder')
        addresses=[]
        filesToSend=[]
        for file in files:
            addresse=lines[int(hash(str(file).strip(f'-{pt.uname().node}')))%len(lines)]
            __sendShuffle(addresse,f'/tmp/mgardette/shuffles/{file}')
        machines.close()
    sys.stdout.write(f'Sending shuffle took {datetime.now()-start}----------------------\n')

def __reduceShuffle (file):
    cpt=0
    with open(f'/tmp/mgardette/shufflesreceived/{file}') as shuffled:
        lines=shuffled.readlines()
        text=lines[0].split(' ')[0]
        for line in lines:
            cpt+=1
    shuffled.close()
    redFile=file.split('-',maxsplit=1)[0]
    with open(f'/tmp/mgardette/reduces/{redFile}','w+') as reduce:
        lines=reduce.readlines()
    reduce.close()
    if len(lines)!=0:
        sys.stdout.write(int(lines[0].split(' ',maxsplit=1)[1]))
        cpt+=int(lines[0].split(' ',maxsplit=1)[1])
    with open(f'/tmp/mgardette/reduces/{redFile}','w') as reduce:
        reduce.write(f'{text} {cpt}\n')
    
    sys.stdout.write(f'{pt.uname().node}: Reduced SMs\n')
        

def reduceShuffles():
    try: 
        yourName="mgardette"
        mkdir = subprocess.run([f'mkdir -p /tmp/{yourName}/reduces/'],encoding="UTF-8",timeout=10,capture_output=True,shell=True)
    except:
        sys.stderr.write(f'{mkdir.stderr}') 
        raise
    try:
        start=datetime.now()
        files = os.listdir('/tmp/mgardette/shufflesreceived')
        #[__reduceShuffle(file) for file in files]
    except:
        sys.stderr.write('Could not open received shuffles folder')
        raise
    sys.stdout.write(f'Reduce took {datetime.now()-start}----------------------\n')

    '''
    try:
        with mp.Pool(3) as p:
            p.starmap(__sendShuffle,addresses,filesToSend)
    except:
        sys.stderr.write("multithread failure")
        raise
    '''
        
def main():
    if len(sys.argv)!=3:
        sys.stderr.write("Program misses an argument ! Usage: slave.py 0/1/2 fileToTreat")
    else:
        if sys.argv[1]=='0': #Mode 0: Calcul du map à partir du split
            try: 
                mkdir = subprocess.run([f'mkdir -p /tmp/mgardette/maps/'],encoding="UTF-8",timeout=3,capture_output=True,shell=True)
            except:
                sys.stderr.write(f'{mkdir.stderr}')
                raise

            try:
                umFile=open(f'/tmp/mgardette/maps/UM{sys.argv[2][-1]}','w+')
                um=unsortedMap(sys.argv[2])
                for item in um:
                    umFile.writelines(f'{item} 1\n')
                #sys.stdout.write(f'Successfully created {umFile.name}')
            except:
                sys.stderr.write("UMsort failed")
            #Je réserve pour le reduce
            '''for key in um.keys():
                umFile.writelines(f'{key} {um[key]}\n')'''
        elif sys.argv[1]=='1' : #Mode 1: Shuffle des Unsorted Maps
            try:
                hashes=hashUM(sys.argv[2])
                sys.stdout.write('Shuffled UMs !\n')
            except:
                sys.stderr.write('Failed to Shuffle UMs :(\n')
            try:
                sendShuffles('/tmp/mgardette/machines')
                sys.stdout.write('Shuffles sent !\n')
            except:
                sys.stderr.write('Could not send shuffles\n')
                
        elif sys.argv[1]=='2' : #Mode 2: Reduce des Shuffles
            reduceShuffles()
        else:
            sys.stderr.write("Wrong mode! Available modes are:\n0: Unsorted map\n")

if __name__ == "__main__":
    main()