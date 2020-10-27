#! /bin/python3

import subprocess
import multiprocessing as mp
from os import sys  

def mPropre(line):
    try:
        process= subprocess.run([f'ssh mgardette@{line.strip()} \"rm -rf /tmp/mgardette\"'],encoding="UTF-8",timeout=5,capture_output=True,shell=True)
        sys.stdout.write(f'Dossier de travail supprimé sur {line.strip()}\n')
    except subprocess.TimeoutExpired:
        sys.stderr.write(f'TimeoutError : {line.strip()}\n')
        return ''
        

def main():
    if len(sys.argv)!=2:
        sys.stderr.write("Program misses an argument ! Usage: clean.py machinesToClean")
    else:
        valid_ips=open(sys.argv[1],'r')
        ips=[]
        for line in valid_ips.readlines():
            ips.append(line)
        with mp.Pool(10) as p:
            oskour = p.map(mPropre,ips)

if __name__ == "__main__":
    main()