#! /bin/python3
    
import subprocess
import multiprocessing as mp
from functools import partial
from os import sys 

def getValidIps(listAddr):
    file=open(f'{listAddr}','r+')
    ips=[]
    for line in file.readlines():
        ips.append(line)
    with mp.Pool(10) as p:
        validIps = list(filter(None,p.map(__test_ips,ips)))
    return validIps

def __test_ips(line):
    try:
        process= subprocess.run([f'ssh mgardette@{line.strip()} \"hostname\"'],encoding="UTF-8",timeout=5,capture_output=True,shell=True)
        return f'{line.strip()}'
    except subprocess.TimeoutExpired:
        return ''
        sys.stderr.write(f'TimeoutError : {line}\n')

def __copySplit(toDeploy,address):
    try: 
        yourName="mgardette"
        mkdir = subprocess.run([f'ssh {yourName}@{address.strip()} \"mkdir -p /tmp/{yourName}/splits/\"'],encoding="UTF-8",timeout=10,capture_output=True,shell=True)
    except:
        return f'{mkdir.stderr}'
        raise
    try:
        process= subprocess.run([f'scp {toDeploy} {yourName}@{address.strip()}:/tmp/{yourName}/splits'],encoding="UTF-8",timeout=10,capture_output=True,shell=True)
        sys.stdout.write(f'{process.stdout} sent to {address.strip()}\n')
        return address.strip()
    except:
        sys.stderr.write(f'Could not send to {process.stderr}\n')
        raise

def copySplits(filesToDeploy,addressesFile):   
    ips=getValidIps(addressesFile)
    splitArg=[]
    for file, ip in zip(filesToDeploy, ips):
        splitArg.append((file,ip))
    with mp.Pool(5) as p:
        ipSent=list(p.starmap(__copySplit,splitArg))
        return (ipSent)

def __copySlave(slave,address):
    yourName="mgardette"
    try:
        process= subprocess.run([f'scp {slave} {yourName}@{address.strip()}:/tmp/{yourName}'],encoding="UTF-8",timeout=10,capture_output=True,shell=True)
        sys.stdout.write(f'Slave sent to {address.strip()}\n')
    except:
        sys.stderr.write(f'Could not send to {process.stderr}\n')

def copySlaves(file,ips):
    partSlave=partial(__copySlave,file)
    with mp.Pool(5) as p:
        p.map(partSlave,ips)
    return (ips)

'''
elif option == '--copyfile':
    
    valid_ips=open('./valid_ips','r+')
    ips=[]
    for line in valid_ips.readlines():
        ips.append(line)

    with mp.Pool(10) as p:
        oskour=p.map(test_ips,ips)
    valid_ips.close()
    valid_ips=open('./valid_ips','w+')
    valid_ips.writelines(oskour)

    function=partial(copyfile,filename)
    with mp.Pool(10) as p:
        oskour = p.map(function,ips)

else:
    print ('unknown option: ' + option)
    sys.exit(1)

if __name__ == '__main__':
main()
'''

# Map des différents veuleurs