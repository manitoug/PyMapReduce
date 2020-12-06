#! /bin/python3
    
import subprocess
import multiprocessing as mp
from functools import partial
from os import sys 

TIMOUTCOPY=100000

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
        sys.stderr.write(f'TimeoutError : {line}\n')
        return ''

def __copySplit(toDeploy,address):
    try: 
        yourName="mgardette"
        mkdir = subprocess.run([f'ssh {yourName}@{address.strip()} \"mkdir -p /tmp/{yourName}/splits/\"'],encoding="UTF-8",timeout=10,capture_output=True,shell=True)
    except:
        return f'{mkdir.stderr}'
        
    try:
        process= subprocess.run([f'scp {toDeploy} {yourName}@{address.strip()}:/tmp/{yourName}/splits'],encoding="UTF-8",timeout=TIMOUTCOPY,capture_output=True,shell=True)
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

def __copyFile(slave,address):
    yourName="mgardette"
    try: 
        mkdir = subprocess.run([f'ssh {yourName}@{address.strip()} \"mkdir -p /tmp/{yourName}\"'],encoding="UTF-8",timeout=10,capture_output=True,shell=True)
    except:
        return f'{mkdir.stderr}'
        
    try:
        process= subprocess.run([f'scp {slave} {yourName}@{address.strip(" ")}:/tmp/{yourName}'],encoding="UTF-8",timeout=TIMOUTCOPY,capture_output=True,shell=True)
    except:
        sys.stdout.write(f'Could not send to {process.stderr}\n')
    sys.stdout.write(f'File sent to {address.strip()}\n')

def copyFiles(file,ips):
    partFile=partial(__copyFile,file)
    with mp.Pool(5) as p:
        p.map(partFile,ips)
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