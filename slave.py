#! /bin/python3

import subprocess
import sys

def unsortedMap(filename):
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
    return list(filter(None,allWords))

def main():
    if len(sys.argv)!=3:
        sys.stderr.write("Program misses an argument ! Usage: slave.py 0/1 fileToTreat")
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
                sys.stdout.write(f'Successfully created {umFile.name}')
            except:
                sys.stderr.write("UMsort failed")
            #Je réserve pour le reduce
            '''for key in um.keys():
                umFile.writelines(f'{key} {um[key]}\n')'''
        else:
            sys.stderr.write("Wrong mode! Available modes are:\n0: Unsorted map\n")

if __name__ == "__main__":
    main()