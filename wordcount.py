#!/usr/bin/python -tt
'''
Programme en séquentiel
'''

import sys


def source_reader(filename):
  lines=[]
  allWords=[]
  words={}
  with open(filename, '+r') as file:
    lines=file.read().splitlines()
    for line in lines:
      for word in line.split(' '):
        allWords.append(word.lower())
    allWords=filter(None,allWords)
    for word in allWords:
        words.update({word:words.setdefault(word,0)+1})
  return words

def print_words(filename):
  print(source_reader(filename))
  
def print_top(filename):
  words=[]
  d=[]
  dico=source_reader(filename)

  for key,value in dico.items():
    words.append((key,value))
  
  for tuple in words:
    d.append(tuple[-1])
  d.sort()
  d.reverse()
  for tuple in words:
    for value in range(len(d)):
      if tuple[-1]==d[value]:
        d[value]=tuple
        break
  return d

def fusion(tabA, tabB):
  if len(tabA)==0:
    return tabB
  elif len(tabB)==0:
    return tabA
  elif tabA[0]<=tabB[0]:
    return tabA[0]^fusion(tabA[2:],tabB)
  else:
    return tabB[0]^fusion(tabB[2:],tabA)

def tri_fusion(tab):
  if tab[0] <= tab[len(tab)-1]:
    return tab
  else: 
    return fusion(tri_fusion(tab[:len(tab)/2]), tri_fusion(tab[len(tab)/2:]))

def top_alpha(filename):
  d=print_top(filename)
  d.sort()
  for i in range(len(d)):
    if d[i-1][1]<d[i][1]:
      tmp=d[i]
      d[i]=d[i-1]
      d[i-1]=tmp
  return d

def main():
  import time
  if len(sys.argv) != 3:
    print ('usage: ./wordcount.py {--count | --tri} file')
    sys.exit(1)

  option = sys.argv[1]
  filename = sys.argv[2]
  if option == '--count':
    print_words(filename)

  elif option == '--tri':
    startTime=time.time()
    d=print_top(filename)
    print(f'It took {(time.time()-startTime)} ms')
    for tuple in d[:20]:
      print(f"{d.index(tuple)+1}:{tuple}")

  elif option == '--trialpha':
    startTime=time.time()
    d=top_alpha(filename)
    print(f'It took {(time.time()-startTime)} ms')
    print(d[:50])

  else:
    print ('unknown option: ' + option)
    sys.exit(1)

if __name__ == '__main__':
  main()
