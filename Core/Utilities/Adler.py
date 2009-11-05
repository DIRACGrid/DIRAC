# $HeadURL$
__RCSID__ = "$Id$"

"""
   Collection of DIRAC useful adler32 related tools
   by default on Error they return None
"""

import os
import types
from zlib import adler32

def intAdlerToHex(intAdler):
  try:
    # Will always be 8 hex digits made from a positive integer
    return hex(intAdler & 0xffffffff).lower().replace('l','').replace('x','0')[-8:]
  except Exception,x:
    print x
    return False

def hexAdlerToInt(hexAdler,pos=True):
  if type(hexAdler) in [types.LongType,types.IntType]:
    return hexAdler & 0xffffffff
  # First make sure we can parse the hex properly
  hexAdler = hexAdler.lower().replace('l','')
  hexAdler = hexAdler[-8:]
  hexAdler = hexAdler.replace('x','0')
  if not pos:
    hexAdler = "-%s" % hexAdler
  try:
    # Will always try to return the positive integer value of the provided hex
    return int(hexAdler,16) & 0xffffffff    
  except Exception,x:
    print x
    return False

def compareAdler(adler1,adler2):
  adler1s = [hexAdlerToInt(adler1),hexAdlerToInt(adler1,False)]
  if False in adler1s: adler1s.remove(False)
  adler2s = [hexAdlerToInt(adler2),hexAdlerToInt(adler2,False)]
  if False in adler2s: adler2s.remove(False)
  for adler1 in adler1s:
    if adler1 in adler2s:
      return True
  return False

def fileAdler(fileName):
  """ Calculate alder32 of the supplied file
  """
  try:
    inputFile = open(fileName)
    inputFileSize = os.stat(fileName)[6]
    increment = 1024*1024
    fullBlocks = inputFileSize/increment
    remainder = inputFileSize%increment
    mbString = inputFile.read(increment)
    myAdler = adler32(mbString)
    for i in range(fullBlocks-1):
      mbString = inputFile.read(increment)
      myAdler = adler32(mbString,myAdler)
    mbString = inputFile.read(remainder)
    myAdler = adler32(mbString,myAdler)
    inputFile.close()
    return intAdlerToHex(myAdler)
  except Exception,x:
    return False

def stringAdler(string):
  """ Calculate adler32 of the supplied string
  """
  try:
    intAdler = adler32(string)
    return intAdlerToHex(intAdler)
  except Exception,x:
    return False

if __name__=="__main__":
  import sys
  for p in sys.argv[1:]:
    print "%s : %s " % ( p, fileAdler( p ) )
