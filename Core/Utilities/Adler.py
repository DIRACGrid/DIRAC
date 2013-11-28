# $HeadURL$
""" Collection of DIRAC useful adler32 related tools.
    By default on Error they return None. 

   .. warning::

      On error False is returned.

   .. warning::

      All exceptions report to the stdout.
"""

__RCSID__ = "$Id$"

import types
from zlib import adler32

def intAdlerToHex(intAdler):
  """Change adler32 checksum base from decimal to hex.
 
  :param integer intAdler: adler32 checksum
  :return: 8 digit hex string  
  """
  try:
    # Will always be 8 hex digits made from a positive integer
    return hex(intAdler & 0xffffffff).lower().replace('l','').replace('x','0')[-8:]
  except Exception, error:
    print error
    return False

def hexAdlerToInt(hexAdler, pos=True):
  """Change hex base to decimal for adler32 checksum.

  :param mixed hexAdler: hex based adler32 checksum integer or a string
  :param boolean pos: flag to determine sign (default True = positive)
  """
  if type(hexAdler) in [ types.LongType, types.IntType ]:
    return hexAdler & 0xffffffff
  # First make sure we can parse the hex properly
  hexAdler = hexAdler.lower().replace('l','')
  hexAdler = hexAdler[-8:]
  hexAdler = hexAdler.replace('x','0')
  if not pos:
    hexAdler = "-%s" % hexAdler
  try:
    # Will always try to return the positive integer value of the provided hex
    return int(hexAdler, 16) & 0xffffffff    
  except Exception, error:
    print error
    return False

def compareAdler( adler1, adler2 ):
  """Check equality between two adler32 checksums.
 
  :param adler1: 1st checksum
  :param adler2: 2nd checksum
  :return: True (False) if cheksums are (not) equal
  """
  adler1s = [ hexAdlerToInt( adler1 ), hexAdlerToInt( adler1, False ) ]
  if False in adler1s: 
    adler1s.remove(False)
  adler2s = [ hexAdlerToInt( adler2 ), hexAdlerToInt( adler2, False ) ]
  if False in adler2s: 
    adler2s.remove(False)
  for adler1 in adler1s:
    if adler1 in adler2s:
      return True
  return False


def fileAdler( fileName ):
  """Calculate alder32 checksum of the supplied file.

  :param str fileName: path to file 
  """
  def readChunk( fd, size=1048576 ):
    """Return data from file descriptor in chunk of size size.

    :param fd: file descriptor
    :param integer size: size of data chunk in bytes (default 1024 * 1024 = 1048576)
    """
    while True:
      data = fd.read( size )
      if not data: 
        break
      yield data
  try:
    inputFile = open(fileName)
  except Exception, error:
    print error
    return False
  myAdler = 1
  for data in readChunk( inputFile ):
    myAdler = adler32( data, myAdler )
  inputFile.close()
  return intAdlerToHex( myAdler) 


def stringAdler( string ):
  """Calculate adler32 of the supplied string.

  :param str string: data
  """
  try:
    intAdler = adler32(string)
    return intAdlerToHex(intAdler)
  except Exception, error:
    print error
    return False

if __name__ == "__main__":
  import sys
  for p in sys.argv[1:]:
    print "%s : %s" % ( p, fileAdler( p ) )
