# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/File.py,v 1.19 2008/02/13 10:27:56 joel Exp $
__RCSID__ = "$Id: File.py,v 1.19 2008/02/13 10:27:56 joel Exp $"

"""
   Collection of DIRAC useful file related modules
   by default on Error they return None
"""

import os, re
import md5
import random

def makeGuid( fileName=None ):
  """
     Utility to create GUID's, if a filename it is provided the
     GUID will correspond to its hexadecimal md5 checksum
     the format is capitalized 8-4-4-4-12, otherwise a random seed it is
     used to create a GUID
  """
  myMd5 = md5.new()
  if fileName:
    try:
      fd = open( fileName, 'r' )
      data = fd.read( 1024*1024 )
      myMd5.update( data )
      fd.close()
    except:
      return None
  else:
    myMd5.update( str( random.getrandbits( 128 ) ) )

  md5HexString = myMd5.hexdigest()

  md5String = "%s-%s-%s-%s-%s" % ( md5HexString[0:8],
                                   md5HexString[8:12],
                                   md5HexString[12:16],
                                   md5HexString[16:20],
                                   md5HexString[20:32] )

  return md5String.upper()

def checkGuid(guid):
    """
       Checks whether a supplied GUID is of the correct format.
       The guid is a string of 36 characters long split into 5 parts of length 8-4-4-4-12.

       INPUT:     guid - string to be checked .
       OPERATION: Split the string on '-', checking each part is correct length.
       OUTPUT:    Returns 1 if the supplied string is a GUID.
                  Returns 0 otherwise.
    """
    guidSplit = guid.split('-')
    if len(guid) == 36 \
      and len(guidSplit[0]) == 8 \
        and len(guidSplit[1]) == 4 \
          and len(guidSplit[2]) == 4 \
            and len(guidSplit[3]) ==4 \
              and len(guidSplit[4]) == 12:
      return True
    else:
      return False

def getSize( fileName ):
  try:
    return os.stat( fileName )[6]
  except Exception, v:
    return -1


def makeProductionLfn(self,filetuple,mode,prodstring):
    """ Constructs the logical file name according to LHCb conventions.
    Returns the lfn without 'lfn:' prepended
    """

    try:
      jobid = int(self.JOB_ID)
      jobindex = string.zfill(jobid/10000,4)
    except:
      jobindex = '0000'

    fname = filetuple[0]
    if re.search('lfn:',fname):
      return fname.replace('lfn:','')
    else:
      if re.search('LFN:',fname):
        return fname.replace('LFN:','')
      else:
#        path = makeProductionPath(self,mode,prodstring)
        return self.LFN_ROOT+'/'+filetuple[1]+'/'+prodstring+'/'+jobindex+'/'+filetuple[0]

def makeProductionPath(self,typeName,mode,prodstring,log=False):
  """ Constructs the path in the logical name space where the output
  data for the given production will go.
  """
#  result = '/lhcb/'+mode+'/'+self.CONFIG_NAME+'/'+self.CONFIG_VERSION+'/'+prodstring+'/'
#  result = '/lhcb/'+self.DataType+'/'+self.YEAR+'/'+self.appType.upper()+'/'+self.CONFIG_NAME+'/'+prodstring+'/'
  result = self.LFN_ROOT+'/'+typeName+'/'+self.CONFIG_NAME+'/'+prodstring+'/'

  if log:
    try:
      jobid = int(self.JOB_ID)
      jobindex = string.zfill(jobid/10000,4)
    except:
      jobindex = '0000'
    result += 'LOG/'+jobindex

  return result
