# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/Os.py,v 1.8 2009/04/28 15:03:51 rgracian Exp $
__RCSID__ = "$Id: Os.py,v 1.8 2009/04/28 15:03:51 rgracian Exp $"
"""
   Collection of DIRAC useful os related modules
   by default on Error they return None
"""

from types                          import StringTypes
from string                         import join

from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.Core.Utilities import List

import shutil, os

DEBUG = 0

def uniquePath( path = None ):
  """
     Utility to squeeze the string containing a PATH-like value to
     leave only unique elements preserving the original order
  """

  if not StringTypes.__contains__( type( path ) ):
    return None

  try:
    elements = List.uniqueElements( List.fromChar( path, ":" ) )
    return join( elements, ":" )
  except:
    return None

def getDiskSpace(path='.'):
  """ Get the free disk space in the partition containing the path.
      The disk space is reported in MBytes. Returned 0 in case of any
      error, e.g. path does not exist
  """

  if not os.path.exists(path):
    return -1
  comm = 'df -P -m %s | tail -1' % path
  resultDF = shellCall(0,comm)
  if resultDF['OK']:
    output = resultDF['Value'][1]
    if output.find(' /afs') >= 0 :    # AFS disk space
      comm = 'fs lq | tail -1'
      resultAFS = shellCall(0,comm)
      if resultAFS['OK']:
        output = resultAFS['Value'][1]
        fields = output.split()
        quota = long(fields[1])
        used = long(fields[2])
        space = (quota-used)/1024
        return int(space)
      else:
        return -1
    else:
      print output
      fields = output.split()
      return int(fields[3])
  else:
    return -1

def getDirectorySize(path):
  """ Get the total size of the given directory in MB
  """

  comm = "du -s -m %s" % path
  result = shellCall(0,comm)
  if not result['OK']:
    return 0
  else:
    output = result['Value'][1]
    print output
    size = int(output.split()[0])
    return size

