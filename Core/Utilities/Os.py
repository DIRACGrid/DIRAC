# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/Os.py,v 1.5 2007/09/20 10:38:11 atsareg Exp $
__RCSID__ = "$Id: Os.py,v 1.5 2007/09/20 10:38:11 atsareg Exp $"
"""
   Collection of DIRAC useful os related modules
   by default on Error they return None
"""

from types                          import StringTypes
from string                         import split,strip,join

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
    return 0
  comm = 'df -P -m %s | tail -1' % path
  resultDF = shellCall(0,comm)
  if resultDF['OK']:
    output = resultDF['Value'][1]
    if output.find('/afs') >= 0 :    # AFS disk space
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
        return 0
    else:
      print output
      fields = output.split()
      return int(fields[3])
  else:
    return 0

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

def fixLDPath( root, ldpath, directory):
  """
      This is a utility to fix the LD_LIBRARY_PATH on Grid WNs. The
      shared libraries from the original LD_LIBRARY_PATH are linked to
      the locally specified directory.  For Windows (and in general)
      this needs some refurbishment.
  """

  if os.path.exists(directory):
    shutil.rmtree(directory)

  os.mkdir(directory)
  os.chdir(directory)
  uniqueLD = uniquePath(ldpath)

  if DEBUG:
    print 'Unique LD LIBRARY PATH is:'
    print uniqueLD
    sys.stdout.flush()

  ldlist = string.split(uniqueLD,':')
  if DEBUG:
    print ''
    print 'LD List is:'
    print ldlist
    print ''
    sys.stdout.flush()

  for path in ldlist:
    if os.path.exists(path):

      if DEBUG:
        print 'Searching for shared libraries in:'
        print path
        print '-----------------------------------------------'
        res = shellCall(0,'ls '+path+'/*.so*')
        if res:
          print res['Value']
        else:
          print res
        print '-----------------------------------------------'

      output = shellCall(0,'ls '+path+'/*.so*')
      #must be tidied for Windows (same below)

      if DEBUG:
        if not output:
          print '**************************'
          print 'Warning, problem with ls:'
          print output
          print '**************************'

      ldlibs = string.split(output)
      for lib in ldlibs:
        if os.path.exists(lib):
          filename = os.path.basename(lib)
          output = shellCall(0,'ln -s '+str(lib)+' '+str(filename))
          #N.B. for Windows this should be a copy...
          if DEBUG:
            if status:
              print '********************************'
              print 'Warning, problem creating link:'
              print 'File: ',filename
              print 'Path: ',lib
              print output
              print '********************************'

      if DEBUG:
        print 'Searching for rootmap file in:'
        print path
        print '-----------------------------------------------'
        res = shellCall(0,'ls '+path+'/*rootmap*')
        if res:
          print res['Value']
        else:
          print res
        print '-----------------------------------------------'

      output = shellCall(0,'ls '+path+'/*rootmap*')

      if DEBUG:
        if not output:
          print '**************************'
          print 'Warning, problem with rootmap:'
          print output
          print '**************************'

      ldlibs = string.split(output)
      for lib in ldlibs:
        if os.path.exists(lib):
          if re.search('RELAX',lib) is not None:
            filename = os.path.basename(lib)
            output = shellCall(0,'ln -s '+str(lib)+' '+str(filename))
            if DEBUG:
              if not output:
                print '********************************'
                print 'Warning, problem creating link:'
                print 'File: ',filename
                print 'Path: ',lib
                print output
                print '********************************'

  os.chdir(start)
  sys.stdout.flush()