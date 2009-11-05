#!/usr/bin/env python
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/WorkloadManagementSystem/Agent/JobCleaningAgent.py $

__RCSID__ = "$Id: JobCleaningAgent.py 18064 2009-11-05 19:40:01Z acasajus $"

import platform
import sys

### Command line interface

def getPlatformString():
  # Modified to return our desired platform string, R. Graciani
  platformTuple = ( platform.system(), platform.machine() )
  if platformTuple[0] == 'Linux':
    # get version of higher libc installed
    if platform.machine().find( '64' ) != -1:
      lib = '/lib64'
    else:
      lib = '/lib'
    libs = []
    for lib in os.listdir( lib ):
      if lib.find( 'libc-' ) == 0 or lib.find( 'libc.so' ) == 0 : libs.append( os.path.join( '/lib' , lib ) )
    libs.sort()
    platformTuple += ( '-'.join( platform.libc_ver( libs[-1] ) ), )
    # platformTuple += ( '-'.join(libc_ver('/lib/libc.so.6')),)
  elif platformTuple[0] == 'Darwin':
    platformTuple += ( '.'.join( platform.mac_ver()[0].split( "." )[:2] ), )
  elif platformTuple[0] == 'Windows':
    platformTuple += ( platform.win32_ver()[0], )
  else:
    platfromTuple += ( platform.release() )

  platformString = "%s_%s_%s" % platformTuple

  return platformString
  
if __name__=="__main__":
  print getPlatformString()