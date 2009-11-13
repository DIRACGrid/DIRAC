#!/usr/bin/env python
# $HeadURL$

__RCSID__ = "$Id$"

import platform
import sys
import os

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
    for libFile in os.listdir( lib ):
      if libFile.find( 'libc-' ) == 0 or libFile.find( 'libc.so' ) == 0 : libs.append( os.path.join( lib , libFile ) )
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