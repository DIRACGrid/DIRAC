#!/usr/bin/env python
########################################################################
# File :   dirac-fix-ld-lib
# Author : Joel Closier
########################################################################
__RCSID__ = "$Id$"
"""  This is a script to fix oversized LD_LIBRARY_PATH variables.
"""
import sys, os, shutil, re
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.Os import uniquePath
from DIRAC.Core.Utilities.Subprocess import shellCall

DEBUG = 0

def fixLDPath( root, ldpath, directory ):
  """
      This is a utility to fix the LD_LIBRARY_PATH on Grid WNs. The
      shared libraries from the original LD_LIBRARY_PATH are linked to
      the locally specified directory.  For Windows (and in general)
      this needs some refurbishment.
  """

  if os.path.exists( directory ):
    shutil.rmtree( directory )

  start = os.getcwd()
  os.mkdir( directory )
  os.chdir( directory )
  uniqueLD = uniquePath( ldpath )

  if DEBUG:
    print 'Unique LD LIBRARY PATH is:'
    print uniqueLD
    sys.stdout.flush()

  ldlist = uniqueLD.split( ':' )
  if DEBUG:
    print ''
    print 'LD List is:'
    print ldlist
    print ''
    sys.stdout.flush()

  for path in ldlist:
    if os.path.exists( path ):

      if DEBUG:
        print 'Searching for shared libraries in:'
        print path
        print '-----------------------------------------------'
        res = shellCall( 0, 'ls ' + path + '/*.so*' )
        if res['OK']:
          print res['Value']
        else:
          print res
        print '-----------------------------------------------'

      output = shellCall( 0, 'ls ' + path + '/*.so*' )
      #must be tidied for Windows (same below)

      if DEBUG:
        if not output['OK']:
          print '**************************'
          print 'Warning, problem with ls:'
          print output
          print '**************************'

      if not output['Value'][0]:
        ldlibs = output['Value'][1].split( '\n' )
        for lib in ldlibs:
          if os.path.exists( lib ):
            filename = os.path.basename( lib )
            output = shellCall( 0, 'ln -s ' + str( lib ) + ' ' + str( filename ) )
            #N.B. for Windows this should be a copy...
            if DEBUG:
              if not output['OK']:
                print '********************************'
                print 'Warning, problem creating link:'
                print 'File: ', filename
                print 'Path: ', lib
                print output
                print '********************************'

      if DEBUG:
        print 'Searching for rootmap file in:'
        print path
        print '-----------------------------------------------'
        res = shellCall( 0, 'ls ' + path + '/*rootmap*' )
        if res['OK']:
          print res['Value']
        else:
          print res
        print '-----------------------------------------------'

      output = shellCall( 0, 'ls ' + path + '/*rootmap*' )

      if DEBUG:
        if not output['OK']:
          print '**************************'
          print 'Warning, problem with rootmap:'
          print output
          print '**************************'

      if not output['Value'][0]:
        ldlibs = output['Value'][1].split( '\n' )
        for lib in ldlibs:
          if os.path.exists( lib ):
            if re.search( 'RELAX', lib ) is not None:
              filename = os.path.basename( lib )
              output = shellCall( 0, 'ln -s ' + str( lib ) + ' ' + str( filename ) )
              if DEBUG:
                if not output['OK']:
                  print '********************************'
                  print 'Warning, problem creating link:'
                  print 'File: ', filename
                  print 'Path: ', lib
                  print output
                  print '********************************'

  os.chdir( start )
  sys.stdout.flush()


Script.parseCommandLine()

positionalArgs = Script.getPositionalArgs()
if len( positionalArgs ) != 3:
  DIRAC.abort( 1, "Must specify which is the role you want" )

fixLDPath( positionalArgs[0], positionalArgs[1], positionalArgs[2] )
