########################################################################
# File :   PilotBundle.py
# Author : Ricardo Graciani
########################################################################
"""
  Collection of Utilities to handle pilot jobs

"""
__RCSID__ = "$Id: DIRACPilotDirector.py 28536 2010-09-23 06:08:40Z rgracian $"

import os, base64, bz2, tempfile

def bundleProxy( executableFile, proxy ):
  """ Create a self extracting archive bundling together an executable script and a proxy
  """
  
  compressedAndEncodedProxy = base64.encodestring( bz2.compress( proxy.dumpAllToString()['Value'] ) ).replace( '\n', '' )
  compressedAndEncodedExecutable = base64.encodestring( bz2.compress( open( executableFile, "rb" ).read(), 9 ) ).replace( '\n', '' )

  bundle = """#!/usr/bin/env python
# Wrapper script for executable and proxy
import os, tempfile, sys, stat, base64, bz2, shutil
try:
  workingDirectory = tempfile.mkdtemp( suffix = '_wrapper', prefix= 'TORQUE_' )
  os.chdir( workingDirectory )
  open( 'proxy', "w" ).write(bz2.decompress( base64.decodestring( "%(compressedAndEncodedProxy)s" ) ) )
  open( '%(executable)s', "w" ).write(bz2.decompress( base64.decodestring( "%(compressedAndEncodedExecutable)s" ) ) )
  os.chmod('proxy', stat.S_IRUSR | stat.S_IWUSR)
  os.chmod('%(executable)s', stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
  os.environ["X509_USER_PROXY"]=os.path.join(workingDirectory, 'proxy')
except Exception as x:
  print >> sys.stderr, x
  sys.exit(-1)
cmd = "./%(executable)s"
print 'Executing: ', cmd
sys.stdout.flush()
os.system( cmd )

shutil.rmtree( workingDirectory )

""" % { 'compressedAndEncodedProxy': compressedAndEncodedProxy, \
        'compressedAndEncodedExecutable': compressedAndEncodedExecutable, \
        'executable': os.path.basename( executableFile ) }

  return bundle

def writeScript( script, writeDir=None ):
  """
    Write script into a temporary unique file under provided writeDir
  """
  fd, name = tempfile.mkstemp( suffix = '_pilotWrapper.py', prefix = 'DIRAC_', dir=writeDir )
  pilotWrapper = os.fdopen(fd, 'w')
  pilotWrapper.write( script )
  pilotWrapper.close()
  return name
  
