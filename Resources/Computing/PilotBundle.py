########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/WorkloadManagementSystem/private/DIRACPilotDirector.py $
# File :   PilotBundle.py
# Author : Ricardo Graciani
########################################################################
"""
  Collection of Utilities to handle pilot jobs

"""
__RCSID__ = "$Id: DIRACPilotDirector.py 28536 2010-09-23 06:08:40Z rgracian $"

import os, base64, bz2, types, tempfile

def getExecutableScript( executable, arguments=[], proxy=None, sandboxDict = {}, environDict={}, execDir='' ):
  """
   Prepare a wrapper script for executable including as required environment, proxy, sandbox,...

   A temporary directory is created and removed at the end where sandbox is unpacked and
   executable called

   In executable, arguments and execDir environmental variables can be used
    :Parameters:
      `executable`
        - string - if included in sandboxDict ./ is prepended
      `arguments`
        - list of strings - arguments to be passed to executable
      `proxy`
        - DIRAC.Core.Security.X509Chain - proxy to be setup in environment
      `sandboxDict`
        - dictionary - name: path 
      `environmentDict`
        - dictionary - key: value
      `execDir`
        - string - path where temporary directory is created 
    :returns:
      - string - produced script
    """
  if type( arguments) in types.StringTypes:
    arguments = arguments.split(' ')
  compressedAndEncodedFiles = {}
  if proxy:
    compressedAndEncodedFiles['.proxy'] = base64.encodestring( bz2.compress( proxy.dumpAllToString()['Value'] ) ).replace('\n','')
  for fileName, filePath in sandboxDict.items():
    encodedFile = base64.encodestring( bz2.compress( open( filePath, "rb" ).read() ) ).replace('\n','')
    compressedAndEncodedFiles[fileName] = encodedFile

  script = """#!/usr/bin/env python
try:
  import os, tempfile, sys, shutil, base64, bz2, subprocess, datetime
except:
  print 'Failed to import os, tempfile, sys, shutil, base64, bz2, subprocess'
  print 'Unsupported python version'
  exit(1)

print 'START TIME:', datetime.datetime(2000,1,1).utcnow(), 'UTC'
print

# 1. Get Name of the executable
executable = '%(executable)s'
cmdTuple = [ executable ]
cmdTuple.extend( %(arguments)s )

# 2. Print environment
print '==========================================================='
print
print 'Existing Environment:'
print
for key, value in os.environ.items():
  print key, '=', value
print
print 'Added Environment:'
print

# 3. Set environment
environDict = %(environDict)s
if 'LD_LIBRARY_PATH' not in os.environ and 'LD_LIBRARY_PATH' not in environDict:
  environDict['LD_LIBRARY_PATH'] = ''

for key, value in environDict.items():
  os.environ[key] = value
  print key, '=', value

# 4. Create Working Directory
execDir = '%(execDir)s'
if not execDir:
  execDir = None
else:
  execDir = os.path.expanduser( os.path.expandvars( execDir ) )
workingDirectory = tempfile.mkdtemp( suffix = 'pilot', prefix = 'DIRAC_', dir = execDir )
os.chdir( workingDirectory )
os.environ['X509_CERT_DIR'] = os.path.join( workingDirectory, 'etc', 'grid-security', 'certificates' )

# 5. Extract Sandbox files
for fileName, fileCont in %(compressedAndEncodedFiles)s.items():
  f = open( fileName, 'w' )
  f.write( bz2.decompress( base64.decodestring( fileCont ) ) )
  f.close()
  if fileName == '.proxy':
    os.chmod( fileName, 0600 )
    os.environ['X509_USER_PROXY'] = os.path.join( workingDirectory, fileName )
    print 'X509_USER_PROXY', '=', os.path.join( workingDirectory, fileName )
  elif fileName == executable:
    os.chmod( fileName, 0755 )
    executable = './' + executable
print
print '==========================================================='
print

# 6. Executing
cmdTuple = [ os.path.expanduser( os.path.expandvars( k ) ) for k in cmdTuple ]
print 'Executing: ', ' '.join( cmdTuple )
print 'at:', os.getcwd()
print
sys.stdout.flush()
try:
  exitCode = subprocess.call( cmdTuple )
  if exitCode < 0:
    print >> sys.stderr, 'Command killed by signal', - exitCode
  if exitCode > 0:
    print >> sys.stderr, 'Command returned', exitCode
except OSError, e:
  exitCode = -1
  print >> sys.stderr, "Execution failed:", e

shutil.rmtree( workingDirectory )
myDate = datetime.datetime(2000,1,1).utcnow()
print
print 'END TIME:', datetime.datetime(2000,1,1).utcnow(), 'UTC'

exit( exitCode )
""" % { 'execDir': execDir,
        'executable': executable,
        'compressedAndEncodedFiles': compressedAndEncodedFiles,
        'arguments': arguments,
        'environDict': environDict, }

  return script

def bundleProxy( executableFile, proxy ):
  """ Create a self extracting archive bundling together an executable script and a proxy
  """
  
  compressedAndEncodedProxy = base64.encodestring( bz2.compress( proxy.dumpAllToString()['Value'] ) ).replace( '\n', '' )
  compressedAndEncodedExecutable = base64.encodestring( bz2.compress( open( executableFile, "rb" ).read(), 9 ) ).replace( '\n', '' )

  bundle = """#!/usr/bin/env python
# Wrapper script for executable and proxy
import os, tempfile, sys, base64, bz2, shutil
try:
  workingDirectory = tempfile.mkdtemp( suffix = '_wrapper', prefix= 'TORQUE_' )
  os.chdir( workingDirectory )
  open( 'proxy', "w" ).write(bz2.decompress( base64.decodestring( "%(compressedAndEncodedProxy)s" ) ) )
  open( '%(executable)s', "w" ).write(bz2.decompress( base64.decodestring( "%(compressedAndEncodedExecutable)s" ) ) )
  os.chmod('proxy',0600)
  os.chmod('%(executable)s',0700)
  os.environ["X509_USER_PROXY"]=os.path.join(workingDirectory, 'proxy')
except Exception, x:
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
  