""" A computing element class that attempts to use glexec if available then
    defaults to the standard InProcess Computing Element behaviour.
"""

__RCSID__ = "$Id$"


import os, stat, tempfile, pickle, shutil, random, base64
from string import Template

from DIRAC.Resources.Computing.ComputingElement             import ComputingElement
from DIRAC.Core.Utilities.ThreadScheduler                   import gThreadScheduler
from DIRAC.Core.Utilities.Subprocess                        import systemCall
from DIRAC.Core.Utilities.Os                                import which
from DIRAC                                                  import S_OK, S_ERROR, gConfig

import DIRAC

class glexecComputingElement( ComputingElement ):

  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    self.__errorCodes = { 127 : 'Shell exited, command not found',
                          129 : 'Shell interrupt signal 1 (SIGHUP)',
                          130 : 'Shell interrupt signal 2 (SIGINT)',
                          201 : 'glexec failed with client error',
                          202 : 'glexec failed with internal error',
                          203 : 'glexec failed with authorization error'
                        }
    self.__gl = False
    self.__mktmp = False
    self.__proxyObj = False
    self.__execFile = False
    self.__glDir = False
    self.__pilotProxyLocation = False
    self.__payloadProxyLocation = False
    self.__glCommand = False
    self.__jobData = {}
    random.seed()
    if os.environ.has_key( 'X509_USER_PROXY' ):
      self.__pilotProxyLocation = os.environ['X509_USER_PROXY']
    ComputingElement.__init__( self, ceUniqueID )
    self.submittedJobs = 0

  def __locate_glexec( self ):
    """ Try to find glexec
    """
    if 'OSG_GLEXEC_LOCATION' in os.environ:
      if os.path.exists( os.environ[ 'OSG_GLEXEC_LOCATION' ] ):
        self.__gl = os.environ['OSG_GLEXEC_LOCATION']
        return S_OK()
    if 'GLITE_LOCATION' in os.environ:
      glpath = '%s/sbin/glexec' % ( os.environ['GLITE_LOCATION'] )
      if os.path.exists( glpath ):
        self.__gl = glpath
        return S_OK()
    glpath = which( "glexec" )
    if glpath:
      self.__gl = glpath
      return S_OK()

    return S_ERROR( "Unable to locate glexec" )

  def __locate_mkgltempdir( self ):
    self.__mktmp = os.path.join( os.path.dirname( self.__gl ), "mkgltempdir" )
    if os.path.exists( self.__mktmp ):
      return S_OK()
    return S_ERROR( "Can't find mkgltempdir at %s" % self.__mktmp )


  def writeProxyToFile( self, proxyObj ):
    """ Write proxy to file + set glexec enforced perms
    """
    result = super( glexecComputingElement, self ).writeProxyToFile( proxyObj )
    if not result[ 'OK' ]:
      return result
    location = result[ 'Value' ]
    os.chmod( location, stat.S_IREAD | stat.S_IWRITE )
    return S_OK( location )

  def __prepare_glenv( self ):
    self.log.verbose( 'Setting up proxy for payload' )
    result = self.writeProxyToFile( self.__proxyObj )
    if not result['OK']:
      return result

    self.__payloadProxyLocation = result['Value']
    os.environ[ 'GLEXEC_CLIENT_CERT' ] = self.__payloadProxyLocation
    os.environ[ 'GLEXEC_SOURCE_PROXY' ] = self.__payloadProxyLocation
    self.log.info( "Payload proxy deployed to %s" % self.__payloadProxyLocation )
    return S_OK()


  def __addperm( self, path, perms ):
    currentPerms = os.stat( path )[0]
    try:
      os.chmod( path, currentPerms | perms )
    except Exception, excp:
      self.log.error( "Could not set perms for %s: %s" % ( path, excp ) )
      return False
    return True


  def __allow_gl_travel( self, dirpath ):
    if dirpath == "/" or not dirpath:
      return
    if self.__addperm( dirpath, stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH ):
      self.__allow_gl_travel( os.path.dirname( dirpath ) )

  def __allow_gl_see( self, dirpath, extraPerm = 0 ):
    if not self.__addperm( dirpath, stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IWOTH | extraPerm ):
      return False
    for entry in os.listdir( dirpath ):
      epath = os.path.join( dirpath, entry )
      if os.path.isdir( epath ):
        if not self.__allow_gl_see( epath ):
          return False
      elif not self.__addperm( epath, stat.S_IRGRP | stat.S_IROTH | extraPerm ):
        return False
    return True

  def __prepare_tmpdir( self ):
    self.log.info( "Setting world-take-a-loot-at-all-my-things permissions..." )
    self.__allow_gl_see( DIRAC.rootPath )
    self.log.info( "Allowing everybody to execute my scripts..." )
    self.__allow_gl_see( os.path.join( DIRAC.rootPath, "scripts" ), stat.S_IXOTH )
    self.log.info( "You're welcome to execute my binaries...." )
    self.__allow_gl_see( os.path.join( DIRAC.rootPath, DIRAC.platform, "bin" ), stat.S_IXOTH )
    self.log.info( "Rob-all-my-house mode ON" )
    finder = 0
    self.__glBaseDir = os.path.join( os.getcwd(), "glexec", "gl-%s.%03d.%.4f" % ( self.__jobData[ 'jid' ], finder, random.random() * 1000 ) )
    while os.path.isdir( self.__glBaseDir ):
      finder += 1
      self.__glBaseDir = os.path.join( os.getcwd(), "glexec", "gl-%s.%03d.%.4f" % ( self.__jobData[ 'jid' ], finder, random.random() * 1000 ) )
    try:
      os.makedirs( self.__glBaseDir )
    except Exception, excp:
      return S_ERROR( "Could not create base dir for glexec: %s" % ( excp ) )
    self.__allow_gl_travel( self.__glBaseDir )
    self.log.info( "Robbers can now get 'hasta-la-cocina'" )
    os.chmod( self.__glBaseDir, stat.S_IRWXU | stat.S_IXGRP | stat.S_IXOTH )
    sticky = os.path.join( self.__glBaseDir, "trans" )
    try:
      os.makedirs( sticky )
      os.chmod( sticky, stat.S_ISVTX | stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO )
    except Exception, excp :
      return S_ERROR( "Could not create %s: %s" % ( sticky, excp ) )
    self.log.info( "Pegajoso dir created" )
    self.__glDir = os.path.join( sticky, "glid" )
    result = self.__execute( [ "/bin/sh", "-c", "mkdir '%s'; chmod 700 '%s'" % ( self.__glDir, self.__glDir ) ] )
    print result
    if not result[ 'OK' ]:
      return S_ERROR( "OOOPS. Something went bad when doobedobedooo: %s" % result[ 'Message' ] )

    self.log.info( "gldir is %s" % self.__glDir )
    return S_OK()

  def __test( self ):

    #Because glexec is SOOOOPER easy to use
    fd, testFile = tempfile.mkstemp( "glexec.test", dir = os.path.dirname( self.__glDir ) )

    self.log.info( "Test script lives in %s" % testFile )

    testdata = """#!/usr/bin/env python

import os
import urllib
import sys
import pickle
import base64

codedEnv="$codedEnv"

print "# Unwrapping env"
env = pickle.loads( base64.b64decode( codedEnv ) )
for k in env:
  if k not in ( 'X509_USER_PROXY', 'HOME', 'LOGNAME', 'USER', '_' ):
    os.environ[ k ] = env[ k ]

print "# glexec test"
print "CWD=%s" % os.getcwd()
print "UID=%s" % os.geteuid()
print "GID=%s" % os.getegid()
try:
  print "LOGIN=%s" % os.getlogin()
except Exception, excp:
  if 'USER' in os.environ:
    print "LOGIN=%s(can't get via os.getlogin)" % os.environ[ 'USER' ]
  else:
    print "LOGIN=UNKNOWN:%s" % str( excp )
for k in os.environ:
  print "ENV:%s=%s" % ( k, os.environ[ k ] )
try:
  open( os.environ[ 'X509_USER_PROXY' ], "r" ).read()
  print "TEST:READ_PROXY=true"
except Exception, excp:
  print "TEST_READ_PROXY=false,%s" % str( excp )
try:
  urllib.urlopen( "http://google.com" ).read()
  print "TEST:OUTBOUND_TCP=true"
except Exception, excp:
  print "TEST:OUTBOUND_TCP=false,%s" % str( excp )
sys.stdout.flush()
if os.system( "voms-proxy-info -all" ) == 0:
  print "TEST:VOMS=true"
else:
  print "TEST:VOMS=false"
sys.stdout.flush()
if os.system( "dirac-proxy-info --steps" ) == 0:
  print "TEST:DIRAC-PROXY-INFO=true"
else:
  print "TEST:DIRAC-PROXY-INFO=false"
"""

    os.write( fd, Template( testdata ).substitute( { 'codedEnv' : base64.b64encode( pickle.dumps( os.environ ) ) } ) )
    os.close( fd )
    self.log.info( 'Changing permissions of test script to 0755' )
    try:
      os.chmod( os.path.abspath( testFile ), stat.S_IRWXU | stat.S_IREAD | stat.S_IEXEC )
    except Exception, x:
      self.log.error( 'Failed to change permissions of test script to 0755 with exception:\n%s' % ( x ) )
      return S_ERROR( 'Could not change permissions of test script' )

    return self.__execute( [ testFile ] )

  def __construct_payload( self ):
    writeDir = os.path.dirname( self.__glDir )
    glwrapper = os.path.join( writeDir, "glwrapper" )
    glwrapperdata = """#!/usr/bin/env python
import pickle
import base64
import os

codedEnv="$codedEnv"

print "Unwrapping env"
env = pickle.loads( base64.b64decode( codedEnv ) )
for k in env:
  if k not in ( 'X509_USER_PROXY', 'HOME', 'LOGNAME', 'USER', '_' ):
    os.environ[ k ] = env[ k ]

#GO TO WORKING DIR
os.chdir( "$workDir" )
os.execl( "$executable" )
"""
    with open( glwrapper, "w" ) as fd:
      fd.write( Template( glwrapperdata ).substitute( { 'codedEnv' : base64.b64encode( pickle.dumps( os.environ ) ),
                                                        'workDir' : self.__glDir,
                                                        'executable' : self.__execFile } ) )

    os.chmod( glwrapper, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH )
    os.chmod( self.__execFile, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH )

    self.log.info( "Written %s" % glwrapper )

    self.__glCommand = glwrapper
    self.log.info( "glexec command will be %s" % self.__glCommand )
    return S_OK()

  def __executeInProcess( self, executableFile ):
    os.environ[ 'X509_USER_PROXY' ] = self.__pilotProxyLocation
    self.__addperm( executableFile, stat.S_IRWXU )

    result = systemCall( 0, [ executableFile ], callbackFunction = self.sendOutput )
    if not result[ 'OK' ]:
      return result
    return self.__analyzeExitCode( result[ 'Value' ] )

  def getDynamicInfo( self ):
    """ Method to return information on running and pending jobs.
    """
    result = S_OK()
    result['SubmittedJobs'] = 0
    result['RunningJobs'] = 0
    result['WaitingJobs'] = 0
    return result

  def getCEStatus( self ):
    #CRAPCRAP
    return self.getDynamicInfo()


  def __cleanup( self ):
    self.log.info( "Cleaning up %s" % self.__glDir )
    workDir = os.path.dirname( self.__glDir )
    for entry in os.listdir( workDir ):
      obj = os.path.join( workDir, entry )
      try:
        if os.path.isdir( obj ):
          shutil.rmtree( obj )
        else:
          os.unlink( obj )
      except:
        pass
    result = self.__execute( [ "/bin/rm", '-rf', self.__glDir ] )
    if not result[ 'OK' ]:
      self.log.error( "Could not cleanup: %s" % result[ 'Message' ] )
    try:
      shutil.rmtree( self.__glBaseDir )
    except Exception, excp:
      self.log.error( "Could not cleanup %s: %s" % ( self.__glBaseDir, excp ) )

  def submitJob( self, executableFile, proxyObj, jobData ):
    """ Method to submit job
    """
    if not self.__pilotProxyLocation:
      return S_ERROR( "X509_USER_PROXY is not defined!" )
    self.log.info( "Executable file is %s" % executableFile )
    self.__proxyObj = proxyObj
    self.__execFile = executableFile
    self.__jobData = jobData

    glok = True
    for step in ( self.__locate_glexec, self.__prepare_glenv, self.__prepare_tmpdir, self.__test, self.__construct_payload ):
      self.log.info( "Running step %s" % step.__name__ )
      result = step()
      if not result[ 'OK' ]:
        self.log.error( "Step %s failed: %s" % ( step.__name__, result[ 'Message' ] ) )
        if self.ceParameters.get( "RescheduleOnError", False ):
          result = S_ERROR( 'glexec CE failed on step %s : %s' % ( step.__name__, result[ 'Message' ] ) )
          result['ReschedulePayload'] = True
          return result
        glok = False
        break

    self.log.verbose( 'Starting process for monitoring payload proxy' )
    gThreadScheduler.addPeriodicTask( self.proxyCheckPeriod, self.monitorProxy,
                                      taskArgs = ( self.__pilotProxyLocation, self.__payloadProxyLocation ),
                                      executions = 0, elapsedTime = 0 )

    if not glok:
      self.log.notice( "glexec failed miserably... Running without it." )
      return self.__executeInProcess( executableFile )
    result = self.__execute( [ self.__glCommand ] )
    self.__cleanup()
    return result

  def __analyzeExitCode( self, resultTuple ):
    """ Analyses the exit codes in case of glexec failures.  The convention for
        glexec exit codes is listed below:

          Shell exit codes:
          127 - command not found
          129 - command died due to signal 1 (SIGHUP)
          130 - command died due to signal 2 (SIGINT)

          glexec specific codes:
          201 - client error
          202 - internal error
          203 - authz error
    """
    if not resultTuple:
      return S_OK()

    # FIXME: the wrapper will return:
    #   > 0 if there are problems with the payload
    #   < 0 if there are problems with the wrapper itself
    #   0 if everything is OK

    status = resultTuple[0]
    stdOutput = resultTuple[1].strip()
    stdError = resultTuple[2].strip()

    if status == 0:
      self.log.info( 'glexec call suceeded' )
    else:
      self.log.info( 'glexec call failed with status %s' % ( status ) )
    if stdOutput:
      self.log.info( 'glexec stdout:\n%s' % stdOutput )
    if stdError:
      self.log.info( 'glexec stderr:\n%s' % stdError )

    if status != 0:
      error = None
      if status in self.__errorCodes:
        error = self.__errorCodes[ status ]
        self.log.error( 'Resolved glexec return code %s = %s' % ( status, error ) )
        return S_ERROR( "Error %s = %s" % ( status, error ) )

      self.log.error( 'glexec exit code %s not in expected list' % status )
      return S_ERROR( "Error code %s" % status )

    return S_OK()


  def __execute( self, executableList ):
    """Run glexec with checking of the exit status code. With no executable it will renew the glexec proxy
    """
    #Just in case
    glCmd = [ self.__gl ]
    if executableList:
      try:
        os.chmod( executableList[0], os.stat( executableList[0] )[0] | stat.S_IEXEC | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH )
      except:
        pass
      glCmd.extend( executableList )
    self.log.info( 'CE submission command is: %s' % glCmd )
    result = systemCall( 0, glCmd, callbackFunction = self.sendOutput )
    if not result[ 'OK' ]:
      return result
    return self.__analyzeExitCode( result[ 'Value' ] )

  def getDynamicInfo( self ):
    """ Method to return information on running and pending jobs.
    """
    result = S_OK()
    result['SubmittedJobs'] = 0
    result['RunningJobs'] = 0
    result['WaitingJobs'] = 0
    return result

  def getCEStatus( self ):
    #CRAPCRAP
    return self.getDynamicInfo()

  def monitorProxy( self, pilotProxy, payloadProxy ):
    """ Monitor the payload proxy and renew as necessary.
    """
    retVal = super( glexecComputingElement, self ).monitorProxy( pilotProxy, payloadProxy )
    if not retVal['OK']:
      # Failed to renew the proxy, nothing else to be done
      return retVal

    if not retVal['Value']:
      # No need to renew the proxy, nothing else to be done
      return retVal

    if self.__gl:
      self.log.info( 'Rerunning glexec without arguments to renew payload proxy' )
      result = self.__execute()
      if not result['OK']:
        self.log.error( result )
    else:
      self.log.info( 'Running without glexec, checking local proxy' )

    return S_OK( 'Proxy checked' )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
