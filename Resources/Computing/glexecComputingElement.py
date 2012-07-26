########################################################################
# $Id$
# File :   glexecComputingElement.py
# Author : Stuart Paterson
########################################################################

""" A computing element class that attempts to use glexec if available then
    defaults to the standard InProcess Computing Element behaviour.
"""

__RCSID__ = "$Id$"

from DIRAC.Resources.Computing.ComputingElement             import ComputingElement
from DIRAC.Core.Utilities.ThreadScheduler                   import gThreadScheduler
from DIRAC.Core.Utilities.Subprocess                        import shellCall
from DIRAC                                                  import S_OK, S_ERROR

import DIRAC

import os
import stat
import shutil
import tempfile

MandatoryParameters = [ ]

class glexecComputingElement( ComputingElement ):

  mandatoryParameters = MandatoryParameters

  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    self.__secureDir = False
    self.__targetDir = False
    self.__stickyDir = False
    self.__sourceDir = False
    self.__wrap = False
    self.__errorCodes = { 127 : 'Shell exited, command not found',
                          129 : 'Shell interrupt signal 1 (SIGHUP)',
                          130 : 'Shell interrupt signal 2 (SIGINT)',
                          201 : 'glexec failed with client error',
                          202 : 'glexec failed with internal error',
                          203 : 'glexec failed with authorization error'
                        }
    ComputingElement.__init__( self, ceUniqueID )
    self.submittedJobs = 0


  def __rmDirs( self ):

    self.log.info( "Deleting temporal glexec directories..." )

    if self.__stickyDir:
      #Make sure sticky dir is writable by glexec
      try:
        os.chmod( self.__stickyDir, stat.S_IRWXU | stat.S_IRWXO )
      except OSError:
        pass

      if self.__targetDir and os.path.isdir( self.__targetDir ):
        result = shellCall( 0, "%s rm -rf '%s'" % ( self.__wrap, self.__targetDir ) )

        if not result[ 'OK' ] or not result[ 'Value' ][0] != 0:
          self.log.error( "Could not delete target dir via glexec:\n %s%s" % ( result['Value'][1], result[ 'Value' ][2] ) )
        else:
          self.log.info( "Properly cleared target dir %s" % self.__targetDir )

    if self.__secureDir:
      try:
        shutil.rmtree( self.__secureDir )
      except OSError, excp:
        self.log.error( "Cannot delete secure dir %s: %s" % ( self.__secureDir, str( excp ) ) )

    self.__secureDir = False
    self.__targetDir = False
    self.__stickyDir = False
    self.__sourceDir = False

  def __createDirs( self ):
    if self.__targetDir:
      return self.__sourceDir, self.__targetDir,
    prefix = "glexec.dirac.%s." % str( os.getpid() )
    for envVar in ( 'GLITE_LOCAL_CUSTOMIZATION_DIR', 'EDG_WL_SCRATCH' ):
      if envVar in os.environ:
        try:
          self.__secureDir = tempfile.mkdtemp( prefix = prefix, dir = os.environ[ envVar ] )
        except OSError:
          pass
    if not self.__secureDir:
      self.__secureDir = tempfile.mkdtemp( prefix = prefix )
    opwd = self.__secureDir
    split = os.path.split( opwd )
    while split[1] != "":
      mode = os.stat( opwd )[stat.ST_MODE] | stat.S_IXOTH | stat.S_IROTH
      try:
        os.chmod( opwd, mode )
      except OSError:
        break
      opwd = split[0]
      split = os.path.split( opwd )

    os.chmod( self.__secureDir, stat.S_IRWXU )
    self.__stickyDir = os.path.join( self.__secureDir, "sticky" )
    try:
      os.mkdir( self.__stickyDir )
    except OSError, excp:
      self.log.error( "Could not create glexec sticky dir: %s" % str( excp ) )
      self.__rmDirs()
      return False
    os.chmod( self.__stickyDir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO | stat.S_ISVTX )

    self.__sourceDir = os.path.join( self.__stickyDir, "source" )
    try:
      os.mkdir( self.__sourceDir )
      os.chmod( self.__sourceDir, stat.S_IRWXU | stat.S_IROTH | stat.S_IXOTH )
    except OSError, excp:
      self.log.error( "Cannot create source dir: %s" % ( self.__sourceDir, str( excp ) ) )
      self.__rmDirs()
      return False

    os.chmod( self.__secureDir, stat.S_IRWXU | stat.S_IROTH | stat.S_IXOTH )
    targetDir = os.path.join( self.__stickyDir, "target" )
    result = shellCall( 0, "cd %s; %s mkdir '%s'" % ( self.__stickyDir, self.__glexec, targetDir ) )
    if not result[ 'OK' ] or not result[ 'Value' ][0] != 0:
      self.log.error( "Could not create target dir via glexec:\n %s%s" % ( result['Value'][1], result[ 'Value' ][2] ) )
      self.__rmDirs()
      return False
    #Absurdity: Just following recommended mkgltempdir rules
    #os.chmod( self.__secureDir, 0755 )
    self.__targetDir = targetDir

    return self.__sourceDir, targetDir

  #############################################################################
  def _addCEConfigDefaults( self ):
    """Method to make sure all necessary Configuration Parameters are defined
    """
    # First assure that any global parameters are loaded
    ComputingElement._addCEConfigDefaults( self )
    # Now glexec specific ones

  #############################################################################
  def submitJob( self, executableFile, proxy, dummy = None ):
    """ Method to submit job, should be overridden in sub-class.
    """
    self.log.info( "Exeutable file is %s" % executableFile )
    self.log.verbose( 'Setting up proxy for payload' )
    result = self.writeProxyToFile( proxy )
    if not result['OK']:
      return result

    payloadProxy = result['Value']
    if not os.environ.has_key( 'X509_USER_PROXY' ):
      self.log.error( 'X509_USER_PROXY variable for pilot proxy not found in local environment' )
      return S_ERROR( 'X509_USER_PROXY not found' )

    pilotProxy = os.environ['X509_USER_PROXY']
    self.log.info( 'Pilot proxy X509_USER_PROXY=%s' % pilotProxy )
    os.environ[ 'GLEXEC_CLIENT_CERT' ] = payloadProxy
    os.environ[ 'GLEXEC_SOURCE_PROXY' ] = payloadProxy
    self.log.info( '\n'.join( [ 'Set payload proxy variables:',
                                'GLEXEC_CLIENT_CERT=%s' % payloadProxy,
                                'GLEXEC_SOURCE_PROXY=%s' % payloadProxy ] ) )

    #Determine glexec location (default to standard InProcess behaviour if not found)
    if self.__find():
      self.log.info( 'glexec found for local site at %s' % self.__wrap )

    if self.__wrap:
      result = self.recursivelyChangePermissions()
      if not result['OK']:
        self.log.error( 'Permissions change failed, continuing regardless...' )
    else:
      self.log.info( 'glexec not found, no permissions to change' )

    #Test glexec with payload proxy prior to submitting the job
    result = self.__test()
    if not result['OK']:
      if 'RescheduleOnError' in self.ceParameters and self.ceParameters['RescheduleOnError']:
        result = S_ERROR( 'gLexec Test Failed: %s' % res['Value'] )
        result['ReschedulePayload'] = True
        self.__rmDirs()
        return result
      self.log.info( 'glexec test failed, will submit payload regardless...' )

    #Revert to InProcess behaviour
    if not self.__wrap:
      self.log.info( 'glexec is not found, setting X509_USER_PROXY for payload proxy' )
      os.environ[ 'X509_USER_PROXY' ] = payloadProxy

    self.log.verbose( 'Starting process for monitoring payload proxy' )
    gThreadScheduler.addPeriodicTask( self.proxyCheckPeriod, self.monitorProxy,
                                      taskArgs = ( pilotProxy, payloadProxy ),
                                      executions = 0, elapsedTime = 0 )

    #Submit job
    self.log.info( 'Changing permissions of executable to 0755' )
    try:
      os.chmod( os.path.abspath( executableFile ), stat.S_IRWXU | stat.S_IROTH | stat.S_IXOTH )
    except Exception, x:
      self.log.error( 'Failed to change permissions of executable to 0755 with exception:\n%s' % ( x ) )

    result = self.__execute( os.path.abspath( executableFile ) )
    if not result['OK']:
      self.log.error( result )
      self.__rmDirs()
      return result

    self.log.debug( 'glexec CE result OK' )
    self.submittedJobs += 1
    self.__rmDirs()
    return S_OK()

  #############################################################################
  def recursivelyChangePermissions( self, startDir = False ):
    """ Ensure that the current directory and all those beneath have the correct
        permissions.
    """
    if not startDir:
      startDir = os.getcwd()
    try:
      self.log.info( 'Trying to explicitly change permissions for parent directory %s' % startDir )
      os.chmod( startDir, stat.S_IRWXU | stat.S_IROTH | stat.S_IXOTH )
    except Exception, x:
      self.log.error( 'Problem changing directory permissions in parent directory', str( x ) )

    cDir = startDir
    split = os.path.split( cDir )
    while split[1] != "":
      mode = os.stat( cDir )[stat.ST_MODE]
      if mode & stat.S_IXOTH == 0:
        try:
          os.chmod( cDir, mode | stat.S_IXOTH )
          self.log.info( "Fixed mode for dir %s now is %s" % ( cDir, mode | stat.S_IXOTH ) )
        except OSError:
          self.log.error( "Not everybody can access dir %s mode is %s" % ( cDir, mode ) )
      else:
        self.log.info( "Mode for dir %s is %s" % ( cDir, mode ) )
      cDir = split[0]
      split = os.path.split( cDir )

    self.log.verbose( 'Changing permissions to 0755 in current directory %s and subdirs' % startDir )
    for dirName, subDirs, files in os.walk( startDir ):
      try:
        if os.path.isdir( dirName ):
          mode = os.stat( dirName )[stat.ST_MODE] | stat.S_IROTH | stat.S_IXOTH
          os.chmod( dirName, mode )
        for filename in files:
          filepath = os.path.join( dirName, filename )
          if os.path.isfile( filepath ):
            mode = os.stat( filepath )[stat.ST_MODE] | stat.S_IROTH
            os.chmod( filepath, mode )
      except Exception, x:
        self.log.error( 'Problem changing permissions', str( x ) )

    self.log.info( 'Permissions in current directory %s updated successfully' % ( startDir ) )
    return S_OK()

  #############################################################################
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

  #############################################################################
  def __test( self ):
    """Ensure that the current DIRAC distribution is group readable e.g. dirac-proxy-info
       also check the status code of the glexec call.
    """
    if not self.__wrap:
      return S_OK( 'Nothing to test' )

    dirs = self.__createDirs()
    if not dirs:
      return S_ERROR( "Could not create glexec target dir" )

    sourceDir, targetDir = dirs
    testFile = os.path.join( sourceDir, "glexecTest.sh" )

    self.log.info( "Test script lives in %s" % testFile )

    testdata = """#!/usr/bin/env python

from DIRAC.Core.Base import Script

Script.parseCommandLine()

from DIRAC import gLogger

gLogger.always( "It works!" )
"""
    fopen = open( testFile, 'w' )
    fopen.write( testdata )
    fopen.close()
    self.log.info( 'Changing permissions of test script to 0755' )
    try:
      os.chmod( os.path.abspath( testFile ), stat.S_IRWXU | stat.S_IREAD | stat.S_IEXEC )
    except Exception, x:
      self.log.error( 'Failed to change permissions of test script to 0755 with exception:\n%s' % ( x ) )
      return S_ERROR( 'Could not change permissions of test script' )

    return self.__execute( os.path.abspath( testFile ) )

  #############################################################################
  def __execute( self, executableFile ):
    """Run glexec with checking of the exit status code.
    """
    if not self.__wrap:
      cmd = executableFile
    else:
      if not executableFile:
        cmd = self.__wrap
      else:
        dirs = self.__createDirs()
        if dirs:
          wrap = os.path.join( dirs[0], "glwrap.%s" % os.getpid() )
          wrapContents = """#!/bin/bash
cd '{tdir}'
exec {glexec} {exe}
""".format( **{ 'tdir' : dirs[1], 'glexec': self.__wrap, 'exe' : executableFile } )
          fd = open( wrap, "w" )
          fd.write( wrapContents )
          fd.close()
          os.chmod( wrap, stat.S_IRWXU | stat.S_IREAD | stat.S_IEXEC )
          self.log.info( "Generated wrap:\n%s" % wrapContents )
          executableFile = wrap
        cmd = executableFile

    self.log.info( 'CE submission command is: %s' % cmd )
    result = shellCall( 0, cmd, callbackFunction = self.sendOutput )
    if self.__wrap:
      return self.__analyzeExitCode( result[ 'Value' ] )
    return result

  #############################################################################
  def __find( self ):
    """Try to find glexec on the local system, if not found default to InProcess.
    """
    self.__wrap = False
    self.__glexec = False

    if not os.environ.has_key( 'GLITE_LOCATION' ):
      self.log.info( 'Unable to locate glexec, site does not have GLITE_LOCATION defined' )
      return False

    wrapPath = '%s/sbin/glexec_wrap.sh' % str( os.environ['GLITE_LOCATION'] )
    if not os.path.exists( wrapPath ):
      self.log.info( '$GLITE_LOCATION/sbin/glexec_wrap.sh not found at path %s' % ( wrapPath ) )
      return True

    glexec = "%s/sbin/glexec" % str( os.environ[ 'GLITE_LOCATION' ] )
    if not os.path.exists( glexec ):
      self.log.info( '$GLITE_LOCATION/sbin/glexec not found at path %s' % ( glexec ) )
      return False

    self.__wrap = wrapPath
    self.__glexec = glexec

    return False

  #############################################################################
  def getDynamicInfo( self ):
    """ Method to return information on running and pending jobs.
    """
    result = S_OK()
    result['SubmittedJobs'] = 0
    result['RunningJobs'] = 0
    result['WaitingJobs'] = 0
    return result

  #############################################################################
  def monitorProxy( self, pilotProxy, payloadProxy ):
    """ Monitor the payload proxy and renew as necessary.
    """
    retVal = self._monitorProxy( pilotProxy, payloadProxy )
    if not retVal['OK']:
      # Failed to renew the proxy, nothing else to be done
      return retVal

    if not retVal['Value']:
      # No need to renew the proxy, nothing else to be done
      return retVal

    if self.__wrap:
      self.log.info( 'Rerunning glexec without arguments to renew payload proxy' )
      result = self.__execute( None )
      if not result['OK']:
        self.log.error( result )
    else:
      self.log.info( 'Running without glexec, checking local proxy' )

    return S_OK( 'Proxy checked' )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
