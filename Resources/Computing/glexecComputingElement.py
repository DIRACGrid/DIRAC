""" A computing element class that attempts to use glexec if available then
    defaults to the standard InProcess Computing Element behaviour.
"""

__RCSID__ = "$Id$"


import os, stat

from DIRAC.Resources.Computing.ComputingElement             import ComputingElement
from DIRAC.Core.Utilities.ThreadScheduler                   import gThreadScheduler
from DIRAC.Core.Utilities.Subprocess                        import shellCall
from DIRAC.Core.Utilities.Os                                import which
from DIRAC                                                  import S_OK, S_ERROR

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
    ComputingElement.__init__( self, ceUniqueID )
    self.submittedJobs = 0

  def __locate( self ):
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

    self.log.info( 'Unable to locate glexec' )
    return S_ERROR( 'glexec not found' )


  def writeProxyToFile( self, proxyObj ):
    """ Write proxy to file + set glexec enforced perms
    """
    result = super( glexecComputingElement, self ).writeProxyToFile( proxyObj )
    if not result[ 'OK' ]:
      return result
    location = result[ 'Value' ]
    os.chmod( location, stat.S_IREAD | stat.S_IWRITE )
    return S_OK( location )


  def submitJob( self, executableFile, proxyObj, dummy = None ):
    """ Method to submit job
    """
    self.log.info( "Executable file is %s" % executableFile )
    self.log.verbose( 'Setting up proxy for payload' )
    result = self.writeProxyToFile( proxyObj )
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
    if self.__locate():
      self.log.info( 'glexec found for local site at %s' % self.__gl )
      #Test glexec with payload proxy prior to submitting the job
      result = self.__test()
      if not result['OK']:
        if 'RescheduleOnError' in self.ceParameters and self.ceParameters['RescheduleOnError']:
          result = S_ERROR( 'gLexec Test Failed: %s' % res['Value'] )
          result['ReschedulePayload'] = True
          return result
        self.log.info( 'glexec test failed, will submit payload regardless...' )
        self.__gl = False

    if not self.__gl:
      self.log.info( 'glexec is not available, setting X509_USER_PROXY for payload proxy' )
      os.environ[ 'X509_USER_PROXY' ] = payloadProxy

    #Prepare crap. For test purposes just run the text
    return self.__execute( executableFile )


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
    fd, testFile = tempfile.mkstemp( "glexec.test", dir = os.path.basename( DIRAC.rootPath ) )

    self.log.info( "Test script lives in %s" % testFile )

    testdata = """#!/usr/bin/env python

import os
import urllib
print "# glexec test"
print "CWD=%s" % os.getgwd()
print "UID=%s" % os.geteuid()
print "GID=%s" % os.getegid()
print "LOGIN=%s" % os.getlogin()
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
"""
    os.write( fd, testdata )
    os.close( fd )
    self.log.info( 'Changing permissions of test script to 0755' )
    try:
      os.chmod( os.path.abspath( testFile ), stat.S_IRWXU | stat.S_IREAD | stat.S_IEXEC )
    except Exception, x:
      self.log.error( 'Failed to change permissions of test script to 0755 with exception:\n%s' % ( x ) )
      return S_ERROR( 'Could not change permissions of test script' )

    return self.__execute( testFile )

  def __execute( self, executableFile = "" ):
    """Run glexec with checking of the exit status code. With no executable it will renew the glexec proxy
    """
    #Just in case
    if self.__gl:
      if executableFile:
        cmd = "%s %s" % ( self.__gl, executableFile )
      else:
        cmd = self.__gl
    else:
      cmd = executableFile

    if executableFile:
      os.chmod( executableFile, os.stat( executableFile )[0] | stat.S_IEXEC )

    self.log.info( 'CE submission command is: %s' % cmd )
    result = shellCall( 0, "%s %s" % ( self.__gl, executableFile ), callbackFunction = self.sendOutput )
    return self.__analyzeExitCode()

  def getDynamicInfo( self ):
    """ Method to return information on running and pending jobs.
    """
    result = S_OK()
    result['SubmittedJobs'] = 0
    result['RunningJobs'] = 0
    result['WaitingJobs'] = 0
    return result

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

    if self.__gl:
      self.log.info( 'Rerunning glexec without arguments to renew payload proxy' )
      result = self.__execute()
      if not result['OK']:
        self.log.error( result )
    else:
      self.log.info( 'Running without glexec, checking local proxy' )

    return S_OK( 'Proxy checked' )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
