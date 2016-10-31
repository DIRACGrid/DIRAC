""" A computing element class that uses sudo
"""

__RCSID__ = "$Id$"

import os
import pwd
import stat
import distutils.spawn

import DIRAC

from DIRAC                                                  import S_OK, S_ERROR

from DIRAC.ConfigurationSystem.Client.Config                import gConfig
from DIRAC.Resources.Computing.ComputingElement             import ComputingElement
from DIRAC.Core.Utilities.ThreadScheduler                   import gThreadScheduler
from DIRAC.Core.Utilities.Subprocess                        import shellCall

MandatoryParameters = [ ]

class SudoComputingElement( ComputingElement ):

  mandatoryParameters = MandatoryParameters

  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    ComputingElement.__init__( self, ceUniqueID )
    self.submittedJobs = 0

  #############################################################################
  def _addCEConfigDefaults( self ):
    """Method to make sure all necessary Configuration Parameters are defined
    """
    # First assure that any global parameters are loaded
    ComputingElement._addCEConfigDefaults( self )
    # Now Sudo CE specific ones

  #############################################################################
  def submitJob( self, executableFile, proxy, dummy = None ):
    """ Method to submit job, overridden from super-class.
    """
    self.log.verbose( 'Setting up proxy for payload' )
    result = self.writeProxyToFile( proxy )
    if not result['OK']:
      return result

    payloadProxy = result['Value']
    if not 'X509_USER_PROXY' in os.environ:
      self.log.error( 'X509_USER_PROXY variable for pilot proxy not found in local environment' )
      return S_ERROR( 'X509_USER_PROXY not found' )

    pilotProxy = os.environ['X509_USER_PROXY']
    self.log.info( 'Pilot proxy X509_USER_PROXY=%s' % pilotProxy )

    # See if a fixed value has been given
    payloadUsername = self.ceParameters.get( 'PayloadUser' )
    
    if payloadUsername:
      self.log.info( 'Payload username %s from PayloadUser in ceParameters' % payloadUsername )
    else:
      # First username in the sequence to use when running payload job
      # If first is pltXXp00 then have pltXXp01, pltXXp02, ...
      try:
        baseUsername = self.ceParameters.get('BaseUsername')
        baseCounter = int( baseUsername[-2:] )
        self.log.info( "Base username from BaseUsername in ceParameters : %s" % baseUsername )
      except:
        baseUsername = os.environ['USER'] + '00p00'
        baseCounter  = 0
        self.log.info( 'Base username from $USER + 00p00 : %s' % baseUsername )

      # Next one in the sequence
      payloadUsername = baseUsername[:-2] + ( '%02d' % (baseCounter + self.submittedJobs) )
      self.log.info( 'Payload username set to %s using jobs counter' % payloadUsername )

    try:
      payloadUID = pwd.getpwnam(payloadUsername).pw_uid
      payloadGID = pwd.getpwnam(payloadUsername).pw_gid
    except:
      error = S_ERROR( 'User "' + str(payloadUsername) + '" does not exist!' )
      error['Value'] = ( 201, '', '' )
      return error

    self.log.verbose( 'Starting process for monitoring payload proxy' )
    gThreadScheduler.addPeriodicTask( self.proxyCheckPeriod, self.monitorProxy,
                                      taskArgs = ( pilotProxy, payloadProxy, payloadUsername, payloadUID, payloadGID ),
                                      executions = 0, elapsedTime = 0 )

    # Submit job
    self.log.info( 'Changing permissions of executable (%s) to 0755' % executableFile )
    try:
      os.chmod( os.path.abspath( executableFile ), stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH )
    except Exception, x:
      self.log.error( 'Failed to change permissions of executable to 0755 with exception', 
                      '\n%s' % ( x ) )

    result = self.SudoExecute( os.path.abspath( executableFile ), payloadProxy, payloadUsername, payloadUID, payloadGID )
    if not result['OK']:
      self.log.error( 'Failed SudoExecute', result )
      return result

    self.log.debug( 'Sudo CE result OK' )
    self.submittedJobs += 1
    return S_OK()

  #############################################################################
  def SudoExecute( self, executableFile, payloadProxy, payloadUsername, payloadUID, payloadGID ):
    """Run sudo with checking of the exit status code.
    """
    # We now implement a file giveaway using groups, to avoid any need to sudo to root.
    # Each payload user must have their own group. The pilot user must be a member
    # of all of these groups. This allows the pilot user to set the group of the
    # payloadProxy file to be that of the payload user. The payload user can then
    # read it and make a copy of it (/tmp/x509up_uNNNN) that it owns. Some grid
    # commands check that the proxy is owned by the current user so the copy stage
    # is necessary.

    # 1) Make sure the payload user can read its proxy via its per-user group
    os.chown( payloadProxy, -1, payloadGID )
    os.chmod( payloadProxy, stat.S_IRUSR + stat.S_IWUSR + stat.S_IRGRP )

    # 2) Now create a copy of the proxy owned by the payload user          
    result = shellCall( 0, 
                        '/usr/bin/sudo -u %s sh -c "cp -f %s /tmp/x509up_u%d ; chmod 0400 /tmp/x509up_u%d"' % ( payloadUsername,  payloadProxy, payloadUID, payloadUID ),
                        callbackFunction = self.sendOutput )

    # Run the executable (the wrapper in fact)
    cmd = "/usr/bin/sudo -u %s PATH=$PATH DIRACSYSCONFIG=/scratch/%s/pilot.cfg LD_LIBRARY_PATH=$LD_LIBRARY_PATH PYTHONPATH=$PYTHONPATH X509_USER_PROXY=/tmp/x509up_u%d sh -c '%s'" % ( payloadUsername, os.environ['USER'], payloadUID, executableFile )
    self.log.info( 'CE submission command is: %s' % cmd )
    result = shellCall( 0, cmd, callbackFunction = self.sendOutput )
    if not result['OK']:
      result['Value'] = ( 0, '', '' )
      return result

    resultTuple = result['Value']
    status = resultTuple[0]
    stdOutput = resultTuple[1]
    stdError = resultTuple[2]
    self.log.info( "Status after the sudo execution is %s" % str( status ) )
    if status >=127:
      error = S_ERROR( status )
      error['Value'] = ( status, stdOutput, stdError )
      return error

    return result

  #############################################################################
  def getCEStatus( self ):
    """ Method to return information on running and pending jobs.
    """
    result = S_OK()
    result['SubmittedJobs'] = 0
    result['RunningJobs'] = 0
    result['WaitingJobs'] = 0
    return result

  #############################################################################
  def monitorProxy( self, pilotProxy, payloadProxy, payloadUsername, payloadUID, payloadGID ):
    """ Monitor the payload proxy and renew as necessary.
    """
    retVal = self._monitorProxy( pilotProxy, payloadProxy )
    if not retVal['OK']:
      # Failed to renew the proxy, nothing else to be done
      return retVal

    if not retVal['Value']:
      # No need to renew the proxy, nothing else to be done
      return retVal

    self.log.info( 'Re-executing sudo to make renewed payload proxy available as before' )

    # New version of the proxy file, so we have to do the copy again
    
    # 1) Make sure the payload user can read its proxy via its per-user group
    os.chown( payloadProxy, -1, payloadGID )
    os.chmod( payloadProxy, stat.S_IRUSR + stat.S_IWUSR + stat.S_IRGRP )

    # 2) Now recreate the copy of the proxy owned by the payload user
    result = shellCall( 0, 
                        '/usr/bin/sudo -u %s sh -c "cp -f %s /tmp/x509up_u%d ; chmod 0400 /tmp/x509up_u%d"' 
                        % ( payloadUsername,  payloadProxy, payloadUID, payloadUID ),
                        callbackFunction = self.sendOutput )
    
    return S_OK( 'Proxy checked' )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
