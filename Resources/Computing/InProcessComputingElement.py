########################################################################
# $Id$
# File :   InProcessComputingElement.py
# Author : Stuart Paterson
########################################################################

""" The simplest Computing Element instance that submits jobs locally.
"""

__RCSID__ = "$Id$"

from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Core.Utilities.ThreadScheduler                import gThreadScheduler
from DIRAC.Core.Utilities.Subprocess                     import systemCall
from DIRAC.Core.Security.ProxyInfo                       import getProxyInfo
from DIRAC                                               import S_OK, S_ERROR

import os

MandatoryParameters = [ ]

class InProcessComputingElement( ComputingElement ):

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
    # Now InProcess specific ones

  #############################################################################
  def submitJob( self, executableFile, proxy, dummy = None ):
    """ Method to submit job, should be overridden in sub-class.
    """
    ret = getProxyInfo()
    if not ret['OK']:
      pilotProxy = None
    else:
      pilotProxy = ret['Value']['path']

    self.log.notice( 'Pilot Proxy:', pilotProxy )

    payloadEnv = dict( os.environ )
    payloadProxy = ''
    if proxy:
      self.log.verbose( 'Setting up proxy for payload' )
      result = self.writeProxyToFile( proxy )
      if not result['OK']:
        return result

      payloadProxy = result['Value']
      # pilotProxy = os.environ['X509_USER_PROXY']
      payloadEnv[ 'X509_USER_PROXY' ] = payloadProxy

    self.log.verbose( 'Starting process for monitoring payload proxy' )

    renewTask = None
    result = gThreadScheduler.addPeriodicTask( self.proxyCheckPeriod, self.monitorProxy, taskArgs = ( pilotProxy, payloadProxy ), executions = 0, elapsedTime = 0 )
    if result[ 'OK' ]:
      renewTask = result[ 'Value' ]

    if not os.access( executableFile, 5 ):
      os.chmod( executableFile, 0755 )
    cmd = os.path.abspath( executableFile )
    self.log.verbose( 'CE submission command: %s' % ( cmd ) )
    result = systemCall( 0, cmd, callbackFunction = self.sendOutput, env = payloadEnv )
    if payloadProxy:
      os.unlink( payloadProxy )

    if renewTask:
      gThreadScheduler.removeTask( renewTask )

    ret = S_OK()

    if not result['OK']:
      self.log.error( 'Fail to run InProcess', result['Message'] )
    elif result['Value'][0] > 128:
      # negative exit values are returned as 256 - exit
      self.log.warn( 'InProcess Job Execution Failed' )
      self.log.info( 'Exit status:', result['Value'][0] - 256 )
      if result['Value'][0] - 256 == -2:
        error = 'Error in the initialization of the DIRAC JobWrapper'
      elif result['Value'][0] - 256 == -1:
        error = 'Error in the execution of the DIRAC JobWrapper'
      else:
        error = 'InProcess Job Execution Failed'
      res = S_ERROR( error )
      res['Value'] = result['Value'][0] - 256
      return res
    elif result['Value'][0] > 0:
      self.log.warn( 'Fail in payload execution' )
      self.log.info( 'Exit status:', result['Value'][0] )
      ret['PayloadFailed'] = result['Value'][0]
    else:
      self.log.debug( 'InProcess CE result OK' )

    self.submittedJobs += 1
    return ret

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
  def monitorProxy( self, pilotProxy, payloadProxy ):
    """ Monitor the payload proxy and renew as necessary.
    """
    return self._monitorProxy( pilotProxy, payloadProxy )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
