########################################################################
# $Id: glexecComputingElement.py,v 1.19 2009/04/18 18:26:57 rgracian Exp $
# File :   glexecComputingElement.py
# Author : Stuart Paterson
########################################################################

""" A computing element class that attempts to use glexec if available then
    defaults to the standard InProcess Computing Element behaviour.
"""

__RCSID__ = "$Id: glexecComputingElement.py,v 1.19 2009/04/18 18:26:57 rgracian Exp $"

from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.FrameworkSystem.Client.ProxyManagerClient     import gProxyManager
from DIRAC.Core.Utilities.ThreadScheduler                import gThreadScheduler
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.Core.Security.Misc                            import getProxyInfoAsString
from DIRAC                                               import gConfig,S_OK,S_ERROR

import DIRAC

import os,sys,string

CE_NAME = 'glexec'

class glexecComputingElement(ComputingElement):

  #############################################################################
  def __init__(self):
    """ Standard constructor.
    """
    ComputingElement.__init__(self,CE_NAME)
    self.minProxyTime = gConfig.getValue( '/Security/MinProxyLifeTime', 10800 ) #secs
    self.defaultProxyTime = gConfig.getValue( '/Security/DefaultProxyLifeTime', 86400 ) #secs
    self.proxyCheckPeriod = gConfig.getValue('/Security/ProxyCheckingPeriod',3600) #secs
    self.submittedJobs = 0

  #############################################################################
  def submitJob(self,executableFile,jdl,proxy,localID):
    """ Method to submit job, should be overridden in sub-class.
    """
    self.log.verbose('Setting up proxy for payload')
    result = self.writeProxyToFile(proxy)
    if not result['OK']:
      return result

    payloadProxy = result['Value']
    if not os.environ.has_key('X509_USER_PROXY'):
      self.log.error('X509_USER_PROXY variable for pilot proxy not found in local environment')
      return S_ERROR('X509_USER_PROXY not found')

    pilotProxy = os.environ['X509_USER_PROXY']
    self.log.info('Pilot proxy X509_USER_PROXY=%s' %pilotProxy)
    os.environ[ 'GLEXEC_CLIENT_CERT' ] = payloadProxy
    os.environ[ 'GLEXEC_SOURCE_PROXY' ] = payloadProxy
    self.log.info('Set payload proxy variables:\nGLEXEC_CLIENT_CERT=%s\nGLEXEC_SOURCE_PROXY=%s' %(payloadProxy,payloadProxy))

    #Determine glexec location (default to standard InProcess behaviour if not found)
    glexecLocation = None
    result = self.glexecLocate()
    if result['OK']:
      glexecLocation = result['Value']
      self.log.info('glexec found for local site at %s' %glexecLocation)

    if glexecLocation:
      result = self.recursivelyChangePermissions()
      if not result['OK']:
        self.log.error('Permissions change failed, continuing regardless...')
    else:
      self.log.info('glexec not found, no permissions to change')

    #Test glexec with payload proxy prior to submitting the job
    result = self.glexecTest(glexecLocation)
    if not result['OK']:
      self.analyseExitCode(result['Value']) #take no action as we currently default to InProcess
      self.log.info('glexec test failed, will submit payload regardless...')
      glexecLocation=None

    #Revert to InProcess behaviour
    if not glexecLocation:
      self.log.info('glexec is not found, setting X509_USER_PROXY for payload proxy')
      os.environ[ 'X509_USER_PROXY' ] = payloadProxy

    self.log.verbose('Starting process for monitoring payload proxy')
    gThreadScheduler.addPeriodicTask(self.proxyCheckPeriod,self.monitorProxy,taskArgs=(glexecLocation,pilotProxy,payloadProxy),executions=0,elapsedTime=0)

    #Submit job
    self.log.info('Changing permissions of executable to 0755')
    try:
      os.chmod(os.path.abspath(executableFile),0755)
    except Exception,x:
      self.log.error('Failed to change permissions of executable to 0755 with exception:\n%s' %(x))

    result = self.glexecExecute(os.path.abspath(executableFile),glexecLocation)
    if not result['OK']:
      self.analyseExitCode(result['Value']) #take no action as we currently default to InProcess
      self.log.error(result)
      return result

    self.log.debug('glexec CE result OK')
    self.submittedJobs += 1
    return S_OK(localID)

  #############################################################################
  def recursivelyChangePermissions(self):
    """ Ensure that the current directory and all those beneath have the correct
        permissions.
    """
    currentDir = os.getcwd()
    try:
      self.log.info('Trying to explicitly change permissions for parent directory %s' %currentDir)
      os.chmod(currentDir,0755)
    except Exception,x:
      self.log.error('Problem changing directory permissions in parent directory',str(x))

    return S_OK()

    userID = None

    res = shellCall(0,'ls -al')
    if res['OK']:
      self.log.info('Contents of the working directory before permissions change:')
      self.log.info(str(res['Value'][1]))
    else:
      self.log.error('Failed to list the log directory contents',str(res['Value'][2]))

    res = shellCall(0,'id -u')
    if res['OK']:
      userID = res['Value'][1]
      self.log.info('Current user ID is: %s' %(userID))
    else:
      self.log.error('Failed to obtain current user ID',str(res['Value'][2]))
      return res

    res = shellCall(0,'ls -al %s/../' %currentDir)
    if res['OK']:
      self.log.info('Contents of the parent directory before permissions change:')
      self.log.info(str(res['Value'][1]))
    else:
      self.log.error('Failed to list the parent directory contents',str(res['Value'][2]))

    self.log.verbose('Changing permissions to 0755 in current directory %s' %currentDir)
    for dirName, subDirs, files in os.walk(currentDir):
      try:
        self.log.info('Changing file and directory permissions to 0755 for %s' %dirName)
        if os.stat('%s' %(dirName))[4] == userID and not os.path.islink('%s' %(dirName)):
          os.chmod('%s' %(dirName),0755)
        for toChange in files:
          if os.stat('%s/%s' %(dirName,toChange))[4] == userID and not os.path.islink('%s/%s' %(dirName,toChange)):
            os.chmod('%s/%s' %(dirName,toChange),0755)
      except Exception,x:
        self.log.error('Problem changing directory permissions',str(x))

    self.log.info('Permissions in current directory %s updated successfully' %(currentDir))
    res = shellCall(0,'ls -al')
    if res['OK']:
      self.log.info('Contents of the working directory after changing permissions:')
      self.log.info(str(res['Value'][1]))
    else:
      self.log.error('Failed to list the log directory contents',str(res['Value'][2]))

    res = shellCall(0,'ls -al %s/../' %currentDir)
    if res['OK']:
      self.log.info('Contents of the parent directory after permissions change:')
      self.log.info(str(res['Value'][1]))
    else:
      self.log.error('Failed to list the parent directory contents',str(res['Value'][2]))

    return S_OK()

  #############################################################################
  def analyseExitCode(self,resultTuple):
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

    codes = {}
    codes[127]='Shell exited, command not found'
    codes[129]='Shell interrupt signal 1 (SIGHUP)'
    codes[130]='Shell interrupt signal 2 (SIGINT)'
    codes[201]='glexec failed with client error'
    codes[202]='glexec failed with internal error'
    codes[203]='glexec failed with authorization error'

    status = resultTuple[0]
    stdOutput = resultTuple[1]
    stdError = resultTuple[2]

    self.log.info('glexec call failed with status %s' %(status))
    self.log.info('glexec stdout:\n%s' %stdOutput)
    self.log.info('glexec stderr:\n%s' %stdError)

    error = None
    for code,msg in codes.items():
      self.log.verbose('Exit code %s => %s' %(code,msg))
      if status==code:
        error=msg

    if not error:
      self.log.error('glexec exit code %s not in expected list' %(status))
    else:
      self.log.error('Resolved glexec return code %s = %s' %(status,error))

    return S_OK(error)

  #############################################################################
  def glexecTest(self,glexecLocation):
    """Ensure that the current DIRAC distribution is group readable e.g. dirac-proxy-info
       also check the status code of the glexec call.
    """
    if not glexecLocation:
      return S_OK('Nothing to test')

    testFile = 'glexecTest.sh'
    cmds = ['#!/bin/sh']
    cmds.append('id')
    cmds.append('hostname')
    cmds.append('date')
    cmds.append('%s/scripts/dirac-proxy-info' %DIRAC.rootPath)
    fopen = open(testFile,'w')
    fopen.write(string.join(cmds,'\n'))
    fopen.close()
    self.log.info('Changing permissions of test script to 0755')
    try:
      os.chmod(os.path.abspath(testFile),0755)
    except Exception,x:
      self.log.error('Failed to change permissions of test script to 0755 with exception:\n%s' %(x))
      return S_ERROR('Could not change permissions of test script')

    return self.glexecExecute(os.path.abspath(testFile),glexecLocation)

  #############################################################################
  def glexecExecute(self,executableFile,glexecLocation):
    """Run glexec with checking of the exit status code.
    """
    cmd = executableFile
    if glexecLocation and executableFile:
      cmd = "%s /bin/bash -lc '%s'" %(glexecLocation,executableFile)
    if glexecLocation and not executableFile:
      cmd = '%s' %(glexecLocation)

    self.log.info('CE submission command is: %s' %cmd)
    result = shellCall(0,cmd,callbackFunction = self.sendOutput)
    if not result['OK']:
      result['Value']=(0,'','')
      return result

    resultTuple = result['Value']
    status = resultTuple[0]
    stdOutput = resultTuple[1]
    stdError = resultTuple[2]
    self.log.info( "Status after the glexec execution is %s" % str( status ) )
    if status:
      error = S_ERROR(status)
      error['Value'] = (status,stdOutput,stdError)
      return error

    return result

  #############################################################################
  def glexecLocate(self):
    """Try to find glexec on the local system, if not found default to InProcess.
    """
    if not os.environ.has_key('GLITE_LOCATION'):
      self.log.info('Unable to locate glexec, site does not have GLITE_LOCATION defined')
      return S_ERROR('glexec not found')

    glexecPath = '%s/sbin/glexec' %(os.environ['GLITE_LOCATION'])
    if not os.path.exists(glexecPath):
      self.log.info('$GLITE_LOCATION/sbin/glexec not found at path %s' %(glexecPath))
      return S_ERROR('glexec not found')

    return S_OK(glexecPath)

  #############################################################################
  def getDynamicInfo(self):
    """ Method to return information on running and pending jobs.
    """
    result = {}
    result['SubmittedJobs'] = 0
    result['RunningJobs'] = 0
    result['WaitingJobs'] = 0
    return S_OK(result)

  #############################################################################
  def monitorProxy(self,glexecLocation,pilotProxy,payloadProxy):
    """ Monitor the payload proxy and renew as necessary.
    """
    if not os.path.exists(pilotProxy):
      return S_ERROR('Pilot proxy not found at %s' %pilotProxy)
    if not os.path.exists(payloadProxy):
      return S_ERROR('Payload proxy not found at %s' %payloadProxy)

    result = getProxyInfoAsString(payloadProxy)
    if not result['OK']:
      self.log.error('Could not get payload proxy info',result)
      return result

    self.log.info('Payload proxy information seen from pilot:\n%s' %result['Value'])
    gProxyManager.renewProxy(minLifeTime=self.minProxyTime,
                             newProxyLifeTime=self.defaultProxyTime,
                             proxyToConnect=pilotProxy)

    if glexecLocation:
      self.log.info('Rerunning glexec without arguments to renew payload proxy')
      result = self.glexecExecute(None,glexecLocation)
      if not result['OK']:
        self.log.error(result)
    else:
      self.log.info('Running without glexec, checking local proxy')

    return S_OK('Proxy checked')

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
