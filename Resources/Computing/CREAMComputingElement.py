########################################################################
# $HeadURL: $
# File :   CREAMComputingElement.py
# Author : A.T.
########################################################################

""" CREAM Computing Element 
"""

__RCSID__ = "$Id: $"

from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.Core.Utilities.Grid                           import executeGridCommand
from DIRAC.Core.Utilities.File                           import makeGuid
from DIRAC                                               import S_OK, S_ERROR
from DIRAC                                               import systemCall, rootPath
from DIRAC                                               import gConfig
from DIRAC.Core.Security.Misc                            import getProxyInfo

import os, sys, time, re, socket, stat, shutil
import string, shutil, tempfile

CE_NAME = 'CREAM'
MANDATORY_PARAMETERS = [ 'Queue' ]

class CREAMComputingElement( ComputingElement ):

  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    ComputingElement.__init__( self, ceUniqueID )
    
    self.ceType = CE_NAME
    self.submittedJobs = 0
    self.mandatoryParameters = MANDATORY_PARAMETERS
    self.pilotProxy = ''

  #############################################################################
  def _addCEConfigDefaults( self ):
    """Method to make sure all necessary Configuration Parameters are defined
    """
    # First assure that any global parameters are loaded
    ComputingElement._addCEConfigDefaults( self )
    
  def __writeJDL(self,executableFile):   
    """ Create the JDL for submission
    """
 
    workingDirectory = self.ceParameters['WorkingDirectory']

    jdl = """
[
  JobType = "Normal";
  Executable = "%(executable)s";
  StdOutput="out.out";
  StdError="err.err";
  InputSandbox={"%(executableFile)s"};
  OutputSandbox={"out.out", "err.err"};
  OutputSandboxBaseDestUri="%(outputURL)s";
]
    """ % {
            'executableFile':executableFile,
            'executable':os.path.basename(executableFile),
            'outputURL':self.outputURL
           }
    
    fd, name = tempfile.mkstemp( suffix = '.jdl', prefix = 'CREAM_', dir = workingDirectory )
    jdlFile = os.fdopen( fd, 'w' )
    jdlFile.write( jdl )
    jdlFile.close()
    
    return name
  
    
  def reset(self):
    self.queue = self.ceParameters['Queue']
    self.outputURL = self.ceParameters['OutputURL']  
    self.gridEnv = self.ceParameters['GridEnv']  

  #############################################################################
  def submitJob( self, executableFile, proxy, numberOfJobs=1 ):
    """ Method to submit job
    """

    self.log.info( "Executable file path: %s" % executableFile )
    if not os.access( executableFile, 5 ):
      os.chmod( executableFile, 0755 )
      
    jdlName = self.__writeJDL(executableFile)      
    batchIDList = []
    if numberOfJobs == 1: 
      cmd = 'glite-ce-job-submit -n -a -N -r %s/%s %s ' % (self.ceName,self.queue,jdlName)
      result = executeGridCommand(proxy,cmd,self.gridEnv)
      if result['OK']:
        batchIDList.append(result['Value'][1].strip())
    else:
      delegationID = makeGuid() 
      cmd = 'glite-ce-delegate-proxy -e %s %s' % (self.ceName,delegationID)
      result = executeGridCommand(proxy,cmd,self.gridEnv)
      for i in range(numberOfJobs):  
        cmd = 'glite-ce-job-submit -n -N -r %s/%s -D %s %s ' % (self.ceName,self.queue,delegationID,jdlName)
        result = executeGridCommand(proxy,cmd,self.gridEnv)
        print result
        if not result['OK']:
          break
        batchIDList.append(result['Value'][1].strip())
    
    os.unlink(executableFile)
    os.unlink(jdlName)
   
    print "AT >>>", batchIDList

    return S_OK( batchIDList )

  #############################################################################
  def getDynamicInfo( self, proxy = '' ):
    """ Method to return information on running and pending jobs.
    """
    statusList = ['REGISTERED','PENDING','IDLE','RUNNING','REALLY-RUNNING']
    cmd = 'glite-ce-job-status -n -a -e %s -s %s' % (self.ceName,':'.join(statusList) ) 
    result = executeGridCommand(proxy,cmd,self.gridEnv)     
    resultDict = {}
    if not result['OK']:
      return result
    if result['Value'][1]:
      resultDict = self.__parseJobStatus(result['Value'][1])
    
    running = 0
    waiting = 0
    for ref,status in resultDict.items():
      if status == 'Waiting':
        waiting += 1
      if status == 'Running':
        running += 1  

    result = S_OK()
    result['RunningJobs'] = running 
    result['WaitingJobs'] = waiting
    result['SubmittedJobs'] = 0
    return result
  
  def getJobStatus(self,jobIDList, proxy=''):
    """ Get the status information for the given list of jobs
    """
        
    fd, idFileName = tempfile.mkstemp( suffix = '.ids', prefix = 'CREAM_', dir = os.getcwd() )
    idFile = os.fdopen( fd, 'w' )
    idFile.write('##CREAMJOBS##')
    for id in jobIDList:
      idFile.write('\n'+id)
    idFile.close()
    
    cmd = 'glite-ce-job-status -n -i %s' % idFileName
    result = executeGridCommand(proxy,cmd,self.gridEnv)
    os.unlink(idFileName)
    resultDict = {} 
    if result['Value'][1]:
      resultDict = self.__parseJobStatus(result['Value'][1])

    # If CE does not know about a job, set the status to Unknown
    for job in jobIDList:
      if not resultDict.has_key(job):
        resultDict[job] = 'Unknown'

    return S_OK(resultDict)      

  def __parseJobStatus(self,output):
    """ Parse the output of the glite-ce-job-status
    """
    resultDict = {}
    ref = ''
    for line in output.split('\n'):
      if not line: continue
      match = re.search('JobID=\[(.*)\]',line)
      if match and len(match.groups()) == 1:
        ref = match.group(1)
      match = re.search('Status.*\[(.*)\]',line)
      if match and len(match.groups()) == 1:
         creamStatus = match.group(1)  
         if creamStatus in ['DONE-OK']:
           resultDict[ref] = 'Done'
         elif creamStatus in ['DONE-FAILED']:
           resultDict[ref] = 'Failed'
         elif creamStatus in ['REGISTERED','PENDING','IDLE']:
           resultDict[ref] = 'Scheduled'
         elif creamStatus in ['ABORTED']:
           resultDict[ref] = 'Aborted'
         elif creamStatus in ['CANCELLED']:
           resultDict[ref] = 'Killed' 
         elif creamStatus in ['RUNNING','REALLY-RUNNING']:
           resultDict[ref] = 'Running'
         else:
           resultDict[ref] = creamStatus.capitalize()  
   

    return resultDict
    
  
  def getJobOutput(self,jobID,localDir=None):
    """ Get the specified job standard output and error files. If the localDir is provided,
        the output is returned as file in this directory. Otherwise, the output is returned 
        as strings. 
    """     
    output = ''
    error = ''
    return S_OK((output,error))

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#

