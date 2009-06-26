""" This is the Job Repository which stores and manipulates DIRAC job metadata in CFG format """

__RCSID__ = "$Id: JobRepository.py,v 1.5 2009/06/26 14:17:06 acsmith Exp $"

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.CFG import CFG

import os, time

class JobRepository:

  def __init__(self, repository=None):
    self.location = repository
    if not self.location:
      if os.environ.has_key('HOME'):
        self.location = '%s/.repo.cfg' % os.environ['HOME']
      else:
        self.location = '%s/.repo.cfg' % os.getcwd()
    self.repo = CFG()
    if os.path.exists(self.location):
      self.repo = CFG()
      self.repo.loadFromFile(self.location)
      if not self.repo.existsKey('Jobs'):
        self.repo.createNewSection('Jobs')
    else:
      self.repo = CFG()
      self.repo.createNewSection('Jobs')
    self.OK = True
    written = self._writeRepository(self.location)
    if not written:
      self.OK = False

  def isOK(self):
    return self.OK

  def readRepository(self):
    return S_OK(self.repo.getAsDict('Jobs'))

  def writeRepository(self, alternativePath=None):
    destination = self.location
    if alternativePath:
      destination = alternativePath
    written = self._writeRepository(destination)
    if not written:
      return S_ERROR("Failed to write repository")
    return S_OK(destination) 

  def _writeRepository(self,path):
    if os.path.exists(path):
      gLogger.debug("Replacing %s" % path)
    return self.repo.writeToFile(path)

  def appendToRepository(self, repoLocation):
    if not os.path.exists(repoLocation):
      gLogger.error("Secondary repository does not exist",repoLocation)
      return S_ERROR("Secondary repository does not exist")
    self.repo = CFG().loadFromFile(repoLocation).mergeWith(self.repo)
    self._writeRepository(self.location)
    return S_OK()
    
  def addJob(self, jobID, name, state='Submitted', retrieved=0, outputData=0, update=False):
    paramDict = { 'Name'        : name,    
                  'State'       : state,
                  'Time'        : self._getTime(),
                  'Retrieved'   : int(retrieved),
                  'OutputData'  : outputData}    
    self._writeJob(jobID, paramDict, update)
    return S_OK(jobID)

  def updateJob(self, jobID, state=None, retrieved=None, outputData=None):
    paramDict = {}
    if state:
      paramDict['State'] = state
    if retrieved:
      paramDict['Retrieved'] = retrieved
    if outputData:
      paramDict['OutputData'] = outputData
    paramDict['Time'] = self._getTime()
    self._writeJob(jobID, paramDict, True)
    return S_OK()

  def _getTime( self ):
    runtime = time.ctime()
    return runtime.replace(" ","_")

  def _writeJob(self, jobID, paramDict, update):
    jobID = str(jobID)
    jobExists = self._existsJob(jobID)
    if jobExists and (not update):
      gLogger.warn("Job exists and not overwriting")
      return S_ERROR("Job exists and not overwriting")
    if not jobExists:
      self.repo.createNewSection('Jobs/%s' % jobID)
    for key,value in paramDict.items():
      self.repo.setOption('Jobs/%s/%s' % (jobID,key),value)
    self._writeRepository(self.location)
    return S_OK()

  def removeJob(self,jobID):
    res = self.repo['Jobs'].deleteKey(str(jobID))
    if res:
      self._writeRepository(self.location)    
    return S_OK()

  def existsJob(self,jobID):
    return S_OK(self._existsJob(jobID))

  def _existsJob(self,jobID):
    return self.repo.isSection('Jobs/%s' % jobID)

  def getLocation(self):
    return S_OK(self.location)
    
  def getSize(self):
    return S_OK(len(self.repo.getAsDict('Jobs')))
