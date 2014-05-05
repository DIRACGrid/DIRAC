########################################################################
# $Id$
# File :   PoolComputingElement.py
# Author : A.T.
########################################################################

""" The Computing Element to run several jobs simultaneously
"""

__RCSID__ = "$Id$"

from DIRAC.Resources.Computing.InProcessComputingElement import InProcessComputingElement, ComputingElement
from DIRAC.Core.Security.ProxyInfo                       import getProxyInfo
from DIRAC                                               import S_OK, S_ERROR
from DIRAC.Core.Utilities.ProcessPool                    import ProcessPool
from DIRAC.Core.Utilities.Os                             import getNumberOfCores 

MandatoryParameters = [ ]

def executeJob( executableFile, proxy, taskID ):

  ce = InProcessComputingElement( "Task-" + str( taskID ) )
  result = ce.submitJob( executableFile, proxy )
  return result

class PoolComputingElement( ComputingElement ):

  mandatoryParameters = MandatoryParameters

  #############################################################################
  def __init__( self, ceUniqueID, cores = 0 ):
    """ Standard constructor.
    """
    ComputingElement.__init__( self, ceUniqueID )
    self.ceType = "Pool"
    self.submittedJobs = 0
    if cores > 0:
      self.cores = cores
    else:  
      self.cores = getNumberOfCores()
    self.pPool = ProcessPool( self.cores, self.cores, poolCallback = self.finalizeJob )
    self.taskID = 0
    self.coresPerTask = {}

  #############################################################################
  def _addCEConfigDefaults( self ):
    """Method to make sure all necessary Configuration Parameters are defined
    """
    # First assure that any global parameters are loaded
    ComputingElement._addCEConfigDefaults( self )
    
  def getCoresInUse( self ):
    """ 
    """ 
    coresInUse = 0
    for _task, cores in self.coresPerTask.items():
      coresInUse += cores 
    return coresInUse  

  #############################################################################
  def submitJob( self, executableFile, proxy, **kwargs ):
    """ Method to submit job, should be overridden in sub-class.
    """
    
    self.pPool.processResults()
    
    coresInUse = self.getCoresInUse()
    if "WholeNode" in kwargs and kwargs['WholeNode']:
      if coresInUse > 0:
        return S_ERROR('Can not take WholeNode job, %d/%d slots used' % (self.slotsInUse,self.slots) )
      else:
        requestedCores = self.cores
    elif "NumberOfCores" in kwargs:           
      requestedCores = int( kwargs['NumberOfCores'] )
      if requestedCores > 0:
        if (coresInUse + requestedCores) > self.cores:
          return S_ERROR( 'Not enough slots: requested %d, available %d' % ( requestedCores, self.cores-coresInUse) )
    else:
      requestedCores = 1   
    if self.cores - coresInUse < requestedCores:
      return S_ERROR( 'Not enough slots: requested %d, available %d' % ( requestedCores, self.cores-coresInUse) )
    
    ret = getProxyInfo()
    if not ret['OK']:
      pilotProxy = None
    else:
      pilotProxy = ret['Value']['path']
    self.log.notice( 'Pilot Proxy:', pilotProxy )

    result = self.pPool.createAndQueueTask( executeJob, 
                                            [executableFile,proxy,self.taskID],None,
                                            self.taskID,
                                            usePoolCallbacks = True )
    self.taskID += 1
    self.coresPerTask[self.taskID] = requestedCores
    
    self.pPool.processResults()
    
    return result
  
  def finalizeJob( self, taskID, result ):
    """ Finalize the job
    """
    del self.coresPerTask[taskID]

  #############################################################################
  def getCEStatus( self ):
    """ Method to return information on running and pending jobs.
    """
    self.pPool.processResults()
    result = S_OK()
    result['SubmittedJobs'] = 0
    nJobs = 0
    for _j, value in self.coresPerTask.items():
      if value > 0: 
        nJobs += 1
    result['RunningJobs'] = nJobs 
    result['WaitingJobs'] = 0
    coresInUse = self.getCoresInUse()
    result['UsedCores'] = coresInUse
    result['AvailableCores'] = self.cores - coresInUse
    return result

  #############################################################################
  def monitorProxy( self, pilotProxy, payloadProxy ):
    """ Monitor the payload proxy and renew as necessary.
    """
    return self._monitorProxy( pilotProxy, payloadProxy )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
