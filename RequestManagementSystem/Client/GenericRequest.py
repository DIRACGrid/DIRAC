"""
GenericRequest is the request base class, offering a generic wrapper, for all request types.
"""
import xml.dom.minidom, time
from DIRAC.Core.Utilities.File import makeGuid
from DIRAC import gLogger, S_OK, S_ERROR

class GenericRequest:

  def __init__(self):

    # The source of the request
    self.sourceComponent = None
    # The recipient of the request i.e. TransferDB,LoggingSvc etc
    self.targetComponent = None
    # Could be 'DATA_MANAGEMENT_REQUEST' or 'LOGGING_REQUEST' or 'MONIORING_REQUEST'
    self.requestType = None
    # The DN with which the request should be executed
    self.ownerDN = ''
    # For resolution by the target (for LoggingSvc/Monitoring)
    self.creationTime = time.strftime('%Y-%m-%d %H:%M:%S')
    # If the request is associated to a job
    self.jobID = None
    # Must be supplied by creator (used in file backends as unique identifier)
    self.requestName = None
    # Used in mysql backends for unique identifier #need not be set by cretor
    self.requestID = None
    # The dirac instance
    self.diracInstance = ''

#####################################################################
  def setSourceComponent(self,sourceComponent):
    """ Set the component which is creating the request
    """
    self.sourceComponent = sourceComponent
    return S_OK()

  def getSourceComponent(self):
    """ Get the component which created the request
    """
    return S_OK(self.sourceComponent)

#####################################################################
  def setTargetComponent(self,targetComponent):
    """ Set the component which is should receive the request
    """
    self.targetComponent = targetComponent
    return S_OK()

  def getTargetComponent(self):
    """ Get the component which will receive the request
    """
    return S_OK(self.targetComponent)

#####################################################################
  def setRequestType(self,requestType):
    """ Set the type of the request being created
    """
    self.requestType = requestType
    return S_OK()

  def getRequestType(self):
    """ Get the type of the request
    """
    return S_OK(self.requestType)

#####################################################################
  def setOwnerDN(self,ownerDN):
    """ Set the DN of the request owner
    """
    self.ownerDN = ownerDN
    return S_OK()

  def getOwnerDN(self):
    """ Get the DN of the request owner
    """
    return S_OK(self.ownerDN)

#####################################################################
  def setCreationTime(self):
    """ Set the creation time to the current data and time
    """
    self.creationTime = time.strftime('%Y-%m-%d %H:%M:%S')
    return S_OK()

  def getCreationTime(self):
    """ Get the date the request was created
    """
    return S_OK(self.creationTime)

#####################################################################
  def setJobID(self,jobID):
    """ Set the JobID associated to the request
    """
    self.jobID = jobID
    return S_OK()

  def getJobID(self):
    """ Get the JobID associated to the request
    """
    return S_OK(self.jobID)

#####################################################################
  def setRequestName(self,requestName):
    """ Set the name given to the request
    """
    self.requestName = requestName
    return S_OK()

  def getRequestName(self):
    """ Get the name associated to the request
    """
    return S_OK(self.requestName)

#####################################################################
  def setRequestID(self,requestID):
    """ Set associated requestID
    """
    self.requestID = requestID
    return S_OK()

  def getRequestID(self):
    """ Get the request ID
    """
    return S_OK(self.requestID)

#####################################################################
  def setDiracInstance(self,diracInstance):
    """ Set the DIRAC WMS mode (instance)
    """
    self.diracInstance = diracInstance
    return S_OK()

  def getDiracInstance(self):
    """ Get the DIRAC WMS mode ( instance )
    """
    return S_OK(self.diracInstance)
