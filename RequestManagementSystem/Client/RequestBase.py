# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/RequestManagementSystem/Client/RequestBase.py,v 1.2 2008/04/02 19:44:17 atsareg Exp $

""" Request base class. Defines the common general parameters that should be present in any
    request
"""

__RCSID__ = "$Id: RequestBase.py,v 1.2 2008/04/02 19:44:17 atsareg Exp $"

import commands
import DIRAC.Core.Utilities.Time as Time

class RequestBase:

  def __init__(self):

    # This is a list of mandatory parameters
    self.genParametersNames = ['RequestName','RequestType','RequestTechnology',
                               'RequestID','DIRACSetup','OwnerDN','OwnerGroup',
                               'SourceComponent','TargetComponent',
                               'CreationTime','ExecutionTime','JobID','Status']

    self.genParameters = {}
    for name in self.genParametersNames:
      self.genParameters[name] = 'Unknown'

    # Set some defaults
    self.genParameters['DIRACSetup'] = "LHCb-Development"
    status,self.genParameters['RequestID'] = commands.getstatusoutput('uuidgen')
    self.genParameters['CreationTime'] = Time.toString(Time.dateTime())
    self.genParameters['Status'] = "New"

    self.parameters = {}
    self.parametersNames = []

  def getGenParameters(self):
    """ Get the dictionary of the generic parameters
    """
    return self.genParameters

#####################################################################
  def setSourceComponent(self,sourceComponent):
    """ Set the component which is creating the request
    """
    self.genParameters['SourceComponent'] = sourceComponent

  def getSourceComponent(self):
    """ Get the component which created the request
    """
    return self.genParameters['SourceComponent']

#####################################################################
  def setTargetComponent(self,targetComponent):
    """ Set the component which is should receive the request
    """
    self.genParameters['TargetComponent'] = targetComponent

  def getTargetComponent(self):
    """ Get the component which will receive the request
    """
    return self.genParameters['TargetComponent']

#####################################################################
  def setRequestType(self,requestType):
    """ Set the type of the request being created
    """
    self.genParameters['RequestType'] = requestType

  def getRequestType(self):
    """ Get the type of the request
    """
    return self.genParameters['RequestType']

#####################################################################
  def setOwnerDN(self,ownerDN):
    """ Set the DN of the request owner
    """
    self.genParameters['OwnerDN'] = ownerDN

  def getOwnerDN(self):
    """ Get the DN of the request owner
    """
    return self.genParameters['OwnerDN']

#####################################################################
  def setCreationTime(self,time):
    """ Set the creation time to the current data and time
    """

    if time.lower() == "now":
      self.genParameters['CreationTime'] = Time.toString(Time.dateTime())
    else:
      self.genParameters['CreationTime'] = time

  def getCreationTime(self):
    """ Get the date the request was created
    """
    return self.genParameters['CreationTime']

#####################################################################
  def setExecutionTime(self,time):
    """ Set the execution time to the current data and time
    """

    if time.lower() == "now":
      self.genParameters['ExecutionTime'] = Time.toString(Time.dateTime())
    else:
      self.genParameters['ExecutionTime'] = time

  def getExecutionTime(self):
    """ Get the date the request was created
    """
    return self.genParameters['ExecutionTime']

#####################################################################
  def setJobID(self,jobID):
    """ Set the JobID associated to the request
    """
    self.genParameters['JobID'] = jobID

  def getJobID(self):
    """ Get the JobID associated to the request
    """
    return self.genParameters['JobID']

#####################################################################
  def setRequestName(self,requestName):
    """ Set the name given to the request
    """
    self.genParameters['RequestName'] = requestName

  def getRequestName(self):
    """ Get the name associated to the request
    """
    return self.genParameters['RequestName']

#####################################################################
  def setRequestTechnology(self,requestTechnology):
    """ Set the technology of the request
    """
    self.genParameters['RequestTechnology'] = requestTechnology

  def getRequestName(self):
    """ Get the echnology of the request
    """
    return self.genParameters['RequestTechnology']

#####################################################################
  def setRequestID(self,requestID):
    """ Set associated requestID
    """
    self.genParameters['RequestID'] = requestID

  def getRequestID(self):
    """ Get the request ID
    """
    return self.genParameters['RequestID']

#####################################################################
  def setDiracSetup(self,diracSetup):
    """ Set the DIRAC WMS mode (instance)
    """
    self.genParameters['DIRACSetup'] = diracSetup

  def getDiracSetup(self):
    """ Get the DIRAC WMS mode ( instance )
    """
    return self.genParameters['DIRACSetup']

 #####################################################################
  def dump(self):
    """ Print out the request contents
    """

    print "=============================================================="
    for pname in self.genParametersNames:
      print (pname+':').ljust(26),self.genParameters[pname]

    if self.parameters:
      print "--------------------------------------------------------"
      for pname in self.parametersNames:
        print (pname+':').ljust(26),self.parameters[pname]
    print "=============================================================="