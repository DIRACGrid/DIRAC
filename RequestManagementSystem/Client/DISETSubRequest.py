# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/RequestManagementSystem/Client/DISETSubRequest.py,v 1.1 2008/04/17 13:59:31 atsareg Exp $
"""
   DISETSubRequest Class encapsulates a request definition to accomplish a DISET
   RPC call

"""

__RCSID__ = "$Id: DISETSubRequest.py,v 1.1 2008/04/17 13:59:31 atsareg Exp $"

import commands, datetime

class DISETSubRequest:

  #############################################################################

  def __init__(self, requestType='Unknown', rpcStub= None):
    """Instantiates the Workflow object and some default parameters.
    """

    self.subAttributeNames = ['Status','SubRequestID','Method','Type','CreationTime','ExecutionTime',
                              'TargetComponent','Call','Arguments']
    self.subAttributes = {}
    
    for attr in self.subAttributeNames:
      self.subAttributes[attr] = "Unknown"

    # Some initial values
    self.subAttributes['Status'] = "NEW"
    status,self.subAttributes['SubRequestID'] = commands.getstatusoutput('uuidgen')
    self.subAttributes['Method'] = "Workflow"
    self.subAttributes['CreationTime'] = str(datetime.datetime.utcnow())
    self.subAttributes['Type'] = requestType
    self.subAttributes['CreationTime'] = 'Unknown'
    
    if rpcStub:
      self.subAttributes['TargetComponent'] = rpcStub[0]
      self.subAttributes['Call'] = rpcStub[2]
      self.subAttributes['Arguments'] = ':::'.join(rpcStub[3])

  def setRPCCall(self,rpcStub):
    """ Define the  RPC call details
    """
    self.subAttributes['TargetComponent'] = rpcStub[0]
    self.subAttributes['Call'] = rpcStub[2]
    self.subAttributes['Arguments'] = ':::'.join(rpcStub[3])

  def getDictionary(self):
    """ Get the request representation as a dictionary
    """
    resultDict = {}
    resultDict['Attributes'] = self.subAttributes
    return resultDict

  def setTargetComponent(self,target):
    """ Set the RPC call target component
    """
    self.subAttributes['TargetComponent'] = target
    
  def setRPCCall(self,call):
    """ Set the RPC call method name
    """
    self.subAttributes['Call'] = call

  def setArguments(self,arguments):
    """ Set the RPC call arguments
    """
    self.subAttributes['Arguments'] = ":::".join(arguments)


