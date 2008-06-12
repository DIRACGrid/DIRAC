# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/RequestManagementSystem/Client/DISETSubRequest.py,v 1.3 2008/06/12 15:09:51 atsareg Exp $
"""
   DISETSubRequest Class encapsulates a request definition to accomplish a DISET
   RPC call

"""

__RCSID__ = "$Id: DISETSubRequest.py,v 1.3 2008/06/12 15:09:51 atsareg Exp $"

import commands, datetime
from DIRAC.Core.Utilities import DEncode

class DISETSubRequest:

  #############################################################################

  def __init__(self,rpcStub= None):
    """Instantiates the Workflow object and some default parameters.
    """

    self.subAttributeNames = ['Status','SubRequestID','Method','Type','CreationTime','ExecutionTime',
                              'TargetComponent','Call','Arguments']
    self.subAttributes = {}

    for attr in self.subAttributeNames:
      self.subAttributes[attr] = "Unknown"

    # Some initial values
    self.subAttributes['Status'] = "New"
    status,self.subAttributes['SubRequestID'] = commands.getstatusoutput('uuidgen')
    self.subAttributes['Method'] = "DISET"
    self.subAttributes['CreationTime'] = str(datetime.datetime.utcnow())
    self.subAttributes['Type'] = 'Unknown'

    if rpcStub:
      self.subAttributes['TargetComponent'] = rpcStub[0]
      self.subAttributes['Call'] = rpcStub[2]
      self.subAttributes['Arguments'] = DEncode.encode(rpcStub[3])

  def setRPCCall(self,rpcStub):
    """ Define the  RPC call details
    """
    self.subAttributes['TargetComponent'] = rpcStub[0]
    self.subAttributes['Call'] = rpcStub[2]
    self.subAttributes['Arguments'] = DEncode.encode(rpcStub[3])

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
    self.subAttributes['Arguments'] = DEncode.encode(arguments)


