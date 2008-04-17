# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/RequestManagementSystem/Client/WorkflowSubRequest.py,v 1.1 2008/04/17 13:31:39 atsareg Exp $
"""
   WorkflowSubRequest Class encapsulates a request definition based on the Workflow
   framework.

"""

__RCSID__ = "$Id: WorkflowSubRequest.py,v 1.1 2008/04/17 13:31:39 atsareg Exp $"

from DIRAC.Core.Workflow.Utility import *
import commands, datetime

class WorkflowSubRequest:

  #############################################################################

  def __init__(self, requestType='Unknown'):
    """Instantiates the Workflow object and some default parameters.
    """

    self.subAttributeNames = ['Status','SubRequestID','Method','Type','CreationTime','ExecutionTime','Workflow']
    self.subAttributes = {}

    # Some initial values
    self.subAttributes['Status'] = "NEW"
    status,self.subAttributes['SubRequestID'] = commands.getstatusoutput('uuidgen')
    self.subAttributes['Method'] = "Workflow"
    self.subAttributes['CreationTime'] = str(datetime.datetime.utcnow())
    self.subAttributes['Type'] = requestType
    self.subAttributes['CreationTime'] = 'Unknown'
    self.subAttributes['Workflow'] = ''

  #############################################################################
  def define(self,moduleType,parameterDict):
    """ Define the new request giving the request module type and
        all the necessary parameters
    """

    # Create the requested workflow
    self.moduleType = moduleType
    self.module = ModuleDefinition(moduleType+'Module')
    for p,value in parameterDict.items():
      self.module.addParameter(Parameter(p,value,'string','','',
                                         True,False,'Request parameter description'))

    self.module.setDescription('Request Failover System execution unit')
    #Below should be changed to a more dynamic approach
    body = 'from DIRAC.RequestManagementSystem.Client.'+moduleType
    body += 'Module import '+moduleType+'Module\n'
    self.module.setBody(body)

  def getDictionary(self):
    """ Get the request representation as a dictionary
    """

    self.workflow = createSingleModuleWorkflow(module,moduleType)
    self.subAttributes['Workflow'] = workflow.toXML()
    resultDict = {}
    resultDict['Attributes'] = self.subAttributes
    return resultDict

  def setBody(self,body):
    """ Set the workflow module body
    """
    self.module.setBody(body)

  def setDescription(self,description):
    """ Set the workflow module description
    """
    self.module.setDescription(description)


