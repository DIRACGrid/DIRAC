# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/RequestManagementSystem/Client/Request.py,v 1.8 2008/04/02 09:28:35 atsareg Exp $
"""
   Request Base Class

   This class provides generic Request submission functionality suitable for any VO.

   Helper functions are documented with example usage for the DIRAC API.

"""

__RCSID__ = "$Id: Request.py,v 1.8 2008/04/02 09:28:35 atsareg Exp $"

from DIRAC.Core.Workflow.Utility                    import *
from DIRAC.RequestManagementSystem.Client.RequestBase import RequestBase

class Request(RequestBase):

  #############################################################################

  def __init__(self,script=None):
    """Instantiates the Workflow object and some default parameters.
    """

    RequestBase.__init__(self)

    self.moduleType = None

    if not script:
      self.workflow = Workflow()
    else:
      self.workflow = Workflow(script)
      for pname in self.genParametersNames:
        value = self.workflow.findParameter(pname).getValue()
        if value:
          self.genParameters[pname] = value

  #############################################################################
  def define(self,moduleType,parameterDict):
    """ Specify the module type of the request and setup the workflow
    """

    for p,value in parameterDict.items():
      if p in self.genParametersNames:
        self.genParameters[p] = value

    self.moduleType = moduleType
    module = ModuleDefinition(moduleType+'Module')
    for p,value in self.genParameters.items():
      module.addParameter(Parameter(p,value,'string','','',True,False,'Request parameter description'))
    for p,value in parameterDict.items():
      module.addParameter(Parameter(p,value,'string','','',True,False,'Request parameter description'))

    module.setDescription('Request Failover System item')
    #Below should be changed to a more dynamic approach
    body = 'from DIRAC.RequestManagementSystem.Client.'+moduleType
    body += 'Module import '+moduleType+'Module\n'
    module.setBody(body)

    self.workflow = createSingleModuleWorkflow(module,moduleType)

  #############################################################################
  def dump(self):
    """ Dump the request parameters
    """

    for p in self.workflow.parameters:
      print (p.getName()+':').ljust(26),p.getValue()

  #############################################################################
  def execute(self):
    """Executes the request locally.
    """
    self.workflow.execute()


  #############################################################################
  def toXML(self):
    """Internal Function.

       Creates an XML representation of itself as a Job,
       wraps around workflow toXML().
    """
    return self.workflow.toXML()
