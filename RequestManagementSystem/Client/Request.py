# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/RequestManagementSystem/Client/Request.py,v 1.9 2008/04/02 19:42:07 atsareg Exp $
"""
   Request Class encapsulates a request definition based on the Workflow
   framework.

"""

__RCSID__ = "$Id: Request.py,v 1.9 2008/04/02 19:42:07 atsareg Exp $"

from DIRAC.Core.Workflow.Utility                    import *
from DIRAC.RequestManagementSystem.Client.RequestBase import RequestBase

class Request(RequestBase):

  #############################################################################

  def __init__(self,script=None):
    """Instantiates the Workflow object and some default parameters.
    """

    RequestBase.__init__(self)
    self.genParameters['RequestTechnology'] = 'Workflow'

    self.moduleType = None

    if not script:
      self.workflow = Workflow()
    else:
      self.workflow = Workflow(script)
      for p in self.workflow.parameters:
        pname = p.getName()
        if pname in self.genParametersNames:
          self.genParameters[pname] = p.getValue()
        else:
          self.parametersNames.append(pname)
          self.parameters[pname] = p.getValue()

  #############################################################################
  def define(self,moduleType,parameterDict):
    """ Define the new request giving the request module type and
        all the necessary parameters
    """

    # Fill in the standard RequestBase part
    for p,value in parameterDict.items():
      if p in self.genParametersNames:
        self.genParameters[p] = value
      elif p in self.parametersNames:
        self.parameters[p] = value
      else:
        self.parametersNames.append(p)
        self.parameters[p] = value

    # Create the requested workflow
    self.moduleType = moduleType
    module = ModuleDefinition(moduleType+'Module')
    for p,value in self.genParameters.items():
      module.addParameter(Parameter(p,value,'string','','',
                                    True,False,'Request parameter description'))
    for p,value in parameterDict.items():
      if p not in self.genParametersNames:
        module.addParameter(Parameter(p,value,'string','','',
                                      True,False,'Request parameter description'))

    module.setDescription('Request Failover System item')
    #Below should be changed to a more dynamic approach
    body = 'from DIRAC.RequestManagementSystem.Client.'+moduleType
    body += 'Module import '+moduleType+'Module\n'
    module.setBody(body)

    self.workflow = createSingleModuleWorkflow(module,moduleType)

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
