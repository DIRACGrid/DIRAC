# $HeadURL:  $
""" CompilerHelper

  It is a helper used to compile the applications

"""

from DIRAC import gConfig, gLogger
from DIRAC.Core.HTTP.Lib import Conf
from DIRAC.Core.HTTP.Core.App import App

_RCSID_ = "$Id$"


class CompilerHelper:
  '''
  This class parse the cfg files and discovers the applications dependencies.
  '''

  def __init__(self):
    '''
    It instantiate the application main class in order to load the configuration files.
    '''
    app = App()
    app._loadWebAppCFGFiles()
    self.__dependencySection = "Dependencies"

  def getAppDependencies(self):
    """
    Generate the dependency dictionary
    :return: Dict
    """
    dependency = {}
    fullName = "%s/%s" % (Conf.BASECS, self.__dependencySection)
    result = gConfig.getOptions(fullName)
    if not result['OK']:
      gLogger.error(result['Message'])
      return dependency
    optionsList = result['Value']
    for opName in optionsList:
      opVal = gConfig.getValue("%s/%s" % (fullName, opName))
      dependency[opName] = opVal

    return dependency
