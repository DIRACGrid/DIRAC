""" Class that encapsulate few commonalities of DIRAC DBs
    (MySQL-based, SQLAlchemy-based, ES-based)
"""

from DIRAC import gLogger, gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection

class DIRACDB(object):

  def __init__(self):
    """c'tor

    :param self: self reference
    """

    self.log = gLogger.getSubLogger(self.__class__.__name__)

  def getCSOption(self, optionName, defaultValue=None):
    cs_path = getDatabaseSection(self.fullname)
    return gConfig.getValue("/%s/%s" % (cs_path, optionName), defaultValue)
