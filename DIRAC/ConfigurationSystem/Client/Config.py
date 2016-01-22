# $HeadURL$
"""
  Instantiate the global Configuration Object
  gConfig is used everywhere within DIRAC to access Configuration data
"""
__RCSID__ = "$Id$"
from DIRAC.ConfigurationSystem.private.ConfigurationClient import ConfigurationClient

gConfig = ConfigurationClient()
def getConfig():
  return gConfig
