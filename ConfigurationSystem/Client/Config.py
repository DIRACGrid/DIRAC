# $HeadURL$
__RCSID__ = "$Id$"
from DIRAC.ConfigurationSystem.private.ConfigurationClient import ConfigurationClient

gConfig = ConfigurationClient()
def getConfig():
    return gConfig
