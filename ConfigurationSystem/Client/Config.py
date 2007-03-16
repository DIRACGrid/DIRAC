# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ConfigurationSystem/Client/Config.py,v 1.1 2007/03/16 11:57:33 rgracian Exp $
__RCSID__ = "$Id: Config.py,v 1.1 2007/03/16 11:57:33 rgracian Exp $"
from DIRAC.ConfigurationSystem.private.ConfigurationClient import ConfigurationClient

gConfig = ConfigurationClient()
def getConfig():
    return gConfig
