# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/Client/Logger.py,v 1.1 2007/03/09 15:45:36 rgracian Exp $
__RCSID__ = "$Id: Logger.py,v 1.1 2007/03/09 15:45:36 rgracian Exp $"
from DIRAC.LoggingSystem.private.Logger import Logger

gLogger = Logger()
def getLogger():
    return gLogger
        
