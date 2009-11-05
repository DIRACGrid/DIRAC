# $HeadURL$
__RCSID__ = "$Id$"
from DIRAC.LoggingSystem.private.Logger import Logger

gLogger = Logger()
def getLogger():
    return gLogger
        
