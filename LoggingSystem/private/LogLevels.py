# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/private/LogLevels.py,v 1.3 2008/01/16 16:23:56 acasajus Exp $
__RCSID__ = "$Id: LogLevels.py,v 1.3 2008/01/16 16:23:56 acasajus Exp $"

class LogLevels:

  def __init__(self):
    self.always    = 'ALWAYS'
    self.info      = 'INFO'
    self.verbose   = 'VERB'
    self.debug     = 'DEBUG'
    self.warn      = 'WARN'
    self.error     = 'ERROR'
    self.exception = 'EXCEPT'
    self.fatal     = 'FATAL'
    self.__levelDict = {
       self.always    : 30,
       self.info      : 20,
       self.verbose   : 10,
       'VERBOSE'      : 10,
       self.debug     : 0,
       self.warn      : -10,
       self.error     : -20,
       self.exception : -20,
       self.fatal     : -30
       }

  def getLevelValue(self, sName):
    if self.__levelDict.has_key( sName ):
      return self.__levelDict[ sName ]
    else:
      return None

  def getLevels( self ):
    return self.__levelDict.keys()
