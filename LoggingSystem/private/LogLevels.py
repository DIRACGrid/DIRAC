# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/private/LogLevels.py,v 1.1 2007/03/09 15:45:53 rgracian Exp $
__RCSID__ = "$Id: LogLevels.py,v 1.1 2007/03/09 15:45:53 rgracian Exp $"

class LogLevels:
  
  def __init__(self):
    self.always    = 'ALWAYS'
    self.info      = 'INFO'
    self.verbose   = 'VERBOSE'
    self.debug     = 'DEBUG'
    self.warn      = 'WARN'
    self.error     = 'ERROR'
    self.exception = 'EXCEPT'
    self.fatal     = 'FATAL'
    self.__levelDict = {
       self.always    : 30,
       self.info      : 20,
       self.verbose   : 10,
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
