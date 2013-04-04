# $HeadURL$
__RCSID__ = "$Id$"

class LogLevels:

  def __init__(self):
    self.always    = 'ALWAYS'
    self.notice    = 'NOTICE'
    self.info      = 'INFO'
    self.verbose   = 'VERB'
    self.debug     = 'DEBUG'
    self.warn      = 'WARN'
    self.error     = 'ERROR'
    self.exception = 'EXCEPT'
    self.fatal     = 'FATAL'
    self.__levelDict = {
       self.always    : 40,
       self.notice    : 30,
       self.info      : 20,
       self.verbose   : 10,
       'VERBOSE'      : 10,
       self.debug     : 0,
       self.warn      : -20,
       self.error     : -30,
       self.exception : -30,
       self.fatal     : -40
       }

  def getLevelValue(self, sName):
    if self.__levelDict.has_key( sName ):
      return self.__levelDict[ sName ]
    else:
      return None
          
  def getLevel( self, level ):
    """ Get level name given the level digital value 
    """  
    for lev in self.__levelDict:
      if self.__levelDict[lev] == level:
        return lev
    return "Unknown"  
  
  def getLevels( self ):
    return self.__levelDict.keys()
