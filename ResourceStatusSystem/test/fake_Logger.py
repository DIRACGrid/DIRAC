""" fake gLogger 
    Every function can simply return S_OK() (or nothing)
"""

from DIRAC import S_OK, S_ERROR

#from DIRAC import gConfig

class Logger:
  
  def __init__(self):
    pass

  def info( self, sMsg, sVarMsg = '' ):
    pass

  def error( self, sMsg, sVarMsg = '' ):
    print sMsg
  
  def exception( self, sMsg = "", sVarMsg = '', lException = False, lExcInfo = False ):
    print sMsg
    print lException
  
gLogger = Logger()


class Config:
  
  def __init__(self):
    pass
  
  def addListenerToNewVersionEvent(self, a):
    pass

gConfig = Config()