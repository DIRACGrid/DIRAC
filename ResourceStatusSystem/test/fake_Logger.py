""" fake gLogger 
    Every function can simply return S_OK() (or nothing)
"""

#import sys
#import DIRAC.ResourceStatusSystem.test.fake_Logger

from DIRAC import S_OK, S_ERROR
#sys.modules["DIRAC.Interfaces.API.DiracAdmin"] = "."

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
