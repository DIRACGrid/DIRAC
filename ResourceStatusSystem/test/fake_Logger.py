""" fake gLogger 
    Every function can simply return S_OK() (or nothing)
"""

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

