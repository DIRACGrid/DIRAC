""" fake RequestHandler class. Every function can simply return S_OK()
"""

from DIRAC import S_OK

class RequestHandler:
  
  def __init__( self, serviceInfoDict,
                transport,
                lockManager ):
    pass

  def initialize( self ):
    pass