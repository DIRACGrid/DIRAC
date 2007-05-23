

class BaseBackend:

    _showCallingFrame = False

    def __init__( self, cfgPath ):
      self.cfgPath = cfgPath

    def flush( self ):
      pass

    def doMessage( self ):
      raise Exception( "This function MUST be overloaded!!" )