

class BaseBackend:

    _backendName = "base"
    _showCallingFrame = False

    def getName( self ):
      return self._backendName

    def flush( self ):
      pass

    def doMessage( self ):
      raise Exception( "This function MUST be overloaded!!" )