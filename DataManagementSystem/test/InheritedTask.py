
from DIRAC.DataManagementSystem.private.RequestTask import RequestTask

class InheritedTask( RequestTask ):

  def __init__( self, *args, **kwargs ):
    RequestTask.__init__( self, args, *kwargs )
  
  def __call__( self ):
    self.always( "in call of %s" % str(self ) )
    self.addMark( "akey", 10 )
    time.sleep(1)
    return S_OK( self.monitor() )

