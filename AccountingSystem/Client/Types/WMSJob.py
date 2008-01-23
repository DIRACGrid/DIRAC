
from DIRAC.AccountingSystem.Client.Types.BaseType import BaseType

class WMSJob( BaseType ):

  def __init__( self ):
    BaseType.__init__( self )
    self.keyFieldsList = [ 'k1', 'k2' ]
    self.valueFieldsList = [ 'v1', 'v2', 'v3' ]
    self.checkType()