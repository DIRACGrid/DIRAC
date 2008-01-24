# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Client/Types/Attic/WMSJob.py,v 1.3 2008/01/24 11:03:33 acasajus Exp $
__RCSID__ = "$Id: WMSJob.py,v 1.3 2008/01/24 11:03:33 acasajus Exp $"

from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType

class WMSJob( BaseAccountingType ):

  def __init__( self ):
    BaseAccountingType.__init__( self )
    self.keyFieldsList = [ 'k1', 'k2' ]
    self.valueFieldsList = [ 'v1', 'v2', 'v3' ]
    self.checkType()