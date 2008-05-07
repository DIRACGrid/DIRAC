# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Client/Types/DataOperation.py,v 1.2 2008/05/07 17:31:22 acasajus Exp $
__RCSID__ = "$Id: DataOperation.py,v 1.2 2008/05/07 17:31:22 acasajus Exp $"

from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType
from DIRAC.Core.Utilities import Network

class DataOperation( BaseAccountingType ):

  def __init__( self ):
    BaseAccountingType.__init__( self )
    self.definitionKeyFields = [ ( 'OperationType' , "VARCHAR(32)" ),
                                 ( 'User', "VARCHAR(32)" ),
                                 ( 'ExecutionSite', 'VARCHAR(32)' ),
                                 ( 'Source', 'VARCHAR(32)' ),
                                 ( 'Destination', 'VARCHAR(32)' ),
                                 ( 'Protocol', 'VARCHAR(32)' ),
                                 ( 'FinalStatus', 'VARCHAR(32)' )
                               ]
    self.definitionAccountingFields = [ ( 'TransferSize', 'BIGINT' ),
                                        ( 'TransferTime', 'FLOAT' ),
                                        ( 'RegistrationTime', 'FLOAT' ),
                                        ( 'TransferOK', 'INT' ),
                                        ( 'TransferTotal', 'INT' ),
                                        ( 'RegistrationOK', 'INT' ),
                                        ( 'RegistrationTotal', 'INT' )
                                      ]
    self.bucketsLength = [ ( 172800, 900 ), #<2d = 15m
                       ( 15552000, 86400 ), #>2d <6m = 1d
                       ( 31104000, 604800 ), #>6m = 1w
                     ]
    self.checkType()
    self.setValueByKey( 'ExecutionSite', Network.getFQDN() )
