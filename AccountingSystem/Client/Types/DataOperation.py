# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType
from DIRAC import gConfig
import DIRAC

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
    self.definitionAccountingFields = [ ( 'TransferSize', 'UNSIGNED BIGINT' ),
                                        ( 'TransferTime', 'FLOAT' ),
                                        ( 'RegistrationTime', 'FLOAT' ),
                                        ( 'TransferOK', 'UNSIGNED INT' ),
                                        ( 'TransferTotal', 'UNSIGNED INT' ),
                                        ( 'RegistrationOK', 'UNSIGNED INT' ),
                                        ( 'RegistrationTotal', 'UNSIGNED INT' )
                                      ]
    self.bucketsLength = [ ( 172800, 900 ), #<2d = 15m
                           ( 604800, 3600 ), #<1w = 1h
                           ( 15552000, 86400 ), #>1w <6m = 1d
                           ( 31104000, 604800 ), #>6m = 1w
                         ]
    self.checkType()
    self.setValueByKey( 'ExecutionSite', DIRAC.siteName() )
