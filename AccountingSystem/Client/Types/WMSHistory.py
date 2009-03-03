# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Client/Types/WMSHistory.py,v 1.3 2009/03/03 14:57:19 acasajus Exp $
__RCSID__ = "$Id: WMSHistory.py,v 1.3 2009/03/03 14:57:19 acasajus Exp $"

from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType

class WMSHistory( BaseAccountingType ):

  def __init__( self ):
    BaseAccountingType.__init__( self )
    self.definitionKeyFields = [ ( 'Status', "VARCHAR(256)" ),
                                 ( 'MinorStatus', 'VARCHAR(256)' ),
                                 ( 'ApplicationStatus', 'VARCHAR(256)' ),
                                 ( 'Site', 'VARCHAR(128)' ),
                                 ( 'User', 'VARCHAR(128)' ),
                                 ( 'UserGroup', 'VARCHAR(128)' ),
                                 ( 'JobGroup', 'VARCHAR(32)' ),
                                 ( 'JobSplitType', 'VARCHAR(32)' )
                               ]
    self.definitionAccountingFields = [ ( 'Jobs', "INT" ),
                                        ( 'Reschedules', "INT" ),
                                      ]
    self.bucketsLength = [ ( 86400*2, 900 ), #<2d = 15m
                           ( 604800, 3600 ), #<1w = 1h
                           ( 15552000, 86400 ), #>1w <6m = 1d
                           ( 31104000, 604800 ), #>6m = 1w
                         ]
    self.checkType()