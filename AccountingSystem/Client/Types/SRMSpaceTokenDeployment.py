# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Client/Types/SRMSpaceTokenDeployment.py,v 1.1 2008/09/16 13:52:06 acasajus Exp $
__RCSID__ = "$Id: SRMSpaceTokenDeployment.py,v 1.1 2008/09/16 13:52:06 acasajus Exp $"

from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType
from DIRAC import gConfig

class SRMSpaceTokenDeployment( BaseAccountingType ):

  def __init__( self ):
    BaseAccountingType.__init__( self )
    self.definitionKeyFields = [ ( 'Site' , "VARCHAR(64)" ),
                                 ( 'Hostname', "VARCHAR(80)" ),
                                 ( 'SpaceTokenDesc', 'VARCHAR(64)' )
                               ]
    self.definitionAccountingFields = [ ( 'AvailableSpace', 'BIGINT' ),
                                        ( 'UsedSpace', 'BIGINT' ),
                                        ( 'TotalOnline', 'BIGINT' ),
                                        ( 'UsedOnline', 'BIGINT' ),
                                        ( 'FreeOnline', 'BIGINT' ),
                                        ( 'TotalNearline', 'BIGINT' ),
                                        ( 'UsedNearline', 'BIGINT' ),
                                        ( 'FreeNearline', 'BIGINT' ),
                                        ( 'ReservedNearline', 'BIGINT' )
                                      ]
    self.bucketsLength = [ ( 15552000, 86400 ), #<6m = 1d
                           ( 31104000, 604800 ), #>6m = 1w
                         ]
    self.checkType()
