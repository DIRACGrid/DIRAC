# $HeadURL$
__RCSID__ = "$Id$"

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
