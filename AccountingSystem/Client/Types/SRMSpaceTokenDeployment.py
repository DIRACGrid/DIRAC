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
    self.definitionAccountingFields = [ ( 'AvailableSpace', 'UNSIGNED BIGINT' ),
                                        ( 'UsedSpace', 'UNSIGNED BIGINT' ),
                                        ( 'TotalOnline', 'UNSIGNED BIGINT' ),
                                        ( 'UsedOnline', 'UNSIGNED BIGINT' ),
                                        ( 'FreeOnline', 'UNSIGNED BIGINT' ),
                                        ( 'TotalNearline', 'UNSIGNED BIGINT' ),
                                        ( 'UsedNearline', 'UNSIGNED BIGINT' ),
                                        ( 'FreeNearline', 'UNSIGNED BIGINT' ),
                                        ( 'ReservedNearline', 'UNSIGNED BIGINT' )
                                      ]
    self.bucketsLength = [ ( 15552000, 86400 ), #<6m = 1d
                           ( 31104000, 604800 ), #>6m = 1w
                         ]
    self.checkType()
