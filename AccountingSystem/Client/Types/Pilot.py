# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Client/Types/Pilot.py,v 1.1 2008/07/17 17:43:15 acasajus Exp $
__RCSID__ = "$Id: Pilot.py,v 1.1 2008/07/17 17:43:15 acasajus Exp $"

from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType
from DIRAC                                                  import gConfig

class Pilot( BaseAccountingType ):

  def __init__( self ):
    BaseAccountingType.__init__( self )
    self.definitionKeyFields = [ ( 'User', 'VARCHAR(32)' ),
                                 ( 'UserGroup', 'VARCHAR(32)' ),
                                 ( 'Site', 'VARCHAR(32)' ),
                                 ( 'GridCE', "VARCHAR(128)" ),
                                 ( 'GridMiddleware', 'VARCHAR(32)' ),
                                 ( 'GridResourceBroker', 'VARCHAR(128)' ),
                                 ( 'GridStatus', 'VARCHAR(32)' ),
                               ]
    self.definitionAccountingFields = [ ( 'Jobs', "INT" ),
                                      ]
    self.checkType()
