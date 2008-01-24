# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Client/Types/Attic/TestType.py,v 1.1 2008/01/24 18:50:01 acasajus Exp $
__RCSID__ = "$Id: TestType.py,v 1.1 2008/01/24 18:50:01 acasajus Exp $"

from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType

class TestType( BaseAccountingType ):

  def __init__( self ):
    BaseAccountingType.__init__( self )
    self.definitionKeyFields = [ ( 'k1', "VARCHAR(265)" ),
                                 ( 'k2', "VARCHAR(10)" )
                               ]
    self.definitionAccountingFields = [ ( 'v1', "FLOAT" ),
                                        ( 'v2', "DOUBLE" ),
                                        ( 'v3', "TINYINT" )
                                      ]
    self.checkType()