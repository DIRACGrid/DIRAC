# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Client/Types/Attic/ProductionJob.py,v 1.1 2008/01/29 15:34:03 acasajus Exp $
__RCSID__ = "$Id: ProductionJob.py,v 1.1 2008/01/29 15:34:03 acasajus Exp $"

from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType

class ProductionJob( BaseAccountingType ):

  def __init__( self ):
    BaseAccountingType.__init__( self )
    self.definitionKeyFields = [ ( 'Setup', "VARCHAR(32)" ),
                                 ( 'ProductionID', "VARCHAR(32)" ),
                                 ( 'ProductionType', 'VARCHAR(32)' ),
                                 ( 'JobType', 'VARCHAR(32)' ),
                                 ( 'Site', 'VARCHAR(32)' ),
                                 ( 'User', 'VARCHAR(32)' ),
                                 ( 'FinalState', 'VARCHAR(32)' )
                               ]
    self.definitionAccountingFields = [ ( 'CPUTime', "INT" ),
                                        ( 'NormCPUTime', "INT" ),
                                        ( 'ExecTime', "INT" ),
                                        ( 'InputData', 'INT' ),
                                        ( 'OutputData', 'INT' ),
                                        ( 'InputEvents', 'INT' ),
                                        ( 'OutputEvents', 'INT' )
                                      ]
    self.checkType()