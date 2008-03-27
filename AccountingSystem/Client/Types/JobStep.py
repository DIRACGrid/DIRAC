# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Client/Types/JobStep.py,v 1.2 2008/03/27 19:02:06 acasajus Exp $
__RCSID__ = "$Id: JobStep.py,v 1.2 2008/03/27 19:02:06 acasajus Exp $"

from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType

class JobStep( BaseAccountingType ):

  def __init__( self ):
    BaseAccountingType.__init__( self )
    self.definitionKeyFields = [ ( 'Setup', "VARCHAR(32)" ),
                                 ( 'User', "VARCHAR(32)" ),
                                 ( 'Group', 'VARCHAR(32)' ),
                                 ( 'JobGroup', 'VARCHAR(32)' ),
                                 ( 'Site', 'VARCHAR(32)' ),
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