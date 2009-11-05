# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType

class JobStep( BaseAccountingType ):

  def __init__( self ):
    BaseAccountingType.__init__( self )
    self.definitionKeyFields = [ ( 'User', "VARCHAR(32)" ),
                                 ( 'Group', 'VARCHAR(32)' ),
                                 ( 'JobGroup', 'VARCHAR(32)' ),
                                 ( 'EventType', 'VARCHAR(32)' ),
                                 ( 'ApplicationName', 'VARCHAR(32)' ),
                                 ( 'ApplicationVersion', 'VARCHAR(32)' ),
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