# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Client/Types/Attic/WMSJob.py,v 1.5 2008/01/29 15:34:03 acasajus Exp $
__RCSID__ = "$Id: WMSJob.py,v 1.5 2008/01/29 15:34:03 acasajus Exp $"

from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType

class WMSJob( BaseAccountingType ):

  def __init__( self ):
    BaseAccountingType.__init__( self )
    self.definitionKeyFields = [ ( 'Setup', "VARCHAR(32)" ),
                                 ( 'JobGroup', "VARCHAR(32)" ),
                                 ( 'JobType', 'VARCHAR(32)' ),
                                 ( 'JobClass', 'VARCHAR(32)' ),
                                 ( 'Site', 'VARCHAR(32)' ),
                                 ( 'ProcType', 'VARCHAR(32)' ),
                                 ( 'FinalDIRACState', 'VARCHAR(32)' ),
                                 ( 'FinalApplicationState', 'VARCHAR(64)' )
                               ]
    self.definitionAccountingFields = [ ( 'CPUTime', "INT" ),
                                        ( 'NormCPUTime', "INT" ),
                                        ( 'ExecTime', "INT" ),
                                        ( 'InputData', 'INT' ),
                                        ( 'OutputData', 'INT' ),
                                        ( 'DiskSpace', 'INT' ),
                                        ( 'InputSandBox', 'INT' ),
                                        ( 'OutputSandBox', 'INT' ),
                                        ( 'WMSStagingTime', 'INT' ),
                                        ( 'WMSMatchingTime', 'INT' )
                                      ]
    self.checkType()