# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/Client/Types/Job.py,v 1.2 2008/05/07 09:45:12 acasajus Exp $
__RCSID__ = "$Id: Job.py,v 1.2 2008/05/07 09:45:12 acasajus Exp $"

from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType
from DIRAC                                                  import gConfig

class Job( BaseAccountingType ):

  def __init__( self ):
    BaseAccountingType.__init__( self )
    self.definitionKeyFields = [ ( 'User', 'VARCHAR(32)' ),
                                 ( 'UserGroup', 'VARCHAR(32)' ),
                                 ( 'JobGroup', "VARCHAR(64)" ),
                                 ( 'JobType', 'VARCHAR(32)' ),
                                 ( 'JobClass', 'VARCHAR(32)' ),
                                 ( 'Site', 'VARCHAR(32)' ),
                                 ( 'ProccessingType', 'VARCHAR(32)' ),
                                 ( 'FinalMajorStatus', 'VARCHAR(32)' ),
                                 ( 'FinalMinorStatus', 'VARCHAR(64)' )
                               ]
    self.definitionAccountingFields = [ ( 'CPUTime', "INT" ),
                                        ( 'NormCPUTime', "INT" ),
                                        ( 'ExecTime', "INT" ),
                                        ( 'InputDataVolume', 'INT' ),
                                        ( 'OutputDataVolume', 'INT' ),
                                        ( 'InputDataFiles', 'INT' ),
                                        ( 'OutputDataFiles', 'INT' ),
                                        ( 'DiskSpace', 'INT' ),
                                        ( 'InputSandBoxVolume', 'INT' ),
                                        ( 'OutputSandBoxVolume', 'INT' ),
                                        ( 'ProcessedEvents', 'INT' )
                                      ]
    self.checkType()
    #Fill the site
    self.setValueByKey( "Site", gConfig.getValue( "/LocalSite/Site", "DIRAC.unknown.no" ) )