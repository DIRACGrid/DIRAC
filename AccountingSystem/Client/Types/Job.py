# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType
from DIRAC                                                  import gConfig
import DIRAC

class Job( BaseAccountingType ):

  def __init__( self ):
    BaseAccountingType.__init__( self )
    self.definitionKeyFields = [ ( 'User', 'VARCHAR(32)' ),
                                 ( 'UserGroup', 'VARCHAR(32)' ),
                                 ( 'JobGroup', "VARCHAR(64)" ),
                                 ( 'JobType', 'VARCHAR(32)' ),
                                 ( 'JobClass', 'VARCHAR(32)' ),
                                 ( 'ProcessingType', 'VARCHAR(32)' ),
                                 ( 'Site', 'VARCHAR(32)' ),
                                 ( 'FinalMajorStatus', 'VARCHAR(32)' ),
                                 ( 'FinalMinorStatus', 'VARCHAR(64)' )
                               ]
    self.definitionAccountingFields = [ ( 'CPUTime', "INT" ),
                                        ( 'NormCPUTime', "INT" ),
                                        ( 'ExecTime', "INT" ),
                                        ( 'InputDataSize', 'INT' ),
                                        ( 'OutputDataSize', 'INT' ),
                                        ( 'InputDataFiles', 'INT' ),
                                        ( 'OutputDataFiles', 'INT' ),
                                        ( 'DiskSpace', 'INT' ),
                                        ( 'InputSandBoxSize', 'INT' ),
                                        ( 'OutputSandBoxSize', 'INT' ),
                                        ( 'ProcessedEvents', 'INT' )
                                      ]
    self.checkType()
    #Fill the site
    self.setValueByKey( "Site", DIRAC.siteName() )