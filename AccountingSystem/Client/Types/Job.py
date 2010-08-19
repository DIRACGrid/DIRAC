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
    self.bucketsLength = [ ( 86400 * 7, 3600 ), #<1w = 1h
                           ( 86400 * 35, 3600 * 4 ), #<35d = 4h
                           ( 86400 * 30 * 6, 86400 ), #<6m = 1d
                           ( 86400 * 365, 86400 * 2 ), #<1y = 2d
                           ( 86400 * 600, 604800 ), #>1y = 1w
                         ]

    self.checkType()
    #Fill the site
    self.setValueByKey( "Site", DIRAC.siteName() )
