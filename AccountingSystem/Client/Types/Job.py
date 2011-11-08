# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType
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
    self.definitionAccountingFields = [ ( 'CPUTime', "INT UNSIGNED" ),
                                        ( 'NormCPUTime', "INT UNSIGNED" ),
                                        ( 'ExecTime', "INT UNSIGNED" ),
                                        ( 'InputDataSize', 'BIGINT UNSIGNED' ),
                                        ( 'OutputDataSize', 'BIGINT UNSIGNED' ),
                                        ( 'InputDataFiles', 'INT UNSIGNED' ),
                                        ( 'OutputDataFiles', 'INT UNSIGNED' ),
                                        ( 'DiskSpace', 'BIGINT UNSIGNED' ),
                                        ( 'InputSandBoxSize', 'BIGINT UNSIGNED' ),
                                        ( 'OutputSandBoxSize', 'BIGINT UNSIGNED' ),
                                        ( 'ProcessedEvents', 'INT UNSIGNED' )
                                      ]
    self.bucketsLength = [ ( 86400 * 8, 3600 ), #<1w+1d = 1h
                           ( 86400 * 35, 3600 * 4 ), #<35d = 4h
                           ( 86400 * 30 * 6, 86400 ), #<6m = 1d
                           ( 86400 * 365, 86400 * 2 ), #<1y = 2d
                           ( 86400 * 600, 604800 ), #>1y = 1w
                         ]

    self.checkType()
    #Fill the site
    self.setValueByKey( "Site", DIRAC.siteName() )
