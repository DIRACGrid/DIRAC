"""
Backward compatibility. This module will probably will be removed in
the future.
"""

from DIRAC.ResourceStatusSystem.Utilities import CS

gencfg            = CS.getTypedDictRootedAt( "GeneralConfig" )

ValidRes          = gencfg[ 'Resource' ]
ValidStatus       = gencfg[ 'Status' ]
ValidPolicyResult = gencfg[ 'PolicyResult' ] + gencfg[ 'Status' ]
ValidSiteType     = gencfg[ 'SiteType' ]
ValidServiceType  = gencfg[ 'ServiceType' ]
ValidResourceType = gencfg[ 'ResourceType' ]
ValidService      = ValidServiceType
PolicyTypes       = gencfg[ 'PolicyTypes' ]

CheckingFreqs     = CS.getTypedDictRootedAt("CheckingFreqs")

#############################################################################
# Clients cache
#############################################################################

Commands_ClientsCache = [
 ( 'ClientsCache_Command', 'JobsEffSimpleEveryOne_Command' ),
 ( 'ClientsCache_Command', 'PilotsEffSimpleEverySites_Command' ),
 #('ClientsCache_Command', 'TransferQualityEverySEs_Command'),
 ( 'ClientsCache_Command', 'DTEverySites_Command' ),
 ( 'ClientsCache_Command', 'DTEveryResources_Command' )
 ]

Commands_AccountingCache = [
 ( 'AccountingCache_Command', 'TransferQualityByDestSplitted_Command', ( 2, ), 'Always' ),
 ( 'AccountingCache_Command', 'FailedTransfersBySourceSplitted_Command', ( 2, ), 'Always' ),
 ( 'AccountingCache_Command', 'TransferQualityByDestSplittedSite_Command', ( 24, ), 'Hourly' ),
 #('AccountingCache_Command', 'TransferQualityBySourceSplittedSite_Command', (24, ), 'Hourly'),
 ( 'AccountingCache_Command', 'SuccessfullJobsBySiteSplitted_Command', ( 24, ), 'Hourly' ),
 ( 'AccountingCache_Command', 'FailedJobsBySiteSplitted_Command', ( 24, ), 'Hourly' ),
 ( 'AccountingCache_Command', 'SuccessfullPilotsBySiteSplitted_Command', ( 24, ), 'Hourly' ),
 ( 'AccountingCache_Command', 'FailedPilotsBySiteSplitted_Command', ( 24, ), 'Hourly' ),
 ( 'AccountingCache_Command', 'SuccessfullPilotsByCESplitted_Command', ( 24, ), 'Hourly' ),
 ( 'AccountingCache_Command', 'FailedPilotsByCESplitted_Command', ( 24, ), 'Hourly' ),
 ( 'AccountingCache_Command', 'RunningJobsBySiteSplitted_Command', ( 24, ), 'Hourly' ),
 ( 'AccountingCache_Command', 'RunningJobsBySiteSplitted_Command', ( 168, ), 'Hourly' ),
 ( 'AccountingCache_Command', 'RunningJobsBySiteSplitted_Command', ( 720, ), 'Daily' ),
 ( 'AccountingCache_Command', 'RunningJobsBySiteSplitted_Command', ( 8760, ), 'Daily' ),
 ]
