"""
Backward compatibility. This module will probably will be removed in
the future.
"""

from DIRAC.ResourceStatusSystem.Utilities import CS

<<<<<<< HEAD
gencfg            = CS.getTypedDict("GeneralConfig")

ValidRes          = gencfg['Resource']
ValidStatus       = gencfg['Status']
ValidPolicyResult = gencfg['PolicyResult'] + gencfg['Status']
ValidSiteType     = gencfg['SiteType']
ValidServiceType  = gencfg['ServiceType']
ValidResourceType = gencfg['ResourceType']
ValidService      = ValidServiceType
PolicyTypes       = gencfg['PolicyTypes']
=======
ValidRes = ['Site', 'Service', 'Resource', 'StorageElementRead', 'StorageElementWrite']
ValidStatus = ['Banned', 'Probing', 'Bad', 'Active']
ValidPolicyResult = ['Error', 'Unknown', 'NeedConfirmation'] + ValidStatus
PolicyTypes = ['Resource_PolType', 'Alarm_PolType', 'Collective_PolType', 'RealBan_PolType']
ValidSiteType = ['T0', 'T1', 'T2', 'T3']
ValidResourceType = ['CE', 'CREAMCE', 'SE', 'LFC_C', 'LFC_L', 'FTS', 'VOMS']
ValidServiceType = ['Computing', 'Storage', 'VO-BOX', 'VOMS']
ValidService = ValidServiceType
#############################################################################
>>>>>>> ses
