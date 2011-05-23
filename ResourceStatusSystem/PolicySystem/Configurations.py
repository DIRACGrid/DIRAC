"""
Generic DIRAC configuration regarding the Policy System. Custom VO
configurations are in the corresponding VO-specific modules, this
module is used as a fallback only if custom configurations are not
provided by VO.
"""

#############################################################################
# general parameters
#############################################################################

ValidRes = ['Site', 'Service', 'Resource', 'StorageElement']
ValidStatus = ['Banned', 'Probing', 'Bad', 'Active']
ValidPolicyResult = ['Error', 'Unknown', 'NeedConfirmation'] + ValidStatus
PolicyTypes = ['Resource_PolType', 'Alarm_PolType', 'Collective_PolType', 'RealBan_PolType']
ValidSiteType = ['T0', 'T1', 'T2', 'T3']
ValidResourceType = ['CE', 'CREAMCE', 'SE', 'LFC_C', 'LFC_L', 'FTS', 'VOMS']
ValidServiceType = ['Computing', 'Storage', 'VO-BOX', 'VOMS']
ValidService = ValidServiceType
#############################################################################
