"""
Generic DIRAC configuration regarding the Policy System. Custom VO
configurations are in the corresponding VO-specific modules, this
module is used as a fallback only if custom configurations are not
provided by VO.
"""

from DIRAC.ResourceStatusSystem.Utilities import CS

gencfg            = CS.getGeneralConfig()
ValidRes          = gencfg['Resource']
ValidStatus       = gencfg['Status']
ValidPolicyResult = gencfg['PolicyResult'] + gencfg['Status']
ValidSiteType     = gencfg['SiteType']
ValidServiceType  = gencfg['ServiceType']
ValidResourceType = gencfg['ResourceType']
ValidService      = ValidServiceType
PolicyTypes       = gencfg['PolicyTypes']
