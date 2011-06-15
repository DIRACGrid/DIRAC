"""
Backward compatibility. This module will probably will be removed in
the future.
"""

from DIRAC.ResourceStatusSystem.Utilities import CS

gencfg            = CS.getTypedDict("GeneralConfig")

ValidRes          = gencfg['Resource']
ValidStatus       = gencfg['Status']
ValidPolicyResult = gencfg['PolicyResult'] + gencfg['Status']
ValidSiteType     = gencfg['SiteType']
ValidServiceType  = gencfg['ServiceType']
ValidResourceType = gencfg['ResourceType']
ValidService      = ValidServiceType
PolicyTypes       = gencfg['PolicyTypes']
