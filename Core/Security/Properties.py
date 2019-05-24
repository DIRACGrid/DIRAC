""" Just listing the possible Properties
This module contains list of Properties that can be assigned to users and groups

In order to add extension specific Properties, you need to create in your extension the file
`Core/Security/Properties.py`, which will contain the following dictionary:

  * extra_properties: keys are the Properties variable name, values are their string description


Example of extension file :

    extra_properties = { 'VO_FC_MANAGEMENT' : 'VoFcManagement' }

"""

__RCSID__ = "$Id$"

import imp
import sys


#: A host property. This property is used::
#: * For a host to forward credentials in a DISET call
TRUSTED_HOST = "TrustedHost"

#: Normal user operations
NORMAL_USER = "NormalUser"

#: CS Administrator - possibility to edit the Configuration Service
CS_ADMINISTRATOR = "CSAdministrator"

#: Job sharing among members of a group
JOB_SHARING = "JobSharing"

#: DIRAC Service Administrator
SERVICE_ADMINISTRATOR = "ServiceAdministrator"

#: Job Administrator can manipulate everybody's jobs
JOB_ADMINISTRATOR = "JobAdministrator"

#: Job Monitor - can get job monitoring information
JOB_MONITOR = "JobMonitor"

#: Private pilot
PILOT = "Pilot"

#: Generic pilot
GENERIC_PILOT = "GenericPilot"

#: Site Manager
SITE_MANAGER = "SiteManager"

#: User, group, VO Registry management
USER_MANAGER = 'UserManager'

#: Operator
OPERATOR = "Operator"

#: Allow getting full delegated proxies
FULL_DELEGATION = "FullDelegation"

#: Allow getting only limited proxies (ie. pilots)
LIMITED_DELEGATION = "LimitedDelegation"

#: Allow getting only limited proxies for one self
PRIVATE_LIMITED_DELEGATION = "PrivateLimitedDelegation"

#: Allow managing proxies
PROXY_MANAGEMENT = "ProxyManagement"

#: Allow managing production
PRODUCTION_MANAGEMENT = "ProductionManagement"

#: Allow production request approval on behalf of PPG
PPG_AUTHORITY = "PPGAuthority"

#: Allow Bookkeeping Management
BOOKKEEPING_MANAGEMENT = "BookkeepingManagement"

#: Allow to set notifications and manage alarms
ALARMS_MANAGEMENT = "AlarmsManagement"

#: Allow FC Management - FC root user
FC_MANAGEMENT = "FileCatalogManagement"

#: Allow staging files
STAGE_ALLOWED = "StageAllowed"


def includeExtensionProperties():
  """ Merge all the Properties of all the extensions into these Properties
      Should be called only at the initialization of DIRAC, so by the parseCommandLine,
      dirac-agent.py, dirac-service.py, dirac-executor.py
  """

  def __recurseImport(modName, parentModule=None, fullName=False):
    """ Internal function to load modules
    """
    if isinstance(modName, basestring):
      modName = modName.split(".")
    if not fullName:
      fullName = ".".join(modName)
    try:
      if parentModule:
        impData = imp.find_module(modName[0], parentModule.__path__)
      else:
        impData = imp.find_module(modName[0])
      impModule = imp.load_module(modName[0], *impData)
      if impData[0]:
        impData[0].close()
    except ImportError:
      return None
    if len(modName) == 1:
      return impModule
    return __recurseImport(modName[1:], impModule, fullName=fullName)

  from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals
  allExtensions = CSGlobals.getCSExtensions()

  for extension in allExtensions:
    ext_properties = None
    try:

      ext_properties = __recurseImport('%sDIRAC.Core.Security.Properties' % extension)
      if ext_properties:

        # Name and value of the properties
        sys.modules[__name__].__dict__.update(ext_properties.extra_properties)

    except BaseException:
      pass
