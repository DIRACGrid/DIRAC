""" Just listing the possible Properties
This module contains list of Properties that can be assigned to users and groups
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"


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
