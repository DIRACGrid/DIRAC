################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id$"

""" 
  fake ResourceStatusDB class.
  Every function can simply return S_OK() (or nothing)
"""

import datetime

from DIRAC.ResourceStatusSystem.Utilities.mock import Mock

################################################################################

class ResourceStatusDB:

  def __init__(self, *args, **kwargs):
    self.db = Mock()
    self.db._update.return_value = {'OK': True, 'Value': []}
    self.db._query.return_value = {'OK': True, 'Value': ()}
    pass

  def getMonitoredsList(self, granularity, paramsList = None, siteName = None,
                        serviceName = None, resourceName = None, storageElementName = None,
                        status = None, siteType = None, resourceType = None,
                        serviceType = None, countries = None, gridSiteName = None):
    return [('NAME@X', 'Banned'), ('NAME@X', 'Active')]

  def getGridSitesList(self, paramsList = None, gridSiteName = None, gridTier = None):
    return [('NAME', 'T0'), ('NAME1', 'T1')]

  def getMonitoredsStatusWeb(self, granularity, selectDict, sortList, startItem, maxItems):
    if granularity in ('Resource', 'Resources'):
      return {'TotalRecords': 1,
              'ParameterNames': ['ResourceName', 'Status', 'SiteName', 'ResourceType', 'Country',
                                 'DateEffective', 'FormerStatus', 'Reason'],
              'Extras': None,
              'Records': [['grid0.fe.infn.it', 'Active', 'LCG.Ferrara.it', 'CE', 'it',
                           '2009-12-15 12:47:31', 'Banned', 'DT:None|PilotsEff:Good']]}
    else:
      return {'TotalRecords': 1,
              'ParameterNames': ['SiteName', 'Tier', 'GridType', 'Country', 'Status',
                                 'DateEffective', 'FormerStatus', 'Reason'],
              'Extras': None,
              'Records': [['LCG.Ferrara.it', 'T2', 'LCG', 'it', 'Active',
                           '2009-12-15 12:47:31', 'Banned', 'DT:None|PilotsEff:Good']]}

  def getMonitoredsHistory(self, granularity, paramsList = None, name = None):
    return []

  def setLastMonitoredCheckTime(self, granularity, name):
    pass

  def setMonitoredReason(self, granularity, name, reason, tokenOwner):
    pass

  def setSiteStatus(self, siteName, status, reason, tokenOwner):
    pass

  def addOrModifySite(self, siteName, siteType, gridSiteName,
                      status, reason, dateEffective, tokenOwner, dateEnd):
    pass

  def _addSiteRow(self, siteName, siteType, gridSiteName, status,
                  reason, dateCreated, dateEffective, dateEnd, tokenOwner):
    pass

  def _addSiteHistoryRow(self, siteName, status, reason, dateCreated, dateEffective, dateEnd, tokenOwner):
    pass

  def removeSite(self, siteName):
    pass

  def setResourceStatus(self, resourceName, status, reason, tokenOwner):
    pass

  def addOrModifyResource(self, resourceName, resourceType, serviceType, siteName, gridSiteName, status,
                          reason, dateEffective, tokenOwner, dateEnd):
    pass

  def _addResourcesRow(self, resourceName, resourceType, serviceType, siteName, gridSiteName, status,
                       reason, dateCreated, dateEffective, dateEnd, tokenOwner):
    pass

  def _addResourcesHistoryRow(self, resourceName, status, reason,
                              dateCreated, dateEffective, dateEnd, tokenOwner):
    pass

  def addType(self, granularity, type, description=''):
    pass

  def removeResource(self, resourceName = None, serviceName = None, siteName = None):
    pass

  def setServiceStatus(self, serviceName, status, reason, tokenOwner):
    pass

  def addOrModifyService(self, serviceName, serviceType, siteName, status, reason,
                         dateEffective, tokenOwner, dateEnd):
    pass

  def _addServiceRow(self, serviceName, serviceType, siteName, status, reason,
                     dateCreated, dateEffective, dateEnd, tokenOwner):
    pass

  def _addServiceHistoryRow(self, serviceName, siteName, status, reason, dateCreated,
                            dateEffective, dateEnd, tokenOwner):
    pass

  def removeService(self, serviceName = None, siteName = None):
    pass

  def setMonitoredToBeChecked(self, monitored, granularity, name):
    pass

  def getResourceStats(self, granularity, name):
    return []

  def setStorageElementStatus(self, storageElementName, status, reason, tokenOwner, access):
    pass

  def addOrModifyStorageElement(self, storageElementName, resourceName, siteName,
                                status, reason, dateEffective, tokenOwner, dateEnd, access):
    pass

  def _addStorageElementRow(self, storageElementName, resourceName, siteName, status,
                            reason, dateCreated, dateEffective, dateEnd, tokenOwner, access):
    pass

  def _addStorageElementHistoryRow(self, storageElementName, resourceName, siteName,
                                    status, reason, dateCreated, dateEffective, dateEnd, tokenOwner, access):
    pass

  def removeStorageElement(self, storageElementName = None, resourceName = None, siteName = None, access = None):
    pass

  def removeRow(self, granularity, name, dateEffective):
    pass

  def getTypesList(self, granularity, type=None):
    return []

  def removeType(self, granularity, type):
    pass

  def getStatusList(self):
    return []

  def getGeneralName(self, name, from_g, to_g):
    return ['LCG.PIC.es', 'LCG.PIC-test.es']

  def getEndings(self, table):
    return []

  def getPeriods(self, granularity, name, status, hours = None, days = None):
    return []

  def getTablesWithHistory(self):
    a = ['Sites']
    return a

  def getServiceStats(self, siteName):
    return {}

  def getStorageElementsStats(self, granularity, name, access):
    return {}

  def addOrModifyPolicyRes(self, granularity, name, policyName,
                           status, reason, dateEffective = None):
    pass

  def addOrModifyGridSite(self, name, tier):
    pass

  def getGridSite(self, name):
    return {}

  def getPolicyRes(self, name, policyName, lastCheckTime = False):
    return ('Active', 'DT:None')

  def addOrModifyAccountingCacheRes(self, name, plotType, plotName, result, dateEffective = None):
    pass

  def addOrModifyClientsCacheRes(self, name, commandName, value, result,
                                 opt_ID = None, dateEffective = None):
    pass

  def getAccountingCacheStuff(self, paramsList = None, acID = None, name = None, plotType = None,
                              plotName = None, result = None, dateEffective = None,
                              lastCheckTime = None):
    return ((), ())

  def getClientsCacheStuff(self,  paramsList = None, ccID = None, name = None, commandName = None,
                           opt_ID = None, value = None, result = None, dateEffective = None,
                           lastCheckTime = None):
    return (('LCG.Liverpool.uk', '33744G0 UKI-NORTHGRID-LIV-HEP', 'StartDate', '2010-09-28 06:00', 'DTEverySites'),
            ('LCG.Liverpool.uk', '33744G0 UKI-NORTHGRID-LIV-HEP', 'EndDate', '2010-09-30 00:00', 'DTEverySites'),
            ('LCG.Liverpool.uk', '33744G0 UKI-NORTHGRID-LIV-HEP', 'Description', 'Reinstall DPM Storage nodes to SL5, improve response of lcg-CE headnode.', 'DTEverySites'),
            ('LCG.Liverpool.uk', '33744G0 UKI-NORTHGRID-LIV-HEP', 'Link', 'https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&object_id=21118&grid_id=0', 'DTEverySites'),
            ('LCG.Liverpool.uk', '33744G0 UKI-NORTHGRID-LIV-HEP', 'Severity', 'OUTAGE', 'DTEverySites'),
            ('LCG.NIPNE-11.ro', '33859G0 RO-11-NIPNE', 'StartDate', '2010-09-25 11:02', 'DTEverySites'),
            ('LCG.NIPNE-11.ro', '33859G0 RO-11-NIPNE', 'EndDate', '2010-10-07 10:02', 'DTEverySites'),
            ('LCG.NIPNE-11.ro', '33859G0 RO-11-NIPNE', 'Description', 'Uptating software on all nodes, Upgrading the hardware on all nodes, installing new CE and SE.', 'DTEverySites'),
            ('LCG.NIPNE-11.ro', '33859G0 RO-11-NIPNE', 'Link', 'https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&object_id=21176&grid_id=0', 'DTEverySites'),
            ('LCG.NIPNE-11.ro', '33859G0 RO-11-NIPNE', 'Severity', 'OUTAGE', 'DTEverySites'),
            ('LCG.Dortmund.de', '33153G0 UNI-DORTMUND', 'StartDate', '2010-10-01 00:00', 'DTEverySites'),
            ('LCG.Dortmund.de', '33153G0 UNI-DORTMUND', 'EndDate', '2010-10-04 23:59', 'DTEverySites'),
            ('LCG.Dortmund.de', '33153G0 UNI-DORTMUND', 'Description', 'Maintenance of the cooling system.', 'DTEverySites'),
            ('LCG.Dortmund.de', '33153G0 UNI-DORTMUND', 'Link', 'https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&object_id=20779&grid_id=0', 'DTEverySites'),
            ('LCG.Dortmund.de', '33153G0 UNI-DORTMUND', 'Severity', 'OUTAGE', 'DTEverySites'),
            ('LCG.CBPF.br', '34010G0 CBPF', 'StartDate', '2010-09-28 16:00', 'DTEverySites'),
            ('LCG.CBPF.br', '34010G0 CBPF', 'EndDate', '2010-09-29 16:00', 'DTEverySites'),
            ('LCG.CBPF.br', '34010G0 CBPF', 'Description', 'Waiting for ROC_LA Nagios to be fixed', 'DTEverySites'),
            ('LCG.CBPF.br', '34010G0 CBPF', 'Link', 'https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&object_id=21226&grid_id=0', 'DTEverySites'),
            ('LCG.CBPF.br', '34010G0 CBPF', 'Severity', 'OUTAGE', 'DTEverySites'),
            ('LCG.WARSAW.pl', '34060G0 WARSAW-EGEE', 'StartDate', '2010-09-29 08:15', 'DTEverySites'),
            ('LCG.WARSAW.pl', '34060G0 WARSAW-EGEE', 'EndDate', '2010-10-04 20:00', 'DTEverySites'),
            ('LCG.WARSAW.pl', '34060G0 WARSAW-EGEE', 'Description', 'Air conditioning repair', 'DTEverySites'),
            ('LCG.WARSAW.pl', '34060G0 WARSAW-EGEE', 'Link', 'https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&object_id=21238&grid_id=0', 'DTEverySites'),
            ('LCG.WARSAW.pl', '34060G0 WARSAW-EGEE', 'Severity', 'OUTAGE', 'DTEverySites'),
            ('LCG.RAL.uk', '34046G0 RAL-LCG2', 'StartDate', '2010-10-02 07:00', 'DTEverySites'),
            ('LCG.RAL.uk', '34046G0 RAL-LCG2', 'EndDate', '2010-10-04 07:30', 'DTEverySites'),
            ('LCG.RAL.uk', '34046G0 RAL-LCG2', 'Description', 'Planned power outage in building hosting networking equipment. All necessary equipment has alternative power but declare as At Risk during this period.', 'DTEverySites'),
            ('LCG.RAL.uk', '34046G0 RAL-LCG2', 'Link', 'https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&object_id=21240&grid_id=0', 'DTEverySites'),
            ('LCG.RAL.uk', '34046G0 RAL-LCG2', 'Severity', 'AT_RISK', 'DTEverySites'),
            ('LCG.ITWM.de', '34147G0 ITWM', 'StartDate', '2010-10-02 06:00', 'DTEverySites'),
            ('LCG.ITWM.de', '34147G0 ITWM', 'EndDate', '2010-10-02 16:00', 'DTEverySites'),
            ('LCG.ITWM.de', '34147G0 ITWM', 'Description', 'UPS tests', 'DTEverySites'),
            ('LCG.ITWM.de', '34147G0 ITWM', 'Link', 'https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&object_id=21287&grid_id=0', 'DTEverySites'),
            ('LCG.ITWM.de', '34147G0 ITWM', 'Severity', 'AT_RISK', 'DTEverySites'),
            ('LCG.UKI-LT2-QMUL.uk', '34207G0 UKI-LT2-QMUL', 'StartDate', '2010-09-30 10:23', 'DTEverySites'),
            ('LCG.UKI-LT2-QMUL.uk', '34207G0 UKI-LT2-QMUL', 'EndDate', '2010-09-30 11:00', 'DTEverySites'),
            ('LCG.UKI-LT2-QMUL.uk', '34207G0 UKI-LT2-QMUL', 'Description', 'Reboot of NAT and se03 into new kernel.', 'DTEverySites'),
            ('LCG.UKI-LT2-QMUL.uk', '34207G0 UKI-LT2-QMUL', 'Link', 'https://next.gocdb.eu/portal/index.php?Page_Type=View_Object&object_id=21326&grid_id=0', 'DTEverySites'),
            ('LCG.UKI-LT2-QMUL.uk', '34207G0 UKI-LT2-QMUL', 'Severity', 'AT_RISK', 'DTEverySites'))

  def transact2History(self, *args):
    pass

  def setDateEnd(self, granularity, name, dateEffective):
    pass

  def addStatus(self, status, description=''):
    pass

  def removeStatus(self, status):
    pass

  def getCountries(self, countries):
    return (('it', ), ('ch', ))

  def unique(self, table, ID):
    pass

  def syncWithCS(self, a, b):
    pass

  def getStuffToCheck(self, granularity, checkFrequency = None, maxN = None, name = None):
    return []

  def getTokens(self, granularity, name = None, dateExpiration = None):
    return [('LCG.Ferrara.it', 'RS_SVC', datetime.datetime(9998, 12, 31, 23, 59, 59))]

  def setToken(self, granularity, name, newTokenOwner, dateExpiration):
    pass

  def whatIs(self, name):
    return 'Site'

  def rankRes(self, granularity, days, startingDate = None):
    pass

  def __convertTime(self, t):
    pass

  def getGridSiteName(self, granularity, name):
    return 'IN2P3-CPPM'

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
################################################################################

'''
  HOW DOES THIS WORK.
    
    will come soon...
'''
            
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF