""" fake ResourceStatusDB class.
    Every function can simply return S_OK() (or nothing)
"""

import datetime

from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC import S_OK


#############################################################################

class RSSDBException(RSSException):
  pass

#############################################################################

class NotAllowedDate(RSSException):
  pass

#############################################################################

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


#    return (('LCG.SPACI-LECCE.it', 78805473L, 'StartDate', '2010-06-21 09:24', 'DTEverySites'),
#            ('LCG.SPACI-LECCE.it', 78805473L, 'Description', 'Server Room maintenance', 'DTEverySites'),
#            ('LCG.SPACI-LECCE.it', 78805473L, 'EndDate', '2010-07-14 09:24', 'DTEverySites'),
#            ('LCG.SPACI-LECCE.it', 78805473L, 'Severity', 'OUTAGE', 'DTEverySites'),
#            ('LCG.Lancashire.uk', 78805481L, 'StartDate', '2010-06-28 07:30', 'DTEverySites'),
#            ('LCG.Lancashire.uk', 78805481L, 'Description', 'Site will be at risk', 'DTEverySites'),
#            ('LCG.Lancashire.uk', 78805481L, 'EndDate', '2010-06-28 18:30', 'DTEverySites'),
#            ('LCG.Lancashire.uk', 78805481L, 'Severity', 'AT_RISK', 'DTEverySites'),
#            ('LCG.ESA-ESRIN.it', 78805480L, 'StartDate', '2010-06-27 20:20', 'DTEverySites'),
#            ('LCG.ESA-ESRIN.it', 78805480L, 'Description', 'Waiting to establish a new INFN RA.', 'DTEverySites'),
#            ('LCG.ESA-ESRIN.it', 78805480L, 'EndDate', '2010-07-05 15:30', 'DTEverySites'),
#            ('LCG.ESA-ESRIN.it', 78805480L, 'Severity', 'OUTAGE', 'DTEverySites'),
#            ('LCG.RAL.uk', 78705450L, 'StartDate', '2010-06-28 07:30', 'DTEverySites'),
#            ('LCG.RAL.uk', 78705450L, 'Description', 'At Risk for site during maintenance work ', 'DTEverySites'),
#            ('LCG.RAL.uk', 78705450L, 'EndDate', '2010-06-30 16:00', 'DTEverySites'),
#            ('LCG.RAL.uk', 78705450L, 'Severity', 'AT_RISK', 'DTEverySites'),
#            ('LCG.WCSS.pl', 79605440L, 'StartDate', '2010-06-29 06:00', 'DTEverySites'),
#            ('LCG.WCSS.pl', 79605440L, 'Description', 'Power configuration change', 'DTEverySites'),
#            ('LCG.WCSS.pl', 79605440L, 'EndDate', '2010-06-30 16:00', 'DTEverySites'),
#            ('LCG.WCSS.pl', 79605440L, 'Severity', 'OUTAGE', 'DTEverySites'),
#            ('LCG.Ferrara.it', 79655445L, 'StartDate', '2010-06-28 12:00', 'DTEverySites'),
#            ('LCG.Ferrara.it', 79655445L, 'Description', 'software update', 'DTEverySites'),
#            ('LCG.Ferrara.it', 79655445L, 'EndDate', '2010-06-28 16:00', 'DTEverySites'),
#            ('LCG.Ferrara.it', 79655445L, 'Severity', 'AT_RISK', 'DTEverySites'),
#            ('LCG.RUG.nl', 79055443L, 'StartDate', '2010-07-01 15:00', 'DTEverySites'),
#            ('LCG.RUG.nl', 79055443L, 'Description', 'Maintenance', 'DTEverySites'),
#            ('LCG.RUG.nl', 79055443L, 'EndDate', '2010-07-02 10:00', 'DTEverySites'),
#            ('LCG.RUG.nl', 79055443L, 'Severity', 'OUTAGE', 'DTEverySites'),
#            ('LCG.ESA-ESRIN.it', 79905437L, 'StartDate', '2010-06-27 20:20', 'DTEverySites'),
#            ('LCG.ESA-ESRIN.it', 79905437L, 'Description', 'Waiting to establish a new INFN RA at ESRIN.', 'DTEverySites'),
#            ('LCG.ESA-ESRIN.it', 79905437L, 'EndDate', '2010-07-05 15:30', 'DTEverySites'),
#            ('LCG.ESA-ESRIN.it', 79905437L, 'Severity', 'OUTAGE', 'DTEverySites'),
#            ('LCG.Pisa.it', 79955437L, 'StartDate', '2010-06-30 08:00', 'DTEverySites'),
#            ('LCG.Pisa.it', 79955437L, 'Description', 'hardware reconfiguration of the networking', 'DTEverySites'),
#            ('LCG.Pisa.it', 79955437L, 'EndDate', '2010-07-01 18:00', 'DTEverySites'),
#            ('LCG.Pisa.it', 79955437L, 'Severity', 'OUTAGE', 'DTEverySites'))

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

#  def getDownTimesWeb(self, selectDict, sortList, startItem, maxItems):
#    return (('LCG.SPACI-LECCE.it', 78805473L, 'StartDate', '2010-06-21 09:24', 'DTEverySites'), ('LCG.SPACI-LECCE.it', 78805473L, 'Description', 'Server Room maintenance', 'DTEverySites'), ('LCG.SPACI-LECCE.it', 78805473L, 'EndDate', '2010-07-14 09:24', 'DTEverySites'), ('LCG.SPACI-LECCE.it', 78805473L, 'Severity', 'OUTAGE', 'DTEverySites'), ('LCG.Lancashire.uk', 78805481L, 'StartDate', '2010-06-28 07:30', 'DTEverySites'), ('LCG.Lancashire.uk', 78805481L, 'Description', 'Site will be at risk while a portion of our compute and storage servers are moved from their temporary home to their new machine', 'DTEverySites'), ('LCG.Lancashire.uk', 78805481L, 'EndDate', '2010-06-28 18:30', 'DTEverySites'), ('LCG.Lancashire.uk', 78805481L, 'Severity', 'AT_RISK', 'DTEverySites'), ('LCG.ESA-ESRIN.it', 78805480L, 'StartDate', '2010-06-27 20:20', 'DTEverySites'), ('LCG.ESA-ESRIN.it', 78805480L, 'Description', 'Waiting to establish a new INFN RA at ESRIN.', 'DTEverySites'), ('LCG.ESA-ESRIN.it', 78805480L, 'EndDate', '2010-07-05 15:30', 'DTEverySites'), ('LCG.ESA-ESRIN.it', 78805480L, 'Severity', 'OUTAGE', 'DTEverySites'), ('LCG.RAL.uk', 78705450L, 'StartDate', '2010-06-28 07:30', 'DTEverySites'), ('LCG.RAL.uk', 78705450L, 'Description', 'At Risk for site during maintenance work on electrical supply (transformers).', 'DTEverySites'), ('LCG.RAL.uk', 78705450L, 'EndDate', '2010-06-30 16:00', 'DTEverySites'), ('LCG.RAL.uk', 78705450L, 'Severity', 'AT_RISK', 'DTEverySites'), ('LCG.WCSS.pl', 79605440L, 'StartDate', '2010-06-29 06:00', 'DTEverySites'), ('LCG.WCSS.pl', 79605440L, 'Description', 'Power configuration change', 'DTEverySites'), ('LCG.WCSS.pl', 79605440L, 'EndDate', '2010-06-30 16:00', 'DTEverySites'), ('LCG.WCSS.pl', 79605440L, 'Severity', 'OUTAGE', 'DTEverySites'), ('LCG.Ferrara.it', 79655445L, 'StartDate', '2010-06-28 12:00', 'DTEverySites'), ('LCG.Ferrara.it', 79655445L, 'Description', 'software update', 'DTEverySites'), ('LCG.Ferrara.it', 79655445L, 'EndDate', '2010-06-28 16:00', 'DTEverySites'), ('LCG.Ferrara.it', 79655445L, 'Severity', 'AT_RISK', 'DTEverySites'), ('LCG.RUG.nl', 79055443L, 'StartDate', '2010-07-01 15:00', 'DTEverySites'), ('LCG.RUG.nl', 79055443L, 'Description', 'Maintenance on the network infrastructure of our compute floor. This means that the whole cluster will be offline.', 'DTEverySites'), ('LCG.RUG.nl', 79055443L, 'EndDate', '2010-07-02 10:00', 'DTEverySites'), ('LCG.RUG.nl', 79055443L, 'Severity', 'OUTAGE', 'DTEverySites'), ('LCG.ESA-ESRIN.it', 79905437L, 'StartDate', '2010-06-27 20:20', 'DTEverySites'), ('LCG.ESA-ESRIN.it', 79905437L, 'Description', 'Waiting to establish a new INFN RA at ESRIN.', 'DTEverySites'), ('LCG.ESA-ESRIN.it', 79905437L, 'EndDate', '2010-07-05 15:30', 'DTEverySites'), ('LCG.ESA-ESRIN.it', 79905437L, 'Severity', 'OUTAGE', 'DTEverySites'), ('LCG.Pisa.it', 79955437L, 'StartDate', '2010-06-30 08:00', 'DTEverySites'), ('LCG.Pisa.it', 79955437L, 'Description', 'hardware reconfiguration of the networking', 'DTEverySites'), ('LCG.Pisa.it', 79955437L, 'EndDate', '2010-07-01 18:00', 'DTEverySites'), ('LCG.Pisa.it', 79955437L, 'Severity', 'OUTAGE', 'DTEverySites'))


#return {'TotalRecords': 4,
#            'ParameterNames': ['Granularity', 'Name', 'Severity', 'When'],
#            'Extras': None,
#            'Records': [['Site', 'LCG.NIPNE-15.ro', 'OUTAGE', 'Ongoing'],
#                        ['Resource', 'ce124.cern.ch', 'OUTAGE', 'in 0 hours'],
#                        ['Site', 'LCG.Poznan.pl', 'OUTAGE', 'Ongoing'],
#                        ['Site', 'LCG.KIAE.ru', 'OUTAGE', 'Ongoing']]}

  def __convertTime(self, t):
    pass

  def getGridSiteName(self, granularity, name):
    return 'IN2P3-CPPM'
