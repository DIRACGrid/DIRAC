""" fake ResourceStatusDB class. 
    Every function can simply return S_OK() (or nothing)
"""

import datetime

from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC import S_OK


#############################################################################

class RSSManagementDBException(RSSException):
  pass

#############################################################################

class ResourceManagementDB:
  
  def __init__(self, *args, **kwargs):
    self.db = Mock()
    self.db._update.return_value = {'OK': True, 'Value': []}
    self.db._query.return_value = {'OK': True, 'Value': ()}
    pass

  def addOrModifyPolicyRes(self, granularity, name, policyName, 
                           status, reason, dateEffective = None):
    pass
  
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

  def addStatus(self, status, description=''):
    pass

  def removeStatus(self, status):
    pass

  def getStatusList(self):   
    return []