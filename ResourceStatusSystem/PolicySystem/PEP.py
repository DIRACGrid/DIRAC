""" The PEP (Policy Enforcement Point) module contains what is necessary 
    for enforcing policies. Its class(es) are used for:

    1. invoke a PDP and collects results

    2. enforcing results by:

       a. saving result on a DB

       b. rasing alarms

       c. other....
"""

from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Policy import Configurations

#class PEPBadInput(Exception):
#  pass


class PEP: 
#############################################################################
  """ 
  PEP (Policy Enforcement Point) initialization
  
  :params:
    :attr:`granularity`: string - a ValidRes (optional)

    :attr:`name`: string - optional name (e.g. of a site)

    :attr:`status`: string - optional status

    :attr:`formerStatus`: string - optional former status

    :attr:`reason`: string - optional reason for last status change

    :attr:`futureEnforcement`: optional
      [ 
        { 
          'PolicyType': a PolicyType
          'Granularity': a ValidRes (optional) 
        } 
      ]
        
  """
 
  def __init__(self, granularity = None, name = None, status = None, formerStatus = None, 
               reason = None, siteType = None, serviceType = None, resourceType = None, 
               futureEnforcement = None):
#    policyType = presentEnforcement['PolicyType']
#    for pt in policyType:
#      if pt not in PolicyTypes :
#        raise InvalidPolicyType, where(self, self.__init__)
#    self.__policyType = policyType
    
    
    try:
#      granularity = presentEnforcement['Granularity']
      if granularity is not None:
        if granularity not in ValidRes:
          raise InvalidRes, where(self, self.__init__)
      self.__granularity = granularity
    except NameError:
      pass

    self.__name = name
    
    self.__status = status
    if self.__status is not None:
      if self.__status not in ValidStatus:
        raise InvalidStatus, where(self, self.__init__)
    
    self.__formerStatus = formerStatus
    if self.__formerStatus is not None:
      if self.__formerStatus not in ValidStatus:
        raise InvalidStatus, where(self, self.__init__)
    
    self.__reason = reason

    self.__siteType = siteType
    if self.__siteType is not None:
      if self.__siteType not in ValidSiteType:
        raise InvalidSiteType, where(self, self.__init__)
    
    self.__serviceType = serviceType
    if self.__serviceType is not None:
      if self.__serviceType not in ValidServiceType:
        raise InvalidServiceType, where(self, self.__init__)
    
    self.__resourceType = resourceType
    if self.__resourceType is not None:
      if self.__resourceType not in ValidResourceType:
        raise InvalidResourceType, where(self, self.__init__)

      
    if futureEnforcement is not None:
      try:
        futureGranularity = futureEnforcement['Granularity']
        if futureGranularity is not None:
          if futureGranularity not in ValidRes:
            raise InvalidRes, where(self, self.__init__)
        self.__futureGranularity = futureGranularity
      except NameError:
        pass
      
#############################################################################
    
  def enforce(self, pdpIn=None, rsDBIn=None, knownInfo=None):
    """ 
    enforce policies, using a PDP  (Policy Decision Point), based on

     self.__granularity (optional)
  
     self.__name (optional)
  
     self.__status (optional) 
  
     self.__formerStatus (optional)
  
     self.__reason (optional)
  
     self.__siteType (optional)
  
     self.__serviceType (optional)
  
     self.__resourceType (optional)
  
     self.__futurePolicyType (optional)
  
     self.__futureGranularity (optional)
     
     pdpIn: a custom PDP object (optional)
  
     rsDBIn: a custom database object (optional)
     
     knownInfo: a string of known provided information (optional)
    """

    if pdpIn is not None:
      pdp = pdpIn
    else:
      # Use standard DIRAC PDP
      from DIRAC.ResourceStatusSystem.PolicySystem.PDP import PDP
      pdp = PDP(granularity = self.__granularity, name = self.__name, status = self.__status, 
                formerStatus = self.__formerStatus, reason = self.__reason, 
                siteType = self.__siteType, serviceType = self.__serviceType, 
                resourceType = self.__resourceType)

    if rsDBIn is not None:
      rsDB = rsDBIn
    else:
      # Use standard DIRAC DB
      from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB
      rsDB = ResourceStatusDB()
    
    # policy decision
    resDecisions = pdp.takeDecision(knownInfo=knownInfo)
    
    for res in resDecisions:
      
      self.__policyType = res['PolicyType']

      if 'Resource_PolType' in self.__policyType:
      # Update the DB
        if self.__granularity == 'Site':
          if res['Action']:
            rsDB.setSiteStatus(self.__name, res['Status'], res['Reason'], 'RS_SVC')
            rsDB.setMonitoredToBeChecked(['Service', 'Resource', 'StorageElement'], 'Site', self.__name)
          else:
            rsDB.setMonitoredReason(self.__granularity, self.__name, res['Reason'], 'RS_SVC')
        
        elif self.__granularity == 'Service':
          if res['Action']:
            rsDB.setServiceStatus(self.__name, res['Status'], res['Reason'], 'RS_SVC')
          else:
            rsDB.setMonitoredReason(self.__granularity, self.__name, res['Reason'], 'RS_SVC')
    
        elif self.__granularity == 'Resource':
          if res['Action']:
            rsDB.setResourceStatus(self.__name, res['Status'], res['Reason'], 'RS_SVC')
            rsDB.setMonitoredToBeChecked(['Site', 'Service', 'StorageElement'], 'Resource', self.__name)
          else:
            rsDB.setMonitoredReason(self.__granularity, self.__name, res['Reason'], 'RS_SVC')

        elif self.__granularity == 'StorageElement':
          if res['Action']:
            rsDB.setStorageElementStatus(self.__name, res['Status'], res['Reason'], 'RS_SVC')
            rsDB.setMonitoredToBeChecked(['Site', 'Service', 'Resource'], 'StorageElement', self.__name)
          else:
            rsDB.setMonitoredReason(self.__granularity, self.__name, res['Reason'], 'RS_SVC')
    
        rsDB.setLastMonitoredCheckTime(self.__granularity, self.__name)
        
        if res.has_key('Enddate'):
          rsDB.setDateEnd(self.__granularity, self.__name, res['Enddate'])
  
        if res['Action']:
          try:
            if self.__futureGranularity != self.__granularity:
              self.__name = rsDB.getGeneralName(self.__name, self.__granularity, 
                                                self.__futureGranularity)
            newPEP = PEP(granularity = self.__futureGranularity, name = self.__name, 
                         status = self.__status, formerStatus = self.__formerStatus, 
                         reason = self.__reason)
            newPEP.enforce(pdpIn = pdp, rsDBIn = rsDB) 
          except AttributeError:
            pass
      
      if 'Alarm_PolType' in self.__policyType:
        # raise alarm, right now makes a simple notification
        
        if res['Action']:
          from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
          nc = NotificationClient()
          
          notif = "ResourceStatusSystem notification: "
          notif = notif + "%s %s is perceived as" %(self.__granularity, self.__name) 
          notif = notif + " %s. Reason: %s." %(res['Status'], res['Reason'])
          
          for user in Configurations.notified_users:
            nc.addNotificationForUser(user, notif)
          
      if 'Collective_PolType' in self.__policyType:
        # do something
        pass
    
