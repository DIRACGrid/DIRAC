"""
    Module used for enforcing policies. Its class is used for:

    1. invoke a PDP and collects results

    2. enforcing results by:

       a. saving result on a DB

       b. rasing alarms

       c. other....
"""

from DIRAC import gConfig

from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Policy import Configurations


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

    :attr:`siteType`: string - optional site type

    :attr:`serviceType`: string - optional service type

    :attr:`resourceType`: string - optional resource type

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
    
  def enforce(self, pdpIn = None, rsDBIn = None, knownInfo = None,
              ncIn = None, setupIn = None):
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
     
     :params:
       :attr:`pdpIn`: a custom PDP object (optional)
  
       :attr:`rsDBIn`: a custom database object (optional)
     
       :attr:`knownInfo`: a string of known provided information (optional)

       :attr:`ncIn`: a custom notification client object (optional)
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
    
    for res in resDecisions['PolicyCombinedResult']:
      
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
        for resP in resDecisions['SinglePolicyResults']:
          rsDB.addOrModifyPolicyRes(self.__granularity, self.__name, 
                                    resP['PolicyName'], resP['Status'], resP['Reason'])
        
        if res.has_key('EndDate'):
          rsDB.setDateEnd(self.__granularity, self.__name, res['EndDate']) 
  
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

          if ncIn is not None:
            nc = ncIn
          else:
            from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
            nc = NotificationClient()
          
          notif = "%s %s is perceived as" %(self.__granularity, self.__name) 
          notif = notif + " %s. Reason: %s." %(res['Status'], res['Reason'])
          
          if setupIn is None:
            setupIn = gConfig.getValue("DIRAC/Setup")
          
          NOTIF_D = self._getUsersToNotify(self.__granularity, 
                                           setupIn, self.__siteType)
          
          for notification in NOTIF_D:
            for user in notification['Users']:
              if 'Web' in notification['Notifications']:
                nc.addNotificationForUser(user, notif)
              if 'Mail' in notification['Notifications']:
                mailMessage = "Granularity = %s \n" %self.__granularity
                mailMessage = mailMessage + "Name = %s\n" %self.__name
                mailMessage = mailMessage + "New perceived status = %s\n" %res['Status']
                mailMessage = mailMessage + "Reason for status change = %s\n" %res['Reason']
                mailMessage = mailMessage + "Setup = %s\n" %setupIn
                nc.sendMail(gConfig.getValue("Security/Users/%s/email" %user), 
                            '%s: %s' %(self.__name, res['Status']), mailMessage)
          
#          for alarm in Configurations.alarms_list:
#            nc.updateAlarm(alarmKey = alarm, comment = notif) 
          
      if 'Collective_PolType' in self.__policyType:
        # do something
        pass
    
#############################################################################

  def _getUsersToNotify(self, granularity, setup, siteType = None):
    
    users = []
    notifications = []
    
    NOTIFinfo = {}
    NOTIF = []
    
    for ag in Configurations.AssigneeGroups.keys():
      
      if setup in Configurations.AssigneeGroups[ag]['Setup'] and granularity in Configurations.AssigneeGroups[ag]['Granularity']:
        if siteType is not None and siteType not in Configurations.AssigneeGroups[ag]['SiteType']:
          continue
        NOTIF.append( {'Users':Configurations.AssigneeGroups[ag]['Users'], 
                       'Notifications':Configurations.AssigneeGroups[ag]['Notifications']} )
          
    return NOTIF

#############################################################################
