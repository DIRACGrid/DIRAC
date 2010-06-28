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

import copy
import time

class PEP: 
#############################################################################
  """ 
  PEP (Policy Enforcement Point) initialization
  
  :params:
    :attr:`VOExtension`: string - VO extension (e.g. 'LHCb')
  
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
 
  def __init__(self, VOExtension, granularity = None, name = None, status = None, formerStatus = None, 
               reason = None, siteType = None, serviceType = None, resourceType = None, 
               operatorCode = None, #futureEnforcement = None, 
               useNewRes = False):
    
    self.VOExtension = VOExtension
    
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

    self.__realBan = False
    if operatorCode is not None:
      if operatorCode == 'RS_SVC':
        self.__realBan = True
    
#    if futureEnforcement is not None:
#      try:
#        futureGranularity = futureEnforcement['Granularity']
#        if futureGranularity is not None:
#          if futureGranularity not in ValidRes:
#            raise InvalidRes, where(self, self.__init__)
#        self.__futureGranularity = futureGranularity
#      except NameError:
#        pass
      
    self.useNewRes = useNewRes

    configModule = __import__(self.VOExtension+"DIRAC.ResourceStatusSystem.Policy.Configurations", 
                              globals(), locals(), ['*'])

    self.AssigneeGroups = copy.deepcopy(configModule.AssigneeGroups)
      
#############################################################################
    
  def enforce(self, pdpIn = None, rsDBIn = None, ncIn = None, setupIn = None, 
              daIn = None, csAPIIn = None, knownInfo = None):
    """ 
    enforce policies, using a PDP  (Policy Decision Point), based on

     self.__granularity (optional)
  
     self.__name (optional)
  
     self.__status (optional) 
  
     self.__formerStatus (optional)
  
     self.__reason (optional)
  
     self.__siteType (optional)
  
     self.__serviceType (optional)
  
     self.__realBan (optional)
  
     self.__user (optional)
  
     self.__futurePolicyType (optional)
  
     self.__futureGranularity (optional)
     
     :params:
       :attr:`pdpIn`: a custom PDP object (optional)
  
       :attr:`rsDBIn`: a custom database object (optional)
     
       :attr:`setupIn`: a string with the present setup (optional)

       :attr:`ncIn`: a custom notification client object (optional)

       :attr:`daIn`: a custom DiracAdmin object (optional)

       :attr:`csAPIIn`: a custom CSAPI object (optional)

       :attr:`knownInfo`: a string of known provided information (optional)
    """

    #PDP
    if pdpIn is not None:
      pdp = pdpIn
    else:
      # Use standard DIRAC PDP
      from DIRAC.ResourceStatusSystem.PolicySystem.PDP import PDP
      pdp = PDP(self.VOExtension, granularity = self.__granularity, name = self.__name, 
                status = self.__status, formerStatus = self.__formerStatus, reason = self.__reason, 
                siteType = self.__siteType, serviceType = self.__serviceType, 
                resourceType = self.__resourceType, useNewRes = self.useNewRes)

    #DB
    if rsDBIn is not None:
      rsDB = rsDBIn
    else:
      # Use standard DIRAC DB
      from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB
      rsDB = ResourceStatusDB()
    
    #setup
    if setupIn is not None:
      setup = setupIn
    else:
      # get present setup
      from DIRAC import gConfig
      setup = gConfig.getValue("DIRAC/Setup")
          
    #notification client
    if ncIn is not None:
      nc = ncIn
    else:
      from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
      nc = NotificationClient()
          
    #DiracAdmin
    if daIn is not None:
      da = daIn
    else:
      from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
      da = DiracAdmin()

    #CSAPI
    if csAPIIn is not None:
      csAPI = csAPIIn
    else:
      from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
      csAPI = CSAPI()
  
    
    # policy decision
    resDecisions = pdp.takeDecision(knownInfo=knownInfo)
    
    for res in resDecisions['PolicyCombinedResult']:
      
      self.__policyType = res['PolicyType']

      if 'Resource_PolType' in self.__policyType:
        self._ResourcePolTypeActions(resDecisions, res, rsDB)
      
      if 'Alarm_PolType' in self.__policyType:
        self._AlarmPolTypeActions(res, nc, setup, rsDB)
          
      if 'RealBan_PolType' in self.__policyType and self.__realBan == True:
        self._RealBanPolTypeActions(res, da, csAPI, setup)
      
      if 'Collective_PolType' in self.__policyType:
        # do something
        pass


#
#      if res['Action']:
#        try:
#          if self.__futureGranularity != self.__granularity:
#            self.__name = rsDB.getGeneralName(self.__name, self.__granularity, 
#                                              self.__futureGranularity)
#          newPEP = PEP(granularity = self.__futureGranularity, name = self.__name, 
#                       status = self.__status, formerStatus = self.__formerStatus, 
#                       reason = self.__reason)
#          newPEP.enforce(pdpIn = pdp, rsDBIn = rsDB) 
#        except AttributeError:
#          pass
    


    
#############################################################################

  def _ResourcePolTypeActions(self, resDecisions, res, rsDB):
    # Update the DB
  
    if res['Action']:
      if self.__granularity == 'Site':
        rsDB.setSiteStatus(self.__name, res['Status'], res['Reason'], 'RS_SVC')
        rsDB.setMonitoredToBeChecked(['Service', 'Resource', 'StorageElement'], 'Site', self.__name)
      
      elif self.__granularity == 'Service':
        rsDB.setServiceStatus(self.__name, res['Status'], res['Reason'], 'RS_SVC')
        rsDB.setMonitoredToBeChecked(['Site', 'Resource', 'StorageElement'], 'Service', self.__name)
      
      elif self.__granularity == 'Resource':
        rsDB.setResourceStatus(self.__name, res['Status'], res['Reason'], 'RS_SVC')
        rsDB.setMonitoredToBeChecked(['Site', 'Service', 'StorageElement'], 'Resource', self.__name)

      elif self.__granularity == 'StorageElement':
        rsDB.setStorageElementStatus(self.__name, res['Status'], res['Reason'], 'RS_SVC')
        rsDB.setMonitoredToBeChecked(['Site', 'Service', 'Resource'], 'StorageElement', self.__name)

    else:
      rsDB.setMonitoredReason(self.__granularity, self.__name, res['Reason'], 'RS_SVC')

    rsDB.setLastMonitoredCheckTime(self.__granularity, self.__name)
    
    for resP in resDecisions['SinglePolicyResults']:
      if not resP.has_key('OLD'):
        rsDB.addOrModifyPolicyRes(self.__granularity, self.__name, 
                                  resP['PolicyName'], resP['Status'], resP['Reason'])
    
    if res.has_key('EndDate'):
      rsDB.setDateEnd(self.__granularity, self.__name, res['EndDate']) 
  
  
#############################################################################

  def _AlarmPolTypeActions(self, res, nc, setup, rsDB):
        # raise alarms, right now makes a simple notification
        
    if res['Action']:

      notif = "%s %s is perceived as" %(self.__granularity, self.__name) 
      notif = notif + " %s. Reason: %s." %(res['Status'], res['Reason'])
      
      NOTIF_D = self.__getUsersToNotify(self.__granularity, 
                                        setup, self.__siteType, 
                                        self.__serviceType,
                                        self.__resourceType)
      
      for notification in NOTIF_D:
        for user in notification['Users']:
          if 'Web' in notification['Notifications']:
            nc.addNotificationForUser(user, notif)
          if 'Mail' in notification['Notifications']:
            mailMessage = "Granularity = %s \n" %self.__granularity
            mailMessage = mailMessage + "Name = %s\n" %self.__name
            mailMessage = mailMessage + "New perceived status = %s\n" %res['Status']
            mailMessage = mailMessage + "Reason for status change = %s\n" %res['Reason']
            
            was = rsDB.getMonitoredsHistory(self.__granularity, 
                                            ['Status', 'Reason', 'DateEffective'], 
                                            self.__name, False, 'DESC', 1)[0]
            
            mailMessage = mailMessage + "Was in status \"%s\", " %(was[0])
            mailMessage = mailMessage + "with reason \"%s\", since %s\n" %(was[1], was[2])
            
            mailMessage = mailMessage + "Setup = %s\n" %setup

            nc.sendMail(gConfig.getValue("Registry/Users/%s/Email" %user), 
                        '%s: %s' %(self.__name, res['Status']), mailMessage)
      
#          for alarm in Configurations.alarms_list:
#            nc.updateAlarm(alarmKey = alarm, comment = notif) 
  
  
#############################################################################
  
  def _RealBanPolTypeActions(self, res, da, csAPI, setup):
    # implement real ban
  
    if res['Action']:
      
      if self.__granularity == 'Site':
  
        banList = da.getBannedSites()
        if not banList['OK']:
          raise RSSException, where(self, self.enforce) + banList['Message']
        else:
          banList = banList['Value']
        
        
        if res['Status'] == 'Banned': 
        
          if self.__name not in banList:
            banSite = da.banSiteFromMask(self.__name, res['Reason'])
            if not banSite['OK']:
              raise RSSException, where(self, self.enforce) + banSite['Message']
            if setup == 'LHCb-Production':
              address = gConfig.getValue('/Operations/EMail/Production','')
            else:
              address = 'fstagni@cern.ch'
            
            subject = '%s is banned for %s setup' %(self.__name, setup)
            body = 'Site %s is removed from site mask for %s ' %(self.__name, setup)
            body += 'setup by the DIRAC RSS on %s.\n\n' %(time.asctime())
            body += 'Comment:\n%s' %res['Reason']
            sendMail = da.sendMail(address,subject,body)
            if not sendMail['OK']:
              raise RSSException, where(self, self.enforce) + sendMail['Message']
        
        else:
          if self.__name in banList:
            addSite = da.addSiteInMask(self.__name, res['Reason'])
            if not addSite['OK']:
              raise RSSException, where(self, self.enforce) + addSite['Message']
            if setup == 'LHCb-Production':
              address = gConfig.getValue('/Operations/EMail/Production','')
            else:
              address = 'fstagni@cern.ch'
            
            subject = '%s is added in site mask for %s setup' %(self.__name, setup)
            body = 'Site %s is added to the site mask for %s ' %(self.__name, setup)
            body += 'setup by the DIRAC RSS on %s.\n\n' %(time.asctime())
            body += 'Comment:\n%s' %res['Reason']
            sendMail = da.sendMail(address,subject,body)
            if not sendMail['OK']:
              raise RSSException, where(self, self.enforce) + sendMail['Message']
              
    
      elif self.__granularity == 'StorageElement':
      
        presentReadStatus = gConfig.getValue("/Resources/StorageElements/%s/ReadAccess" %(self.__name) )
        presentWriteStatus = gConfig.getValue("/Resources/StorageElements/%s/WriteAccess" %(self.__name) )
  
        if res['Status'] == 'Banned':
          
          if presentReadStatus != 'InActive':
            banSE = csAPI.setOption("/Resources/StorageElements/%s/ReadAccess" %(self.__name), "InActive")
            if not banSE['OK']:
              raise RSSException, where(self, self.enforce) + banSE['Message']
            banSE = csAPI.setOption("/Resources/StorageElements/%s/WriteAccess" %(self.__name), "InActive")
            if not banSE['OK']:
              raise RSSException, where(self, self.enforce) + banSE['Message']
            commit = csAPI.commit()
            if not commit['OK']:
              raise RSSException, where(self, self.enforce) + commit['Message']
            if setup == 'LHCb-Production':
              address = gConfig.getValue('/Operations/EMail/Production','')
            else:
              address = 'fstagni@cern.ch'
            
            subject = '%s is banned for %s setup' %(self.__name, setup)
            body = 'SE %s is removed from mask for %s ' %(self.__name, setup)
            body += 'setup by the DIRAC RSS on %s.\n\n' %(time.asctime())
            body += 'Comment:\n%s' %res['Reason']
            sendMail = da.sendMail(address,subject,body)
            if not sendMail['OK']:
              raise RSSException, where(self, self.enforce) + sendMail['Message']
        
        else:
  
          if presentReadStatus == 'InActive':
  
            allowSE = csAPI.setOption("/Resources/StorageElements/%s/ReadAccess" %(self.__name), "Active")
            if not allowSE['OK']:
              raise RSSException, where(self, self.enforce) + allowSE['Message']
            allowSE = csAPI.setOption("/Resources/StorageElements/%s/WriteAccess" %(self.__name), "Active")
            if not allowSE['OK']:
              raise RSSException, where(self, self.enforce) + allowSE['Message']
            commit = csAPI.commit()
            if not commit['OK']:
              raise RSSException, where(self, self.enforce) + commit['Message']
            if setup == 'LHCb-Production':
              address = gConfig.getValue('/Operations/EMail/Production','')
            else:
              address = 'fstagni@cern.ch'
            
            subject = '%s is allowed for %s setup' %(self.__name, setup)
            body = 'SE %s is added to the mask for %s ' %(self.__name, setup)
            body += 'setup by the DIRAC RSS on %s.\n\n' %(time.asctime())
            body += 'Comment:\n%s' %res['Reason']
            sendMail = da.sendMail(address,subject,body)
            if not sendMail['OK']:
              raise RSSException, where(self, self.enforce) + sendMail['Message']
                  
        

#############################################################################
  
  
  def __getUsersToNotify(self, granularity, setup, siteType = None, serviceType = None,
                         resourceType = None):
    
    NOTIF = []
    
    for ag in self.AssigneeGroups.keys():
      
      if setup in self.AssigneeGroups[ag]['Setup'] \
      and granularity in self.AssigneeGroups[ag]['Granularity']:
        if siteType is not None and siteType not in self.AssigneeGroups[ag]['SiteType']:
          continue
        if serviceType is not None and serviceType not in self.AssigneeGroups[ag]['ServiceType']:
          continue
        if resourceType is not None and resourceType not in self.AssigneeGroups[ag]['ResourceType']:
          continue
        NOTIF.append( {'Users':self.AssigneeGroups[ag]['Users'], 
                       'Notifications':self.AssigneeGroups[ag]['Notifications']} )
          
    return NOTIF

#############################################################################

