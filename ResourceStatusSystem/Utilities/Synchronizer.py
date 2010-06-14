"""
This module contains a class to synchronize the content of the DataBase with what is the CS  
"""

import time

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.SiteCEMapping import getSiteCEMapping
from DIRAC.Core.Utilities.SiteSEMapping import getSiteSEMapping
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import getDIRACSiteName

from DIRAC.ResourceStatusSystem.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.Core.LCG.GOCDBClient import GOCDBClient

class Synchronizer:
  
#############################################################################

  def __init__(self, rsDBin = None):

    self.rsDB = rsDBin

    if self.rsDB is None:
      from DIRAC.ResourceStatusSystem.ResourceStatusDB import ResourceStatusDB
      self.rsDB = ResourceStatusDB()
    
    self.GOCDBClient = GOCDBClient()
      
#############################################################################
      
  def sync(self, thingsToSync):
    """
    :params:
      :attr:`thingsToSync`: list of things to sync
    """
    for thing in thingsToSync:
      getAttr(self, '_sync'+thing)()
    
#############################################################################

  def _syncUtils(self):
    """
    Sync DB content with what is in :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
    """
  
    statusIn = self.rsDB.getStatusList()
    #delete status not more in Utils
    for stIn in statusIn:
      if stIn not in ValidStatus:
        self.rsDB.removeStatus(stIn)
    #Add new status
    for s in ValidStatus:
      if s not in statusIn:
        self.rsDB.addStatus(s)
    
    for g in ('Site', 'Service', 'Resource'):
      typeIn = self.rsDB.getTypesList(g)
      if g == 'Site':
        typesList = ValidSiteType
      elif g == 'Service':
        typesList = ValidServiceType
      if g == 'Resource':
        typesList = ValidResourceType
      #delete types not more in Utils
      for tIn in typeIn:
        if tIn not in typesList:
          self.rsDB.removeType(g, tIn)
      #Add new types
      for t in typesList:
        if t not in typeIn:
          self.rsDB.addType(g, t)

#############################################################################
  
  def _syncSites(self):
    """
    Sync DB content with sites that are in the CS 
    """
    
    # sites in the DB now
    sitesIn = self.rsDB.getMonitoredsList('Site', paramsList = ['SiteName'])
    sitesIn = [s[0] for s in sitesIn]

    # sited in CS now
    sitesList = gConfig.getSections('Resources/Sites/LCG', True)
    sitesList = sitesList['Value']
    try:
      sitesList.remove('LCG.Dummy.ch')
    except ValueError:
      pass
    
    # remove sites from the DB not more in the CS
    for site in sitesIn:
      if site not in sitesList:
        self.rsDB.removeStorageElement(siteName = site)
        self.rsDB.removeResource(siteName = site)
        self.rsDB.removeService(siteName = site)
        self.rsDB.removeSite(site)
    
    # add to DB what is CS now and wasn't before
    for site in sitesList:
      if site not in sitesIn:
        tier = gConfig.getValue("Resources/Sites/LCG/%s/MoUTierLevel" %site)
        if tier == 0 or tier == '0':
          t = 'T0'
        elif tier == 1 or tier == '1':
          t = 'T1'
        else:
          t = 'T2'
        self.rsDB.addOrModifySite(site, t, 'Active', 'init', 
                                  datetime.utcnow().replace(microsecond = 0), 'RS_SVC', 
                                  datetime(9999, 12, 31, 23, 59, 59))
        sitesIn.append(site)
    
#############################################################################

  def _syncServices(self):
    pass
  
#############################################################################

  def _syncResources(self):

    # resources in the DB now
    resourcesIn = self.getMonitoredsList('Resource', paramsList = ['ResourceName'])
    resourcesIn = [s[0] for s in resourcesIn]

    # Site-CE mapping in CS now
    siteCE = getSiteCEMapping('LCG')['Value']
    # Site-SE mapping in CS now
    siteSE = getSiteSEMapping('LCG')['Value']

    # CEs in CS now
    CEList = []
    for i in siteCE.values():
      for ce in i:
        if ce is None:
          continue
        CEList.append(ce)
    
    # SEs in CS now
    SEList = []
    for i in siteSE.values():
      for x in i:
        SEList.append(x)
        
    # SE Nodes in CS now 
    SENodeList = []
    for SE in SEList:
      node = gConfig.getValue("/Resources/StorageElements/%s/AccessProtocol.1/Host" %SE)
      if node is None:
        continue
      if node not in SENodeList:
        SENodeList.append(node)
  
    # LFC Nodes in CS now
    LFCNodeList = []
    for site in gConfig.getSections('Resources/FileCatalogs/LcgFileCatalogCombined', True)['Value']:
      for readable in ('ReadOnly', 'ReadWrite'):
        LFCNode = gConfig.getValue('Resources/FileCatalogs/LcgFileCatalogCombined/%s/%s' %(site, readable))
        if LFCNode is None:
          continue
        if LFCNode not in LFCNodeList:
          LFCNodeList.append(LFCNode)

    # FTS Nodes in CS now
    FTSNodeList = []
    sitesWithFTS = gConfig.getOptions("/Resources/FTSEndpoints")
    if not sitesWithFTS['OK']:
      raise RSSException, sitesWithFTS['Message']
    for site in sitesWithFTS['Value']:
      fts =  gConfig.getValue("/Resources/FTSEndpoints/%s" %site).split('/')[2][0:-5]
      if FTSNodeList is None:
        continue
      if fts not in FTSNodeList:
        FTSNodeList.append(fts)

    # complete list of resources in CS now
    resourcesList = CEList + SENodeList + LFCNodeList + FTSNodeList

    #remove resources no more in the CS
    for res in resourcesIn:
      if res not in resourcesList:
        self.rsDB.removeResource(res)
        self.rsDB.removeStorageElement(resourceName = res)

    
    # add to DB what is in CS now and wasn't before
    
    # CEs
    for site in siteCE.keys():
      if site == 'LCG.Dummy.ch':
        continue
      for ce in siteCE[site]:
        if ce is None:
          continue
        service = 'Computing@' + site
        if ce not in  resourcesIn:
          CEType = gConfig.getValue('Resources/Sites/LCG/%s/CEs/%s/CEType')
          ceType = 'CE'
          if CEType == 'CREAM':
            ceType = 'CREAMCE'
          self.addOrModifyResource(ce, ceType, service, site, 'Active', 'init', 
                                   datetime.utcnow().replace(microsecond = 0), 'RS_SVC', 
                                   datetime(9999, 12, 31, 23, 59, 59))
          resourcesIn.append(ce)

    # SRMs
    for srm in SENodeList:
      siteInGOCDB = self.GOCDBClient.getServiceEndpointInfo('hostname', srm)[0]['SITENAME']
      siteInDIRAC = getDIRACSiteName(siteInGOCDB)
      if not siteInDIRAC['OK']:
        raise RSSException, siteInDIRAC['Message']
      site = siteInDIRAC['Value']
      service = 'Storage@' + site
      if srm not in resourcesIn and srm is not None:
        self.addOrModifyResource(srm, 'SE', service, site, 'Active', 'init', 
                                 datetime.utcnow().replace(microsecond = 0), 'RS_SVC', 
                                 datetime(9999, 12, 31, 23, 59, 59))
        resourcesIn.append(srm)
        
    # LFCs
    for lfc in LFCNodeList:
      siteInGOCDB = self.GOCDBClient.getServiceEndpointInfo('hostname', lfc)[0]['SITENAME']
      siteInDIRAC = getDIRACSiteName(siteInGOCDB)
      if not siteInDIRAC['OK']:
        raise RSSException, siteInDIRAC['Message']
      site = siteInDIRAC['Value']
      service = 'Storage@' + site
      if lfc not in resourcesIn and lfc is not None:
        self.addOrModifyResource(lfc, 'SE', service, site, 'Active', 'init', 
                                 datetime.utcnow().replace(microsecond = 0), 'RS_SVC', 
                                 datetime(9999, 12, 31, 23, 59, 59))
        resourcesIn.append(lfc)
      
      
    # FTSs
    for fts in FTSNodeList:
      siteInGOCDB = self.GOCDBClient.getServiceEndpointInfo('hostname', lfc)[0]['SITENAME']
      siteInDIRAC = getDIRACSiteName(siteInGOCDB)
      if not siteInDIRAC['OK']:
        raise RSSException, siteInDIRAC['Message']
      site = siteInDIRAC['Value']
      service = 'Storage@' + site
      if fts not in resourcesIn and fts is not None:
        self.addOrModifyResource(fts, 'FTS', service, site, 'Active', 'init', 
                                 datetime.utcnow().replace(microsecond = 0), 'RS_SVC', 
                                 datetime(9999, 12, 31, 23, 59, 59))
        resourcesIn.append(fts)
      
#############################################################################

  def _syncStorageElements(self):
    pass
  
#############################################################################
