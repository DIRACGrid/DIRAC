#!/bin/env python
# $HeadURL$
""" Convert the old Resources CS schema to the new one
"""
__RCSID__ = "$Id$"

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base import Script

storageFlag = False
def setStorageFlag( args ):
  global storageFlag
  storageFlag = True
  return S_OK()

computingFlag = False
def setComputingFlag( args ):
  global computingFlag
  computingFlag = True
  return S_OK()

catalogFlag = False
def setCatalogFlag( args ):
  global catalogFlag
  catalogFlag = True
  return S_OK()

transferFlag = False
def setTransferFlag( args ):
  global transferFlag
  transferFlag = True
  return S_OK()

dbFlag = False
def setDBFlag( args ):
  global dbFlag
  dbFlag = True
  return S_OK()

def setAllFlag( args ):
  global storageFlag, computingFlag, catalogFlag, transferFlag, dbFlag
  storageFlag = True
  computingFlag = True
  catalogFlag = True
  transferFlag = True
  dbFlag = True
  return S_OK()

defaultSite = None
def setDefaultSite( args ):
  global defaultSite
  defaultSite = args
  return S_OK()

Script.registerSwitch( "S", "se", "Convert storage element data", setStorageFlag )
Script.registerSwitch( "C", "ce", "Convert computing element data", setComputingFlag )
Script.registerSwitch( "F", "catalog", "Convert file catalog data", setCatalogFlag )
Script.registerSwitch( "T", "transfer", "Convert transfer service data", setTransferFlag )
Script.registerSwitch( "B", "dbserver", "Convert Database service data", setDBFlag )
Script.registerSwitch( "A", "all", "Convert all resources", setAllFlag )

Script.registerSwitch( "D:", "defaultSite=", "Default site name", setDefaultSite )

Script.setUsageMessage('\n'.join( [ __doc__.split( '\n' )[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ...' % Script.scriptName, ] )   )
Script.parseCommandLine()

from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.Core.Utilities.CFG import CFG
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources, getSiteName, getSites
from DIRAC.Core.Utilities import List

csapi = CSAPI()

RESOURCES_NEW_SECTION = '/Resources_new'

def convertSites():
  
  global csapi
  
  gLogger.notice( 'Converting Computing services' )
  
  # Collect site info
  infoDict = {}
  result = gConfig.getSections( '/Resources/Sites' ) 
  
  print result
  
  if not result['OK']:
    return result
  domains = result['Value']
  for domain in domains:
    gLogger.notice( 'Analyzing domain %s' % domain )
    result = gConfig.getSections( '/Resources/Sites/%s' % domain )
    if not result['OK']:
      return result 
    sites = result['Value']
    for site in sites:
      result = getSiteName( site )
      if not result['OK']:
        gLogger.error( 'Invalid site name %s' % site )
        continue
      siteName = result['Value']
      country = result['Country']
      
      print "AT >>> siteName, country", siteName, country
      
      gLogger.notice( 'Analyzing site %s' % siteName )
      result = gConfig.getOptionsDict( '/Resources/Sites/%s/%s' % (domain,site) )
      if not result['OK']:
        return result 
      siteDict = result['Value']
      siteDict['Country'] = country
      if 'Name' in siteDict:
        siteDict['GOCName'] = siteDict['Name']
        del siteDict['Name']
      if "CE" in siteDict:
        del siteDict['CE']
      if 'SE' in siteDict:
        del siteDict['SE']  
      infoDict.setdefault( siteName, siteDict )
      infoDict[siteName].setdefault( 'Domain', [] )
      infoDict[siteName]['Domain'].append( domain )
      if 'VO' in siteDict:
        communities = List.fromChar( siteDict['VO'] )
        infoDict[siteName]['VO'] = communities
      result = gConfig.getSections('/Resources/Sites/%s/%s/CEs' % (domain,site))
      if not result['OK']:
        if 'does not exist' in result['Message']:
          continue
        return result
      ces = result['Value']
      for ce in ces:
        result = gConfig.getOptionsDict( '/Resources/Sites/%s/%s/CEs/%s' % (domain,site,ce) )
        if not result['OK']:
          return result 
        ceDict = result['Value']
        
        ceName = ce.split('.')[0]
        if not 'Host' in ceDict:
          ceDict['Host'] = ce
        if not "SubmissionMode" in ceDict or ceDict['SubmissionMode'].lower() != "direct":
          ceDict['SubmissionMode'] = 'gLite'   
        
        infoDict[siteName].setdefault( 'Computing', {} )
        infoDict[siteName]['Computing'][ceName] = ceDict
        if 'VO' in ceDict:
          communities = List.fromChar( ceDict['VO'] )
          infoDict[siteName]['Computing'][ceName]['VO'] = communities
          del ceDict['VO']
        result = gConfig.getSections('/Resources/Sites/%s/%s/CEs/%s/Queues' % (domain,site,ce))
        if not result['OK']:
          if 'does not exist' in result['Message']:
            continue
          return result
        queues = result['Value']
        for queue in queues:
          result = gConfig.getOptionsDict( '/Resources/Sites/%s/%s/CEs/%s/Queues/%s' % (domain,site,ce,queue) )
          if not result['OK']:
            return result 
          queueDict = result['Value']
          infoDict[siteName]['Computing'][ceName].setdefault( 'Queues', {} )
          infoDict[siteName]['Computing'][ceName]['Queues'][queue] = queueDict
          if 'VO' in queueDict:
            communities = List.fromChar( queueDict['VO'] )
            infoDict[siteName]['Computing'][ceName]['Queues'][queue]['VO'] = communities
            del queueDict['VO']
        
      cfg = CFG()
      cfg.loadFromDict( infoDict[siteName] )
      
      print "AT >>> siteName, cfg", siteName, cfg.serialize()
      
      csapi.mergeCFGUnderSection( '%s/Sites/%s' % (RESOURCES_NEW_SECTION,siteName), cfg)
      csapi.sortSection( '%s/Sites/%s/Computing' % (RESOURCES_NEW_SECTION,siteName) )
             
    for domain in domains:
      csapi.createSection( '%s/Domains/%s' % (RESOURCES_NEW_SECTION,domain) )
    csapi.sortSection( '%s/Sites' % RESOURCES_NEW_SECTION )  
 
  return S_OK()
      
def convertSEs():
  
  global csapi, defaultSite
  
  gLogger.notice( 'Converting Storage services' )
  
  result = gConfig.getSections('/Resources/StorageElements' )
  if not result['OK']:
    return result
  ses = result['Value']  
  result = gConfig.getSections('/Resources/Sites')
  if not result['OK']:
    return result
  grids = result['Value']
  sites = []
  for grid in grids:
    result = gConfig.getSections('/Resources/Sites/%s' % grid)
    if not result['OK']:
      return result  
    sites += result['Value']
  sites = [ getSiteName(site)['Value'] for site in sites ]  
    
    
  for se in ses:
    
    cfg = CFG()
    
    # Try to guess the site
    seSite = 'Unknown'
    seName = 'Unknown'
    for site in sites:
      if se.startswith(site):
        seSite = site
        seName = se.replace( site, '' )[1:]
    if seName == 'Unknown':
      seName = se    
    
    defaultFlag = False
    if defaultSite:
      if seSite == "Unknown":
        seSite = defaultSite
        defaultFlag = True
  
    inputSite = raw_input("Processing SE %s, new name %s located at site [%s]: " % ( se,seName,seSite ) )    
    if not inputSite:
      if seSite == 'Unknown':
        inputSite = raw_input("Please, provide the site name for SE %s: " % se )
      else:
        site = seSite
    else:
      site = inputSite
    if defaultFlag:
      inputSE = raw_input("New SE name [%s]: " % seName )
      if inputSE:
        seName = inputSE                      
    
    sePath = '/Resources/StorageElements/%s' % se  
    result = gConfig.getOptionsDict(sePath)
    if not result['OK']:
      gLogger.error(result['Message'])
      return result      
    seDict = result['Value']
    result = gConfig.getSections(sePath)
    if not result['OK']:
      gLogger.error(result['Message'])
      return result   
    
    seDict['AccessProtocols'] = {}
    protocols = result['Value']
    for protocol in protocols:
      result = gConfig.getOptionsDict(sePath+'/'+protocol)
      if not result['OK']:
        gLogger.error(result['Message'])
        return result      
      protoDict = result['Value']
      protoName = protoDict['ProtocolName']
      seDict['AccessProtocols'][protoName] = protoDict
      
    cfg = CFG()  
    cfg.loadFromDict( seDict )
    csapi.createSection('%s/Sites/%s/Storage/%s' % (RESOURCES_NEW_SECTION,site,seName))
    csapi.mergeCFGUnderSection( '%s/Sites/%s/Storage/%s' % (RESOURCES_NEW_SECTION,site,seName), cfg)    
          
  return S_OK()       
          
def convertCatalogs():
  
  global csapi, defaultSite
  
  gLogger.notice( 'Converting Catalog services' )
  
  result = gConfig.getSections('/Resources/FileCatalogs')
  if not result['OK']:
    gLogger.error(result['Message'])
    return result
  catalogs = result['Value']
  
  for catalog in catalogs:
    gLogger.notice( 'Processing catalog %s' % catalog )
    if defaultSite:
      inputSite = raw_input('Hosting site for %s [%s]: ' % (catalog, defaultSite) )
      if not inputSite:
        site = defaultSite
      else:
        site = inputSite
    else:
      inputSite = raw_input('Hosting site for %s: ' % catalog )
      if not inputSite:
        gLogger.error(result['Message'])
        return
      site = inputSite
      
    result = csapi.copySection( '/Resources/FileCatalogs/%s' % catalog, '%s/Sites/%s/Catalog/%s' % (RESOURCES_NEW_SECTION,site,catalog) )
    if not result['OK']:
      gLogger.error(result['Message'])
      return result    
    csapi.setOptionComment( '%s/Sites/%s/Catalog' % (RESOURCES_NEW_SECTION,site), 'Catalog resources' )
    
  return S_OK()
        
def convertTransfers():
  
  global csapi
  
  gLogger.notice( 'Converting Transfer services' )
  
  result = gConfig.getOptionsDict('/Resources/FTSEndpoints')
  if not result['OK']:
    gLogger.error(result['Message'])
    return result
  
  ftsDict = result['Value']
  for site in ftsDict:
    result = getSiteName(site)
    siteName = result['Value']
    gLogger.notice( 'Processing FTS endpoint at site %s' % siteName )
    csapi.createSection( '%s/Sites/%s/Transfer/FTS' % (RESOURCES_NEW_SECTION,siteName) )
    csapi.setOptionComment( '%s/Sites/%s/Transfer/FTS' % (RESOURCES_NEW_SECTION,siteName),
                            'File Transfer Service' )
    csapi.setOption( '%s/Sites/%s/Transfer/FTS/URL' % (RESOURCES_NEW_SECTION,siteName), ftsDict[site] )
    
  csapi.setOptionComment( '%s/Sites/%s/Transfer' % (RESOURCES_NEW_SECTION,siteName), 
                          'Data Transfer Service resources' )  

def convertDBServers():
  
  global csapi
  
  gLogger.notice( 'Converting Database servers' )
  
  result = gConfig.getSections('/Resources/CondDB')
  if not result['OK']:
    gLogger.error(result['Message'])
    return result        
  
  sites = result['Value']
  for site in sites:
    result = getSiteName(site)
    siteName = result['Value']
    gLogger.notice( 'Processing CondDB endpoint at site %s' % siteName )
    csapi.copySection( '/Resources/CondDB/%s' % site, 
                       '%s/Sites/%s/DBServer/CondDB' % (RESOURCES_NEW_SECTION,siteName) )
    csapi.setOptionComment( '%s/Sites/%s/DBServer' % (RESOURCES_NEW_SECTION,siteName),
                            'Database server resource' )
    csapi.setOptionComment( '%s/Sites/%s/DBServer/CondDB' % (RESOURCES_NEW_SECTION,siteName), 
                            'Conditions database' )
          
if __name__ == '__main__':
  
  if computingFlag:
    result = convertSites()
  if storageFlag:
    result = convertSEs()   
  if catalogFlag:
    result = convertCatalogs()  
  if transferFlag:
    result = convertTransfers()  
  if dbFlag:
    result = convertDBServers()     
  
  csapi.commitChanges()
  print csapi.getCurrentCFG()['Value'].serialize()   
              
      
           



  