# $HeadURL$
""" 
The Grid module contains several utilities for grid operations
"""
__RCSID__ = "$Id$"

import os
import types
import re
from DIRAC.Core.Utilities.Os import sourceEnv
from DIRAC.FrameworkSystem.Client.ProxyManagerClient  import gProxyManager
from DIRAC.Core.Security.ProxyInfo                    import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers         import Local
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import systemCall, shellCall

def executeGridCommand( proxy, cmd, gridEnvScript = None ):
  """ 
  Execute cmd tuple after sourcing GridEnv
  """
  currentEnv = dict( os.environ )

  if not gridEnvScript:
    # if not passed as argument, use default from CS Helpers
    gridEnvScript = Local.gridEnv()

  if gridEnvScript:
    command = gridEnvScript.split()
    ret = sourceEnv( 10, command )
    if not ret['OK']:
      return S_ERROR( 'Failed sourcing GridEnv: %s' % ret['Message'] )
    gridEnv = ret['outputEnv']
    #
    # Preserve some current settings if they are there
    #
    if currentEnv.has_key( 'X509_VOMS_DIR' ):
      gridEnv['X509_VOMS_DIR'] = currentEnv['X509_VOMS_DIR']
    if currentEnv.has_key( 'X509_CERT_DIR' ):
      gridEnv['X509_CERT_DIR'] = currentEnv['X509_CERT_DIR']
  else:
    gridEnv = currentEnv

  if not proxy:
    res = getProxyInfo()
    if not res['OK']:
      return res
    gridEnv['X509_USER_PROXY' ] = res['Value']['path']
  elif type( proxy ) in types.StringTypes:
    if os.path.exists( proxy ):
      gridEnv[ 'X509_USER_PROXY' ] = proxy
    else:
      return S_ERROR( 'Can not treat proxy passed as a string' )
  else:
    ret = gProxyManager.dumpProxyToFile( proxy )
    if not ret['OK']:
      return ret
    gridEnv[ 'X509_USER_PROXY' ] = ret['Value']

  result = systemCall( 120, cmd, env = gridEnv )
  return result

def ldapsearchBDII( filt = None, attr = None, host = None, base = None ):
  """ Python wrapper for ldapserch at bdii.
      
      :param  filt:    Filter used to search ldap, default = '', means select all
      :param  attr:    Attributes returned by ldapsearch, default = '*', means return all
      :param  host:    Host used for ldapsearch, default = 'lcg-bdii.cern.ch:2170', can be changed by $LCG_GFAL_INFOSYS
      
      :return: standard DIRAC answer with Value equals to list of ldapsearch responses

      Each element of list is dictionary with keys:
      
        'dn':                 Distinguished name of ldapsearch response
        'objectClass':        List of classes in response
        'attr':               Dictionary of attributes
  """

  if filt == None:
    filt = ''
  if attr == None:
    attr = '*'
  if host == None:
    host = 'lcg-bdii.cern.ch:2170'
  if base == None:
    base = 'Mds-Vo-name=local,o=grid'

  cmd = 'ldapsearch -x -LLL -h %s -b %s "%s" "%s"' % ( host, base, filt, attr )
  result = shellCall( 0, cmd )

  response = []

  if not result['OK']:
    return result

  status = result['Value'][0]
  stdout = result['Value'][1]
  stderr = result['Value'][2]

  if not status == 0:
    return S_ERROR( stderr )

  lines = []
  for line in stdout.split( "\n" ):
    if line.find( " " ) == 0:
      lines[-1] += line.strip()
    else:
      lines.append( line.strip() )

  record = None
  for line in lines:
    if line.find( 'dn:' ) == 0:
      record = {'dn':line.replace( 'dn:', '' ).strip(),
                'objectClass':[],
                'attr':{'dn':line.replace( 'dn:', '' ).strip()}}
      response.append( record )
      continue
    if record:
      if line.find( 'objectClass:' ) == 0:
        record['objectClass'].append( line.replace( 'objectClass:', '' ).strip() )
        continue
      if line.find( 'Glue' ) == 0:
        index = line.find( ':' )
        if index > 0:
          attr = line[:index]
          value = line[index + 1:].strip()
          if record['attr'].has_key( attr ):
            if type( record['attr'][attr] ) == type( [] ):
              record['attr'][attr].append( value )
            else:
              record['attr'][attr] = [record['attr'][attr], value]
          else:
            record['attr'][attr] = value

  return S_OK( response )


def ldapSite( site, attr = None, host = None ):
  """ Site information from bdii.

:param  site: Site as it defined in GOCDB or part of it with globing, for example: \UKI-*
:return: standard DIRAC answer with Value equals to list of sites.

Each site is dictionary which contains attributes of site.
For example result['Value'][0]['GlueSiteLocation']
  """
  filt = '(GlueSiteUniqueID=%s)' % site

  result = ldapsearchBDII( filt, attr, host )

  if not result['OK']:
    return result

  sites = []
  for value in result['Value']:
    sites.append( value['attr'] )

  return S_OK( sites )

def ldapCluster( ce, attr = None, host = None ):
  """ CE (really SubCluster in definition of bdii) information from bdii.
It contains by the way host information for ce.

:param  ce: ce or part of it with globing, for example, "ce0?.tier2.hep.manchester*"
:return: standard DIRAC answer with Value equals to list of clusters.

Each cluster is dictionary which contains attributes of ce.
For example result['Value'][0]['GlueHostBenchmarkSI00']
  """
  filt = '(GlueClusterUniqueID=%s)' % ce

  result = ldapsearchBDII( filt, attr, host )

  if not result['OK']:
    return result

  clusters = []
  for value in result['Value']:
    clusters.append( value['attr'] )

  return S_OK( clusters )

def ldapCE( ce, attr = None, host = None ):
  """ CE (really SubCluster in definition of bdii) information from bdii.
It contains by the way host information for ce.

:param  ce: ce or part of it with globing, for example, "ce0?.tier2.hep.manchester*"
:return: standard DIRAC answer with Value equals to list of clusters.

Each cluster is dictionary which contains attributes of ce.
For example result['Value'][0]['GlueHostBenchmarkSI00']
  """
  filt = '(GlueSubClusterUniqueID=%s)' % ce

  result = ldapsearchBDII( filt, attr, host )

  if not result['OK']:
    return result

  ces = []
  for value in result['Value']:
    ces.append( value['attr'] )

  return S_OK( ces )

def ldapCEState( ce, vo, attr = None, host = None ):
  """ CEState information from bdii. Only CE with CEAccessControlBaseRule=VO:lhcb are selected.

:param  ce: ce or part of it with globing, for example, "ce0?.tier2.hep.manchester*"
:return: standard DIRAC answer with Value equals to list of ceStates.

Each ceState is dictionary which contains attributes of ce.
For example result['Value'][0]['GlueCEStateStatus']
  """
  voFilters = '(GlueCEAccessControlBaseRule=VOMS:/%s/*)' % vo
  voFilters += '(GlueCEAccessControlBaseRule=VOMS:/%s)' % vo
  voFilters += '(GlueCEAccessControlBaseRule=VO:%s)' % vo
  filt = '(&(GlueCEUniqueID=%s*)(|%s))' % ( ce, voFilters )
  
  result = ldapsearchBDII( filt, attr, host )

  if not result['OK']:
    return result

  states = []
  for value in result['Value']:
    states.append( value['attr'] )

  return S_OK( states )

def ldapCEVOView( ce, vo, attr = None, host = None ):
  """ CEVOView information from bdii. Only CE with CEAccessControlBaseRule=VO:lhcb are selected.

:param  ce: ce or part of it with globing, for example, "ce0?.tier2.hep.manchester*"
:return: standard DIRAC answer with Value equals to list of ceVOViews.

Each ceVOView is dictionary which contains attributes of ce.
For example result['Value'][0]['GlueCEStateRunningJobs']
  """

  voFilters = '(GlueCEAccessControlBaseRule=VOMS:/%s/*)' % vo
  voFilters += '(GlueCEAccessControlBaseRule=VOMS:/%s)' % vo
  voFilters += '(GlueCEAccessControlBaseRule=VO:%s)' % vo
  filt = '(&(GlueCEUniqueID=%s*)(|%s))' % ( ce, voFilters )
  
  result = ldapsearchBDII( filt, attr, host )

  if not result['OK']:
    return result

  ces = result['Value']

  filt = '(&(objectClass=GlueVOView)(|%s))' % ( voFilters )
  views = []

  for ce in ces:
    dn = ce['dn']
    result = ldapsearchBDII( filt, attr, host, base = dn )
    if result['OK']:
      views.append( result['Value'][0]['attr'] )

  return S_OK( views )

def ldapSE( site, vo, attr = None, host = None ):
  """ SE/SA information from bdii.

:param  site: site with globing, for example, "ce0?.tier2.hep.manchester*" or just "*"
:param  vo: VO name with globing, "*" if all VOs
:return: standard DIRAC answer with Value equals to list of SE/SA merged items.

Each SE is dictionary which contains attributes of SE and corresponding SA.
For example result['Value'][0]['GlueSESizeFree']
  """
  voFilters = '(GlueSAAccessControlBaseRule=VOMS:/%s/*)' % vo
  voFilters += '(GlueSAAccessControlBaseRule=VOMS:/%s)' % vo
  voFilters += '(GlueSAAccessControlBaseRule=VO:%s)' % vo
  filt = '(&(objectClass=GlueSA)(|%s))' % voFilters
  result = ldapsearchBDII( filt, attr, host )
  if not result['OK']:
    return result
  sas = result['Value']

  saDict = {}

  seIDFilter = ''
  for sa in sas:
    chunk = sa['attr'].get('GlueChunkKey','')
    if chunk:
      seID = sa['attr']['GlueChunkKey'].replace('GlueSEUniqueID=','')
      saDict[seID] = sa['attr']
      seIDFilter += '(GlueSEUniqueID=%s)' % seID

  if vo == "*":
    filt = '(&(objectClass=GlueSE)(GlueForeignKey=GlueSiteUniqueID=%s))' % site
  else:
    filt = '(&(objectClass=GlueSE)(|%s)(GlueForeignKey=GlueSiteUniqueID=%s))' % ( seIDFilter, site )
  result = ldapsearchBDII( filt, attr, host )
  if not result['OK']:
    return result
  ses = result['Value']

  seDict = {}
  for se in ses:
    seID = se['attr']['GlueSEUniqueID']
    seDict[seID] = se['attr']
    siteName = se['attr']['GlueForeignKey'].replace('GlueSiteUniqueID=','')
    seDict[seID]['GlueSiteUniqueID'] = siteName
    if seID in saDict:
      seDict[seID].update( saDict[seID] )

  seList = seDict.values()
  return S_OK( seList )

def ldapSEAccessProtocol( se, attr = None, host = None ):
  """ SE access protocol information from bdii

:param  se: se or part of it with globing, for example, "ce0?.tier2.hep.manchester*"
:return: standard DIRAC answer with Value equals to list of access protocols.

  """
  filt = '(&(objectClass=GlueSEAccessProtocol)(GlueChunkKey=GlueSEUniqueID=%s))' % se
  result = ldapsearchBDII( filt, attr, host )

  if not result['OK']:
    return result

  protocols = []
  for value in result['Value']:
    protocols.append( value['attr'] )

  return S_OK( protocols )

def ldapService( serviceID = '*', serviceType = '*', vo = '*', attr = None, host = None):
  """ Service BDII info for a given VO

:param  service: service type, e.g. SRM
:return: standard DIRAC answer with Value equals to list of services
  """
  voFilters = '(GlueServiceAccessControlBaseRule=VOMS:/%s/*)' % vo
  voFilters += '(GlueServiceAccessControlBaseRule=VOMS:/%s)' % vo
  voFilters += '(GlueServiceAccessControlBaseRule=VO:%s)' % vo
  filt = '(&(GlueServiceType=%s)(GlueServiceUniqueID=%s)(|%s))' % ( serviceType, serviceID, voFilters )
  
  result = ldapsearchBDII( filt, attr, host )

  if not result['OK']:
    return result

  services = []
  for value in result['Value']:
    services.append( value['attr'] )

  return S_OK( services )
  

def getBdiiCEInfo( vo, host = None ):
  """ Get information for all the CEs/queues for a given VO
  
:param vo: BDII VO name
:return result structure: result['Value'][siteID]['CEs'][ceID]['Queues'][queueName]. For
               each siteID, ceID, queueName all the BDII/Glue parameters are retrieved  
  """  
  result = ldapCEState( '', vo, host = host )
  if not result['OK']:
    return result

  siteDict = {}
  ceDict = {}
  queueDict = {}
  
  for queue in result['Value']:
    ceID = queue.get('GlueForeignKey','').replace('GlueClusterUniqueID=','')
    queueDict[queue['GlueCEUniqueID']] = queue
    queueDict[queue['GlueCEUniqueID']]['CE'] = ceID
    if not ceID in ceDict:
      result = ldapCluster( ceID, host = host )
      if not result['OK']:
        continue
      if not result['Value']:
        continue
  
      ce = result['Value'][0]
      ceDict[ceID] = ce
  
      fKey = ce['GlueForeignKey']
      siteID = ''
      for key in fKey:
        if key.startswith('GlueSiteUniqueID'):
          siteID = key.replace('GlueSiteUniqueID=','')
      ceDict[ceID]['Site'] = siteID
  
      result = ldapCE( ceID, host = host )
      ce = {}
      if result['OK'] and result['Value']:
        ce = result['Value'][0]  
      ceDict[ceID].update( ce )
  
      if not siteID in siteDict:
        site = {}
        result = ldapSite( siteID, host = host )
        if result['OK'] and result['Value']:
          site = result['Value'][0]
        siteDict[siteID] = site
  
  for ceID in ceDict:
    siteID = ceDict[ceID]['Site']
    if siteID in siteDict:
      siteDict[siteID].setdefault('CEs',{})
      siteDict[siteID]['CEs'][ceID] = ceDict[ceID]
  
  for queueID in queueDict:
    ceID = queueDict[queueID]['CE']
    siteID = ceDict[ceID]['Site']
    siteDict[siteID]['CEs'][ceID].setdefault('Queues',{})
    queueName = re.split( ':\d+/', queueDict[queueID]['GlueCEUniqueID'] )[1]
    siteDict[siteID]['CEs'][ceID]['Queues'][queueName] = queueDict[queueID]
    
  return S_OK( siteDict )  

def getBdiiSEInfo( vo, host = None ):
  """ Get information for all the SEs for a given VO

:param vo: BDII VO name
:return result structure: result['Value'][siteID]['SEs'][seID]. For
               each siteID, seIDall the BDII/Glue SE/SA parameters are retrieved
  """
  result = ldapSE( '*', vo, host = host )
  if not result['OK']:
    return result
  ses = result['Value']

  siteDict = {}
  for se in ses:
    siteName = se['GlueSiteUniqueID']
    siteDict.setdefault( siteName, { "SEs": {} } )
    seID = se['GlueSEUniqueID']
    siteDict[siteName]["SEs"][seID] = se
    result = ldapSEAccessProtocol( seID, host = host )
    siteDict[siteName]["SEs"][seID]['AccessProtocols'] = {}
    if result['OK']:
      for entry in result['Value']:
        apType = entry['GlueSEAccessProtocolType']
        if apType in siteDict[siteName]["SEs"][seID]['AccessProtocols']:
          count = 0
          for p in siteDict[siteName]["SEs"][seID]['AccessProtocols']:
            if p.startswith( apType+'.' ):
              count += 1
          apType = '%s.%d' % ( apType, count + 1 )       
          siteDict[siteName]["SEs"][seID]['AccessProtocols'][apType] = entry    
        else:  
          siteDict[siteName]["SEs"][seID]['AccessProtocols'][apType] = entry
    else:
      continue    

  return S_OK( siteDict )
