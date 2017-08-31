"""
Probes for DIRAC hosts
"""

import GSI
import urllib
import ssl
import xml.etree.ElementTree as ET
from sets import Set
from StringIO import StringIO
import socket
import os
import re
from datetime import datetime
from DIRAC.Core.Utilities import Os
from DIRAC import S_OK, S_ERROR, gConfig, rootPath, gLogger
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.Core.Utilities.Time import dateTime
from DIRAC.ConfigurationSystem.Client.Helpers.Registry  import getVOs, getVOOption
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.Core.Security.Locations import getHostCertificateAndKeyLocation, getCAsLocation, getVOMSESLocation, getVOMSDIRLocation
from DIRAC.FrameworkSystem.Client.ComponentMonitoringClient import ComponentMonitoringClient


def probeCA():
  """ CAs check test
  """

  result = dict()

  # Test property
  minDaysCA = 10
  minDaysCRL = 5
  Msg = list()

  gLogger.info('CA check..')
  
  # EGI repo properties
  repos = ['ca-policy-egi-core','ca-policy-lcg']
  repoURLs = ['http://repository.egi.eu/sw/production/cas/1/current/meta/' + u for u in repos]
  # Get DNs, ObsolDNs, RPMs
  repoDNs = list()
  repoDNsOld = list()
  repoRPMs = list()
  for repo in repoURLs:
    Subjects = urllib.urlopen(repo + '.subjectdn').readlines()
    getDNs = [s.strip() for s in Subjects if not s.startswith('#')]
    SubjectsOld = urllib.urlopen(repo + '.obsoleted-subjectdn').readlines()
    getDNsOld = [s.strip() for s in SubjectsOld if not s.startswith('#')]
    RPMs = urllib.urlopen(repo + '.list').readlines()
    getRPMs = [s.strip() for s in RPMs if not s.startswith('#')]
    repoDNs += getDNs
    repoDNsOld += getDNsOld
    repoRPMs += getRPMs
  getVersion = urllib.urlopen(repoURLs[0] + '.release').readlines()
  repoVersion = re.findall(r'\d.\d\d-\d', ''.join(getVersion))[0]

  # Get local CA RPMs
  if not Os.which('rpm'):
    gLogger.info( 'Missing rpm commandline' )
  else:
    getRPMs = shellCall( 100, 'rpm -qa' )['Value'][1].split('\n')
    caRPMs = [r for r in getRPMs if re.match(r'ca[_-].*\-\d\.\d\d\-\d\.noarch', r)]

  # Check contain repo RPMs in local find RPMs
  upRPMs = list()
  missRPMs = list()
  if caRPMs:
    locRPMNames = [re.sub(r'\-\d\.\d\d\-\d\.noarch','',n) for n in caRPMs]
    for repoRPM in repoRPMs:
      repoRPMName = re.sub(r'\-\d\.\d\d\-\d','',repoRPM)
      if repoRPMName in locRPMNames:
        for locRPM in caRPMs:
          locRPMName = re.sub(r'\-\d\.\d\d\-\d\.noarch','',locRPM)
          locRPMVers = re.findall(r'\d\.\d\d\-\d',locRPM)[0]
          if (repoRPMName == locRPMName) and (repoVersion != locRPMVers):
            upRPMs.append(repoRPMName+'-'+locRPMVers)
      else:
        missRPMs.append(repoRPM)
    if missRPMs:
      gLogger.info('Not installed next package(s):\n %s' % missRPMs)
      Msg.append('WARNING: %s is not installed.\n' % missRPMs)
    if upRPMs:
      gLogger.info('Not updated to current(%s) vesrsion next package(s):\n%s' % (repoVersion,','.join(upRPMs)))
      Msg.append('WARNING: Package %s is need update to %s version.\n' % (','.join(upRPMs),repoVersion))
  else:
    gLogger.info('CA RPMs not installed.')
    Msg.append('WARNING: CA RPMs not installed.\n')

  # CA certificates properties
  pathCA = getCAsLocation()+'/'
  gLogger.info('CA location: %s' % pathCA)
  # Get CA and CRL locations lists
  hashes = [f.split('.')[0] for f in os.listdir(pathCA) if re.match(r'.*\.[0-9]', f)]
  missCRLs = list()
  for hashCA in hashes:
    if not os.path.exists(pathCA+hashCA+'.r0'):
      missCRLs.append(str(pathCA))
  if missCRLs:
#      gLogger.info('crls %s is missing.' % missCRLs)
    Msg.append('WARNING: crls %s is missing.\n' % missCRLs)

  cas = [f for f in os.listdir(pathCA) if re.match(r'.*\.[0-9]', f)]
  crls = [f for f in os.listdir(pathCA) if re.match(r'.*\.r[0-9]', f)]

  # Get DNs
  caDNs = list()
  for ca in cas:
    # Read CA.0 file
    fObj = file( str(pathCA+ca), "rb" )
    pemData = fObj.read()
    fObj.close()
    # Load cert and read subject
    caCert = GSI.crypto.load_certificate( GSI.crypto.FILETYPE_PEM, pemData )
    caDN = caCert.get_subject().one_line()
    caDNs.append(caDN)
    
    # Check expired
    if caCert.has_expired():
      if caDN in repoDNs:
        gLogger.error('%s expired!' % caDN)
        Msg.append('  ERROR: %s expired!\n' % caDN)
      else:
        gLogger.info('%s expired' % caDN)
        Msg.append('   INFO: %s expired (not from thrust repos).\n' % caDN)
      continue

    caNoAfter = caCert.get_not_after()
    if not caNoAfter:
      #gLogger.info('GSI module return None for notAfter parameter, try get it from openssl command')
      if not Os.which('openssl'):
        gLogger.info('Missing openssl commandline')
        continue
      shellResult = shellCall( 100, "openssl x509 -in %s -noout -enddate" % str(pathCA+ca) )
      caNoAfter = shellResult['Value'][1].split('=')[1].strip()
      caNoAfter = datetime.strptime(caNoAfter, "%b %d %H:%M:%S %Y %Z")
      if not caNoAfter:
        gLogger.info('Can`t get notAfter from ' + ca)
        #gLogger.info(caDN+' unknown validation state.')
        Msg.append('WARNING: %s unknown validation state.\n' % caDN)
        continue

    left = ( caNoAfter - dateTime() ).days
    if left < minDaysCA:
      #gLogger.info('For %s %s day(s) left' % caDN left)
      Msg.append('WARNING: %s day(s) left for %s.\n' % (left,ca))

  # Check contain repo DNs in local DNs
  for dn in repoDNs:
    if not dn in caDNs:
      #gLogger.error('%s no found' % dn)
      Msg.append('  ERROR: %s missing.\n' % dn)

  # Check contain local DNs in repo obsoleted DNs
  for dn in caDNs:
    if dn in repoDNsOld:
      #gLogger.info('%s is obsoleted' % ca)
      Msg.append('   INFO: %s is obsoleted.\n' % ca)

  status = 'OK'
  for line in Msg:
    if 'WARNING:' in line:
      status = 'WARNING'
    if 'ERROR:' in line:
      status = 'CRITICAL'
      break

  gLogger.info("Result: %s" % status)

  result['Probe'] = 'CA'
  result['Status'] = status
  result['Errors'] = len([m for m in Msg if 'ERROR:' in m])
  result['Warnings'] = len([m for m in Msg if 'WARNING:' in m])
  result['Infos'] = len([m for m in Msg if 'INFO:' in m])
  result['Messages'] = ''.join(Msg)
  return S_OK(result)

def probeVO( vo ):
  """ VOs check test
  """

  def sslVOMSServer(hostname,port):
    """ Check openssl accsses to VOMS and get attributs.
    """
    certKey = getHostCertificateAndKeyLocation()
    if certKey:
      hostCertKey = "-cert %s -key %s" % (certKey[0],certKey[1])

    result = shellCall( 100, "echo | openssl s_client -connect %s:%s %s 2>/dev/null | openssl x509" % (hostname,port,hostCertKey) )
    if not result['OK']:
      return False 
    
    cert=''.join(re.findall(r'-{5}BEGIN CERTIFICATE-{5}.*-{5}END CERTIFICATE-{5}', \
        ''.join(result['Value'][1]),re.DOTALL))
    if not cert:
      return False
    fObj = StringIO(cert)
    pemData = fObj.read()
    fObj.close()
    cert = GSI.crypto.load_certificate( GSI.crypto.FILETYPE_PEM, pemData )

    result = dict()
    result['CA'] = cert.get_issuer().one_line()
    result['DN'] = cert.get_subject().one_line()
    result['Expired'] = cert.has_expired()      
    return result
    
  # Test property
  Msg = list()

  gLogger.info('==> VO \"%s\" check..' % vo)

  # Check needed tools
  if not Os.which('openssl'):
    return S_ERROR('Not found openssl command.')

  xmlVOIDs = dict()
  getUrl='http://operations-portal.egi.eu/xml/voIDCard/public/voname/'
  # Get info xml about current vo from repo
  #voIDs={'<voname_1>':[{'hostname':{'port':<port>,'subject':<DN>,'issue':[<DN1>,<DN2>,..],'pem':<PEM>}},
  #                     {..}],
  #       '<voname_N>':[{'hostname':{'port':<port>,'subject':<DN>,'issue':[<DN1>,<DN2>,..],'pem':<PEM>}},
  #                     {}]} 
  try:
    xml=ET.parse(urllib.urlopen(getUrl+vo)).getroot()
  except Exception, excp:
    gLogger.error("Can`t upload %s xml file. Error:%s" % ((getUrl+vo),excp))
    Msg.append("ERROR:%s Can`t get %s xml file.\n" % (getUrl+vo))
    return S_ERROR(excp)
  gLogger.info("Info from repo uploaded.")

  # Get dictiration from xml file and check ssl connection
  xmlDict = {}
  for vomsServer in xml.iter('VOMS_Server'):
    xmlHostname = vomsServer.findtext('hostname')
    xmlPort = vomsServer.get('VomsesPort')
    xmlDN = vomsServer.findtext('./X509Cert/DN')
    xmlCA = vomsServer.findtext('./X509Cert/CA_DN')
    sslVOMS = sslVOMSServer(xmlHostname,xmlPort) 
    if sslVOMS:
      if (sslVOMS['DN'] == xmlDN) and (sslVOMS['CA'] in xmlCA):
        xmlDict[xmlHostname]={'port':xmlPort,'DN':xmlDN,'CA':xmlCA}
  xmlVOMSs = Set(xmlDict.keys())
  
  # Check vomses and vomsdir
  vmssPath = getVOMSESLocation()
  vmsdrPath = getVOMSDIRLocation()
  if vmssPath:
    if vo in [f for f in os.listdir(vmssPath) if os.path.isfile(vmssPath+'/'+f)]:
      with open(vmssPath+'/'+vo) as f:
        lines = f.readlines()
      vmssDict = dict()
      for line in [l.strip('\"').split('\" \"') for l in lines]:
        vmssVO = line[0]
        vmssHostname = line[1]
        vmssPort = line[2]
        vmssDN = line[3]
        if vmssVO != vo:
          gLogger.error('In %s set incorrect vo(%s), true is %s.' % (vmssPath+'/'+vo,vmssVO,vo))
          Msg.append('ERROR: In %s set incorrect vo(%s), true is %s.' % (vmssPath+'/'+vo,vmssVO,vo))
        vmssDict[vmssHostname] = {'port':vmssPort,'DN':vmssDN} 
      vmssVOMSs = vmssDict.keys()
    else:
      gLogger.error('Settings in vomses is absend.')
  else:
    gLogger.error("Directory vomses not exist.")
    Msg.append("ERROR: %s not found." % vmssPath)

  if vmsdrPath:
    if vo in os.listdir(vmsdrPath):
      vmsdrVOMSs = Set([f.strip('.lsc') for f in os.listdir(vmsdrPath+'/'+vo) if os.path.isfile(vmsdrPath+'/'+vo+'/'+f)])
    else:
      gLogger.error('Settings in vomsdir is absend.')
      Msg.append("ERROR: %s/%s not found." % (vmsdrPath,vo))
  else:
    gLogger.error("Directory vomsdir not exist.")
    Msg.append("ERROR: %s not found." % vmsdrPath)

  # Get list VOMSs from cfg of DIRAC
  getResult = gConfig.getSections('/Registry/VO/%s/VOMSServers/' %  vo)
  if getResult['OK']:
    cfgVOMSs=Set(getResult['Value'])
  else:
    Msg.append("ERROR: VOMSes are not set.\n")
  
  # Comparison VOMSes between DIRAC cfg & repo xml
  gLogger.info("Compration VOMSes.")
  for hostname in cfgVOMSs|xmlVOMSs:
    logAdd = '%s:' % hostname
    
    # VOMSServer not in DIRAC cfg
    if hostname in xmlVOMSs-cfgVOMSs:
      gLogger.warn('%s VOMS server found to add.' % logAdd)
      gLogger.warn('%s |_Port=\"%s\"' % (logAdd,xmlDict[hostname]['port']))
      gLogger.warn('%s |_DN=\"%s\"' % (logAdd,xmlDict[hostname]['DN']))
      gLogger.warn('%s \_CA=\"%s\"' % (logAdd,xmlDict[hostname]['CA']))
      Msg.append('WARNING:%s VOMS server found to add.\n' % logAdd)
      Msg.append('WARNING:%s |_Port=\"%s\"\n' % (logAdd,xmlDict[hostname]['port']))
      Msg.append('WARNING:%s |_DN=\"%s\"\n' % (logAdd,xmlDict[hostname]['DN']))
      Msg.append('WARNING:%s \_CA=\"%s\"\n' % (logAdd,xmlDict[hostname]['CA']))
    else:
      cfgPort=''
      cfgDN=''
      cfgCA=''
      getResult=gConfig.getOption('/Registry/VO/%s/VOMSServers/%s/Port' % (vo,hostname))
      if getResult['OK']:
        cfgPort=getResult['Value']
      getResult=gConfig.getOption('/Registry/VO/%s/VOMSServers/%s/DN' % (vo,hostname))
      if getResult['OK']:
        cfgDN=getResult['Value']
      getResult=gConfig.getOption('/Registry/VO/%s/VOMSServers/%s/CA' % (vo,hostname))
      if getResult['OK']:
        cfgCA=getResult['Value']
      #vomsdir
      if hostname in vmsdrVOMSs:
        with open(vmsdrPath+'/'+vo+'/'+hostname+'.lsc') as f:
          lines = f.readlines()
        lscDN = [l.strip('\n') for l in lines][0]
        if lscDN != cfgDN:
          gLogger.error('%s DN=\"%s\" on lsc file set incorect. In DIRAC configuration is \"%s\"' % (logAdd,lscDN,cfgDN))
          Msg.append('  ERROR:%s DN=\"%s\" on lsc file set incorect. In DIRAC configuration is \"%s\"\n' % (logAdd,lscDN,cfgDN))
        lscCA = [l.strip('\n') for l in lines][1]
        if lscCA != cfgCA:
          gLogger.error('%s CA=\"%s\" on lsc file set incorect. In DIRAC configuration is \"%s\"' % (logAdd,lscCA,cfgCA))
          Msg.append('  ERROR:%s CA=\"%s\" on lsc file set incorect. In DIRAC configuration is \"%s\"\n' % (logAdd,lscCA,cfgCA))
      else:
        gLogger.warn('%s Not exist %s' % (logAdd,vmsdrPath+'/'+vo+'/'+hostname+'.lsc'))
        gLogger.warn('%s |_DN=\"%s\"' % (logAdd,cfgDN))
        gLogger.warn('%s \_CA=\"%s\"' % (logAdd,cfgCA))
        Msg.append('  ERROR:%s Not exist %s\n' % (logAdd,vmsdrPath+'/'+vo+'/'+hostname+'.lsc'))
        Msg.append('  ERROR:%s |_DN=\"%s\"\n' % (logAdd,cfgDN))
        Msg.append('  ERROR:%s \_CA=\"%s\"\n' % (logAdd,cfgCA))
      #vomses
      if hostname in vmssVOMSs:
        if vmssDict[hostname]['DN'] != cfgDN:
          gLogger.error('%s DN=\"%s\" on vomses file set incorect. In DIRAC configuration is \"%s\"' % (logAdd,vmssDict[hostname]['DN'],cfgDN))
          Msg.append('  ERROR:%s DN=\"%s\" on vomses file set incorect. In DIRAC configuration is \"%s\"\n' % (logAdd,vmssDict[hostname]['DN'],cfgDN))
        if vmssDict[hostname]['port'] != cfgPort:
          gLogger.error('%s port=\"%s\" on vomses file set incorect. In DIRAC configuration is \"%s\"' % (logAdd,vmssDict[hostname]['port'],cfgPort))
          Msg.append('  ERROR:%s port=\"%s\" on vomses file set incorect. In DIRAC configuration is \"%s\"\n' % (logAdd,vmssDict[hostname]['port'],cfgPort))
      else:
        gLogger.warn('%s In vomses file not exit line from %s VOMS' % (logAdd,hostname))
        gLogger.warn('%s |_Port=\"%s\"' % (logAdd,cfgPort))
        gLogger.warn('%s \_DN=\"%s\"' % (logAdd,cfgDN))
        Msg.append('  ERROR:%s In vomses file not exit line from %s VOMS\n' % (logAdd,hostname))
        Msg.append('  ERROR:%s |_Port=\"%s\"\n' % (logAdd,cfgPort))
        Msg.append('  ERROR:%s \_DN=\"%s\"\n' % (logAdd,cfgDN))
      
      # VOMSServer not in Thrust xml
      if hostname in cfgVOMSs-xmlVOMSs:
        sslVOMS = sslVOMSServer(hostname,cfgPort) 
        if not sslVOMS:
          gLogger.error('%s VOMS server and/or port %s incorect.' % (logAdd,cfgPort))
          Msg.append('  ERROR:%s VOMS server and/or port %s incorect.\n' % (logAdd,cfgPort))
        elif sslVOMS['DN'] and sslVOMS['CA']:
          gLogger.info('%s VOMS not found, but it connect on port %s!' % (logAdd,cfgPort))
          Msg.append('   INFO:%s VOMS not found, but it connect on port %s!\n' % (logAdd,cfgPort))
          if cfgDN != sslVOMS['DN']:
            gLogger.error('%s  DN=\"%s\" set incorect. True is \"%s\"' % (logAdd,cfgDN,sslVOMS['DN']))
            Msg.append('  ERROR:%s  DN=\"%s\" set incorect, true is \"%s\"\n' % (logAdd,cfgDN,sslVOMS['DN']))
          if cfgCA != sslVOMS['CA']:
            gLogger.error('%s  CA=\"%s\" set incorect. True is \"%s\"' % (logAdd,cfgCA,sslVOMS['CA']))
            Msg.append('  ERROR:%s  CA=\"%s\" set incorect, true is \"%s\"\n' % (logAdd,cfgCA,sslVOMS['CA']))
        else:
          gLogger.error('%s VOMS connect on port %s, but can`t get certificat.' % (logAdd,cfgPort))
          Msg.append('  ERROR:%s VOMS connect on port %s, but can`t get certificat.\n' % (logAdd,cfgPort))
      
      # VOMSServer in DIRAC cfg and Trust xml
      else:
        if cfgPort != xmlDict[hostname]['port']:
          sslVOMS = sslVOMSServer(hostname,cfgPort) 
          if not sslVOMS:
            gLogger.error('%s Port=%s can`t connect. Use %s' % (logAdd,cfgPort,xmlDict[hostname]['port']))
            Msg.append('  ERROR:%s Port=%s can`t connect. Use %s\n' % (logAdd,cfgPort,xmlDict[hostname]['port']))
          elif sslVOMS['DN'] and sslVOMS['CA']:
            gLogger.warn('%s Port=%s not default(%s), but it connect!' % (logAdd,cfgPort,xmlDict[hostname]['port']))
            Msg.append('WARNING:%s Port=%s not default(%s), but it connect!\n' % (logAdd,cfgPort,xmlDict[hostname]['port']))
            if cfgDN != sslVOMS['DN']:
              gLogger.error('%s  DN=\"%s\" set incorect. True is \"%s\"' % (logAdd,cfgDN,sslVOMS['DN']))
              Msg.append('  ERROR:%s  DN=\"%s\" set incorect, true is \"%s\"\n' % (logAdd,cfgDN,sslVOMS['DN']))
            if cfgCA != sslVOMS['CA']:
              gLogger.error('%s  CA=\"%s\" set incorect. True is \"%s\"' % (logAdd,cfgCA,sslVOMS['CA']))
              Msg.append('  ERROR:%s  CA=\"%s\" set incorect, true is \"%s\"\n' % (logAdd,cfgCA,sslVOMS['CA']))
          else:
            gLogger.error('%s Port=%s connect, but can`t get certificat. Use %s' % (logAdd,cfgPort,xmlDict[hostname]['port']))
            Msg.append('  ERROR:%s Port=%s connect, but can`t get certificat. Use %s\n' % (logAdd,cfgPort,xmlDict[hostname]['port']))
        if cfgDN != xmlDict[hostname]['DN']:
          gLogger.error('%s DN=\"%s\" incorect, true is \"%s\"' % (logAdd,cfgDN,xmlDict[hostname]['DN']))
          Msg.append('  ERROR:%s DN=\"%s\" incorect, true is \"%s\"\n' % (logAdd,cfgDN,xmlDict[hostname]['DN']))
        if not cfgCA in xmlDict[hostname]['CA']:
          gLogger.error('%s CA=\"%s\" incorect, true is \"%s\"' % (logAdd,cfgCA,xmlDict[hostname]['CA']))
          Msg.append('  ERROR:%s CA=\"%s\" incorect, true is \"%s\"\n' % (logAdd,cfgCA,xmlDict[hostname]['CA']))
    
  status = 'OK'
  for line in Msg:
    if 'WARNING:' in line:
      status = 'WARNING'
    if 'ERROR:' in line:
      status = 'CRITICAL'
      break
  
  gLogger.info("Result: %s\n" % status)
  
  result = dict()
  result['Probe'] = 'VO:%s' % vo
  result['Status'] = status
  result['Errors'] = len([m for m in Msg if 'ERROR:' in m])
  result['Warnings'] = len([m for m in Msg if 'WARNING:' in m])
  result['Infos'] = len([m for m in Msg if 'INFO:' in m])
  result['Messages'] = ''.join(Msg)

  return S_OK(result)

types_getHostStatus = []
def export_getHostStatus( self ):
  """ Retrieve host parameters
  """
  client = ComponentMonitoringClient()
  result = client.getStatus( socket.getfqdn() )
  if result[ 'OK' ]:
    return S_OK( result[ 'Value' ][0] )
  else:
    return self.__probeCA()

def submitProbeCA():
  """
  Retrieves and stores into a MySQL database information about the host state
  """
  result = probeCA()

  if not result[ 'OK' ]:
    gLogger.error( result[ 'Message' ] )
    return result
  fields = result[ 'Value' ]
  fields[ 'HostName' ] = socket.getfqdn()
  fields[ 'Timestamp' ] = datetime.utcnow()

  client = ComponentMonitoringClient()
  result = client.updateStatus( fields )
  if not result[ 'OK' ]:
    gLogger.error( result[ 'Message' ] )
    return result

  return S_OK( 'Status information added correctly' )

def submitProbeVO():
  """
  Retrieves and stores into a MySQL database information about the host state
  """

  for vo in getVOs():  
    result = probeVO(vo)
    if not result[ 'OK' ]:
      gLogger.error( result[ 'Message' ] )
      return result
    fields = result['Value']
    fields[ 'HostName' ] = socket.getfqdn()
    fields[ 'Timestamp' ] = datetime.utcnow()
    
    client = ComponentMonitoringClient()
    result = client.updateStatus( fields )
    if not result[ 'OK' ]:
      gLogger.error( result[ 'Message' ] )
      return S_ERROR( result[ 'Message' ] )

  return S_OK( 'Status information added correctly' )

def notifyStatus():
  """
  Notification probe status 
  """  
  host = socket.getfqdn()
  
  client = ComponentMonitoringClient()
  result = client.getStatus(host,'')
  if not result[ 'OK' ]:
    gLogger.error( result[ 'Message' ] )
    return result

  msg = 'Host: '+host+'\n'
  for val in result['Value']:
    if val['Status'] != 'OK':
      msg += '\nProbe \"'+val['Probe']+'\" status: '+val['Status']+' ('+str(val['Timestamp'])+')\n'
      msg += '[errors:'+val['Errors']+',warnings:'+val['Warnings']+',infos:'+val['Infos']+']\n'
      msg += '==logs==\n'
      msg += val['Messages']
      msg += '========\n'
  
  result = NotificationClient().sendMail( 'yokutayk@gmail.com', "Status "+host, msg, 'dirac@cyf-kr.edu.pl', False)
  if not result[ 'OK' ]:
    gLogger.error( result[ 'Message' ] )
    return result

  return S_OK( 'Status notify correctly' )