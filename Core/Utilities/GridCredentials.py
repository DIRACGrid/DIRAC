# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/Attic/GridCredentials.py,v 1.18 2008/02/14 13:26:50 atsareg Exp $

""" Grid Credentials module contains utilities to manage user and host
    certificates and proxies.

    The following utilities are available:

    General credentials functions
    getGridProxy()
    getCAsLocation()
    getHostCertificateAndKey()
    setDIRACGroup()
    getDIRACGroup()

    Generic Grid Proxy related functions:
    parseProxy()
    getProxyTimeLeft()
    getProxyDN()
    getCurrentDN()
    getProxySubject()
    getProxyIssuer()
    getProxySerial()
    setupProxy()
    restoreProxy()
    destroyProxy()
    createProxy()
    renewProxy()

    VOMS proxy specific functions:
    createVOMSProxy()
    getVOMSAttributes()
    getVOMSProxyFQAN()
    getVOMSProxyInfo()
"""

__RCSID__ = "$Id: GridCredentials.py,v 1.18 2008/02/14 13:26:50 atsareg Exp $"

import os
import os.path
import threading
import socket
import time
import shutil
import tempfile
import stat
import getpass
import re

import DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import shellCall

securityConfPath = "/DIRAC/Security"
PROXY_COMMAND_TIMEOUT = 30
MAX_PROXY_VALIDITY_DAYS = 7

def getGridProxy():
  """ Get the path of the currently active grid proxy file
  """

  for envVar in [ 'GRID_PROXY_FILE', 'X509_USER_PROXY' ]:
    if os.environ.has_key( envVar ):
      proxyPath = os.path.realpath( os.environ[ envVar ] )
      if os.path.isfile( proxyPath ):
        return proxyPath
  #/tmp/x509up_u<uid>
  proxyName = "x509up_u%d" % os.getuid()
  if os.path.isfile( "/tmp/%s" % proxyName ):
    return "/tmp/%s" % proxyName

  #No gridproxy found
  return False


#Retrieve CA's location
def getCAsLocation():
  """ Retrieve the CA's files location
  """

  #Grid-Security
  retVal = gConfig.getOption( '%s/Grid-Security' % securityConfPath )
  if retVal[ 'OK' ]:
    casPath = "%s/certificates" % retVal[ 'Value' ]
    gLogger.debug( "Trying %s for CAs" % casPath )
    if os.path.isdir( casPath ):
      gLogger.debug( "Using %s/Grid-Security + /certificates as location for CA's" % securityConfPath )
      return casPath
  #CAPath
  retVal = gConfig.getOption( '%s/CALocation' % securityConfPath )
  if retVal[ 'OK' ]:
    casPath = retVal[ 'Value' ]
    gLogger.debug( "Trying %s for CAs" % casPath )
    if os.path.isdir( casPath ):
      gLogger.debug( "Using %s/CALocation as location for CA's" % securityConfPath )
      return casPath
  # Look up the X509_CERT_DIR environment variable
  if os.environ.has_key( 'X509_CERT_DIR' ):
    gLogger.debug( "Using X509_CERT_DIR env var as location for CA's" )
    casPath = os.environ[ 'X509_CERT_DIR' ]
    return casPath
  #rootPath./etc/grid-security/certificates
  casPath = "%s/etc/grid-security/certificates" % DIRAC.rootPath
  gLogger.debug( "Trying %s for CAs" % casPath )
  if os.path.isdir( casPath ):
    gLogger.debug( "Using <DIRACRoot>/etc/grid-security/certificates as location for CA's" )
    return casPath
  #/etc/grid-security/certificates
  casPath = "/etc/grid-security/certificates"
  gLogger.debug( "Trying %s for CAs" % casPath )
  if os.path.isdir( casPath ):
    gLogger.debug( "Using autodiscovered %s location for CA's" % casPath )
    return casPath
  #No CA's location found
  return False

#TODO: Static depending on files specified on CS
#Retrieve certificate
def getHostCertificateAndKey():
  """ Retrieve the host certificate files location
  """

  fileDict = {}
  for fileType in ( "cert", "key" ):
    #Direct file in config
    retVal = gConfig.getOption( '%s/%sFile' % ( securityConfPath, fileType.capitalize() ) )
    if retVal[ 'OK' ]:
      gLogger.debug( 'Using %s/%sFile' % ( securityConfPath, fileType.capitalize() ) )
      fileDict[ fileType ] = retVal[ 'Value' ]
      continue
    else:
      gLogger.debug( '%s/%sFile is not defined' % ( securityConfPath, fileType.capitalize() ) )
    fileFound = False
    for filePrefix in ( "server", "host", "dirac", "service" ):
      #Possible grid-security's
      paths = []
      retVal = gConfig.getOption( '%s/Grid-Security' % securityConfPath )
      if retVal[ 'OK' ]:
        paths.append( retVal[ 'Value' ] )
      paths.append( "%s/etc/grid-security/" % DIRAC.rootPath )
      #paths.append( os.path.expanduser( "~/.globus" ) )
      for path in paths:
        filePath = os.path.realpath( "%s/%s%s.pem" % ( path, filePrefix, fileType ) )
        gLogger.debug( "Trying %s for %s file" % ( filePath, fileType ) )
        if os.path.isfile( filePath ):
          gLogger.debug( "Using %s for %s" % ( filePath, fileType ) )
          fileDict[ fileType ] = filePath
          fileFound = True
          break
      if fileFound:
        break
  if "cert" not in fileDict.keys() or "key" not in fileDict.keys():
    return False
  return ( fileDict[ "cert" ], fileDict[ "key" ] )

def getCertificateAndKey():
  """ Get the locations of the user X509 certificate and key pem files
  """

  certfile = ''
  if os.environ.has_key("X509_USER_CERT"):
    if os.path.exists(os.environ["X509_USER_CERT"]):
      certfile = os.environ["X509_USER_CERT"]
  if not certfile:
    if os.path.exists(os.environ["HOME"]+'/.globus/usercert.pem'):
      certfile = os.environ["HOME"]+'/.globus/usercert.pem'

  if not certfile:
    return False

  keyfile = ''
  if os.environ.has_key("X509_USER_KEY"):
    if os.path.exists(os.environ["X509_USER_KEY"]):
      keyfile = os.environ["X509_USER_KEY"]
  if not keyfile:
    if os.path.exists(os.environ["HOME"]+'/.globus/userkey.pem'):
      keyfile = os.environ["HOME"]+'/.globus/userkey.pem'

  if not keyfile:
     return False

  return (certfile,keyfile)

#Following two functions completely replace VOMS
#It's a horrible hack, but against all odds it works
#(VOMS people: I was forced to do that!)
def setDIRACGroup( userGroup ):
  """ Define the user group in the DIRAC framework
  """
  proxyLocation = getGridProxy()
  fd = file( proxyLocation, "r" )
  proxy = fd.read()
  fd.close()
  new_proxy = setDIRACGroupInProxy(proxy,userGroup)
  mode = os.stat(proxyLocation)[0]
  # Make sure that the proxy is writable
  os.chmod(proxyLocation,mode | stat.S_IWUSR)
  fd = file( proxyLocation, "w" )
  fd.write( new_proxy )
  fd.close()
  # Restore the initial mode
  os.chmod(proxyLocation,mode)

def setDIRACGroupInProxy( proxyData, group ):
  """ Add the DIRAC group string to the proxy. If the group value is None, strip the group
      from the proxy
  """
  proxyLines = proxyData.split( "\n" )
  #Strip previous group
  if proxyLines[0].find( ":::diracgroup=" ) > -1:
    proxyLines.pop(0)

  #If group is not None add it
  if group != None:
    proxyLines.insert( 0, ":::diracgroup=%s" % group.strip() )

  return "%s\n" % "\n".join( proxyLines )

def getDIRACGroup( defaultGroup = "none" ):
  """ Get the user group in the DIRAC framework
  """

  proxyLocation = getGridProxy()
  if not proxyLocation:
    return defaultGroup
  fd = file( proxyLocation, "r" )
  groupLine = fd.readline()
  fd.close()
  if groupLine.find( ":::diracgroup=" ) == 0:
    return groupLine.split( "=" )[1].strip()
  else:
    return defaultGroup

def __makeProxyFile(proxy_string):
  """ Create a random file containing the proxy
  """

  ifd,proxy_file_name = tempfile.mkstemp()
  os.write(ifd,proxy_string)
  os.close(ifd)
  os.chmod(proxy_file_name, stat.S_IRUSR | stat.S_IWUSR)
  return proxy_file_name

def parseProxy(proxy=None,option=None):
  """
  This function is parsing a grid X509 proxy and returns the
  proxy time left in seconds, DN, Subject or Issuer according to option values.
  @type proxy: string or file name
  @param proxy: string containing the proxy or proxy file name
  @type option: string
  @param option: the proxy parameter to be returned. If not given, a dictionary
                 with all the available parameters is returned. Possible values are:
                 "TimeLeft","DN","Subject","Issuer".
  """

  temp_proxy_file=""
  proxy_file=""
  if proxy:
    if os.path.exists(proxy):
      proxy_file = proxy
    else:
      # Create temporary proxy file, do not forget to remove it before leaving
      temp_proxy_file = __makeProxyFile(proxy)
      proxy_file = temp_proxy_file
  else:
    proxy_file = getGridProxy()

  proxy_f = file(proxy_file,'r')
  proxy_line = proxy_f.readline()
  proxy_f.close()
  diracGroup = ""
  if proxy_line.find( ":::diracgroup=" ) == 0:
    diracGroup = proxy_line.split( "=" )[1].strip()

  resultVOMS = isVOMS(proxy_file)
  voms = False
  if resultVOMS['OK']:
    if resultVOMS['Value']:
      voms = True

  cmd = "openssl x509 -noout -text -in %s" % proxy_file
  result = shellCall(PROXY_COMMAND_TIMEOUT,cmd)

  if temp_proxy_file:
    os.remove(temp_proxy_file)

  if not result['OK']:
    result = S_ERROR('OpenSSL call failed')
    result['VOMS'] = voms
    return result

  status,output,error = result['Value']

  if status != 0 :
    result = S_ERROR('Failed to execute command. Cmd: %s; StdOut: %s; StdErr: %s' % (cmd,output,error))
    result['VOMS'] = voms
    return result

  text_lines = output.split("\n")
  proxyDict = {}
  proxyDict['DiracGroup'] = diracGroup
  for line in text_lines:
    fields = line.split(":")
    if len(fields) < 5 and len(fields) > 1:
      item = fields[0].strip()
      if item == "Not After":
        ind = line.find(':')
        date = line[ind+1:].strip()
        date1 = time.strptime(date,"%b %d %H:%M:%S %Y %Z")
        date2 = time.gmtime()
        diff = time.mktime(date1) - time.mktime(date2)
        if diff <= 0:
          proxyDict['TimeLeft'] = 0
        else:
          proxyDict['TimeLeft'] = diff
      if item == "Subject":
        subject = '/'+fields[1].replace(", ","/").strip()
        proxyDict['Subject'] = subject
        # Assume full legacy proxy
        # This should be updated with more modern proxy types
        #DN = subject.replace('/CN=proxy','').replace('/CN=limited proxy','')
        DN = subject

        # Suppress proxy indicators
        cn_list = re.findall('/CN=proxy|/CN=limited proxy+',DN)
        for cn in cn_list:
          DN = DN.replace(cn,'')
        cn_list = re.findall('/CN=[0-9]+$',DN)
        for cn in cn_list:
          DN = DN.replace(cn,'')

        proxyDict['DN'] = DN
      if item == "Issuer":
        issuer = fields[1].replace(", ","/").strip()
        proxyDict['Issuer'] = issuer
      if item == "Serial Number":
        if fields[1]:
          serial = int(fields[1].split()[0])
        else:
          serial = 0
        proxyDict['Serial'] = serial

  if option:
    try:
      value = proxyDict[option]
      result = S_OK(value)
    except KeyValue:
      result = S_ERROR('Illegal option '+option)
  else:
    result = S_OK(proxyDict)

  result['VOMS'] = voms
  return result

def getProxyTimeLeft(proxy=None):
  """ Get proxy time left, returns S_OK structure
  """
  result = parseProxy(proxy,option="TimeLeft")
  timeleft = result['Value']
  if int(timeleft) <= 0:
    return S_OK(0)
  actimeleft = 99999999
  if result['VOMS']:
    result = getVOMSProxyInfo(proxy,'actimeleft')
    actimeleft = result['Value']

  return S_OK(min(timeleft,actimeleft))

def getProxyDN(proxy= None):
  """ Get proxy DN, returns S_OK structure
  """
  return parseProxy(proxy,option="DN")

def getCurrentDN():
  """ Get the currently
  """
  return parseProxy(proxy=None,option="DN")

def getProxySubject(proxy=None):
  """ Get proxy subject, returns S_OK structure
  """
  return parseProxy(proxy,option="Subject")

def getProxyIssuer(proxy = None):
  """ Get proxy issuer, returns S_OK structure
  """
  return parseProxy(proxy,option="Issuer")

def getProxySerial(proxy = None):
  """ Get proxy issuer, returns S_OK structure
  """
  return parseProxy(proxy,option="Serial")

def setupProxy(proxy, fname=None):
  """ Setup the given proxy to be the current proxy
      @type proxy: string
      @param proxy: string containing the proxy
      @type proxy: string
      @param fname: the name of the file where the proxy will be put

      Returns S_OK structure. In case of success returns a tuple of
      the new and old proxy file names
  """

  if os.path.exists(proxy):
    f = open(proxy, 'r')
    proxy_string = f.read()
    f.close()
  else:
    proxy_string = proxy
  try:
    if not fname:
      proxy_file_name = __makeProxyFile(proxy_string)
    else:
      if os.path.exists(fname):
        os.remove(fname)
      f = open(fname, 'w')
      f.write(proxy)
      f.close()
      proxy_file_name = os.path.realpath(fname)
      os.chmod(proxy_file_name, stat.S_IRUSR | stat.S_IWUSR)
  except IOError, x:
    return  S_ERROR('IOError while creating proxy file: '+str(x))

  result = getProxyTimeLeft(proxy_file_name)
  if result["OK"]:
    time_left = int(result["Value"])
  else:
    if os.path.exists(proxy_file_name):
      os.remove(proxy_file_name)
    return S_ERROR('Failed while getProxyTimeLeft() call')
  if time_left <= 0:
    if os.path.exists(proxy_file_name):
      os.remove(proxy_file_name)
    return S_ERROR('Proxy timelife less then or equal to 0')

  # Switch the environment to the new proxy now
  old_proxy = ''
  if os.environ.has_key('X509_USER_PROXY'):
    old_proxy = os.environ['X509_USER_PROXY']
  os.environ['X509_USER_PROXY'] = proxy_file_name
  result = S_OK((proxy_file_name,old_proxy))
  result['TimeLeft'] = time_left
  return result

def restoreProxy(new_proxy,old_proxy):
  """ Restore the proxy valid before the setupProxy() call
  """

  if old_proxy:
    os.environ['X509_USER_PROXY'] = old_proxy
  if os.path.exists(new_proxy):
    os.remove(new_proxy)

def destroyProxy():
  """ Destroy the current user proxy
  """

  proxy_file_name = getGridProxy()
  if os.path.exists(proxy_file_name):
    os.remove(proxy_file_name)
  if os.environ.has_key('X509_USER_PROXY'):
    del os.environ['X509_USER_PROXY']

def __makeX509ConfigFile(configfile,certfile,bits):
  """ Make a configuration file for the new certificate request
  """

  n_bits = 512
  if bits:
    n_bits = int(bits)

  cf = open(configfile,'w')
  cf.write("""
extensions            = ext_section

[ req ]
encrypt_key = no
default_bits = %d
prompt = no
distinguished_name = dn_section

[ ext_section ]
keyUsage = critical, digitalSignature, keyEncipherment, dataEncipherment

[ dn_section ]

""" % n_bits )

  result = getProxySubject(certfile)
  if not result['OK']:
    return None

  subject = result['Value']
  psubject = subject+"/CN=proxy"
  fields = psubject.split('/')
  fieldKeys = {}
  for OID in fields:
    if len( OID ) == 0:
      continue
    key,value = OID.split( "=" )
    if key in fieldKeys:
      count = fieldKeys[key]+1
      fieldKeys[key] = count
    else:
      fieldKeys[key] = 0
      count = 0
    cf.write( "%s.%s = %s\n" % ( count, key,value ) )

  cf.write('\n')
  cf.close()
  return configfile

########################################################################
def createProxy(certfile='',keyfile='',hours=0,bits=512,password=''):
  """ Create a new full legacy proxy and return it as a string
  """
  debug = 0
  openssl = "openssl"
  # Directory where all the temporary files will be put
  tmpdir = tempfile.mkdtemp()

  if hours > MAX_PROXY_VALIDITY_DAYS*24:
    hours = MAX_PROXY_VALIDITY_DAYS*24

  if not certfile:
    result = getCertificateAndKey()
    if result:
      certfile,keyfile = result
  if not certfile:
    return S_ERROR('Grid credentials are not found')

  serial = 1000
  result = getProxySerial(certfile)
  if result['OK']:
    serial = int(result['Value'])
  # Transform int into a hex string
  serial_string = hex(serial).replace('0x','').zfill(4).upper()
  serialfile = tmpdir+'/cert_serial'
  sfile = open(serialfile,'w')
  sfile.write(serial_string)
  sfile.close()

  configfile = tmpdir+'/proxy_config'
  newkeyfile = tmpdir+'/proxy_key'

  if debug:
    print "\nCreating new certificate request in %s" % configfile
    print "and placing private key in %s:" % newkeyfile

  configfile = __makeX509ConfigFile(configfile,certfile,bits)
  comm = openssl+" req -new -nodes -config %s -keyout %s " % (configfile,newkeyfile)
  if debug:
    print "\n",comm
  result = shellCall(0,comm)
  if not result['OK']:
    shutil.rmtree(tmpdir)
    return S_ERROR('Failed to create the proxy request')
  status,output,error = result['Value']
  if status:
    if debug:
      print "--- Error creating proxy certificate!"
      print output
    shutil.rmtree(tmpdir)
    return S_ERROR('Failed to create the proxy request')

  requestfile = configfile+'.req'
  rfile = open(requestfile,'w')
  rfile.write(output)
  rfile.close()

  if debug:
    print "\nSigning certificate request"

  days = int(hours)/24 + 1

  comm = openssl+" x509 -req -in %s -CAkey %s " % (requestfile,keyfile)
  comm = comm + "-extfile %s -extensions ext_section -days %s " % (configfile,days)
  comm = comm + "-CA %s -CAcreateserial -CAserial %s" % (certfile,serialfile)

  if not password:
    passwd = getpass.getpass("Enter GRID pass phrase:")
  else:
    passwd = password

  os.environ['TMPTICKET'] = passwd
  comm = comm + " -passin pass:$TMPTICKET"
  if debug:
    print "\n",comm,"\n"

  result = shellCall(0,comm)
  del os.environ['TMPTICKET']
  if not result['OK']:
    shutil.rmtree(tmpdir)
    return result
  status,proxy,error = result['Value']

  if status > 0:
    shutil.rmtree(tmpdir)
    return S_ERROR('Failed to create proxy: '+error)

  kf = open(newkeyfile,'r')
  newkey = kf.read()

  if debug:
    print "\nAppending user certificate"
    print "\nopenssl x509 -in "+certfile

  result = shellCall(0,openssl+' x509 -in '+certfile)
  if not result['OK']:
    shutil.rmtree(tmpdir)
    return S_ERROR('Failed to read in certificate')

  status,certificate,error = result['Value']
  if status > 0:
    shutil.rmtree(tmpdir)
    return S_ERROR('Failed to read in certificate: '+error)

  shutil.rmtree(tmpdir)
  result_proxy = proxy+newkey+certificate
  return S_OK(result_proxy)

###########################################################################
def isVOMS(proxy):
  """ Determine if the proxy is of VOMS type or not
  """

  temp_proxy_file=""
  if proxy:
    if os.path.exists(proxy):
      cmd = "openssl x509 -noout -text -in %s" % proxy
    else:
      # Create temporary proxy file, do not forget to remove it before leaving
      temp_proxy_file = __makeProxyFile(proxy)
      cmd = "openssl x509 -noout -text -in %s" % temp_proxy_file
  else:
    proxy_file = getGridProxy()
    cmd = "openssl x509 -noout -text -in %s" % proxy_file

  result = shellCall(PROXY_COMMAND_TIMEOUT,cmd)
  if temp_proxy_file:
    os.remove(temp_proxy_file)

  if not result['OK']:
    return S_ERROR('OpenSSL call failed')

  status,output,error = result['Value']

  if status != 0 :
    return S_ERROR('Failed to execute command. Cmd: %s; StdOut: %s; StdErr: %s' % (cmd,output,error))

  if output.find('1.3.6.1.4.1.8005.100.100.5') != -1:
    return S_OK(True)
  else:
    return S_OK(False)

###########################################################################
def renewProxy(proxy,lifetime=72,
                server="myproxy.cern.ch",
                server_key="/opt/dirac/etc/grid-security/serverkey.pem",
                server_cert="/opt/dirac/etc/grid-security/servercert.pem",
                vo='lhcb'):

  """ This function returns the VOMS proxy as a string.
      lifetime is a preferable lifetime of returned proxy.
      server is the address of MyProxy service server.
  """

  #print proxy,lifetime,server,server_key,server_cert,vo

  if lifetime > MAX_PROXY_VALIDITY_DAYS*24:
    lifetime = MAX_PROXY_VALIDITY_DAYS*24

  rm_proxy = 0
  try:
    if not os.path.exists(proxy):
      result = setupProxy(proxy)
      if result["OK"]:
        new_proxy,old_proxy = result["Value"]
        rm_proxy = 1
      else:
        return S_ERROR('Failed to setup given proxy. Proxy is: %s' % (proxy))
    else:
      new_proxy = proxy
  except ValueError:
    return S_ERROR('Failed to setup given proxy. Proxy is: %s' % (proxy))

  # Get voms extensions if any and convert it to option format
  result = getVOMSAttributes(new_proxy,"option")
  if result["OK"]:
    voms_attr = result["Value"]
  else:
    voms_attr = ""

  # Get proxy from MyProxy service
  try:
    f_descriptor,my_proxy = tempfile.mkstemp()
    os.close(f_descriptor)####
  except IOError:
    if os.path.exists(new_proxy) and rm_proxy == 1:
      os.remove(new_proxy)
      restoreProxy(new_proxy,old_proxy)
    return S_ERROR('Failed to create temporary file for store proxy from MyProxy service')
  #os.chmod(my_proxy, stat.S_IRUSR | stat.S_IWUSR)

  # myproxy-get-delegation works only with environment variables
  old_server_key = ''
  if os.environ.has_key("X509_USER_KEY"):
    old_server_key = os.environ["X509_USER_KEY"]
  old_server_cert = ''
  if os.environ.has_key("X509_USER_CERT"):
    old_server_cert = os.environ["X509_USER_CERT"]
  os.environ["X509_USER_KEY"] = server_key
  os.environ["X509_USER_CERT"] = server_cert

  # Here "lifetime + 1" used just for get rid off warning status rised by voms-proxy-init
  cmd = "myproxy-get-delegation -s %s -a %s -d -t %s -o %s" % (server, new_proxy, lifetime + 1, my_proxy)
  result = shellCall(PROXY_COMMAND_TIMEOUT,cmd)

  if not result['OK']:
    return S_ERROR('Call to myproxy-get-delegation failed')
  status,output,error = result['Value']

  # Clean-up files
  if status:
    if os.path.exists(new_proxy) and rm_proxy == 1:
      restoreProxy(new_proxy,old_proxy)
    if os.path.exists(my_proxy):
      os.remove(my_proxy)
    return S_ERROR('Failed to get delegations. Command: %s; StdOut: %s; StdErr: %s' % (cmd,result,error))

  if old_server_key:
    os.environ["X509_USER_KEY"] = old_server_key
  else:
    del os.environ["X509_USER_KEY"]
  if old_server_cert:
    os.environ["X509_USER_CERT"] = old_server_cert
  else:
    del os.environ["X509_USER_CERT"]

  try:
    f = open(my_proxy, 'r')
    proxy_string = f.read() # extended proxy as a string
    f.close()
  except IOError:
    if os.path.exists(new_proxy) and rm_proxy == 1:
      restoreProxy(new_proxy,old_proxy)
    if os.path.exists(my_proxy):
      os.remove(my_proxy)
    return S_ERROR('Failed to read proxy received from MyProxy service')

  if len(voms_attr) > 0:
    result = createVOMSProxy(proxy_string,voms_attr)

    print result

    if result["OK"]:
      proxy_string = result["Value"]
    else:
      if os.path.exists(new_proxy) and rm_proxy == 1:
        restoreProxy(new_proxy,old_proxy)
      if os.path.exists(my_proxy):
        os.remove(my_proxy)
      return S_ERROR('Failed to create VOMS proxy')

  if os.path.exists(new_proxy) and rm_proxy == 1:
    restoreProxy(new_proxy,old_proxy)
  if os.path.exists(my_proxy):
    os.remove(my_proxy)
  return S_OK(proxy_string)

def createVOMSProxy(proxy,attributes="",vo=""):
  """ This function takes a proxy (grid or voms) as a string and returns
      the voms proxy as a string with disaired attributes (second argument)
      OR if vo is set the function will return the voms proxy as a string with
      default attributes.
  """

  voms_attr = attributes
  rm_proxy = 0
  lifetime = 0
  try:
    if not os.path.exists(proxy):
      result = setupProxy(proxy)
      if result["OK"]:
        new_proxy,old_proxy = result["Value"]
        rm_proxy = 1
        result = getProxyTimeLeft(new_proxy)
        if not result["OK"]:
          return S_ERROR('Failed to setup given proxy. Proxy is: %s' % (proxy))
        lifetime = result['Value']
      else:
        return S_ERROR('Failed to setup given proxy. Proxy is: %s' % (proxy))
    else:
      new_proxy = proxy
  except ValueError:
    return S_ERROR('Failed to setup given proxy. Proxy is: %s' % (proxy))

  # Creation of VOMS proxy file
  try:
    f_descriptor,voms_proxy = tempfile.mkstemp()
    os.close(f_descriptor)
  except IOError:
    if os.path.exists(new_proxy) and rm_proxy == 1:
      restoreProxy(new_proxy,old_proxy)
    return S_ERROR('Failed to create temporary file to store VOMS proxy')

  # Lifetime of proxy

  if lifetime > MAX_PROXY_VALIDITY_DAYS*24*3600:
    lifetime = MAX_PROXY_VALIDITY_DAYS*24*3600

  lifetime = lifetime - 300 # lifetime of extension should be less by 5min
  minutes, seconds = divmod(lifetime,60)
  hours, minutes = divmod(minutes,60)

  # VOMS proxy init
  if not len(voms_attr) > 0 and not len(vo) > 0:
    return S_ERROR('Neither VOMS attributes nor VO is set')
  elif len(voms_attr) > 0 and not len(vo) > 0:
    cmd = "voms-proxy-init --voms %s " % voms_attr
  elif not len(voms_attr) > 0 and len(vo) > 0:
    cmd = "voms-proxy-init --voms %s " % vo
  cmd += "-cert %s -key %s -out %s " % (new_proxy,new_proxy,voms_proxy)
  cmd += "-valid %s:%s -vomslife %s:%s" % (hours,minutes,hours,minutes)
  result = shellCall(PROXY_COMMAND_TIMEOUT,cmd)

  if not result['OK']:
    return S_ERROR('Failed to call voms-proxy-init')

  status,result,error = result['Value']
  # Clean-up files
  if status:
    if os.path.exists(new_proxy) and rm_proxy == 1:
      restoreProxy(new_proxy,old_proxy)
    if os.path.exists(voms_proxy):
      os.remove(voms_proxy)
    return S_ERROR('Failed during voms-proxy-init. Command: %s; StdOut: %s; StdErr: %s' % (cmd,result,error))

  # Read voms proxy
  f = open(voms_proxy, 'r')
  result = f.read()
  f.close()

  # Clean-up files
  if os.path.exists(new_proxy) and rm_proxy == 1:
    os.remove(new_proxy)
  if os.path.exists(voms_proxy):
    os.remove(voms_proxy)

  # Write it in special order to conform with the standards
  lines = result.split("\n")
  subjectProxy = {}
  block = []
  for i in lines:
    block.append(i)
    if i.count("-----END"):
      if i.count("CERTIFICATE"):
# write proxy to file
        result = setupProxy("\n".join(block))
        if result["OK"]:
          new_proxy,old_proxy = result["Value"]
        else:
          if os.path.exists(new_proxy):
            os.remove(new_proxy)
          restoreProxy(new_proxy,old_proxy)
          return S_ERROR('Failed to setup given proxy. Proxy is: %s' % ("\n".join(block)) )
# Create subject=proxy pair
        result = getProxySubject(new_proxy)
        if result["OK"]:
          subject = result["Value"]
          subjectProxy[subject] =  "\n".join(block)
        else:
          if os.path.exists(new_proxy):
            os.remove(new_proxy)
          restoreProxy(new_proxy,old_proxy)
          return S_ERROR("Unable to process VOMS proxy. Failed during getDN() call")
# Clean-up
        if os.path.exists(new_proxy):
          os.remove(new_proxy)
        restoreProxy(new_proxy,old_proxy)
      else:
        someKey = "\n".join(block)
      block = []

  keys = subjectProxy.keys()
  keys.sort()
  keys.reverse()
  mark = 1
  for i in keys:
    if mark:
      proxy_string = subjectProxy[i] + "\n" + someKey
      mark = 0
    else:
      proxy_string = proxy_string + "\n" + subjectProxy[i]
  return S_OK(proxy_string)

def getVOMSAttributes(proxy,switch="all"):
  """
  Return VOMS proxy attributes as list elements if switch="all" (default) OR
  return the string prepared to be stored in DB if switch="db" OR
  return the string of elements to be used as the option string in voms-proxy-init
  if switch="option".
  If a given proxy is a grid proxy, then function will return an empty list.
  """

# Setup proxy if it came as string
  rm_proxy = 0
  switch=str(switch)
  try:
    if not os.path.exists(proxy):
      result = setupProxy(proxy)
      if result["OK"]:
        new_proxy,old_proxy = result["Value"]
        rm_proxy = 1
      else:
        return S_ERROR('Failed to setup given proxy. Proxy is: %s' % (proxy))
    else:
      new_proxy = proxy
  except ValueError:
    return S_ERROR('Failed to setup given proxy. Proxy is: %s' % (proxy))

  # Get all possible info from voms proxy
  result = getVOMSProxyInfo(new_proxy,"all")
  if result["OK"]:
    voms_info_output = result["Value"]
    voms_info_output = voms_info_output.split("\n")
  else:
    if os.path.exists(new_proxy) and rm_proxy == 1:
      restoreProxy(new_proxy,old_proxy)
    return S_ERROR('Failed to extract info from proxy. Given proxy is: %s' % (new_proxy))

  # Clean-up files
  if os.path.exists(new_proxy) and rm_proxy == 1:
    restoreProxy(new_proxy,old_proxy)

  # Parse output of voms-proxy-info command
  attributes = []
  voName = ''
  for i in voms_info_output:
    j = i.split(":")
    if j[0].strip() == "VO":
      voName = j[1].strip()
    elif j[0].strip()=="attribute":
      # Cut off unsupported Capability selection part
      j[1] = j[1].replace("/Capability=NULL","")
      if j[1].find('Role=NULL') == -1 and j[1].find('Role') != -1:
        attributes.append(j[1].strip())

  # Sorting and joining attributes
  if switch == "db":
    attributes.sort()
    returnValue = ":".join(attributes)
  elif switch == "option":
    if len(attributes)>1:
      returnValue = voName+" -order "+' -order '.join(attributes)
    elif attributes:
      returnValue = voName+":"+attributes[0]
    else:
      returnValue = voName
  elif switch == 'all':
    returnValue = attributes

  return S_OK(returnValue)

def getVOMSProxyFQAN(proxy):
  """ Get the VOMS proxy fqan attributes
  """
  return getVOMSProxyInfo(proxy,"fqan")

def getVOMSProxyInfo(proxy_file,option=None):
  """ Returns information about a proxy certificate (both grid and voms).
      Available information is:
        1. Full (grid)voms-proxy-info output
        2. Proxy Certificate Timeleft in seconds (the output is an int)
        3. DN
        4. voms group (if any)
      @type  proxy_file: a string
      @param proxy_file: the proxy certificate location.
      @type  option: a string
      @param option: None is the default value. Other option available are:
        - timeleft
        - actimeleft
        - identity
        - fqan
        - all
      @rtype:   tuple
      @return:  status, output, error, pyerror.
  """

  options = ['actimeleft','timeleft','identity','fqan','all']
  if option:
    if option not in options:
      S_ERROR('Non valid option %s' % option)
  rm_proxy = 0
  try:
    if not os.path.exists(proxy_file):
      result = setupProxy(proxy_file)
      if result["OK"]:
        new_proxy,old_proxy = result["Value"]
        rm_proxy = 1
      else:
        return S_ERROR('Failed to setup given proxy. Proxy is: %s' % (proxy_file))
    else:
      new_proxy = proxy_file
  except ValueError:
    return S_ERROR('Failed to setup given proxy. Proxy is: %s' % (proxy))

  #a = ''
  b = ''
  c = ''
  #if os.environ.has_key('X509_CERT_DIR'):
  #  a = os.environ['X509_CERT_DIR']
  if os.environ.has_key('X509_USER_CERT'):
    b = os.environ['X509_USER_CERT']
  if os.environ.has_key('X509__USER_KEY'):
    c = os.environ['X509__USER_KEY']
  #os.putenv('X509_CERT_DIR','/etc/grid-security/certificates')
  servercert = gConfig.getValue('DIRAC/Security/CertFile','opt/dirac/etc/grid-security/servercert.pem')
  os.putenv('X509_USER_CERT',servercert)
  serverkey = gConfig.getValue('DIRAC/Security/KeyFile','opt/dirac/etc/grid-security/serverkey.pem')
  os.putenv('X509_USER_KEY',serverkey)

  cmd = 'voms-proxy-info -file %s' % new_proxy
  if option:
    cmd += ' -%s' % option

  result = shellCall(20,cmd)
  if not result['OK']:
    return S_ERROR('Failed to call voms-proxy-info')

  status, output, error = result['Value']

  #os.putenv('X509_CERT_DIR',a)
  os.putenv('X509_USER_KEY',b)
  os.putenv('X509_USER_CERT',c)

  if option == 'fqan':
    if output:
      output = output.split('/Role')[0]
    else:
      output = '/lhcb'
  if status:
    return S_ERROR('Failed to get delegations. Command: %s; StdOut: %s; StdErr: %s' % (cmd,output,error))
  else:
    return S_OK(output)
