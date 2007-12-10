# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/Attic/GridCredentials.py,v 1.1 2007/12/10 18:15:30 atsareg Exp $
""" Grid Credentials module contains utilities to manage user and host
    certificates and proxies.

    The following utilities are available:

    getGridProxy()
    getCAsLocation()
    getHostCertificateAndKey()
    setDIRACGroup()
    getDIRACGroup()
"""

__RCSID__ = "$Id: GridCredentials.py,v 1.1 2007/12/10 18:15:30 atsareg Exp $"

import os
import os.path
import threading
import socket
import time
import shutil
import tempfile

import DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import systemCall

securityConfPath = "/DIRAC/Security"
PROXY_COMMAND_TIMEOUT = 30

def getGridProxy():
  """ Get the path of the grid proxy file in the DIRAC framework
  """

  #UserProxy
  retVal = gConfig.getOption( '%s/UserProxy' % securityConfPath )
  if retVal[ 'OK' ]:
    filePath = os.path.realpath( retVal[ 'Value' ] )
    if os.path.isfile( filePath ):
      gLogger.verbose( "Using %s/UserProxy value for grid proxy" % securityConfPath )
      return retVal[ 'Value' ]
  #UserProxyPath
  proxyName = "x509up_u%d" % os.getuid()
  retVal = gConfig.getOption( '%s/UserProxyPath' % securityConfPath )
  if retVal[ 'OK' ]:
    for proxyPath in [ "%s/%s" % ( retVal[ 'Value' ], proxyName ), "%s/tmp/%s" % ( retVal[ 'Value' ], proxyName ) ]:
      proxyPath = os.path.realpath( proxyPath )
      if os.path.isfile( proxyPath ):
        gLogger.verbose( "Using %s/UserProxyPath value for grid proxy (%s)" % ( securityConfPath, proxyPath ) )
        return proxyPath
  #Environment vars
  for envVar in [ 'GRID_PROXY_FILE', 'X509_USER_PROXY' ]:
    if os.environ.has_key( envVar ):
      proxyPath = os.path.realpath( os.environ[ envVar ] )
      if os.path.isfile( proxyPath ):
        gLogger.verbose( "Using %s env var for grid proxy" % proxyPath )
        return proxyPath
  #/tmp/x509up_u<uid>
  if os.path.isfile( "/tmp/%s" % proxyName ):
    gLogger.verbose( "Using auto-discovered proxy in /tmp/%s" % proxyName )
    return "/tmp/%s" % proxyName
  #No gridproxy found
  return False

def getActiveGridProxy():
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
      gLogger.verbose( "Using %s/Grid-Security + /certificates as location for CA's" % securityConfPath )
      return casPath
  #CAPath
  retVal = gConfig.getOption( '%s/CALocation' % securityConfPath )
  if retVal[ 'OK' ]:
    casPath = retVal[ 'Value' ]
    gLogger.debug( "Trying %s for CAs" % casPath )
    if os.path.isdir( casPath ):
      gLogger.verbose( "Using %s/CALocation as location for CA's" % securityConfPath )
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
      gLogger.verbose( 'Using %s/%sFile' % ( securityConfPath, fileType.capitalize() ) )
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
          gLogger.verbose( "Using %s for %s" % ( filePath, fileType ) )
          fileDict[ fileType ] = filePath
          fileFound = True
          break
      if fileFound:
        break
  if "cert" not in fileDict.keys() or "key" not in fileDict.keys():
    return False
  return ( fileDict[ "cert" ], fileDict[ "key" ] )

def setDIRACGroup( userGroup ):
  """ Define the user group in the DIRAC framework
  """

  filename = "/tmp/diracGroup-%s" % os.getuid()
  fd = file( filename, "w" )
  fd.write( userGroup )
  fd.close()

def getDIRACGroup( defaultGroup = "none" ):
  """ Get the user group in the DIRAC framework
  """

  filename = "/tmp/diracGroup-%s" % os.getuid()
  try:
    fd = file( filename )
    userGroup = fd.readline()
    fd.close()
    return userGroup.strip()
  except:
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
  if proxy:
    if os.path.exists(proxy):
      cmd = "openssl x509 -noout -text -in %s" % proxy
    else:
      # Create temporary proxy file, do not forget to remove it before leaving
      temp_proxy_file = __makeProxyFile(proxy)
      cmd = "openssl x509 -noout -text -in %s" % temp_proxy_file
  else:
    proxy_file = getActiveGridProxy()
    cmd = "openssl x509 -noout -text -in %s" % proxy_file

  result = systemCall(PROXY_COMMAND_TIMEOUT,cmd)
  if temp_proxy_file:
    os.remove(temp_proxy_file)

  if not result['OK']:
    return S_ERROR('OpenSSL call failed')

  status,output,error = result['Value']

  if status != 0 :
    return S_ERROR('Failed to execute command. Cmd: %s; StdOut: %s; StdErr: %s' % (cmd,output,error))

  text_lines = output.split("\n")
  proxyDict = {}
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
        subject = fields[1].replace(", ","/").strip()
        proxyDict['Subject'] = subject
        # Assume full legacy proxy
        # This should be updated with more modern proxy types
        DN = subject.replace('/CN=proxy','')
        proxyDict['DN'] = DN
      if item == "Issuer":
        issuer = fields[1].replace(", ","/").strip()
        proxyDict['Issuer'] = issuer
      if item == "Serial Number":
        serial = int(fields[1].split()[0])
        proxyDict['Serial'] = serial

  if option:
    try:
      value = proxyDict[option]
      return S_OK(value)
    except KeyValue:
      return S_ERROR('Illegal option '+option)
  else:
    return S_OK(proxyDict)

def getProxyTimeLeft(proxy=None):
  """ Get proxy time left, returns S_OK structure
  """
  return parseProxy(proxy,option="TimeLeft")

def getProxyDN(proxy_file = None):
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
  return S_OK((proxy_file_name,old_proxy))

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

  proxy_file_name = getActiveGridProxy()
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
  fields = string.split(psubject,'/')
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

def createProxy(certfile='',keyfile='',hours=0,bits=512,password=''):
  """ Create a new full legacy proxy and return it as a string
  """
  debug = 0
  openssl = "openssl"
  tmpdir = tempfile.mkdtemp()

  serial = getProxySerial(certfile)
  serialfile = tmpdir+'/cert_serial'
  sfile = open(serialfile,'w')
  sfile.write(str(serial))
  sfile.close()

  configfile = tmpdir+'/proxy_config'
  newkeyfile = tmpdir+'/proxy_key'

  if debug:
    print "\nCreating new certificate request in %s and placing" % (configfile,)
    print "private key in %s:" % (outputfile,)

  configfile = __makeX509ConfigFile(configfile,certfile,bits)

  comm = openssl+" req -new -nodes -config %s -keyout %s " % (configfile,newkeyfile)
  if debug:
    print "\n",comm
  result = systemCall(0,comm)
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
    print "\nSigning certificate request, and append new proxy to",outputfile,":"

  days = int(hours)/24 + 1

  comm = openssl+" x509 -req -in %s -CAkey %s " % (requestfile,keyfile)
  comm = comm + "-extfile %s -extensions ext_section -days %s " % (configfile,days)
  comm = comm + "-CA %s -CAcreateserial -CAserial %s" % (certfile,serialfile)

  if not password:
    passwd = getpass.getpass("Enter GRID pass phrase:")
  else:
    passwd = password

  # Should be done in a secure way
  comm = comm + " -passin pass:%s" % passwd

  if debug:
    print "\n",comm,"\n"

  result = systemCall(0,comm)
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
    print "\nAppending user certificate to",outputfile,":"
    print "\nopenssl x509 -in "+certfile

  result = systemCall(0,openssl+' x509 -in '+certfile)
  if not result['OK']:
    shutil.rmtree(tmpdir)
    return S_ERROR('Failed to read in certificate')

  status,certificate,error = result['Value']
  if status > 0:
    shutil.rmtree(tmpdir)
    return S_ERROR('Failed to read in certificate: '+error)

  result_proxy = proxy+'/n'+newkey+'/n'+certificate
  return S_OK(result_proxy)


