
import DIRAC
from DIRAC.Core.Security import Locations

class MyProxy:

  def __init__( self,
                maxProxyHoursLifeTime = False,
                server = False,
                serverCert = False,
                serverKey = False,
                voName = False ):
    if not maxProxyDaysLifeTime:
      self.__maxProxyLifeTime = 7 * 24
    else:
      self.__maxProxyLifeTime = maxProxyHoursLifeTime
    if not server:
      self.__mpServer = "myproxy.cern.ch"
    else:
      self.__mpServer = server
    if not voName:
      self.__mpVO = gConfig.getValue( "/DIRAC/VirtualOrganization", "" )
    else:
      self.__mpVO = voName
    ckLoc = Locations.getHostCertificateAndKeyLocation()
    if serverCert:
      self.__mpCertLoc = serverCert
    else:
      if ckLoc:
        self.__mpCertLoc = ckLoc[0]
      else:
        self.__mpCertLoc = "%s/etc/grid-security/servercert.pem" % DIRAC.rootPath
    if serverKey:
      self.__mpKeyLoc = serverKey
    else:
      if ckLoc:
        self.__mpKeyLoc = ckLoc[1]
      else:
        self.__mpKeyLoc = "%s/etc/grid-security/serverkey.pem" % DIRAC.rootPath

def renewProxy( proxy, lifeTime = 72, ):
  """ This function returns the VOMS proxy as a string.
      lifetime is a preferable lifetime of returned proxy.
      server is the address of MyProxy service server.
  """

  if lifetime > self.__maxProxyLifeTime:
    lifetime = self.__maxProxyLifeTime

  # Get voms extensions if any and convert it to option format
  result = getVOMSAttributes(proxy,"option")
  if result["OK"]:
    voms_attr = result["Value"]
  else:
    voms_attr = ""

  # Get extended plain proxy from the MyProxy server
  result = getMyProxyDelegation(proxy,lifetime,server,server_key,server_cert)
  if not result['OK']:
    return result

  proxy_string = result['Value']

  if len(voms_attr) > 0:
    result = createVOMSProxy(proxy_string,voms_attr)
    if result["OK"]:
      proxy_string = result["Value"]
    else:
      return S_ERROR('Failed to create VOMS proxy')

  return S_OK(proxy_string)


def getDelegation( proxy, lifeTime = 72 ):
  """ Get delegated proxy from MyProxy server
  """

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

  try:
    f_descriptor,my_proxy = tempfile.mkstemp()
    os.close(f_descriptor)
  except IOError:
    if os.path.exists(new_proxy) and rm_proxy == 1:
      os.remove(new_proxy)
      restoreProxy(new_proxy,old_proxy)
    return S_ERROR('Failed to create temporary file for store proxy from MyProxy service')
  #os.chmod(my_proxy, stat.S_IRUSR | stat.S_IWUSR)

  # myproxy-get-delegation works only with environment variables

  environment = {}
  environment["PATH"] = os.environ['PATH']
  environment["LD_LIBRARY_PATH"] = os.environ['LD_LIBRARY_PATH']
  environment["X509_USER_KEY"] = server_key
  environment["X509_USER_CERT"] = server_cert

  # Here "lifetime + 1" used just to get rid off warning status raised by the voms-proxy-init
  cmd = "myproxy-get-delegation -s %s -a %s -d -t %s -o %s" % (server, new_proxy, lifetime + 1, my_proxy)
  start = time.time()
  result = shellCall(PROXY_COMMAND_TIMEOUT,cmd,env=environment)
  query_time = time.time() - start
  #print "#################################################"
  #print cmd
  #print "#################################################"
  #print "myproxy-get-delegation took %f.2 seconds" % query_time
  #print result

  if not result['OK']:
    if os.path.exists(new_proxy) and rm_proxy == 1:
      os.remove(new_proxy)
      restoreProxy(new_proxy,old_proxy)
    return S_ERROR('Call to myproxy-get-delegation failed')

  status,output,error = result['Value']

  # Clean-up files
  if status:
    if os.path.exists(new_proxy) and rm_proxy == 1:
      restoreProxy(new_proxy,old_proxy)
    if os.path.exists(my_proxy):
      os.remove(my_proxy)
    return S_ERROR('Failed to get delegations. Command: %s; StdOut: %s; StdErr: %s' % (cmd,result,error))

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

  if os.path.exists(new_proxy) and rm_proxy == 1:
    restoreProxy(new_proxy,old_proxy)
  if os.path.exists(my_proxy):
    os.remove(my_proxy)

  return S_OK(proxy_string)