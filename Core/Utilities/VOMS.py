# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/Attic/VOMS.py,v 1.2 2008/01/06 19:12:16 atsareg Exp $

""" VOMS module contains utilities to manage VOMS proxies

    The following utilities are available:

    renewVOMSProxy()
    getVOMSAttributes()
    createVOMSProxy()
"""

__RCSID__ = "$Id: VOMS.py,v 1.2 2008/01/06 19:12:16 atsareg Exp $"

import os
import time
import shutil
import tempfile

import DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.Core.Utilities.GridCredentials import *

TIMEOUT = 10

def renewProxy(proxy,lifetime=72,
                server="myproxy.cern.ch",
                server_key="/opt/dirac/etc/grid-security/serverkey.pem",
                server_cert="/opt/dirac/etc/grid-security/servercert.pem",
                vo='lhcb'):

  """ This function returns the VOMS proxy as a string.
      lifetime is a preferable lifetime of returned proxy.
      server is the address of MyProxy service server.
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

  # Get voms extensions if any and convert it to option format
  result = getVOMSAttributes(new_proxy,"option")
  if result["OK"]:
    voms_attr = str(result["Value"])
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
  result = shellCall(TIMEOUT,cmd)
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
        lifetime = result['TimeLeft']
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
  lifetime = lifetime - 300 # lifetime of extension should be less than 5min
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
  result = shellCall(TIMEOUT,cmd)
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
  attribute = []
  for i in voms_info_output:
    j = i.split(":")
    if j[0].strip() == "VO":
      if switch == "option":
        attribute.append("--voms %s" % j[1].strip())
    elif j[0].strip()=="attribute":
      if switch == "option":

        # Cut off unsupported Capability selection part
        j[1] = j[1].replace("/Capability=NULL","")
        attribute.append(" -order %s" % j[1].strip())
      else:
        attribute.append(j[1].strip())

  # Sorting and joining attributes
  if switch == "db":
    attribute.sort()
    attribute = "".join(attribute)
  elif switch == "option":
    attribute = "".join(attribute)
  return S_OK(attribute)

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
        - identity
        - fqan
        - all
      @rtype:   tuple
      @return:  status, output, error, pyerror.
  """

  options = ['timeleft','identity','fqan','all']
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
        return S_ERROR('Failed to setup given proxy. Proxy is: %s' % (proxy))
    else:
      new_proxy = proxy_file
  except ValueError:
    return S_ERROR('Failed to setup given proxy. Proxy is: %s' % (proxy))
  a = ''
  b = ''
  c = ''
  if os.environ.has_key('X509_CERT_DIR'):
    a = os.environ['X509_CERT_DIR']
  if os.environ.has_key('X509_USER_CERT'):
    b = os.environ['X509_USER_CERT']
  if os.environ.has_key('X509__USER_KEY'):
    c = os.environ['X509__USER_KEY']
  os.putenv('X509_CERT_DIR','/etc/grid-security/certificates')
  os.putenv('X509_USER_CERT','opt/dirac/etc/grid-security/servercert.pem')
  os.putenv('X509_USER_KEY','/opt/dirac/etc/grid-security/serverkey.pem')
  cmd = 'voms-proxy-info -file %s' % new_proxy
  if option:
    cmd += ' -%s' % option

  result = shellCall(20,cmd)
  if not result['OK']:
    return S_ERROR('Failed to call voms-proxy-info')

  status, output, error = result['Value']
  os.putenv('X509_CERT_DIR',a)
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
