from DIRAC import S_OK, S_ERROR, gLogger
import os,re

def pfnunparse(pfnDict):
  """ This method takes a dictionary containing a the pfn attributes and contructs it
  """
  gLogger.debug("Pfn.pfnunparse: Attempting to un-parse pfnDict.")
  try:
    # All files must have a path and a filename. Or else...
    # fullPath = '/some/path/to/a/file.file'
    fullPath = "%s/%s" % (pfnDict['Path'],pfnDict['FileName'])
    fullPath = os.path.realpath(fullPath)

    # If they have a port they must also have a host...
    if pfnDict['Port']:
      pfnHost ="%s:%s" % (pfnDict['Host'],pfnDict['Port'])
      # pfnHost = 'host:port'
      # If there is a port then there may be a web service url
      if pfnDict['WSUrl']:
        pfnHost = "%s%s" % (pfnHost,pfnDict['WSUrl'])
        #pfnHost = 'host:port/wsurl'
    else:
      # It is possible that the host is an empty string
      pfnHost = pfnDict['Host']
      #pfnHost = 'host'
      #pfnHost = ''

    # But, if the host is not an empty string we must put a protocol infront of it...
    if pfnHost:
      pfnHost = "%s://%s" % (pfnDict['Protocol'],pfnHost)
      #pfnHost = 'protocol://host'
      #pfnHost = 'protocol://host:port'
      #pfnHost = 'protocol://host:port/wsurl'
    else:
      # If there is no host there may or may not be a protocol
      if pfnDict['Protocol']:
        pfnHost = '%s:' % pfnDict['Protocol']
        #pfnHost = 'protocol:'
      else:
        pfnHost = ''

    fullPfn = '%s%s' % (pfnHost,fullPath)
    #fullPfn = 'fullPath'
    #fullPfn = 'protocol:/fullPath'
    #fullPfn = 'protocol://host/fullPath'
    #fullPfn = 'protocol://host:port/fullPath'
    #fullPfn = 'protocol://host:port/wsurl/fullPath'
    gLogger.debug("Pfn.pfnunparse: Successfully un-parsed pfn dictionary.")
    return S_OK(fullPfn)

  except Exception,x:
    errStr = "Pfn.pfnunparse: Failed to un-parse pfn dictionary: %s" % x
    return S_ERROR(errStr)


def pfnparse(pfn):

  pfnDict = {'Protocol':'','Host':'','Port':'','WSUrl':'','Path':'','FileName':''}
  try:
    gLogger.debug("Pfn.pfnunparse: Attempting to parse pfn %s." % pfn)
    if not re.search(':',pfn):
      # pfn = 'fullPath'
      directory = os.path.dirname(pfn)
      pfnDict['Path'] = directory
      fileName = os.path.basename(pfn)
      pfnDict['FileName'] = fileName
    else:
      #pfn = 'protocol:/fullPath'
      #pfn = 'protocol://host/fullPath'
      #pfn = 'protocol://host:port/fullPath'
      #pfn = 'protocol://host:port/wsurl/fullPath'
      protocol = pfn.split(':',1)[0]
      pfnDict['Protocol'] = protocol
      if re.search('%s://' % protocol,pfn):
        pfn = pfn.replace('%s://' % protocol,'')
      else:
        pfn = pfn.replace('%s:' % protocol,'')
      #pfn = 'fullPath'
      #pfn = 'host/fullPath'
      #pfn = 'host:port/fullPath'
      #pfn = 'host:port/wsurl/fullPath'
      if pfn[0] == '/':
        #pfn = 'fullPath'
        directory = os.path.dirname(pfn)
        pfnDict['Path'] = directory
        fileName = os.path.basename(pfn)
        pfnDict['FileName'] = fileName
      else:
        #pfn = 'host/fullPath'
        #pfn = 'host:port/fullPath'
        #pfn = 'host:port/wsurl/fullPath'
        if not re.search(':',pfn):
          #pfn = 'host/fullPath'
          host = pfn.split('/',1)[0]
          pfnDict['Host'] = host
          fullPath = pfn.replace(host,'')
          directory = os.path.dirname(fullPath)
          pfnDict['Path'] = directory
          fileName = os.path.basename(fullPath)
          pfnDict['FileName'] = fileName
        else:
          #pfn = 'host:port/fullPath'
          #pfn = 'host:port/wsurl/fullPath'
          host = pfn.split(':',1)[0]
          pfnDict['Host'] = host
          pfn = pfn.replace('%s:'%host,'')
          port = pfn.split('/',1)[0]
          pfnDict['Port'] = port
          pfn = pfn.replace(port,'')
          #pfn = '/fullPath'
          #pfn = '/wsurl/fullPath'
          if re.search('\?',pfn):
            #/wsurl/fullPath'
            wsurl = '%s=' % pfn.split('=',1)[0]
            pfnDict['WSUrl'] = wsurl
            pfn = pfn.replace(wsurl,'')
          #pfn = '/fullPath'
          directory = os.path.dirname(pfn)
          pfnDict['Path'] = directory
          fileName = os.path.basename(pfn)
          pfnDict['FileName'] = fileName
    gLogger.debug("Pfn.pfnparse: Successfully parsed pfn.")
    return S_OK(pfnDict)
  except Exception,x:
    errStr = "Pfn.pfnparse: Failed to parse pfn: %s" % x
    return S_ERROR(errStr)
