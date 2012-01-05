# $HeadURL$
__RCSID__ = "$Id$"


from DIRAC import S_OK, S_ERROR, gLogger
import os, re

def pfnunparse_old( pfnDict ):
  """ This method takes a dictionary containing a the pfn attributes and constructs it
  """
  #gLogger.debug("Pfn.pfnunparse: Attempting to un-parse pfnDict.")
  try:
    # All files must have a path and a filename. Or else...
    # fullPath = '/some/path/to/a/file.file'
    if pfnDict['Path']:
      fullPath = "%s/%s" % ( pfnDict['Path'], pfnDict['FileName'] )
    else:
      fullPath = pfnDict['FileName']
    fullPath = os.path.normpath( fullPath )

    port = pfnDict.get( 'Port' )
    wsUrl = pfnDict.get( 'WSUrl' )
    pfnHost = pfnDict['Host']
    # If they have a port they must also have a host...
    if port:
      pfnHost = "%s:%s" % ( pfnHost, port )
      # pfnHost = 'host:port'
      # If there is a port then there may be a web service url
      if wsUrl:
        if re.search( '\?.*=', wsUrl ) :
          pfnHost = "%s%s" % ( pfnHost, wsUrl )
        else:
          pfnHost = "%s%s?=" % ( pfnHost, wsUrl )
        #pfnHost = 'host:port/wsurl'

    # But, if the host is not an empty string we must put a protocol infront of it...
    if pfnHost:
      pfnHost = "%s://%s" % ( pfnDict['Protocol'], pfnHost )
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

    fullPfn = '%s%s' % ( pfnHost, fullPath )
    #fullPfn = 'fullPath'
    #fullPfn = 'protocol:/fullPath'
    #fullPfn = 'protocol://host/fullPath'
    #fullPfn = 'protocol://host:port/fullPath'
    #fullPfn = 'protocol://host:port/wsurl/fullPath'
    #gLogger.debug("Pfn.pfnunparse: Successfully un-parsed pfn dictionary.")
    return S_OK( fullPfn )

  except Exception:
    errStr = "Pfn.pfnunparse: Exception while un-parsing pfn dictionary."
    gLogger.exception( errStr )
    return S_ERROR( errStr )


def pfnunparse( pfnDict ):
  """ create pfn URI from pfnDict

  :param dict pfnDict: 
  """
  ## make sure all keys are in
  allDict = dict.fromkeys( [ "Protocol", "Host", "Port", "WSUrl", "Path", "FileName" ], "" )
  try:
    allDict.update( pfnDict )
  except TypeError, error:
    ## if pfnDict isn't iterable 
    return S_ERROR( "pfnunparse: wrong type for pfnDict argument: %s" % str(error) )
  except ValueError, error:
    ## iterable but not pairwise grouped
    return S_ERROR( "pfnunparse: wrong value for pfnDict argument: %s" % str(error) )
  pfnDict = allDict

  ## fileName
  if not pfnDict["FileName"]:
    return S_ERROR("pfnunparse: 'FileName' value is missing in pfnDict")
  ## c
  ## /a/b/c
  filePath = os.path.normpath( os.path.join( pfnDict["Path"], pfnDict["FileName"] ) ) 
    
  ## host
  uri = pfnDict["Host"]
  if pfnDict["Host"]:
    if pfnDict["Port"]:
      # host:port
      uri = "%s:%s" % ( pfnDict["Host"], pfnDict["Port"] )
    if pfnDict["WSUrl"]:
      if "?" in pfnDict["WSUrl"] and "=" in pfnDict["WSUrl"]:
        # host/wsurl
        # host:port/wsurl
        uri = "%s%s" % ( uri, pfnDict["WSUrl"] )
      else:
        # host/wsurl
        # host:port/wsurl
        uri = "%s%s?=" % ( uri, pfnDict["WSUrl"] )

  if pfnDict["Protocol"]:
    if uri:
      # proto://host
      # proto://host:port
      # proto://host:port/wsurl
      uri = "%s://%s" % ( pfnDict["Protocol"], uri )
    else:
      # proto:
      uri = "%s:" % pfnDict["Protocol"]

  pfn = "%s%s" % ( uri, filePath )
  # c
  # /a/b/c
  # proto:/a/b/c
  # proto://host/a/b/c
  # proto://host:port/a/b/c
  # proto://host:port/wsurl/a/b/c 
  #gLogger.debug("Pfn.pfnunparse: Successfully un-parsed pfn dictionary.")
  return S_OK( pfn )

def pfnparse( pfn ):
  """ parse pfn and save all bits of information into dictionary

  :param str pfn: pfn string
  """
  if not pfn:
    return S_ERROR("wrong 'pfn' argument value in function call, expected non-empty string, got %s" % str(pfn) )
  pfnDict = dict.fromkeys( [ "Protocol", "Host", "Port", "WSUrl", "Path", "FileName" ], "" )
  try:
    if ":" not in pfn:
      # pfn = /a/b/c
      pfnDict["Path"] = os.path.dirname( pfn )
      pfnDict["FileName"] = os.path.basename( pfn )
    else:
      # pfn = protocol:/a/b/c
      # pfn = protocol://host/a/b/c
      # pfn = protocol://host:port/a/b/c
      # pfn = protocol://host:port/wsurl?=/a/b/c
      pfnDict["Protocol"] = pfn[ 0:pfn.index(":") ]
      ## remove protocol:
      pfn = pfn[len(pfnDict["Protocol"]):] 
      ## remove :// or :
      pfn = pfn[3:] if pfn.startswith("://") else pfn[1:]
      if pfn.startswith("/"):
        ## /a/b/c
        pfnDict["Path"] = os.path.dirname( pfn )
        pfnDict["FileName"] = os.path.basename( pfn )
      else:
        ## host/a/b/c  
        ## host:port/a/b/c
        ## host:port/wsurl?=/a/b/c
        if ":" not in pfn:
          ## host/a/b/c
          pfnDict["Host"] = pfn[ 0:pfn.index("/") ]
          pfn = pfn[len(pfnDict["Host"]):]
          pfnDict["Path"] = os.path.dirname( pfn )
          pfnDict["FileName"] = os.path.basename( pfn )
        else:
          ## host:port/a/b/c
          ## host:port/wsurl?=/a/b/c
          pfnDict["Host"] = pfn[0:pfn.index(":")]
          ## port/a/b/c
          ## port/wsurl?=/a/b/c
          pfn = pfn[ len(pfnDict["Host"])+1: ]
          pfnDict["Port"] = pfn[0:pfn.index("/")]
          ## /a/b/c
          ## /wsurl?=/a/b/c
          pfn = pfn[ len(pfnDict["Port"]): ]
          WSUrl = pfn.find("?")
          WSUrlEnd = pfn.find("=")
          if WSUrl == -1 and WSUrlEnd == -1:
            ## /a/b/c
            pfnDict["Path"] = os.path.dirname( pfn )
            pfnDict["FileName"] = os.path.basename( pfn )
          else:
            ## /wsurl?blah=/a/b/c
            pfnDict["WSUrl"] = pfn[ 0:WSUrlEnd+1 ]
            ## /a/b/c
            pfn = pfn[ len(pfnDict["WSUrl"]):]
            pfnDict["Path"] = os.path.dirname( pfn )
            pfnDict["FileName"] = os.path.basename( pfn )
    return S_OK( pfnDict )
  except Exception:
    errStr = "Pfn.pfnparse: Exception while parsing pfn: " + str( pfn )
    gLogger.exception( errStr )
    return S_ERROR( errStr )

def pfnparse_old( pfn ):

  if not pfn:
    return S_ERROR("wrong 'pfn' argument value in function call, expected non-empty string, got %s" % str(pfn) )

  pfnDict = {'Protocol':'', 'Host':'', 'Port':'', 'WSUrl':'', 'Path':'', 'FileName':''}

  try:
    #gLogger.debug("Pfn.pfnunparse: Attempting to parse pfn %s." % pfn)
    if not re.search( ':', pfn ):
      # pfn = 'fullPath'
      directory = os.path.dirname( pfn )
      pfnDict['Path'] = directory
      fileName = os.path.basename( pfn )
      pfnDict['FileName'] = fileName
    else:
      #pfn = 'protocol:/fullPath'
      #pfn = 'protocol://host/fullPath'
      #pfn = 'protocol://host:port/fullPath'
      #pfn = 'protocol://host:port/wsurl/fullPath'
      protocol = pfn.split( ':', 1 )[0]
      pfnDict['Protocol'] = protocol
      if re.search( '%s://' % protocol, pfn ):
        pfn = pfn.replace( '%s://' % protocol, '' )
      else:
        pfn = pfn.replace( '%s:' % protocol, '' )
      #pfn = 'fullPath'
      #pfn = 'host/fullPath'
      #pfn = 'host:port/fullPath'
      #pfn = 'host:port/wsurl/fullPath'
      if pfn[0] == '/':
        #pfn = 'fullPath'
        directory = os.path.dirname( pfn )
        pfnDict['Path'] = directory
        fileName = os.path.basename( pfn )
        pfnDict['FileName'] = fileName
      else:
        #pfn = 'host/fullPath'
        #pfn = 'host:port/fullPath'
        #pfn = 'host:port/wsurl/fullPath'
        if not re.search( ':', pfn ):
          #pfn = 'host/fullPath'
          host = pfn.split( '/', 1 )[0]
          pfnDict['Host'] = host
          fullPath = pfn.replace( host, '' )
          directory = os.path.dirname( fullPath )
          pfnDict['Path'] = directory
          fileName = os.path.basename( fullPath )
          pfnDict['FileName'] = fileName
        else:
          #pfn = 'host:port/fullPath'
          #pfn = 'host:port/wsurl/fullPath'
          host = pfn.split( ':', 1 )[0]
          pfnDict['Host'] = host
          pfn = pfn.replace( '%s:' % host, '' )
          port = pfn.split( '/', 1 )[0]
          pfnDict['Port'] = port
          pfn = pfn.replace( port, '', 1 )
          #pfn = '/fullPath'
          #pfn = '/wsurl/fullPath'
          if re.search( '\?', pfn ):
            #/wsurl/fullPath'
            wsurl = '%s' % pfn.split( '=', 1 )[0]
            pfnDict['WSUrl'] = wsurl + '='
            pfn = pfn.replace( wsurl + '=', '' )
          #pfn = '/fullPath'
          directory = os.path.dirname( pfn )
          pfnDict['Path'] = directory
          fileName = os.path.basename( pfn )
          pfnDict['FileName'] = fileName
    #gLogger.debug("Pfn.pfnparse: Successfully parsed pfn.")
    return S_OK( pfnDict )
  except Exception:
    errStr = "Pfn.pfnparse: Exception while parsing pfn: " + str( pfn )
    gLogger.exception( errStr )
    return S_ERROR( errStr )
