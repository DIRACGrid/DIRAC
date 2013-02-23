# $HeadURL$

""":mod:`Pfn`

    .. module: Pfn
      :synopsis: pfn URI (un)parsing
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com
"""

__RCSID__ = "$Id$"

## imports
import os
import re
## from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger

def pfnunparse( pfnDict ):
  """ create pfn URI from pfnDict

  :param dict pfnDict: 
  """
  ## make sure all keys are in
  allDict = dict.fromkeys( [ "Protocol", "Host", "Port", "WSUrl", "Path", "FileName" ], "" )
  if type( allDict ) != type( pfnDict ):
    return S_ERROR( "pfnunparse: wrong type fot pfnDict argument, expected a dict, got %s" % type(pfnDict) )
  allDict.update( pfnDict )
  pfnDict = allDict

  ## c
  ## /a/b/c
  filePath = os.path.normpath( '/' + pfnDict["Path"] + '/' + pfnDict["FileName"] ).replace( '//','/' )
    
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

