########################################################################
# $HeadURL$
########################################################################

""" DIRAC FileCatalog utilities
"""

__RCSID__ = "$Id$"

import md5, random
from types import *
from DIRAC import S_OK, S_ERROR

def checkArgumentFormat(path):
  """ Check and process format of the arguments to FileCatalog methods
  """
  if type(path) in StringTypes:
    urls = {path:False}
  elif type(path) == ListType:
    urls = {}
    for url in path:
      urls[url] = False
  elif type(path) == DictType:
   urls = path
  else:
    return S_ERROR("checkArgumentFormat: Supplied path is not of the correct format")
  return S_OK(urls)  

def generateGuid(checksum,checksumtype):
    """ Generate a GUID based on the file checksum
    """
    
    if checksum:
      if checksumtype == "MD5":
        checksumString = checksum
      elif checksumtype == "Adler32":
        checksumString = str(checksum).zfill(32)  
      else:
        checksumString = ''
      if checksumString:    
        guid = "%s-%s-%s-%s-%s" % ( checksumString[0:8],
                                    checksumString[8:12],
                                    checksumString[12:16],
                                    checksumString[16:20],
                                    checksumString[20:32] )
        guid = guid.upper()
        return guid
        
    # Failed to use the check sum, generate a new guid    
    myMd5 = md5.new()
    myMd5.update( str( random.getrandbits( 128 ) ) )
    md5HexString = myMd5.hexdigest()    
    guid = "%s-%s-%s-%s-%s" % ( md5HexString[0:8],
                                md5HexString[8:12],
                                md5HexString[12:16],
                                md5HexString[16:20],
                                md5HexString[20:32] )
    guid = guid.upper()
    return guid