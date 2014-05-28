from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC import gConfig

def checkCAOfUser ( user, CA ):
  """check the CA of user"""
  if gConfig.getValue( "/Registry/Users/%s/CA" % user, "noCA") == CA:
      return S_OK()
  
  return S_ERROR("CA does not match")

