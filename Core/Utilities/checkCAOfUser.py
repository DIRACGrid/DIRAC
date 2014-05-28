from DIRAC import gConfig
from DIRAC import S_OK, S_ERROR

def checkCAOfUser( user, CA ):
  """ user, and CA are string
  """
  if CA == gConfig.getValue( "/Registry/Users/%s/CA" % user,  "noCA" ):
    return S_OK()
  return S_ERROR("Ops, CA didn't match")
