#Import gConfig, S_OK and S_ERROR
from DIRAC import gConfig, S_OK, S_ERROR

def checkCAOfUser( user, CA ):
  if CA == gConfig.getValue( "/Registry/Users/%s/CA" % user, "noCA" ):
    return S_OK()
  return S_ERROR( "Oops, CA doesn't match" )
