from DIRAC import gConfig, S_OK, S_ERROR

def checkCAOfUser( user, CA ):
  """ user, and CA are string
  """

  if CA == gConfig.getValue( "/Registry/Users/%s/CA" % user, "noCA" ):
    return S_OK()
  return S_ERROR("CA doesn't match")
