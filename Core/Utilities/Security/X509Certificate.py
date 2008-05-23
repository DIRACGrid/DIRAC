
class X509Certificate:

  def __init__( self, x509Obj = False ):
    self.__valid = False
    if x509Obj:
      self.__certObj = x509Obj
      self.__valid = True

  def load(self,certificate):
    """ Load a x509 certificate either from a file or from a string
    """

    if os.path.exists(certificate):
      return self.loadFromFile(certificate)
    else:
      return self.loadFromString(certificate)

  def loadFromFile( self, certLocation ):
    """
    Load a x509 cert from a pem file
    Return : S_OK / S_ERROR
    """
    try:
      fd = file( certLocation )
      pemData = fd.read()
      fd.close()
    except IOError:
      return S_ERROR( "Can't open %s file" % certLocation )
    return self.loadFromString( pemData )

  def loadFromString( self, pemData ):
    """
    Load a x509 cert from a string containing the pem data
    Return : S_OK / S_ERROR
    """
    try:
      self.__certObj = GSI.crypto.load_certificate( GSI.crypto.FILETYPE_PEM, pemData )
    except Exception, e:
      return S_ERROR( "Can't load pem data: %s" % str(e) )
    self.__valid = True
    return S_OK()

  def isExpired( self ):
    """
    Check if a certificate file/proxy is still valid
    Return: S_OK( True/False )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )
    return S_OK( self.__certObj.has_expired() )

  def getNotAfterDate( self ):
    """
    Get not after date of a certificate
    Return: S_OK( datetime )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )
    return S_OK( self.__certObj.get_not_after() )

  def getNotBeforeDate( self ):
    """
    Get not before date of a certificate
    Return: S_OK( datetime )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )
    return S_OK( self.__certObj.get_not_before() )

  def getSubjectDN( self ):
    """
    Get subject DN
    Return: S_OK( string )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )
    return S_OK( self.__certObj.get_subject().one_line() )

  def getIssuerDN( self ):
    """
    Get issuer DN
    Return: S_OK( string )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )
    return S_OK( self.__certObj.get_issuer().one_line() )
