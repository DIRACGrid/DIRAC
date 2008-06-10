
import GSI
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import Time

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

  def setCertificate( self, x509Obj ):
    if type( x509Obj ) != GSI.crypto.X509Type:
      return S_ERROR( "Object %s has to be of type X509" % str( X509Obj ) )
    self.__certObj = x509Obj
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

  def getSubjectNameObject(self):
    """
    Get subject name object
    Return: S_OK( X509Name )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )
    return S_OK( self.__certObj.get_subject() )

  def getIssuerNameObject(self):
    """
    Get issuer name object
    Return: S_OK( X509Name )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )
    return S_OK( self.__certObj.get_issuer() )

  def getDIRACGroup(self):
    """
    Get the dirac group if present
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )
    extList = self.__certObj.get_extensions()
    for ext in extList:
      if ext.get_sn() == "diracGroup":
        return S_OK( ext.get_value() )
    return S_OK( False )

  def hasVOMSExtensions(self):
    """
    Has voms extensions
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )
    extList = self.__certObj.get_extensions()
    for ext in extList:
      if ext.get_sn() == "vomsExtensions":
        return S_OK( True )
    return S_OK( False )

  def __proxyExtensionList(self):
    return [ GSI.crypto.X509Extension( 'keyUsage', 'critical, digitalSignature, keyEncipherment, dataEncipherment' ) ]

  def generateProxyRequest( self, bitStrength = 1024, forceLimited = False ):
    """
    Generate a proxy request
    Return S_OK( X509Request ) / S_ERROR
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )

    from DIRAC.Core.Security.X509Request import X509Request

    request = GSI.crypto.X509Req()
    certSubj = self.__certObj.get_subject().clone()
    lastEntry = certSubj.get_entry( certSubj.num_entries() -1 )
    if forceLimited or ( lastEntry[0] == 'CN' and lastEntry[1] == 'limitedproxy' ):
      certSubj.insert_entry( "CN", "limitedproxy" )
    else:
      certSubj.insert_entry( "CN", "proxy" )
    request.set_subject( certSubj )
    request.add_extensions( self.__proxyExtensionList() )

    requestKey = GSI.crypto.PKey()
    requestKey.generate_key( GSI.crypto.TYPE_RSA, bitStrength )

    request.set_pubkey( requestKey )
    return S_OK( X509Request( request, requestKey )  )

  def getRemainingSecs( self ):
    """
    Get remaining lifetime in secs
    """
    if not self.__valid:
      return S_ERROR( "No certificate loaded" )
    notAfter = self.__certObj.get_not_after()
    remaining = notAfter - Time.dateTime()
    return S_OK( max( 0, remaining.days * 86400 + remaining.seconds ) )