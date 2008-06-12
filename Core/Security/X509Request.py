
import GSI
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security.X509Chain import X509Chain

class X509Request:

  def __init__( self, reqObj = None, pkeyObj = None ):
    self.__valid = False
    self.__reqObj = reqObj
    self.__pkeyObj = pkeyObj
    if reqObj and pkeyObj:
      self.__valid = True

  def setParentCerts( self, certList ):
    self.__cerList = certList

  def dumpRequest( self ):
    if not self.__valid:
      return S_ERROR( "No request loaded" )
    """
    Get the request as a string
    """
    try:
      reqStr = GSI.crypto.dump_certificate_request( GSI.crypto.FILETYPE_PEM, self.__reqObj  )
    except Exception, e:
      return S_ERROR( "Can't serialize request: %s" % str( e ) )
    return S_OK( reqStr )

  def getPKey( self ):
    """
    Get PKey Internal
    """
    return self.__pkeyObj

  def dumpPKey( self ):
    if not self.__valid:
      return S_ERROR( "No request loaded" )
    """
    Get the pkey as a string
    """
    try:
      pkeyStr = GSI.crypto.dump_privatekey( GSI.crypto.FILETYPE_PEM, self.__pkeyObj  )
    except Exception, e:
      return S_ERROR( "Can't serialize pkey: %s" % str( e ) )
    return S_OK( pkeyStr )

  def dumpAll( self ):
    if not self.__valid:
      return S_ERROR( "No request loaded" )

    try:
      reqStr = GSI.crypto.dump_certificate_request( GSI.crypto.FILETYPE_PEM, self.__reqObj  )
    except Exception, e:
      return S_ERROR( "Can't serialize request: %s" % str( e ) )
    try:
      pkeyStr = GSI.crypto.dump_privatekey( GSI.crypto.FILETYPE_PEM, self.__pkeyObj  )
    except Exception, e:
      return S_ERROR( "Can't serialize pkey: %s" % str( e ) )
    return S_OK( "%s%s" % ( reqStr, pkeyStr ) )

  def loadAllFromString( self, pemData ):
    try:
      self.__reqObj = GSI.crypto.load_certificate_request( GSI.crypto.FILETYPE_PEM, pemData )
    except Exception, e:
      return S_ERROR( "Can't load request: %s" % str( e ) )
    try:
      self.__pkeyObj = GSI.crypto.load_privatekey( GSI.crypto.FILETYPE_PEM, pemData  )
    except Exception, e:
      return S_ERROR( "Can't load pkey: %s" % str( e ) )
    self.__valid = True
    return S_OK()

  def generateChainFromResponse( self, pemData ):
    if not self.__valid:
      return S_ERROR( "No request loaded" )
    """
    Generate a X509 Chain from the pkey and the pem data passed as the argument
    Return : S_OK( X509Chain ) / S_ERROR
    """
    try:
      certList = crypto.load_certificate_chain( crypto.FILETYPE_PEM, pemData )
    except Exception, e:
      return S_ERROR( "Can't load pem data: %s" % str(e) )
    chain = X509Chain()
    chain.setChain( certList )
    chain.setPKey( self.__pkeyObj )
    return chain

  def getSubjectDN( self ):
    if not self.__valid:
      return S_ERROR( "No request loaded" )
    """
    Get subject DN
    Return: S_OK( string )/S_ERROR
    """
    return S_OK( self.__reqObj.get_subject().one_line() )

  def getIssuerDN( self ):
    if not self.__valid:
      return S_ERROR( "No request loaded" )
    """
    Get issuer DN
    Return: S_OK( string )/S_ERROR
    """
    return S_OK( self.__reqObj.get_issuer().one_line() )