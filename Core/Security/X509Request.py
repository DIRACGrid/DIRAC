
import GSI
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security.X509Chain import X509Chain

class X509Request:

  def __init__( self, reqObj, pkeyObj ):
    self.__reqObj = reqObj
    self.__pkeyObj = pkeyObj

  def setParentCerts( self, certList ):
    self.__cerList = certList

  def dumpRequest( self ):
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

  def generateChainFromResponse( self, pemData ):
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
    """
    Get subject DN
    Return: S_OK( string )/S_ERROR
    """
    return S_OK( self.__reqObj.get_subject().one_line() )

  def getIssuerDN( self ):
    """
    Get issuer DN
    Return: S_OK( string )/S_ERROR
    """
    return S_OK( self.__reqObj.get_issuer().one_line() )