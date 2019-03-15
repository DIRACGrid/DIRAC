""" X509Request is a class for managing X509 requests with their Pkeys
"""

__RCSID__ = "$Id$"

import GSI
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Security.pygsi.X509Chain import X509Chain

class X509Request( object ):

  def __init__( self, reqObj = None, pkeyObj = None ):
    self.__valid = False
    self.__reqObj = reqObj
    self.__pkeyObj = pkeyObj
    if reqObj and pkeyObj:
      self.__valid = True

  # It is not used
  # def setParentCerts( self, certList ):
  #   self.__cerList = certList

  def generateProxyRequest( self, bitStrength = 1024, limited = False ) :
    self.__pkeyObj = GSI.crypto.PKey()
    self.__pkeyObj.generate_key( GSI.crypto.TYPE_RSA, bitStrength )
    self.__reqObj = GSI.crypto.X509Req()
    self.__reqObj.set_pubkey( self.__pkeyObj )
    if limited:
      self.__reqObj.get_subject().insert_entry( "CN", "limited proxy" )
    else:
      self.__reqObj.get_subject().insert_entry( "CN", "proxy" )
    self.__reqObj.sign( self.__pkeyObj, "SHA256" )
    self.__valid = True

  def dumpRequest( self ):
    """
    Get the request as a string
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    try:
      reqStr = GSI.crypto.dump_certificate_request( GSI.crypto.FILETYPE_PEM, self.__reqObj )
    except Exception as e:
      return S_ERROR( DErrno.EX509, "Can't serialize request: %s" % e )
    return S_OK( reqStr )

  def getPKey( self ):
    """
    Get PKey Internal
    """
    return self.__pkeyObj

  def dumpPKey( self ):
    """
    Get the pkey as a string
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    try:
      pkeyStr = GSI.crypto.dump_privatekey( GSI.crypto.FILETYPE_PEM, self.__pkeyObj )
    except Exception as e:
      return S_ERROR( DErrno.EX509, "Can't serialize pkey: %s" % e )
    return S_OK( pkeyStr )

  def dumpAll( self ):
    """
    Dump the contents into a string
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )

    try:
      reqStr = GSI.crypto.dump_certificate_request( GSI.crypto.FILETYPE_PEM, self.__reqObj )
    except Exception as e:
      return S_ERROR( DErrno.EX509, "Can't serialize request: %s" % e )
    try:
      pkeyStr = GSI.crypto.dump_privatekey( GSI.crypto.FILETYPE_PEM, self.__pkeyObj )
    except Exception as e:
      return S_ERROR( DErrno.EX509, "Can't serialize pkey: %s" % e )
    return S_OK( "%s%s" % ( reqStr, pkeyStr ) )

  def loadAllFromString( self, pemData ):
    try:
      self.__reqObj = GSI.crypto.load_certificate_request( GSI.crypto.FILETYPE_PEM, pemData )
    except Exception as e:
      return S_ERROR( DErrno.ENOCERT, str( e ) )
    try:
      self.__pkeyObj = GSI.crypto.load_privatekey( GSI.crypto.FILETYPE_PEM, pemData )
    except Exception as e:
      return S_ERROR( DErrno.ENOPKEY, str( e ) )
    self.__valid = True
    return S_OK()

  def generateChainFromResponse( self, pemData ):
    """
    Generate a X509 Chain from the pkey and the pem data passed as the argument
    Return : S_OK( X509Chain ) / S_ERROR
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    try:
      certList = GSI.crypto.load_certificate_chain( GSI.crypto.FILETYPE_PEM, pemData )
    except Exception as e:
      return S_ERROR( DErrno.ENOCERT, str( e ) )
    chain = X509Chain()
    chain.setChain( certList )
    chain.setPKey( self.__pkeyObj )
    return chain

  def getSubjectDN( self ):
    """
    Get subject DN
    Return: S_OK( string )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    return S_OK( self.__reqObj.get_subject().one_line() )

  def getIssuerDN( self ):
    """
    Get issuer DN
    Return: S_OK( string )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    return S_OK( self.__reqObj.get_issuer().one_line() )

  def checkChain( self, chain ):
    """
    Check that the chain matches the request
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    retVal = chain.getCertInChain()
    if not retVal[ 'OK' ]:
      return retVal
    lastCert = retVal[ 'Value' ]
    chainPubKey = GSI.crypto.dump_publickey( GSI.crypto.FILETYPE_PEM, lastCert.getPublicKey()[ 'Value' ] )
    reqPubKey = GSI.crypto.dump_publickey( GSI.crypto.FILETYPE_PEM, self.__pkeyObj )
    if not chainPubKey == reqPubKey:
      retVal = S_OK( False )
      retVal[ 'Message' ] = "Public keys do not match"
      return retVal
    return S_OK( True )
