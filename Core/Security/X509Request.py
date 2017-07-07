""" X509Request is a class for managing X509 requests with their Pkeys
"""

__RCSID__ = "$Id$"

import GSI
import M2Crypto
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Security.X509Chain import X509Chain

class X509Request( object ):
  """
  Class representing X509 Certificate Request
  """

  def __init__( self, reqObj = None, pkeyObj = None ):
    self.__valid = False
    self.__reqObj = reqObj
    self.__pkeyObj = pkeyObj
    if reqObj and pkeyObj:  # isn't it a bit too liberal?
      self.__valid = True

  def generateProxyRequest( self, bitStrength = 1024, limited = False ) :
    """
    Generate proxy request
    """
    self.__pkeyObj = M2Crypto.EVP.PKey()
    self.__pkeyObj.assign_rsa(M2Crypto.RSA.gen_key(bitStrength, 65537, callback = M2Crypto.util.quiet_genparam_callback ))
    self.__reqObj = M2Crypto.X509.Request()
    self.__reqObj.set_pubkey( self.__pkeyObj )
    if limited:
      self.__reqObj.get_subject().add_entry_by_txt( field = "CN", type = M2Crypto.ASN1.MBSTRING_ASC, entry =  "limited proxy", len=-1, loc=-1, set=0 )
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

  def getRequestObject( self ):
    """
    Get internal X509Request object
    """
    return S_OK( self.__reqObj )

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
      pkeyStr = GSI.crypto.dump_privatekey(GSI.crypto.FILETYPE_PEM, self.__pkeyObj)
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
      reqStr = GSI.crypto.dump_certificate_request(GSI.crypto.FILETYPE_PEM, self.__reqObj)
    except Exception as e:
      return S_ERROR(DErrno.EX509, "Can't serialize request: %s" % e)
    try:
      pkeyStr = GSI.crypto.dump_privatekey(GSI.crypto.FILETYPE_PEM, self.__pkeyObj)
    except Exception as e:
      return S_ERROR(DErrno.EX509, "Can't serialize pkey: %s" % e)
    return S_OK("%s%s" % (reqStr, pkeyStr))

  def loadAllFromString( self, pemData ):
    try:
      self.__reqObj = GSI.crypto.load_certificate_request(GSI.crypto.FILETYPE_PEM, pemData)
    except Exception as e:
      return S_ERROR( DErrno.ENOCERT, str( e ) )
    try:
      self.__pkeyObj = GSI.crypto.load_privatekey(GSI.crypto.FILETYPE_PEM, pemData)
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
    ret = chain.loadChainFromString( pemData )
    if not ret['OK']:
      return ret
    ret = chain.setPKey( self.__pkeyObj )
    if not ret['OK']:
      return ret
    return chain

  def getSubjectDN( self ):
    """
    Get subject DN
    Return: S_OK( string )/S_ERROR
    """
    if not self.__valid:
      return S_ERROR( DErrno.ENOCERT )
    return S_OK( str( self.__reqObj.get_subject() ) )

  # it doesn't seem to be used anywhere...
  #def getIssuerDN( self ):
  #  """
  #  Get issuer DN
  #  Return: S_OK( string )/S_ERROR
  #  """
  #  if not self.__valid:
  #    return S_ERROR( DErrno.ENOCERT )
  #  return S_OK( '' )# self.__reqObj.get_issuer() ) # XXX no get_issuer for request in m2crypto

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
