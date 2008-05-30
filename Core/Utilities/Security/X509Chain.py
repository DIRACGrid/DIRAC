
import types
import os
import stat
from GSI import crypto
from DIRAC.Core.Utilities.Security.X509Certificate import X509Certificate
from DIRAC import S_OK, S_ERROR

class X509Chain:

  __validExtensionValueTypes = ( types.StringType, types.UnicodeType )

  def __init__(self):
    self.__valid = False

  def loadChainFromFile( self, chainLocation ):
    """
    Load a x509 chain from a pem file
    Return : S_OK / S_ERROR
    """
    try:
      fd = file( chainLocation )
      pemData = fd.read()
      fd.close()
    except IOError:
      return S_ERROR( "Can't open %s file" % chainLocation )
    return self.loadChainFromString( pemData )

  def loadChainFromFiles( self, certChainLocation, keyChainLocation ):
    """
    Load a x509 chain from a two pem files
    Return : S_OK / S_ERROR
    """
    try:
      fd = file( certChainLocation )
      pemData = fd.read()
      fd.close()
    except IOError:
      return S_ERROR( "Can't open %s file" % certChainLocation )
    try:
      fd = file( keyChainLocation )
      pemData += fd.read()
      fd.close()
    except IOError:
      return S_ERROR( "Can't open %s file" % keyChainLocation )
    return self.loadChainFromString( pemData )

  def loadChainFromString( self, pemData, password = False ):
    """
    Load a x509 cert from a string containing the pem data
    Return : S_OK / S_ERROR
    """
    try:
      self.__certList = crypto.load_certificate_chain( crypto.FILETYPE_PEM, pemData )
    except Exception, e:
      return S_ERROR( "Can't load pem data: %s" % str(e) )
    try:
      self.__keyObj = crypto.load_privatekey( crypto.FILETYPE_PEM, pemData, password )
    except Exception, e:
      return S_ERROR( "Can't load key file: %s" % str(e) )
    self.__valid = True
    return S_OK()

  def __getProxyExtensionList( self, diracGroup = False ):
    """
    Get the list of extensions for a proxy
    """
    extList = []
    extList.append( crypto.X509Extension( 'keyUsage', 'critical, digitalSignature, keyEncipherment, dataEncipherment' ) )
    if diracGroup and type( diracGroup ) in self.__validExtensionValueTypes:
      extList.append( crypto.X509Extension( 'diracGroup', diracGroup ) )
    return extList

  def getCertInChain( self, certPos = 0 ):
    """
    Get a certificate in the chain
    """
    if not self.__valid:
      return S_ERROR( "No chain loaded" )
    return X509Certificate( self.__certList[ certPos ] )

  def getNumCertsInChain( self ):
    """
    Numbers of certificates in chain
    """
    if not self.__valid:
      return S_ERROR( "No chain loaded" )
    return len( self.__certList )

  def generateProxyToString( self, lifeTime, diracGroup = False, bitsStrength = 1024, limited = False ):
    """
    Generate a proxy and get it as a string
      Args:
        - lifeTime : expected lifetime of proxy
        - diracGroup : diracGroup to add to the certificate
        - bitStrength : length in bits of the pair
        - limited : Create a limited proxy
    """
    if not self.__valid:
      return S_ERROR( "No chain loaded" )
    issuerCert = self.__certList[0]

    proxyKey = crypto.PKey()
    proxyKey.generate_key( crypto.TYPE_RSA, bitsStrength )

    proxyCert = crypto.X509()
    cloneSubject = issuerCert.get_subject().clone()
    if limited:
      cloneSubject.add_entry( "CN", "limitedproxy" )
    else:
      cloneSubject.add_entry( "CN", "proxy" )
    proxyCert.set_subject( cloneSubject )

    proxyCert.set_serial_number( issuerCert.get_serial_number() )
    proxyCert.set_issuer( issuerCert.get_subject() )
    proxyCert.set_version( issuerCert.get_version() )
    proxyCert.set_pubkey( proxyKey )
    proxyCert.add_extensions( self.__getProxyExtensionList( diracGroup ) )
    proxyCert.gmtime_adj_notBefore( -10 )
    proxyCert.gmtime_adj_notAfter( lifeTime )
    proxyCert.sign( self.__keyObj, 'md5' )

    proxyString = "%s%s" % ( crypto.dump_certificate( crypto.FILETYPE_PEM, proxyCert ),
                               crypto.dump_privatekey( crypto.FILETYPE_PEM, proxyKey ) )
    for i in range( len( self.__certList ) ):
      proxyString += crypto.dump_certificate( crypto.FILETYPE_PEM, self.__certList[i] )

    return S_OK( proxyString )

  def generateProxyToFile( self, filePath, lifeTime, diracGroup = False, bitsStrength = 1024, limited = False ):
    """
    Generate a proxy and put it into a file
      Args:
        - filePath : file to write
        - lifeTime : expected lifetime of proxy
        - diracGroup : diracGroup to add to the certificate
        - bitStrength : length in bits of the pair
        - limited : Create a limited proxy
    """
    if not self.__valid:
      return S_ERROR( "No chain loaded" )
    retVal = self.generateProxyToString( lifeTime, diracGroup, bitsStrength, limited )
    if not retVal[ 'OK' ]:
      return retVal
    try:
      fd = open( filePath, 'w' )
      fd.write( retVal['Value'] )
      fd.close()
    except Exception, e:
      return S_ERROR( "Cannot write to file %s :%s" % ( filePath, str(e) ) )
    try:
      os.chmod( filePath, stat.S_IRUSR | stat.S_IWUSR )
    except Exception, e:
      return S_ERROR( "Cannot set permissions to file %s :%s" % ( filePath, str(e) ) )
    return S_OK()

  def isProxy(self):
    """
    Check wether this chain is a proxy
    """
    if not self.__valid:
      return S_ERROR( "No chain loaded" )
    if len( self.__certList ) < 2:
      ret = S_OK( False )
      ret[ 'Message' ] = "At least two certificates are required"
      return ret
    for i in range( len( self.__certList )-1, 0, -1 ):
      print i
      retVal = self.__checkProxyness( i, i-1 )
      if not retVal[ 'OK' ] or not retVal[ 'Value' ]:
        return retVal
    return S_OK()

  def __checkProxyness( self, issuerId, certId ):
    """
    Check proxyness between two certs in the chain
    """
    issuerCert = self.__certList[ issuerId ]
    issuerSubject = issuerCert.get_subject()
    proxyCert = self.__certList[ certId ]
    proxySubject = proxyCert.get_subject()
    if not proxyCert.verify( issuerCert.get_pubkey() ):
      ret[ 'Message' ] = "Signature mismatch\n Issuer %s Proxy %s" % ( issuerSubject.one_line(),
                                                                       proxySubject.one_line() )
      return ret
    issuerModifiedSubject = issuerSubject.clone()
    proxySubject = proxyCert.get_subject()
    setattr( issuerModifiedSubject, 'CN', 'proxy' )
    if not issuerModifiedSubject == proxySubject:
      ret = S_OK( False )
      ret[ 'Message' ] = "Proxy DN is not Issuer DN + CN=proxy\n Issuer %s Proxy %s" % ( issuerSubject.one_line(),
                                                                       proxySubject.one_line() )
      return ret
    return S_OK( True )

  def getDIRACGroup(self):
    """
    Get the dirac group if present
    """
    retVal = self.isProxy()
    if not retVal['OK'] or not retVal[ 'Value' ]:
      return retVal
    return self.__certList[-2].getDIRACGroup()



