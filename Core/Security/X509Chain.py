
import types
import os
import stat
from GSI import crypto
from DIRAC.Core.Security.X509Certificate import X509Certificate
from DIRAC.Core.Security import File, CS
from DIRAC import S_OK, S_ERROR

class X509Chain:

  __validExtensionValueTypes = ( types.StringType, types.UnicodeType )

  def __init__( self, certList = False, keyObj = False ):
    self.__isProxy = False
    if certList:
      self.__loadedChain = True
      self.__certList = certList
    else:
      self.__loadedChain = False
    if keyObj:
      self.__loadedPKey = True
      self.__keyObj = keyObj
    else:
      self.__loadedPKey = False
    if self.__loadedChain:
      self.__checkProxyness()

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

  def loadChainFromString( self, pemData ):
    """
    Load a x509 cert from a string containing the pem data
    Return : S_OK / S_ERROR
    """
    self.__loadedChain = False
    try:
      self.__certList = crypto.load_certificate_chain( crypto.FILETYPE_PEM, pemData )
    except Exception, e:
      return S_ERROR( "Can't load pem data: %s" % str(e) )
    self.__loadedChain = True
    #Update internals
    self.__checkProxyness()
    return S_OK()

  def setChain( self, certList ):
    """
    Set the chain
    Return : S_OK / S_ERROR
    """
    self.__certList = certList
    self.__loadedChain = True
    return S_OK()

  def loadKeyFromFile( self, chainLocation, password = False ):
    """
    Load a PKey from a pem file
    Return : S_OK / S_ERROR
    """
    try:
      fd = file( chainLocation )
      pemData = fd.read()
      fd.close()
    except IOError:
      return S_ERROR( "Can't open %s file" % chainLocation )
    return self.loadKeyFromString( pemData, password )

  def loadKeyFromString( self, pemData, password = False ):
    """
    Load a xPKey from a string containing the pem data
    Return : S_OK / S_ERROR
    """
    self.__loadedPKey = False
    try:
      self.__keyObj = crypto.load_privatekey( crypto.FILETYPE_PEM, pemData, password )
    except Exception, e:
      return S_ERROR( "Can't load key file: %s" % str(e) )
    self.__loadedPKey = True
    return S_OK()

  def setPKey( self, pkeyObj ):
    """
    Set the chain
    Return : S_OK / S_ERROR
    """
    self.__keyObj = pkeyObj
    self.__loadedPKey = True
    return S_OK()

  def loadProxyFromFile( self, chainLocation ):
    """
    Load a Proxy from a pem file
    Return : S_OK / S_ERROR
    """
    try:
      fd = file( chainLocation )
      pemData = fd.read()
      fd.close()
    except IOError:
      return S_ERROR( "Can't open %s file" % chainLocation )
    return self.loadProxyFromString( pemData )

  def loadProxyFromString( self, pemData ):
    """
    Load a Proxy from a pem buffer
    Return : S_OK / S_ERROR
    """
    retVal = self.loadChainFromString( pemData )
    if not retVal[ 'OK' ]:
      return retVal
    return self.loadKeyFromString( pemData )

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
    if not self.__loadedChain:
      return S_ERROR( "No chain loaded" )
    return S_OK( X509Certificate( self.__certList[ certPos ] ) )

  def getIssuerCert( self ):
    """
    Get a issuer cert in the chain
    """
    if not self.__loadedChain:
      return S_ERROR( "No chain loaded" )
    if self.__isProxy:
      return S_OK( X509Certificate( self.__certList[ self.__firstProxyStep + 1 ] ) )
    else:
      return S_OK( X509Certificate( self.__certList[ -1 ] ) )

  def getPKeyObj( self ):
    """
    Get the pkey obj
    """
    if not self.__loadedPKey:
      return S_ERROR( "No pkey loaded" )
    return S_OK( self.__keyObj )

  def getCertList( self ):
    """
    Get the cert list
    """
    if not self.__loadedChain:
      return S_ERROR( "No chain loaded" )
    return S_OK( self.__certList )

  def getNumCertsInChain( self ):
    """
    Numbers of certificates in chain
    """
    if not self.__loadedChain:
      return S_ERROR( "No chain loaded" )
    return S_OK( len( self.__certList ) )

  def generateProxyToString( self, lifeTime, diracGroup = False, strength = 1024, limited = False ):
    """
    Generate a proxy and get it as a string
      Args:
        - lifeTime : expected lifetime in seconds of proxy
        - diracGroup : diracGroup to add to the certificate
        - strength : length in bits of the pair
        - limited : Create a limited proxy
    """
    if not self.__loadedChain:
      return S_ERROR( "No chain loaded" )
    if not self.__loadedPKey:
      return S_ERROR( "No pkey loaded" )

    issuerCert = self.__certList[0]

    proxyKey = crypto.PKey()
    proxyKey.generate_key( crypto.TYPE_RSA, strength )

    proxyCert = crypto.X509()
    cloneSubject = issuerCert.get_subject().clone()
    if limited:
      cloneSubject.insert_entry( "CN", "limitedproxy" )
    else:
      cloneSubject.insert_entry( "CN", "proxy" )
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

  def generateProxyToFile( self, filePath, lifeTime, diracGroup = False, strength = 1024, limited = False ):
    """
    Generate a proxy and put it into a file
      Args:
        - filePath : file to write
        - lifeTime : expected lifetime in seconds of proxy
        - diracGroup : diracGroup to add to the certificate
        - strength : length in bits of the pair
        - limited : Create a limited proxy
    """
    retVal = self.generateProxyToString( lifeTime, diracGroup, strength, limited )
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
    if not self.__loadedChain:
      return S_ERROR( "No chain loaded" )
    return S_OK( self.__isProxy )

  def isVOMS(self):
    """
    Check wether this chain is a proxy
    """
    retVal = self.isProxy()
    if not retVal[ 'OK' ] or not retVal[ 'Value' ]:
      return retVal
    for i in range( len( self.__certList ) ):
      cert = self.getCertInChain( i )[ 'Value' ]
      if cert.hasVOMSExtensions()[ 'Value' ]:
        return S_OK( True )
    return S_OK( False )

  def __checkProxyness( self ):
    self.__firstProxyStep = len( self.__certList )-2 # -1 is user cert by default, -2 is first proxy step
    self.__isProxy = True
    checkProxyPart = True
    #If less than 2 steps in the chain is no proxy
    if len( self.__certList ) < 2:
      self.__isProxy = False
      return
    #Check proxyness in steps
    for step in range( len( self.__certList ) - 1 ):
      issuerMatch = self.__checkIssuer( step, step + 1 )
      if not issuerMatch:
        self.__isProxy = False
        return
      if checkProxyPart:
        dnMatch = self.__checkProxyDN( step, step + 1 )
        if not dnMatch:
          if step > 0:
            self.__firstProxyStep = step
            checkProxyParth = False
          else:
            self.__isProxy = False
            return

  def __checkProxyDN( self, certStep, issuerStep ):
    """
    Check the proxy DN in a step in the chain
    """
    issuerSubject = self.__certList[ issuerStep ].get_subject()
    proxySubject = self.__certList[ certStep ].get_subject().clone()
    psEntries =  proxySubject.num_entries()
    lastEntry = proxySubject.get_entry( psEntries - 1 )
    if lastEntry[0] != 'CN' or lastEntry[1] not in ( 'proxy', 'limitedproxy' ):
      return False
    proxySubject.remove_entry( psEntries - 1 )
    if not issuerSubject.one_line() == proxySubject.one_line():
      return False
    return True

  def __checkIssuer( self, certStep, issuerStep ):
    """
    Check the issuer is really the issuer
    """
    issuerCert = self.__certList[ issuerStep ]
    cert = self.__certList[ certStep ]
    return cert.verify_pkey_is_issuer( issuerCert.get_pubkey() )

  def getDIRACGroup(self):
    """
    Get the dirac group if present
    """
    if not self.__loadedChain:
      return S_ERROR( "No chain loaded" )
    if not self.__isProxy:
      return S_ERROR( "Chain does not contain a valid proxy" )
    #ADRI: Below will find first match of dirac group
    #for i in range( len( self.__certList ) -1, -1, -1 ):
    #  retVal = self.getCertInChain( i )[ 'Value' ].getDIRACGroup()
    #  if retVal[ 'OK' ] and 'Value' in retVal and retVal[ 'Value' ]:
    #    return retVal
    return self.getCertInChain( self.__firstProxyStep )[ 'Value' ].getDIRACGroup()

  def isExpired( self ):
    """
    Is any of the elements in the chain expired?
    """
    if not self.__loadedChain:
      return S_ERROR( "No chain loaded" )
    for iC in range( len( self.__certList )-1, -1, -1 ):
      if self.__certList[iC].has_expired():
        return S_OK( True )
    return S_OK( False )

  def getNotAfterDate( self ):
    """
    Get the smallest not after date
    """
    if not self.__loadedChain:
      return S_ERROR( "No chain loaded" )
    for iC in range( len( self.__certList )-1, -1, -1 ):
      if self.__certList[iC].has_expired():
        return S_OK( self.__certList[iC].get_not_after() )
    return S_OK( self.__certList[-1].get_not_after() )

  def generateProxyRequest( self, bitStrength = 1024, limited = False ):
    """
    Generate a proxy request
    Return S_OK( X509Request ) / S_ERROR
    """
    if not self.__loadedChain:
      return S_ERROR( "No chain loaded" )
    x509 = self.getCertInChain(0)[ 'Value' ]
    return x509.generateProxyRequest( bitStrength, limited )

  def generateChainFromRequestString( self, pemData, lifeTime = 86400 ):
    """
    Generate a x509 chain from a request
    return S_OK( string ) / S_ERROR
    """
    if not self.__loadedChain:
      return S_ERROR( "No chain loaded" )
    if not self.__loadedPKey:
      return S_ERROR( "No pkey loaded" )
    try:
      req = crypto.load_certificate_request( crypto.FILETYPE_PEM, pemData )
    except Exception, e:
      return S_ERROR( "Can't load request data: %s" % str(e) )

    issuerCert = self.__certList[0]

    childCert = crypto.X509()
    childCert.set_subject( req.get_subject() )

    childCert.set_serial_number( issuerCert.get_serial_number() )
    childCert.set_issuer( issuerCert.get_subject() )
    childCert.set_version( issuerCert.get_version() )
    childCert.set_pubkey( req.get_pubkey() )
    childCert.add_extensions( req.get_extensions() )
    childCert.gmtime_adj_notBefore( 0 )
    childCert.gmtime_adj_notAfter( lifeTime )
    childCert.sign( self.__keyObj, 'md5' )

    childString = crypto.dump_certificate( crypto.FILETYPE_PEM, childCert )
    for i in range( len( self.__certList ) ):
      childString += crypto.dump_certificate( crypto.FILETYPE_PEM, self.__certList[i] )

    return S_OK( childString )

  def getRemainingSecs( self ):
    """
    Get remaining time
    """
    if not self.__loadedChain:
      return S_ERROR( "No chain loaded" )
    return self.getCertInChain(0)[ 'Value' ].getRemainingSecs()

  def dumpAllToString( self ):
    """
    Dump all to string
    """
    if not self.__loadedChain:
      return S_ERROR( "No chain loaded" )
    buffer = crypto.dump_certificate( crypto.FILETYPE_PEM, self.__certList[0] )
    if self.__loadedPKey:
      buffer += crypto.dump_privatekey( crypto.FILETYPE_PEM, self.__keyObj )
    for i in range( 1, len( self.__certList ) ):
      buffer += crypto.dump_certificate( crypto.FILETYPE_PEM, self.__certList[i] )
    return S_OK( buffer )

  def dumpChainToString( self ):
    """
    Dump only cert chain to string
    """
    if not self.__loadedChain:
      return S_ERROR( "No chain loaded" )
    for i in range( len( self.__certList ) ):
      buffer += crypto.dump_certificate( crypto.FILETYPE_PEM, self.__certList[1] )
    return S_OK( buffer )

  def dumpPKeyToString( self ):
    """
    Dump key to string
    """
    if not self.__loadedPKey:
      return S_ERROR( "No chain loaded" )
    return S_OK( crypto.dump_privatekey( crypto.FILETYPE_PEM, self.__keyObj ) )

  def __str__(self):
    repStr = "<X509Chain"
    if self.__loadedChain:
      repStr += " %s certs " % len( self.__certList )
      for cert in self.__certList:
        repStr += "[%s]" % cert.get_subject().one_line()
    if self.__loadedPKey:
      repStr += " with key"
    repStr += ">"
    return repStr

  def __repr__(self):
    return self.__str__()

  def getCredentials(self):
    retVal = self.isProxy()
    if not retVal[ 'OK' ]:
      return retVal
    isProxy = retVal[ 'Value' ]
    credDict = { 'subject' : self.__certList[0].get_subject().one_line(),
                 'isProxy' : isProxy,
                 'validDN' : False,
                 'validGroup' : False }
    if isProxy:
      retVal = self.getDIRACGroup()
      if not retVal[ 'OK' ]:
        return retVal
      diracGroup = retVal[ 'Value' ]
      if not diracGroup:
        diracGroup = CS.getDefaultUserGroup()
      credDict[ 'group' ] = diracGroup
      credDict[ 'identity'] = self.__certList[-1].get_subject().one_line()
      retVal = CS.getUsernameForDN( credDict[ 'identity' ] )
      if retVal[ 'OK' ]:
        credDict[ 'username' ] = retVal[ 'Value' ]
        credDict[ 'validDN' ] = True
        retVal = CS.getGroupsForUser( credDict[ 'username' ] )
        if retVal[ 'OK' ] and diracGroup in retVal[ 'Value']:
          credDict[ 'validGroup' ] = True
    else:
      retVal = CS.getHostNameForDN( credDict['subject'] )
      retVal[ 'group' ] = 'hosts'
      if retVal[ 'OK' ]:
        credDict[ 'hostname' ] = retVal[ 'Value' ]
        credDict[ 'validDN' ] = True
        credDict[ 'validGroup' ] = True
    return S_OK( credDict )


g_X509ChainType = type( X509Chain() )
