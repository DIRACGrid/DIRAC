########################################################################
# $HeadURL$
########################################################################
""" X509Chain is a class for managing X509 chains with their Pkeys
"""
__RCSID__ = "$Id$"

import types
import os
import stat
import tempfile
import hashlib
from GSI import crypto
from DIRAC.Core.Security.X509Certificate import X509Certificate
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC import S_OK, S_ERROR

class X509Chain:

  __validExtensionValueTypes = ( types.StringType, types.UnicodeType )

  def __init__( self, certList = False, keyObj = False ):
    self.__isProxy = False
    self.__firstProxyStep = 0
    self.__isLimitedProxy = True
    self.__hash = False
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

  @classmethod
  def instanceFromFile( cls, chainLocation ):
    """ Instance a X509Chain from a file
    """
    chain = cls()
    result = chain.loadChainFromFile( chainLocation )
    if not result[ 'OK' ]:
      return result
    return S_OK( chain )

  def loadChainFromFile( self, chainLocation ):
    """
    Load a x509 chain from a pem file
    Return : S_OK / S_ERROR
    """
    try:
      fd = file( chainLocation )
      pemData = fd.read()
      fd.close()
    except Exception, e:
      return S_ERROR( "Can't open %s file: %s" % ( chainLocation, str( e ) ) )
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
      return S_ERROR( "Can't load pem data: %s" % str( e ) )
    if not self.__certList:
      return S_ERROR( "No certificates in the contents" )
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
    except Exception, e:
      return S_ERROR( "Can't open %s file: %s" % ( chainLocation, str( e ) ) )
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
      return S_ERROR( "Can't load key file: %s (Probably bad pass phrase?)" % str( e ) )
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
    except Exception, e:
      return S_ERROR( "Can't open %s file: %s" % ( chainLocation, str( e ) ) )
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
    extList.append( crypto.X509Extension( 'keyUsage',
                                          'critical, digitalSignature, keyEncipherment, dataEncipherment' ) )
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
      cloneSubject.insert_entry( "CN", "limited proxy" )
    else:
      cloneSubject.insert_entry( "CN", "proxy" )
    proxyCert.set_subject( cloneSubject )

    proxyCert.set_serial_number( issuerCert.get_serial_number() )
    proxyCert.set_issuer( issuerCert.get_subject() )
    proxyCert.set_version( issuerCert.get_version() )
    proxyCert.set_pubkey( proxyKey )
    proxyCert.add_extensions( self.__getProxyExtensionList( diracGroup ) )
    proxyCert.gmtime_adj_notBefore( -900 )
    proxyCert.gmtime_adj_notAfter( lifeTime )
    proxyCert.sign( self.__keyObj, 'sha1' )

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
      return S_ERROR( "Cannot write to file %s :%s" % ( filePath, str( e ) ) )
    try:
      os.chmod( filePath, stat.S_IRUSR | stat.S_IWUSR )
    except Exception, e:
      return S_ERROR( "Cannot set permissions to file %s :%s" % ( filePath, str( e ) ) )
    return S_OK()

  def isProxy( self ):
    """
    Check wether this chain is a proxy
    """
    if not self.__loadedChain:
      return S_ERROR( "No chain loaded" )
    return S_OK( self.__isProxy )

  def isLimitedProxy( self ):
    """
    Check wether this chain is a proxy
    """
    if not self.__loadedChain:
      return S_ERROR( "No chain loaded" )
    return S_OK( self.__isProxy and self.__isLimitedProxy )

  def isValidProxy( self, ignoreDefault = False ):
    """
    Check wether this chain is a valid proxy
      checks if its a proxy
      checks if its expired
    """
    if not self.__loadedChain:
      return S_ERROR( "No chain loaded" )
    if not self.__isProxy:
      return S_ERROR( "Chain is not a proxy" )
    elif self.hasExpired()['Value']:
      return S_ERROR( "Chain has expired" )
    elif ignoreDefault:
      groupRes = self.getDIRACGroup( ignoreDefault = ignoreDefault )
      if not groupRes[ 'OK' ]:
        return groupRes
      if not groupRes[ 'Value' ]:
        return S_ERROR( "Proxy does not have an explicit group" )
    return S_OK( True )

  def isVOMS( self ):
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
    self.__hash = False
    self.__firstProxyStep = len( self.__certList ) - 2 # -1 is user cert by default, -2 is first proxy step
    self.__isProxy = True
    self.__isLimitedProxy = False
    prevDNMatch = 2
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
      #Do we need to check the proxy DN?
      if prevDNMatch:
        dnMatch = self.__checkProxyDN( step, step + 1 )
        #No DN match
        if dnMatch == 0:
          #If we are not in the first step we've found the entity cert
          if step > 0:
            self.__firstProxyStep = step - 1
          #If we are in the first step this is not a proxy
          else:
            self.__isProxy = False
            return
        #Limited proxy DN match
        elif dnMatch == 2:
          self.__isLimitedProxy = True
          if prevDNMatch != 2:
            self.__isProxy = False
            self.__isLimitedProxy = False
            return
        prevDNMatch = dnMatch

  def __checkProxyDN( self, certStep, issuerStep ):
    """
    Check the proxy DN in a step in the chain
     0 = no match
     1 = proxy match
     2 = limited proxy match
    """
    issuerSubject = self.__certList[ issuerStep ].get_subject()
    proxySubject = self.__certList[ certStep ].get_subject().clone()
    psEntries = proxySubject.num_entries()
    lastEntry = proxySubject.get_entry( psEntries - 1 )
    if lastEntry[0] != 'CN' or lastEntry[1] not in ( 'proxy', 'limited proxy' ):
      return 0
    proxySubject.remove_entry( psEntries - 1 )
    if not issuerSubject.one_line() == proxySubject.one_line():
      return 0
    if lastEntry[1] == "limited proxy":
      return 2
    return 1

  def __checkIssuer( self, certStep, issuerStep ):
    """
    Check the issuer is really the issuer
    """
    issuerCert = self.__certList[ issuerStep ]
    cert = self.__certList[ certStep ]
    return cert.verify_pkey_is_issuer( issuerCert.get_pubkey() )

  def getDIRACGroup( self, ignoreDefault = False ):
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
    return self.getCertInChain( self.__firstProxyStep )[ 'Value' ].getDIRACGroup( ignoreDefault = ignoreDefault )

  def hasExpired( self ):
    """
    Is any of the elements in the chain expired?
    """
    if not self.__loadedChain:
      return S_ERROR( "No chain loaded" )
    for iC in range( len( self.__certList ) - 1, -1, -1 ):
      if self.__certList[iC].has_expired():
        return S_OK( True )
    return S_OK( False )

  def getNotAfterDate( self ):
    """
    Get the smallest not after date
    """
    if not self.__loadedChain:
      return S_ERROR( "No chain loaded" )
    notAfter = self.__certList[0].get_not_after()
    for iC in range( len( self.__certList ) - 1, -1, -1 ):
      stepNotAfter = self.__certList[iC].get_not_after()
      if self.__certList[iC].has_expired():
        return S_OK( stepNotAfter )
      if notAfter > stepNotAfter:
        notAfter = stepNotAfter
    return S_OK( notAfter )

  def generateProxyRequest( self, bitStrength = 1024, limited = False ):
    """
    Generate a proxy request
    Return S_OK( X509Request ) / S_ERROR
    """
    if not self.__loadedChain:
      return S_ERROR( "No chain loaded" )
    if not bitStrength:
      return S_ERROR( "bitStrength has to be greater than 1024 (%s)" % bitStrength )
    x509 = self.getCertInChain( 0 )[ 'Value' ]
    return x509.generateProxyRequest( bitStrength, limited )

  def generateChainFromRequestString( self, pemData, lifetime = 86400, requireLimited = False, diracGroup = False ):
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
      return S_ERROR( "Can't load request data: %s" % str( e ) )

    issuerCert = self.__certList[0]

    reqSubj = req.get_subject()
    newSubj = issuerCert.get_subject().clone()

    isLimited = False
    lastEntry = newSubj.get_entry( newSubj.num_entries() - 1 )
    if lastEntry[0] == "CN" and lastEntry[1] == "limited proxy":
      isLimited = True
    for entryTuple in reqSubj.get_components():
      if isLimited  and entryTuple[0] == "CN" and entryTuple[1] == "proxy":
        return S_ERROR( "Request is for a full proxy and chain is a limited one" )
      if entryTuple[0] == "CN" and entryTuple[1] == "limited proxy":
        isLimited = True
      newSubj.insert_entry( entryTuple[0], entryTuple[1] )

    if requireLimited and not isLimited:
      return S_ERROR( "Only limited proxies are allowed to be delegated but request was for a full one" )


    childCert = crypto.X509()
    childCert.set_subject( newSubj )
    childCert.set_issuer( issuerCert.get_subject() )
    childCert.set_serial_number( issuerCert.get_serial_number() )
    childCert.set_version( issuerCert.get_version() )
    childCert.set_pubkey( req.get_pubkey() )
    childCert.add_extensions( self.__getProxyExtensionList( diracGroup ) )
    childCert.gmtime_adj_notBefore( -900 )
    childCert.gmtime_adj_notAfter( int( lifetime ) )
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
    remainingSecs = self.getCertInChain( 0 )[ 'Value' ].getRemainingSecs()[ 'Value' ]
    for i in range( 1, len( self.__certList ) ):
      stepRS = self.getCertInChain( i )[ 'Value' ].getRemainingSecs()[ 'Value' ]
      remainingSecs = min( remainingSecs, stepRS )
    return S_OK( remainingSecs )

  def dumpAllToString( self ):
    """
    Dump all to string
    """
    if not self.__loadedChain:
      return S_ERROR( "No chain loaded" )
    data = crypto.dump_certificate( crypto.FILETYPE_PEM, self.__certList[0] )
    if self.__loadedPKey:
      data += crypto.dump_privatekey( crypto.FILETYPE_PEM, self.__keyObj )
    for i in range( 1, len( self.__certList ) ):
      data += crypto.dump_certificate( crypto.FILETYPE_PEM, self.__certList[i] )
    return S_OK( data )

  def dumpAllToFile( self, filename = False ):
    """
    Dump all to file. If no filename specified a temporal one will be created
    """
    retVal = self.dumpAllToString()
    if not retVal[ 'OK' ]:
      return retVal
    pemData = retVal['Value']
    try:
      if not filename:
        fd, filename = tempfile.mkstemp()
        os.write( fd, pemData )
        os.close( fd )
      else:
        fd = file( filename, "w" )
        fd.write( pemData )
        fd.close()
    except Exception, e:
      return S_ERROR( "Cannot write to file %s :%s" % ( filename, str( e ) ) )
    try:
      os.chmod( filename, stat.S_IRUSR | stat.S_IWUSR )
    except Exception, e:
      return S_ERROR( "Cannot set permissions to file %s :%s" % ( filename, str( e ) ) )
    return S_OK( filename )

  def dumpChainToString( self ):
    """
    Dump only cert chain to string
    """
    if not self.__loadedChain:
      return S_ERROR( "No chain loaded" )
    data = ''
    for i in range( len( self.__certList ) ):
      data += crypto.dump_certificate( crypto.FILETYPE_PEM, self.__certList[i] )
    return S_OK( data )

  def dumpPKeyToString( self ):
    """
    Dump key to string
    """
    if not self.__loadedPKey:
      return S_ERROR( "No chain loaded" )
    return S_OK( crypto.dump_privatekey( crypto.FILETYPE_PEM, self.__keyObj ) )

  def __str__( self ):
    repStr = "<X509Chain"
    if self.__loadedChain:
      repStr += " %s certs " % len( self.__certList )
      for cert in self.__certList:
        repStr += "[%s]" % cert.get_subject().one_line()
    if self.__loadedPKey:
      repStr += " with key"
    repStr += ">"
    return repStr

  def __repr__( self ):
    return self.__str__()

  def getCredentials( self, ignoreDefault = False ):
    if not self.__loadedChain:
      return S_ERROR( "No chain loaded" )
    credDict = { 'subject' : self.__certList[0].get_subject().one_line(),
                 'issuer' : self.__certList[0].get_issuer().one_line(),
                 'secondsLeft' : self.getRemainingSecs()[ 'Value' ],
                 'isProxy' : self.__isProxy,
                 'isLimitedProxy' : self.__isProxy and self.__isLimitedProxy,
                 'validDN' : False,
                 'validGroup' : False }
    if self.__isProxy:
      credDict[ 'identity'] = self.__certList[ self.__firstProxyStep + 1 ].get_subject().one_line()
      retVal = Registry.getUsernameForDN( credDict[ 'identity' ] )
      if not retVal[ 'OK' ]:
        return S_OK( credDict )
      credDict[ 'username' ] = retVal[ 'Value' ]
      credDict[ 'validDN' ] = True
      retVal = self.getDIRACGroup( ignoreDefault = ignoreDefault )
      if retVal[ 'OK' ]:
        diracGroup = retVal[ 'Value' ]
        credDict[ 'group' ] = diracGroup
        retVal = Registry.getGroupsForUser( credDict[ 'username' ] )
        if retVal[ 'OK' ] and diracGroup in retVal[ 'Value' ]:
          credDict[ 'validGroup' ] = True
          credDict[ 'groupProperties' ] = Registry.getPropertiesForGroup( diracGroup )
    else:
      retVal = Registry.getHostnameForDN( credDict['subject'] )
      if retVal[ 'OK' ]:
        credDict[ 'group' ] = 'hosts'
        credDict[ 'hostname' ] = retVal[ 'Value' ]
        credDict[ 'validDN' ] = True
        credDict[ 'validGroup' ] = True
        credDict[ 'groupProperties' ] = Registry.getHostOption( credDict[ 'hostname' ], 'Properties' )
      retVal = Registry.getUsernameForDN( credDict[ 'subject' ] )
      if retVal[ 'OK' ]:
        credDict[ 'username' ] = retVal[ 'Value' ]
        credDict[ 'validDN' ] = True
    return S_OK( credDict )

  def hash( self ):
    if not self.__loadedChain:
      return S_ERROR( "No chain loaded" )
    if self.__hash:
      return S_OK( self.__hash )
    sha1 = hashlib.sha1()
    for cert in self.__certList:
      sha1.update( cert.get_subject().one_line() )
    sha1.update( str( self.getRemainingSecs()[ 'Value' ] / 3600 ) )
    sha1.update( self.getDIRACGroup()[ 'Value' ] )
    if self.isVOMS():
      sha1.update( "VOMS" )
      from DIRAC.Core.Security.VOMS import VOMS
      result = VOMS().getVOMSAttributes( self )
      if result[ 'OK' ]:
        sha1.update( str( result[ 'Value' ] ) )
    self.__hash = sha1.hexdigest()
    return S_OK( self.__hash )


g_X509ChainType = type( X509Chain() )
