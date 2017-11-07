""" X509Chain is a class for managing X509 chains with their Pkeys
"""
__RCSID__ = "$Id$"

import os
import stat
import tempfile
import hashlib
import random

import M2Crypto
import re
import time
import GSI # XXX Still needed for some parts I haven't finished yet

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Security.X509Certificate import X509Certificate, LIMITED_PROXY_OID
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Security.X509Certificate import X509Certificate, LIMITED_PROXY_OID

random.seed()


class X509Chain(object):
  
  __pass = None
  def __getPass(self):
    return self.__pass

  __validExtensionValueTypes = ( basestring, )

  __validExtensionValueTypes = (basestring, )

  def __init__(self, certList=False, keyObj=False):
    self.__isProxy = False
    self.__firstProxyStep = 0
    self.__isLimitedProxy = True
    self.__hash = False
    if certList:
      self.__loadedChain = True
      self.__certList = []
      for cert in certList:
        if not isinstance( cert, M2Crypto.X509.X509 ):
          # XXX walkaround for legacy code that is not updated yet, should be removed later
          tmpCert = X509Certificate( certString = GSI.crypto.dump_certificate( GSI.crypto.FILETYPE_PEM, cert) )
          cert = tmpCert
        self.__certList.append( cert )
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
  def instanceFromFile(cls, chainLocation):
    """ Instance a X509Chain from a file
    """
    chain = cls()
    result = chain.loadChainFromFile(chainLocation)
    if not result['OK']:
      return result
    return S_OK(chain)

  def loadChainFromFile(self, chainLocation):
    """
    Load a x509 chain from a pem file
    Return : S_OK / S_ERROR
    """
    try:
      with open(chainLocation) as fd:
        pemData = fd.read()
    except IOError as e:
      return S_ERROR( DErrno.EOF, "%s: %s" % ( chainLocation, repr( e ).replace( ',)', ')' ) ) )
    return self.loadChainFromString( pemData )

  def loadChainFromString( self, data, dataFormat = M2Crypto.X509.FORMAT_PEM ):
    """
    Load a x509 cert from a string containing the pem data
    Return : S_OK / S_ERROR
    """
    self.__loadedChain = False
    try:
      self.__certList = self.__certListFromPemString(data, dataFormat)
      self.loadKeyFromString(data)
    except Exception as e:
      return S_ERROR(DErrno.ECERTREAD, "%s" % repr(e).replace(',)', ')'))
    if not self.__certList:
      return S_ERROR(DErrno.EX509)
    self.__loadedChain = True
    # Update internals
    self.__checkProxyness()
    return S_OK()

  def __certListFromPemString( self, certString, format = M2Crypto.X509.FORMAT_PEM ):
    """
    Create certificates list from string. String sould contain certificates, just like plain text proxy file.
    """
    # To get list of X509 certificates (not X509 Certificate Chain) from string it has to be parsed like that (constructors are not able to deal with big string)
    return [ X509Certificate( certString = cert[0] ) for cert in re.findall(r"(-----BEGIN CERTIFICATE-----((.|\n)*?)-----END CERTIFICATE-----)", certString) ]

  def __certListFromPemString( self, certString, format = M2Crypto.X509.FORMAT_PEM ):
    """
    Create certificates list from string. String sould contain certificates, just like plain text proxy file.
    """
    # To get list of X509 certificates (not X509 Certificate Chain) from string it has to be parsed like that (constructors are not able to deal with big string)
    return [ X509Certificate( certString = cert[0] ) for cert in re.findall(r"(-----BEGIN CERTIFICATE-----((.|\n)*?)-----END CERTIFICATE-----)", certString) ]

  def setChain( self, certList ):
    """
    Set the chain
    Return : S_OK / S_ERROR
    """
    self.__certList = certList
    self.__loadedChain = True
    return S_OK()

  def loadKeyFromFile(self, chainLocation, password=False):
    """
    Load a PKey from a pem file
    Return : S_OK / S_ERROR
    """
    try:
      with open(chainLocation) as fd:
        pemData = fd.read()
    except Exception as e:
      return S_ERROR(DErrno.EOF, "%s: %s" % (chainLocation, repr(e).replace(',)', ')')))
    return self.loadKeyFromString(pemData, password)

  def loadKeyFromString(self, pemData, password=False):
    """
    Load a xPKey from a string containing the pem data
    Return : S_OK / S_ERROR
    """
    self.__loadedPKey = False
    if password:
      self.__pass = password
    try:
      self.__keyObj = M2Crypto.EVP.load_key_string( pemData, lambda x: self.__pass )
    except Exception as e:
      return S_ERROR(DErrno.ECERTREAD, "%s (Probably bad pass phrase?)" % repr(e).replace(',)', ')'))
    self.__loadedPKey = True
    return S_OK()

  def setPKey(self, pkeyObj):
    """
    Set the chain
    Return : S_OK / S_ERROR
    """
    self.__keyObj = pkeyObj
    self.__loadedPKey = True
    return S_OK()

  def loadProxyFromFile(self, chainLocation):
    """
    Load a Proxy from a pem file
    Return : S_OK / S_ERROR
    """
    try:
      with open(chainLocation) as fd:
        pemData = fd.read()
    except Exception as e:
      return S_ERROR(DErrno.EOF, "%s: %s" % (chainLocation, repr(e).replace(',)', ')')))
    return self.loadProxyFromString(pemData)

  def loadProxyFromString(self, pemData):
    """
    Load a Proxy from a pem buffer
    Return : S_OK / S_ERROR
    """
    retVal = self.loadChainFromString(pemData)
    if not retVal['OK']:
      return retVal
    return self.loadKeyFromString( pemData )

  def __getProxyExtensionList(self, diracGroup=False, limited=False):
    """
    Get the list of extensions for a proxy
    """
    extList = []
    extList.append(crypto.X509Extension('keyUsage',
                                        'critical, digitalSignature, keyEncipherment, dataEncipherment'))
    if diracGroup and isinstance(diracGroup, self.__validExtensionValueTypes):
      extList.append(crypto.X509Extension('diracGroup', diracGroup))
    if limited:
      asn1Obj = crypto.ASN1([[LIMITED_PROXY_OID]])
      asn1Obj[0][0].convert_to_object()
      asn1dump = binascii.hexlify(asn1Obj.dump())
      extval = "critical,DER:" + ":".join(asn1dump[i:i + 2] for i in range(0, len(asn1dump), 2))
      ext = crypto.X509Extension("proxyCertInfo", extval)
      extList.append(ext)
    return extList

  def getCertInChain(self, certPos=0):
    """
    Get a certificate in the chain
    """
    if not self.__loadedChain:
      return S_ERROR( DErrno.ENOCHAIN )
    return S_OK( self.__certList[ certPos ] )

  def getIssuerCert(self):
    """
    Get a issuer cert in the chain
    """
    if not self.__loadedChain:
      return S_ERROR(DErrno.ENOCHAIN)
    if self.__isProxy:
      return S_OK( self.__certList[ self.__firstProxyStep + 1 ] )
    else:
      return S_OK( self.__certList[ -1 ] )

  def getPKeyObj(self):
    """
    Get the pkey obj
    """
    if not self.__loadedPKey:
      return S_ERROR(DErrno.ENOCHAIN)
    return S_OK(self.__keyObj)

  def getCertList(self):
    """
    Get the cert list
    """
    if not self.__loadedChain:
      return S_ERROR(DErrno.ENOCHAIN)
    return S_OK(self.__certList)

  def getNumCertsInChain(self):
    """
    Numbers of certificates in chain
    """
    if not self.__loadedChain:
      return S_ERROR(DErrno.ENOCHAIN)
    return S_OK(len(self.__certList))

  def generateProxyToString(self, lifeTime, diracGroup=False, strength=1024, limited=False, proxyKey=False):
    """
    Generate a proxy and get it as a string

    Args:
        lifeTime (int): expected lifetime in seconds of proxy
        diracGroup (str): diracGroup to add to the certificate
        strength (int): length in bits of the pair
        limited (bool): Create a limited proxy

    """
    if not self.__loadedChain:
      return S_ERROR(DErrno.ENOCHAIN)
    if not self.__loadedPKey:
      return S_ERROR(DErrno.ENOPKEY)

    issuerCert = self.__certList[0]

    if not proxyKey:
      # Generating key is a two step process: create key object and then assign RSA key.
      proxyKey = M2Crypto.EVP.PKey()
      proxyKey.assign_rsa(M2Crypto.RSA.gen_key(strength, 65537, callback = M2Crypto.util.quiet_genparam_callback ))

    proxyCert = M2Crypto.X509.X509()

    proxyCert.set_serial_number(str(int(random.random() * 10 ** 10)))
    cloneSubject = issuerCert.get_subject().clone()
    cloneSubject.insert_entry("CN", str(int(random.random() * 10 ** 10)))
    proxyCert.set_subject(cloneSubject)
    proxyCert.add_extensions(self.__getProxyExtensionList(diracGroup, limited))

    subject = issuerCert.getSubjectNameObject()
    if subject['OK']:
      proxyCert.setIssuer( subject['Value'] )
    else:
      return subject
    version = issuerCert.getVersion()
    if version['OK']:
      proxyCert.setVersion( version['Value'] )
    else:
      return version
    proxyCert.setPublicKey( proxyKey )
    proxyNotBefore = M2Crypto.ASN1.ASN1_UTCTIME()
    proxyNotBefore.set_time( int( time.time() ) - 900 )
    proxyCert.setNotBefore( proxyNotBefore )
    proxyNotAfter = M2Crypto.ASN1.ASN1_UTCTIME()
    proxyNotAfter.set_time( int( time.time() ) + lifeTime )
    proxyCert.set_not_after( proxyNotAfter )
    subject = issuerCert.getSubjectNameObject()
    if subject['OK']:
      proxyCert.setIssuer( subject['Value'] )
    else:
      return subject
    version = issuerCert.getVersion()
    if version['OK']:
      proxyCert.setVersion( version['Value'] )
    else:
      return version
    proxyCert.setPublicKey( proxyKey )
    proxyNotBefore = M2Crypto.ASN1.ASN1_UTCTIME()
    proxyNotBefore.set_time( int( time.time() ) - 900 )
    proxyCert.setNotBefore( proxyNotBefore )
    proxyNotAfter = M2Crypto.ASN1.ASN1_UTCTIME()
    proxyNotAfter.set_time( int( time.time() ) + lifeTime )
    proxyCert.setNotAfter( proxyNotAfter )
    proxyCert.sign( self.__keyObj, 'sha256' )
    proxyString = "%s%s" % ( proxyCert.asPem(), proxyKey.as_pem( cipher = None, callback = M2Crypto.util.no_passphrase_callback ) )
    for i in range( len( self.__certList ) ):
      crt = self.__certList[i]
      proxyString += crt.asPem()
    return S_OK( proxyString )

    proxyString = "%s%s" % (crypto.dump_certificate(crypto.FILETYPE_PEM, proxyCert),
                            crypto.dump_privatekey(crypto.FILETYPE_PEM, proxyKey))
    for i in range(len(self.__certList)):
      proxyString += crypto.dump_certificate(crypto.FILETYPE_PEM, self.__certList[i])

    return S_OK(proxyString)

  def generateProxyToFile(self, filePath, lifeTime, diracGroup=False, strength=1024, limited=False):
    """
    Generate a proxy and put it into a file

    Args:
        filePath: file to write
        lifeTime: expected lifetime in seconds of proxy
        diracGroup: diracGroup to add to the certificate
        strength: length in bits of the pair
        limited: Create a limited proxy
    """
    retVal = self.generateProxyToString(lifeTime, diracGroup, strength, limited)
    if not retVal['OK']:
      return retVal
    try:
      with open(filePath, 'w') as fd:
        fd.write(retVal['Value'])
    except Exception as e:
      return S_ERROR(DErrno.EWF, "%s :%s" % (filePath, repr(e).replace(',)', ')')))
    try:
      os.chmod(filePath, stat.S_IRUSR | stat.S_IWUSR)
    except Exception as e:
      return S_ERROR(DErrno.ESPF, "%s :%s" % (filePath, repr(e).replace(',)', ')')))
    return S_OK()

  def isProxy(self):
    """
    Check wether this chain is a proxy
    """
    if not self.__loadedChain:
      return S_ERROR(DErrno.ENOCHAIN)
    return S_OK(self.__isProxy)

  def isLimitedProxy(self):
    """
    Check wether this chain is a proxy
    """
    if not self.__loadedChain:
      return S_ERROR(DErrno.ENOCHAIN)
    return S_OK(self.__isProxy and self.__isLimitedProxy)

  def isValidProxy(self, ignoreDefault=False):
    """
    Check wether this chain is a valid proxy
      checks if its a proxy
      checks if its expired
    """
    if not self.__loadedChain:
      return S_ERROR(DErrno.ENOCHAIN)
    if not self.__isProxy:
      return S_ERROR(DErrno.ENOCHAIN, "Chain is not a proxy")
    elif self.hasExpired()['Value']:
      return S_ERROR(DErrno.ENOCHAIN)
    elif ignoreDefault:
      groupRes = self.getDIRACGroup(ignoreDefault=ignoreDefault)
      if not groupRes['OK']:
        return groupRes
      if not groupRes['Value']:
        return S_ERROR(DErrno.ENOGROUP)
    return S_OK(True)

  def isVOMS(self):
    """
    Check wether this chain is a proxy
    """
    retVal = self.isProxy()
    if not retVal['OK'] or not retVal['Value']:
      return retVal
    for i in range(len(self.__certList)):
      cert = self.getCertInChain(i)['Value']
      if cert.hasVOMSExtensions()['Value']:
        return S_OK(True)
    return S_OK(False)

  def getVOMSData(self):
    """
    Check wether this chain is a proxy
    """
    retVal = self.isProxy()
    if not retVal['OK'] or not retVal['Value']:
      return retVal
    for i in range(len(self.__certList)):
      cert = self.getCertInChain(i)['Value']
      res = cert.getVOMSData()
      if res['OK']:
        return res
    return S_ERROR(DErrno.EVOMS)


  def __checkProxyness( self ):
    # XXX to describe
    self.__hash = False
    self.__firstProxyStep = len(self.__certList) - 2  # -1 is user cert by default, -2 is first proxy step
    self.__isProxy = True
    self.__isLimitedProxy = False
    prevDNMatch = 2
    # If less than 2 steps in the chain is no proxy
    if len(self.__certList) < 2:
      self.__isProxy = False
      return
    # Check proxyness in steps
    for step in range(len(self.__certList) - 1):
      issuerMatch = self.__checkIssuer(step, step + 1)
      if not issuerMatch:
        self.__isProxy = False
        return
      # Do we need to check the proxy DN?
      if prevDNMatch:
        dnMatch = self.__checkProxyDN(step, step + 1)
        # No DN match
        if dnMatch == 0:
          # If we are not in the first step we've found the entity cert
          if step > 0:
            self.__firstProxyStep = step - 1
          # If we are in the first step this is not a proxy
          else:
            self.__isProxy = False
            return
        # Limited proxy DN match
        elif dnMatch == 2:
          self.__isLimitedProxy = True
          if prevDNMatch != 2:
            self.__isProxy = False
            self.__isLimitedProxy = False
            return
        prevDNMatch = dnMatch

  def __checkProxyDN(self, certStep, issuerStep):
    """
    Check the proxy DN in a step in the chain
     0 = no match
     1 = proxy match
     2 = limited proxy match
    """
    issuerSubject = self.__certList[ issuerStep ].getSubjectNameObject()
    if issuerSubject[ 'OK' ]:
      issuerSubject = issuerSubject[ 'Value' ]
    else:
      return 0
    proxySubject = self.__certList[ certStep ].getSubjectNameObject()
    if proxySubject[ 'OK' ]:
      proxySubject = proxySubject[ 'Value' ]
    else:
      return 0
    lastEntry = str(proxySubject).split('/')[-1].split('=')
    limited = False
    if lastEntry[0] != 'CN':
      return 0
    if lastEntry[1] not in ('proxy', 'limited proxy'):
      extList = self.__certList[certStep].get_extensions()
      for ext in extList:
        if ext.get_sn() == "proxyCertInfo":
          contraint = [line.split(":")[1].strip() for line in ext.get_value().split("\n")
                       if line.split(":")[0] == "Path Length Constraint"]
          if not contraint:
            return 0
          if contraint[0] == LIMITED_PROXY_OID:
            limited = True
    else:
      if lastEntry[1] == "limited proxy":
        limited = True
    if not str(issuerSubject) == str(proxySubject)[:str( proxySubject ).rfind( '/' )]:
      return 0
    return 1 if not limited else 2

  def __checkIssuer(self, certStep, issuerStep):
    """
    Check the issuer is really the issuer
    """
    issuerCert = self.__certList[ issuerStep ]
    cert = self.__certList[ certStep ]
    pubKey = issuerCert.getPublicKey()
    if pubKey['OK']:
      return cert.verify( pubKey['Value'] )
    else:
      return pubKey

  def getDIRACGroup(self, ignoreDefault=False):
    """
    Get the dirac group if present
    """
    if not self.__loadedChain:
      return S_ERROR(DErrno.ENOCHAIN)
    if not self.__isProxy:
      return S_ERROR(DErrno.EX509, "Chain does not contain a valid proxy")
    if self.isPUSP()['Value']:
      return self.getCertInChain(self.__firstProxyStep - 2)['Value'].getDIRACGroup(ignoreDefault=ignoreDefault)
    # The code below will find the first match of the DIRAC group
    for i in range(len(self.__certList) - 1, -1, -1):
      retVal = self.getCertInChain(i)['Value'].getDIRACGroup(ignoreDefault=True)
      if retVal['OK'] and 'Value' in retVal and retVal['Value']:
        return retVal
    # No DIRAC group found, try to get the default one
    return self.getCertInChain(self.__firstProxyStep)['Value'].getDIRACGroup(ignoreDefault=ignoreDefault)

  def hasExpired(self):
    """
    Is any of the elements in the chain expired?
    """
    if not self.__loadedChain:
      return S_ERROR( DErrno.ENOCHAIN )
    for iC in range( len( self.__certList ) - 1, -1, -1 ):
      expired = self.__certList[iC].hasExpired()
      if expired['OK']:
        if expired[ 'Value' ]:
          return S_OK( True )
      else:
        return expired
    return S_OK( False )

  def getNotAfterDate(self):
    """
    Get the smallest not after date
    """
    if not self.__loadedChain:
      return S_ERROR( DErrno.ENOCHAIN )
    notAfter = self.__certList[0].getNotAfterDate()
    if not notAfter['OK']:
      return notAfter
    notAfter = notAfter['Value']
    for iC in range( len( self.__certList ) - 1, -1, -1 ):
      stepNotAfter = self.__certList[iC].getNotAfterDate()
      if not stepNotAfter['OK']:
        return stepNotAfter
      expired = self.__certList[iC].hasExpired()
      if not expired['OK']:
        return expired
      if expired['Value']:
        return S_OK( stepNotAfter )
      if notAfter > stepNotAfter:
        notAfter = stepNotAfter
    return S_OK(notAfter)

  def generateProxyRequest(self, bitStrength=1024, limited=False):
    """
    Generate a proxy request
    Return S_OK( X509Request ) / S_ERROR
    """
    if not self.__loadedChain:
      return S_ERROR(DErrno.ENOCHAIN)
    if not bitStrength:
      return S_ERROR(DErrno.EX509, "bitStrength has to be greater than 1024 (%s)" % bitStrength)
    x509 = self.getCertInChain(0)['Value']
    return x509.generateProxyRequest(bitStrength, limited)

  def generateChainFromRequestString(self, pemData, lifetime=86400, requireLimited=False, diracGroup=False):
    """
    Generate a x509 chain from a request
    return S_OK( string ) / S_ERROR
    """
    if not self.__loadedChain:
      return S_ERROR(DErrno.ENOCHAIN)
    if not self.__loadedPKey:
      return S_ERROR(DErrno.ENOPKEY)
    try:
      req = M2Crypto.X509.load_request_string( pemData, format=M2Crypto.X509.FORMAT_PEM )
    except Exception as e:
      return S_ERROR(DErrno.ECERTREAD, "Can't load request data: %s" % repr(e).replace(',)', ')'))
    limited = requireLimited and self.isLimitedProxy().get('Value', False)
    return self.generateProxyToString(lifetime, diracGroup, 1024, limited, req.get_pubkey())

  def getRemainingSecs(self):
    """
    Get remaining time
    """
    if not self.__loadedChain:
      return S_ERROR(DErrno.ENOCHAIN)
    remainingSecs = self.getCertInChain(0)['Value'].getRemainingSecs()['Value']
    for i in range(1, len(self.__certList)):
      stepRS = self.getCertInChain(i)['Value'].getRemainingSecs()['Value']
      remainingSecs = min(remainingSecs, stepRS)
    return S_OK(remainingSecs)

  def dumpAllToString(self):
    """
    Dump all to string
    """
    if not self.__loadedChain:
      return S_ERROR( DErrno.ENOCHAIN )
    data = self.__certList[0].asPem()
    if self.__loadedPKey:
      data += self.__keyObj.as_pem( cipher = None, callback = M2Crypto.util.no_passphrase_callback )
    for i in range( 1, len( self.__certList ) ):
      data += self.__certList[i].asPem()
    return S_OK( data )

  def dumpAllToFile(self, filename=False):
    """
    Dump all to file. If no filename specified a temporal one will be created
    """
    retVal = self.dumpAllToString()
    if not retVal['OK']:
      return retVal
    pemData = retVal['Value']
    try:
      if not filename:
        fd, filename = tempfile.mkstemp()
        os.write(fd, pemData)
        os.close(fd)
      else:
        with open(filename, "w") as fd:
          fd.write(pemData)
    except Exception as e:
      return S_ERROR(DErrno.EWF, "%s :%s" % (filename, repr(e).replace(',)', ')')))
    try:
      os.chmod(filename, stat.S_IRUSR | stat.S_IWUSR)
    except Exception as e:
      return S_ERROR(DErrno.ESPF, "%s :%s" % (filename, repr(e).replace(',)', ')')))
    return S_OK(filename)

  def dumpChainToString(self):
    """
    Dump only cert chain to string
    """
    if not self.__loadedChain:
      return S_ERROR(DErrno.ENOCHAIN)
    data = ''
    for i in range( len( self.__certList ) ):
      res =  self.__certList[i].asPem()
      if res[ 'OK' ]:
        data += res[ 'Value' ]
    return S_OK( data )

  def dumpPKeyToString(self):
    """
    Dump key to string
    """
    if not self.__loadedPKey:
      return S_ERROR( DErrno.ENOCHAIN )
    return S_OK( self.__keyObj.as_pem( cipher = None, callback = M2Crypto.util.no_passphrase_callback ) )

  def __str__(self):
    repStr = "<X509Chain"
    if self.__loadedChain:
      repStr += " %s certs " % len(self.__certList)
      for cert in self.__certList:
        repStr += "[%s]" % str(cert.getSubjectNameObject())
    if self.__loadedPKey:
      repStr += " with key"
    repStr += ">"
    return repStr

  def __repr__(self):
    return self.__str__()

  def isPUSP(self):
    if self.__isProxy:
      # Check if we have a subproxy
      dn = self.__certList[ self.__firstProxyStep ].getSubjectDN()
      if dn[ 'OK' ]:
        dn = dn[ 'Value' ]
      else:
        return dn
      subproxyUser = isPUSPdn( dn )
      if subproxyUser:
        result = S_OK(True)
        result['Identity'] = dn
        result['SubproxyUser'] = subproxyUser
        return result

    return S_OK(False)

  def getCredentials(self, ignoreDefault=False):
    if not self.__loadedChain:
      return S_ERROR( DErrno.ENOCHAIN )
    credDict = { 'subject' : str(self.__certList[0].getSubjectDN()['Value']),  # ['Value'] :(
                 'issuer' : self.__certList[0].getIssuerDN()['Value'],  # ['Value'] :(
                 'secondsLeft' : self.getRemainingSecs()[ 'Value' ],
                 'isProxy' : self.__isProxy,
                 'isLimitedProxy' : self.__isProxy and self.__isLimitedProxy,
                 'validDN' : False,
                 'validGroup' : False }
    if self.__isProxy:
      credDict[ 'identity'] = str(self.__certList[ self.__firstProxyStep + 1 ].getSubjectDN()['Value'])  # ['Value'] :(

      # Check if we have the PUSP case
      result = self.isPUSP()
      if result['OK'] and result['Value']:
        credDict['identity'] = result['Identity']
        credDict['subproxyUser'] = result['SubproxyUser']

      retVal = Registry.getUsernameForDN(credDict['identity'])
      if not retVal['OK']:
        return S_OK(credDict)
      credDict['username'] = retVal['Value']
      credDict['validDN'] = True
      retVal = self.getDIRACGroup(ignoreDefault=ignoreDefault)
      if retVal['OK']:
        diracGroup = retVal['Value']
        credDict['group'] = diracGroup
        retVal = Registry.getGroupsForUser(credDict['username'])
        if retVal['OK'] and diracGroup in retVal['Value']:
          credDict['validGroup'] = True
          credDict['groupProperties'] = Registry.getPropertiesForGroup(diracGroup)
    else:
      retVal = Registry.getHostnameForDN(credDict['subject'])
      if retVal['OK']:
        credDict['group'] = 'hosts'
        credDict['hostname'] = retVal['Value']
        credDict['validDN'] = True
        credDict['validGroup'] = True
        credDict['groupProperties'] = Registry.getHostOption(credDict['hostname'], 'Properties')
      retVal = Registry.getUsernameForDN(credDict['subject'])
      if retVal['OK']:
        credDict['username'] = retVal['Value']
        credDict['validDN'] = True
    return S_OK(credDict)

  def hash(self):
    if not self.__loadedChain:
      return S_ERROR(DErrno.ENOCHAIN)
    if self.__hash:
      return S_OK(self.__hash)
    sha1 = hashlib.sha1()
    for cert in self.__certList:
      sha1.update( str(cert.getSubjectNameObject()) )
    sha1.update( str( self.getRemainingSecs()[ 'Value' ] / 3600 ) )
    sha1.update( self.getDIRACGroup()[ 'Value' ] )
    if self.isVOMS():
      sha1.update("VOMS")
      from DIRAC.Core.Security.VOMS import VOMS
      result = VOMS().getVOMSAttributes(self)
      if result['OK']:
        sha1.update(str(result['Value']))
    self.__hash = sha1.hexdigest()
    return S_OK(self.__hash)


def isPUSPdn(userDN):
  """ Evaluate if the DN is of the PUSP type or not

  :param str userDN: user DN string
  :return: the subproxy user name or None
  """
  lastEntry = userDN.split('/')[-1].split('=')
  if lastEntry[0] == "CN" and lastEntry[1].startswith("user:"):
    return userDN.split('/')[-1].split(':')[1]
  return None


g_X509ChainType = type(X509Chain())
