""" X509Chain is a class for managing X509 chains with their Pkeys
"""
__RCSID__ = "$Id$"

import os
import stat
import tempfile
import hashlib
import random
import binascii

from GSI import crypto

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Security.pygsi.X509Certificate import X509Certificate, LIMITED_PROXY_OID
from DIRAC.ConfigurationSystem.Client.Helpers import Registry

random.seed()


class X509Chain(object):

  __pass = None

  def __getPass(self):
    return self.__pass

  __validExtensionValueTypes = (basestring,)

  def __init__(self, certList=False, keyObj=False):
    self.__isProxy = False
    self.__firstProxyStep = 0
    self.__isLimitedProxy = True
    self.__isRFC = False
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
      return S_ERROR(DErrno.EOF, "%s: %s" % (chainLocation, repr(e).replace(',)', ')')))
    return self.loadChainFromString(pemData)

  def loadChainFromString(self, data, dataFormat=crypto.FILETYPE_PEM):
    """
    Load a x509 cert from a string containing the pem data
    Return : S_OK / S_ERROR
    """
    self.__loadedChain = False
    try:
      self.__certList = crypto.load_certificate_chain(crypto.FILETYPE_PEM, data)
    except Exception as e:
      return S_ERROR(DErrno.ECERTREAD, "%s" % repr(e).replace(',)', ')'))
    if not self.__certList:
      return S_ERROR(DErrno.EX509)
    self.__loadedChain = True
    # Update internals
    self.__checkProxyness()
    return S_OK()

  def setChain(self, certList):
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
    try:
      self.__keyObj = crypto.load_privatekey(crypto.FILETYPE_PEM, pemData, password)
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
    return self.loadKeyFromString(pemData)

  def __getProxyExtensionList(self, diracGroup=False, rfc=False, rfcLimited=False):
    """
    Get the list of extensions for a proxy
    """
    extList = []
    extList.append(crypto.X509Extension('keyUsage',
                                        'critical, digitalSignature, keyEncipherment, dataEncipherment'))
    if diracGroup and isinstance(diracGroup, self.__validExtensionValueTypes):
      extList.append(crypto.X509Extension('diracGroup', diracGroup))
    if rfc or rfcLimited:
      blob = [["1.3.6.1.5.5.7.21.1"]] if not rfcLimited else [[LIMITED_PROXY_OID]]
      asn1Obj = crypto.ASN1(blob)
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
      return S_ERROR(DErrno.ENOCHAIN)
    return S_OK(X509Certificate(self.__certList[certPos]))

  def getIssuerCert(self):
    """
    Get a issuer cert in the chain
    """
    if not self.__loadedChain:
      return S_ERROR(DErrno.ENOCHAIN)
    if self.__isProxy:
      return S_OK(X509Certificate(self.__certList[self.__firstProxyStep + 1]))

    return S_OK(X509Certificate(self.__certList[-1]))

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

  def generateProxyToString(self, lifeTime, diracGroup=False, strength=1024, limited=False, rfc=False, proxyKey=False):
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

    if self.__isProxy:
      rfc = self.isRFC().get('Value', False)

    issuerCert = self.__certList[0]

    if not proxyKey:
      proxyKey = crypto.PKey()
      proxyKey.generate_key(crypto.TYPE_RSA, strength)

    proxyCert = crypto.X509()

    if rfc:
      proxyCert.set_serial_number(str(int(random.random() * 10 ** 10)))
      cloneSubject = issuerCert.get_subject().clone()
      cloneSubject.insert_entry("CN", str(int(random.random() * 10 ** 10)))
      proxyCert.set_subject(cloneSubject)
      proxyCert.add_extensions(self.__getProxyExtensionList(diracGroup, rfc and not limited, rfc and limited))
    else:
      proxyCert.set_serial_number(issuerCert.get_serial_number())
      cloneSubject = issuerCert.get_subject().clone()
      if limited:
        cloneSubject.insert_entry("CN", "limited proxy")
      else:
        cloneSubject.insert_entry("CN", "proxy")
      proxyCert.set_subject(cloneSubject)
      proxyCert.add_extensions(self.__getProxyExtensionList(diracGroup))

    proxyCert.set_issuer(issuerCert.get_subject())
    proxyCert.set_version(issuerCert.get_version())
    proxyCert.set_pubkey(proxyKey)
    proxyCert.gmtime_adj_notBefore(-900)
    proxyCert.gmtime_adj_notAfter(int(lifeTime))
    proxyCert.sign(self.__keyObj, 'sha256')

    proxyString = "%s%s" % (crypto.dump_certificate(crypto.FILETYPE_PEM, proxyCert),
                            crypto.dump_privatekey(crypto.FILETYPE_PEM, proxyKey))
    for i in range(len(self.__certList)):
      proxyString += crypto.dump_certificate(crypto.FILETYPE_PEM, self.__certList[i])

    return S_OK(proxyString)

  def generateProxyToFile(self, filePath, lifeTime, diracGroup=False, strength=1024, limited=False, rfc=False):
    """
    Generate a proxy and put it into a file

    Args:
        filePath: file to write
        lifeTime: expected lifetime in seconds of proxy
        diracGroup: diracGroup to add to the certificate
        strength: length in bits of the pair
        limited: Create a limited proxy
    """
    retVal = self.generateProxyToString(lifeTime, diracGroup, strength, limited, rfc)
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

  def __checkProxyness(self):
    self.__hash = False
    self.__firstProxyStep = len(self.__certList) - 2  # -1 is user cert by default, -2 is first proxy step
    self.__isProxy = True
    self.__isRFC = None
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

    issuerSubject = self.__certList[issuerStep].get_subject()
    proxySubject = self.__certList[certStep].get_subject().clone()
    psEntries = proxySubject.num_entries()
    lastEntry = proxySubject.get_entry(psEntries - 1)
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
          if self.__isRFC is None:
            self.__isRFC = True
          if contraint[0] == LIMITED_PROXY_OID:
            limited = True
    else:
      if self.__isRFC is None:
        self.__isRFC = False
      if lastEntry[1] == "limited proxy":
        limited = True
    proxySubject.remove_entry(psEntries - 1)
    if not issuerSubject.one_line() == proxySubject.one_line():
      return 0
    return 1 if not limited else 2

  def __checkIssuer(self, certStep, issuerStep):
    """
    Check the issuer is really the issuer
    """
    issuerCert = self.__certList[issuerStep]
    cert = self.__certList[certStep]
    return cert.verify_pkey_is_issuer(issuerCert.get_pubkey())

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
      return S_ERROR(DErrno.ENOCHAIN)
    for iC in range(len(self.__certList) - 1, -1, -1):
      if self.__certList[iC].has_expired():
        return S_OK(True)
    return S_OK(False)

  def getNotAfterDate(self):
    """
    Get the smallest not after date
    Does not return the smallest limitation
    """
    if not self.__loadedChain:
      return S_ERROR(DErrno.ENOCHAIN)
    notAfter = self.__certList[0].get_not_after()
    for iC in range(len(self.__certList) - 1, -1, -1):
      stepNotAfter = self.__certList[iC].get_not_after()
      if self.__certList[iC].has_expired():
        return S_OK(stepNotAfter)
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

  def generateChainFromRequestString(self, pemData, lifetime=86400, requireLimited=False, diracGroup=False, rfc=True):
    """
    Generate a x509 chain from a request
    return S_OK( string ) / S_ERROR
    """
    if not self.__loadedChain:
      return S_ERROR(DErrno.ENOCHAIN)
    if not self.__loadedPKey:
      return S_ERROR(DErrno.ENOPKEY)
    try:
      req = crypto.load_certificate_request(crypto.FILETYPE_PEM, pemData)
    except Exception as e:
      return S_ERROR(DErrno.ECERTREAD, "Can't load request data: %s" % repr(e).replace(',)', ')'))
    limited = requireLimited and self.isLimitedProxy().get('Value', False)
    return self.generateProxyToString(lifetime, diracGroup, 1024, limited, rfc, req.get_pubkey())

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
      return S_ERROR(DErrno.ENOCHAIN)
    data = crypto.dump_certificate(crypto.FILETYPE_PEM, self.__certList[0])
    if self.__loadedPKey:
      data += crypto.dump_privatekey(crypto.FILETYPE_PEM, self.__keyObj)
    for i in range(1, len(self.__certList)):
      data += crypto.dump_certificate(crypto.FILETYPE_PEM, self.__certList[i])
    return S_OK(data)

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

  def isRFC(self):
    if not self.__loadedChain:
      return S_ERROR(DErrno.ENOCHAIN)
    return S_OK(self.__isRFC)

  def dumpChainToString(self):
    """
    Dump only cert chain to string
    """
    if not self.__loadedChain:
      return S_ERROR(DErrno.ENOCHAIN)
    data = ''
    for i in range(len(self.__certList)):
      data += crypto.dump_certificate(crypto.FILETYPE_PEM, self.__certList[i])
    return S_OK(data)

  def dumpPKeyToString(self):
    """
    Dump key to string
    """
    if not self.__loadedPKey:
      return S_ERROR(DErrno.ENOCHAIN)
    return S_OK(crypto.dump_privatekey(crypto.FILETYPE_PEM, self.__keyObj))

  def __str__(self):
    repStr = "<X509Chain"
    if self.__loadedChain:
      repStr += " %s certs " % len(self.__certList)
      for cert in self.__certList:
        repStr += "[%s]" % cert.get_subject().one_line()
    if self.__loadedPKey:
      repStr += " with key"
    repStr += ">"
    return repStr

  def __repr__(self):
    return self.__str__()

  def isPUSP(self):
    if self.__isProxy:
      # Check if we have a subproxy
      trialSubidentity = self.__certList[self.__firstProxyStep].get_subject()
      dn = trialSubidentity.one_line()
      subproxyUser = isPUSPdn(dn)
      if subproxyUser:
        result = S_OK(True)
        result['Identity'] = dn
        result['SubproxyUser'] = subproxyUser
        return result

    return S_OK(False)

  def getCredentials(self, ignoreDefault=False):
    if not self.__loadedChain:
      return S_ERROR(DErrno.ENOCHAIN)
    credDict = {'subject': self.__certList[0].get_subject().one_line(),
                'issuer': self.__certList[0].get_issuer().one_line(),
                'secondsLeft': self.getRemainingSecs()['Value'],
                'isProxy': self.__isProxy,
                'isLimitedProxy': self.__isProxy and self.__isLimitedProxy,
                'validDN': False,
                'validGroup': False}
    if self.__isProxy:
      credDict['identity'] = self.__certList[self.__firstProxyStep + 1].get_subject().one_line()

      # Check if we have the PUSP case
      result = self.isPUSP()
      if result['OK'] and result['Value']:
        credDict['identity'] = result['Identity']
        credDict['subproxyUser'] = result['SubproxyUser']

      credDict['rfc'] = self.__isRFC
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
      sha1.update(cert.get_subject().one_line())
    sha1.update(str(self.getRemainingSecs()['Value'] / 3600))
    sha1.update(self.getDIRACGroup()['Value'])
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
