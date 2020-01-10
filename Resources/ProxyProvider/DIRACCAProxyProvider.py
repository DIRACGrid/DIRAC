""" ProxyProvider implementation for the proxy generation using local (DIRAC)
    CA credentials
"""

import re
import time
import random
import datetime

from M2Crypto import m2, util, X509, ASN1, EVP, RSA

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Core.Security.X509Certificate import X509Certificate  # pylint: disable=import-error
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Resources.ProxyProvider.ProxyProvider import ProxyProvider

__RCSID__ = "$Id$"


class DIRACCAProxyProvider(ProxyProvider):

  def __init__(self, parameters=None):
    """ Constructor
    """
    super(DIRACCAProxyProvider, self).__init__(parameters)
    self.log = gLogger.getSubLogger(__name__)
    # Initialize
    self.maxDict = {}
    self.minDict = {}
    self.bits = 2048
    self.algoritm = 'sha256'
    self.match = []
    self.supplied = ['CN']
    self.optional = ['C', 'O', 'OU', 'emailAddress']
    # Add not supported distributes names
    self.fs2nid = X509.X509_Name.nid.copy()
    self.fs2nid['DC'] = -1
    self.fs2nid['domainComponent'] = -1
    self.fs2nid['organizationalUnitName'] = 18
    self.fs2nid['countryName'] = 14
    self.n2field = {}  # nid: most short or specidied in CS distributes name
    self.n2fields = {}  # nid: list of distributes names
    # Specify standart fields
    for field in self.fs2nid:
      if self.fs2nid[field] not in self.n2fields:
        self.n2fields[self.fs2nid[field]] = []
      self.n2fields[self.fs2nid[field]].append(field)
    for nid in self.n2fields:
      for field in self.n2fields[nid]:
        if nid not in self.n2field:
          self.n2field[nid] = field
        self.n2field[nid] = len(field) < len(self.n2field[nid]) and field or self.n2field[nid]
    self.caDict = {}
    self.caFieldByNid = {}

  def setParameters(self, parameters):
    """ Set new parameters

        :param dict parameters: provider parameters

        :return: S_OK()/S_ERROR()
    """
    # If CA configuration file exist
    self.parameters = parameters
    if parameters.get('CAConfigFile'):
      self.supplied, self.optional = [], []
      self.__parseCACFG()
    if 'Bits' in parameters:
      self.bits = int(parameters['Bits'])
    if 'Algoritm' in parameters:
      self.algoritm = parameters['Algoritm']
    if 'Match' in parameters:
      self.match = []
      for field in parameters['Match'].replace(' ', '').split(','):
        self.match.append(self.fs2nid[field])
    if 'Supplied' in parameters:
      self.supplied = []
      for field in parameters['Supplied'].replace(' ', '').split(','):
        self.supplied.append(self.fs2nid[field])
    if 'Optional' in parameters:
      self.optional = []
      for field in parameters['Optional'].replace(' ', '').split(','):
        self.optional.append(self.fs2nid[field])

    # Set defaults for distridutes names
    self.defDict = {}
    for field, value in parameters.items():
      if field in self.fs2nid:
        self.defDict[field] = value
    self.defFieldByNid = dict([[self.fs2nid[field], field] for field in self.defDict])
    for nid in self.n2field:
      if nid in self.defFieldByNid:
        self.n2field[nid] = self.defFieldByNid[nid]
    self.match.sort()
    self.supplied.sort()

    # Read CA certificate
    chain = X509Chain()
    result = chain.loadChainFromFile(self.parameters['CertFile'])
    if not result['OK']:
      return result
    result = chain.getCredentials()
    if not result['OK']:
      return result
    caDN = result['Value']['subject']
    self.caDict = dict([field.split('=') for field in caDN.lstrip('/').split('/')])
    self.caFieldByNid = dict([[self.fs2nid[field], field] for field in self.caDict])
    return S_OK()

  def checkStatus(self, userDN):
    """ Read ready to work status of proxy provider

        :param str userDN: user DN

        :return: S_OK(dict)/S_ERROR() -- dictionary contain fields:
                  - 'Status' with ready to work status[ready, needToAuth]
    """
    dnDict = dict([field.split('=') for field in userDN.lstrip('/').split('/')])
    dnFieldByNid = dict([[self.fs2nid[field], field] for field in dnDict])
    for nid in self.supplied:
      if nid not in dnFieldByNid:
        return S_ERROR('Current DN is invalid, "%s" field must be set.' % self.n2field[nid])
    for nid in dnFieldByNid:
      if nid not in self.supplied + self.match + self.optional:
        return S_ERROR('Current DN is invalid, "%s" field is not found for current CA.' % dnFieldByNid[nid])
      if nid in self.match and not self.caDict[self.caFieldByNid[nid]] == dnDict[dnFieldByNid[nid]]:
        return S_ERROR('Current DN is invalid, "%s" field must be %s.' % (dnFieldByNid[nid],
                                                                          self.caDict[self.caFieldByNid[nid]]))
      if nid in self.maxDict and len(dnDict[dnFieldByNid[nid]]) > self.maxDict[nid]:
        return S_ERROR('Current DN is invalid, "%s" field must be less then %s.' % (dnDict[dnFieldByNid[nid]],
                                                                                    self.maxDict[nid]))
      if nid in self.minDict and len(dnDict[dnFieldByNid[nid]]) < self.minDict[nid]:
        return S_ERROR('Current DN is invalid, "%s" field must be more then %s.' % (dnDict[dnFieldByNid[nid]],
                                                                                    self.minDict[nid]))
    userDict = {}
    for k, v in dnDict.items():
      if self.defDict.get(k):
        self.defDict[k] = v
      if self.fs2nid[k] == self.fs2nid['CN']:
        userDict['FullName'] = v
      if self.fs2nid[k] == self.fs2nid['emailAddress']:
        userDict['Email'] = v

    if not userDict.get('FullName') or not dnDict.get('CN'):
      return S_ERROR("Incomplete user information: no full name found")
    if not userDict.get('Email') or not dnDict.get('emailAddress'):
      return S_ERROR("Incomplete user information: no email found")

    return S_OK({'Status': 'ready'})

  def getProxy(self, userDN):
    """ Generate user proxy

        :param str userDN: user DN

        :return: S_OK(dict)/S_ERROR() -- dict contain 'proxy' field with is a proxy string
    """
    dnDict = dict([field.split('=') for field in userDN.lstrip('/').split('/')])
    result = self.getUserDN(FullName=dnDict.get('CN'), Email=dnDict.get('emailAddress'))
    if not result['OK']:
      return result
    if userDN != result['Value']:
      return S_ERROR('Cannot generate proxy with %s DN.' % userDN)

    result = self.__createCertM2Crypto()
    if not result['OK']:
      return result
    certStr, keyStr = result['Value']

    chain = X509Chain()
    result = chain.loadChainFromString(certStr)
    if not result['OK']:
      return result
    result = chain.loadKeyFromString(keyStr)
    if not result['OK']:
      return result

    result = chain.generateProxyToString(365 * 24 * 3600, rfc=True)
    if not result['OK']:
      return result
    return S_OK({'proxy': result['Value']})

  def getUserDN(self, **kwargs):
    """ Get DN of the user certificate that will be created

        :param dict kwargs: user description dictionary with possible fields:
               - FullName, UserName to create DN
               - set userDN to check this DN

        :return: S_OK()/S_ERROR(), Value is the DN string
    """
    userDict = kwargs or {}
    if userDict.get('userDN'):
      dnDict = dict([field.split('=') for field in userDict['userDN'].lstrip('/').split('/')])
      userDict['FullName'] = dnDict.get('CN')
      userDict['Email'] = dnDict.get('emailAddress')

    # Fill DN subject name
    if not userDict.get('FullName'):
      return S_ERROR("Incomplete user information: no full name found")
    if not userDict.get('Email'):
      return S_ERROR("Incomplete user information: no email found")
    self.__X509Name = X509.X509_Name()
    self.log.info('Creating distributes names chain')
    # Test match fields
    for nid in self.match:
      if nid not in self.caFieldByNid:
        return S_ERROR('Distributes name(%s) must be present in CA certificate.' % ', '.join(self.n2fields[nid]))
      result = self.__fillX509Name(self.caFieldByNid[nid], self.caDict[self.caFieldByNid[nid]])
      if not result['OK']:
        return result
    # Test supplied fields
    for nid in self.supplied:
      if self.defDict.get(self.n2field[nid]):
        result = self.__fillX509Name(self.n2field[nid], self.defDict[self.n2field[nid]])
        if not result['OK']:
          return result
    for nid, value in [(self.fs2nid['CN'], userDict['FullName']),
                       (self.fs2nid['emailAddress'], userDict['Email'])]:
      if nid in self.supplied + self.optional:
        result = self.__fillX509Name(self.n2field[nid], value)
        if not result['OK']:
          return result

    # WARN: This logic not support list of distribtes name elements
    resDN = m2.x509_name_oneline(self.__X509Name.x509_name)  # pylint: disable=no-member

    if userDN and userDN != resDN:
      return S_ERROR('%s not matched with created %s' % (userDN, resDN))
    return S_OK(resDN)

  def __parseCACFG(self):
    """ Parse CA configuration file
    """
    block = ''
    self.cfg = {}
    with open(self.parameters['CAConfigFile'], "r") as caCFG:
      for line in caCFG:
        line = re.sub(r'#.*', '', line)
        if re.findall(r"\[([A-Za-z0-9_]+)\]", line.replace(' ', '')):
          block = ''.join(re.findall(r"\[([A-Za-z0-9_]+)\]", line.replace(' ', '')))
          if block not in self.cfg:
            self.cfg[block] = {}
        if not block:
          continue
        if len(re.findall('=', line)) == 1:
          field, val = line.split('=')
          field = field.strip()
          variables = re.findall(r'[$]([A-Za-z0-9_]+)', val)
          for v in variables:
            for b in self.cfg:
              if v in self.cfg[b]:
                val = val.replace('$' + v, self.cfg[b][v])
          self.cfg[block][field] = val.strip()

    self.bits = int(self.cfg['req'].get('default_bits') or self.bits)
    self.algoritm = self.cfg[self.cfg['ca']['default_ca']].get('default_md') or self.algoritm
    if not self.parameters.get('CertFile'):
      self.parameters['CertFile'] = self.cfg[self.cfg['ca']['default_ca']]['certificate']
      self.parameters['KeyFile'] = self.cfg[self.cfg['ca']['default_ca']]['private_key']
    for k, v in self.cfg[self.cfg[self.cfg['ca']['default_ca']]['policy']].items():
      nid = self.fs2nid[k]
      if k + '_default' in self.cfg['req']['distinguished_name']:
        self.parameters[nid] = self.cfg['req']['distinguished_name'][k + '_default']
      if k + '_min' in self.cfg['req']['distinguished_name']:
        self.minDict[nid] = self.cfg['req']['distinguished_name'][k + '_min']
      if k + '_max' in self.cfg['req']['distinguished_name']:
        self.maxDict[nid] = self.cfg['req']['distinguished_name'][k + '_max']
      if v == 'supplied':
        self.supplied.append(nid)
      elif v == 'optional':
        self.optional.append(nid)
      elif v == 'match':
        self.match.append(nid)

  def __fillX509Name(self, field, value):
    """ Fill x509_Name object by M2Crypto

        :param str field: DN field name
        :param str value: value of field

        :return: S_OK()/S_ERROR()
    """
    if value and m2.x509_name_set_by_nid(self.__X509Name.x509_name,  # pylint: disable=no-member
                                         self.fs2nid[field], value) == 0:
      if not self.__X509Name.add_entry_by_txt(field=field, type=ASN1.MBSTRING_ASC,
                                              entry=value, len=-1, loc=-1, set=0) == 1:
        return S_ERROR('Cannot set "%s" field.' % field)
    return S_OK()

  def __createCertM2Crypto(self):
    """ Create new certificate for user

        :return: S_OK(tuple)/S_ERROR() -- tuple contain certificate and pulic key as strings
    """
    # Create publik key
    userPubKey = EVP.PKey()
    userPubKey.assign_rsa(RSA.gen_key(self.bits, 65537, util.quiet_genparam_callback))
    # Create certificate
    userCert = X509.X509()
    userCert.set_pubkey(userPubKey)
    userCert.set_version(2)
    userCert.set_subject(self.__X509Name)
    userCert.set_serial_number(int(random.random() * 10 ** 10))
    # Add extentionals
    userCert.add_ext(X509.new_extension('basicConstraints', 'CA:' + str(False).upper()))
    userCert.add_ext(X509.new_extension('extendedKeyUsage', 'clientAuth', critical=1))
    # Set livetime
    validityTime = datetime.timedelta(days=400)
    notBefore = ASN1.ASN1_UTCTIME()
    notBefore.set_time(int(time.time()))
    notAfter = ASN1.ASN1_UTCTIME()
    notAfter.set_time(int(time.time()) + int(validityTime.total_seconds()))
    userCert.set_not_before(notBefore)
    userCert.set_not_after(notAfter)
    # Add subject from CA
    with open(self.parameters['CertFile']) as cf:
      caCertStr = cf.read()
    caCert = X509.load_cert_string(caCertStr)
    userCert.set_issuer(caCert.get_subject())
    # Use CA key
    with open(self.parameters['KeyFile']) as cf:
      caKeyStr = cf.read()
    pkey = EVP.PKey()
    pkey.assign_rsa(RSA.load_key_string(caKeyStr, callback=util.no_passphrase_callback))
    # Sign
    userCert.sign(pkey, self.algoritm)

    userCertStr = userCert.as_pem()
    userPubKeyStr = userPubKey.as_pem(cipher=None, callback=util.no_passphrase_callback)
    return S_OK((userCertStr, userPubKeyStr))
