""" ProxyProvider implementation for the proxy generation using local (DIRAC)
    CA credentials
"""

import os
import re
import glob
import shutil
import tempfile
import commands

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Resources.ProxyProvider.ProxyProvider import ProxyProvider

__RCSID__ = "$Id$"


userConf = """[ req ]
default_bits           = 2048
encrypt_key            = yes
distinguished_name     = req_dn
prompt                 = no
req_extensions         = v3_req

[ req_dn ]
C                      = %%s
O                      = %%s
OU                     = %%s
CN                     = %%s
emailAddress           = %%s

[ v3_req ]
# Extensions for client certificates (`man x509v3_config`).
nsComment = "OpenSSL Generated Client Certificate"
keyUsage = critical, nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
%s""" % ''

caConf = """[ ca ]
default_ca = CA_default

[ CA_default ]
dir               = %%s
database          = $dir/index.txt
serial            = $dir/serial
new_certs_dir     = $dir/newcerts
default_md        = sha256
private_key       = %%s
certificate       = %%s
name_opt          = ca_default
cert_opt          = ca_default
default_days      = 375
preserve          = no
copy_extensions   = copy
policy            = policy_loose

[ policy_loose ]
# Allow the intermediate CA to sign a more diverse range of certificates.
# See the POLICY FORMAT section of the `ca` man page.
countryName             = optional
stateOrProvinceName     = optional
localityName            = optional
organizationName        = optional
organizationalUnitName  = optional
commonName              = supplied
emailAddress            = optional

[ usr_cert ]
basicConstraints = CA:FALSE
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer
keyUsage = critical, nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
%s""" % ''


class DIRACCAProxyProvider(ProxyProvider):

  def __init__(self, parameters=None):

    super(DIRACCAProxyProvider, self).__init__(parameters)

  def getProxy(self, userDict):
    """ Generate user proxy

        :param dict userDict: user description dictionary with possible fields:
               FullName, UserName, DN, EMail, DiracGroup

        :return: S_OK(basestring)/S_ERROR() -- basestring is a proxy string
    """

    def __createProxy():
      """ Create proxy

          :return: S_OK()/S_ERROR()
      """
      # Evaluate full name and e-mail of the user
      fullName = userDict.get('FullName')
      eMail = userDict.get('EMail')
      if "DN" in userDict:
        # Get the DN info as a dictionary
        dnDict = dict([field.split('=') for field in userDict['DN'].lstrip('/').split('/')])
        if not fullName:
          fullName = dnDict.get('CN')
        if not eMail:
          eMail = dnDict.get('emailAddress')
      if not fullName or not eMail:
        return S_ERROR("Incomplete user information")

      userConfFile = os.path.join(userDir, fullName.replace(' ', '_') + '.cnf')
      userReqFile = os.path.join(userDir, fullName.replace(' ', '_') + '.req')
      userKeyFile = os.path.join(userDir, fullName.replace(' ', '_') + '.key.pem')
      userCertFile = os.path.join(userDir, fullName.replace(' ', '_') + '.cert.pem')

      dnFields = {}
      for field in ['C', 'O', 'OU']:
        dnFields[field] = self.parameters.get(field)

      # Write user configuration file
      with open(userConfFile, "w") as f:
        f.write(userConf % (dnFields['C'], dnFields['O'], dnFields['OU'], fullName, eMail))

      # Create user certificate
      status, output = commands.getstatusoutput('openssl genrsa -out %s 2048' % userKeyFile)
      if status:
        return S_ERROR(output)
      status, output = commands.getstatusoutput('openssl req -config %s -key %s -new -out %s' %
                                                (userConfFile, userKeyFile, userReqFile))
      if status:
        return S_ERROR(output)
      cmd = 'openssl ca -config %s -extensions usr_cert -batch -days 375 -in %s -out %s'
      cmd = cmd % (caConfigFile, userReqFile, userCertFile)
      status, output = commands.getstatusoutput(cmd)
      if status:
        return S_ERROR(output)

      chain = X509Chain()
      result = chain.loadChainFromFile(userCertFile)
      if not result['OK']:
        return result
      result = chain.loadKeyFromFile(userKeyFile)
      if not result['OK']:
        return result

      result = chain.getCredentials()
      if not result['OK']:
        return result
      userDN = result['Value']['subject']

      # Add DIRAC group if requested
      diracGroup = userDict.get('DiracGroup')
      if diracGroup:
        result = Registry.getGroupsForDN(userDN)
        if not result['OK']:
          return result
        if diracGroup not in result['Value']:
          return S_ERROR('Requested group is not valid for the user')

      return chain.generateProxyToString(365 * 24 * 3600, diracGroup=diracGroup, rfc=True)

    # Prepare CA
    cfg = {}
    caConfigFile = self.parameters.get('CAConfigFile')
    if caConfigFile:
      with open(caConfigFile, "r") as caCFG:
        for line in caCFG:
          if re.findall('=', re.sub(r'#.*', '', line)):
            field, val = re.sub(r'#.*', '', line).replace(' ', '').rstrip().split('=')
            if field in ['dir', 'database', 'serial', 'new_certs_dir', 'private_key', 'certificate']:
              for i in ['dir', 'database', 'serial', 'new_certs_dir', 'private_key', 'certificate']:
                if cfg.get(i):
                  val = val.replace('$%s' % i, cfg[i])
              cfg[field] = val

    workingDirectory = self.parameters.get('WorkingDirectory')
    caWorkingDirectory = cfg.get('dir') or tempfile.mkdtemp(dir=workingDirectory)
    certLocation = cfg.get('certificate') or self.parameters.get('CertFile')
    keyLocation = cfg.get('private_key') or self.parameters.get('KeyFile')

    # Write configuration file
    if not caConfigFile:
      caConfigFile = os.path.join(caWorkingDirectory, 'CA.cnf')
      with open(caConfigFile, "w") as caCFG:
        caCFG.write(caConf % (caWorkingDirectory, keyLocation, certLocation))

    # Check directory for new certificates
    newCertsDir = cfg.get('new_certs_dir') or os.path.join(caWorkingDirectory, 'newcerts')
    if not os.path.exists(newCertsDir):
      os.makedirs(newCertsDir)

    # Empty the certificate database
    indexTxt = cfg.get('database') or caWorkingDirectory + '/index.txt'
    with open(indexTxt, 'w') as ind:
      ind.write('')

    # Write down serial
    serialLocation = cfg.get('serial') or '%s/serial' % caWorkingDirectory
    with open(serialLocation, 'w') as serialFile:
      serialFile.write('1000')

    # Create user proxy
    userDir = tempfile.mkdtemp(dir=caWorkingDirectory)
    result = __createProxy()

    # Clean up temporary files
    if cfg.get('dir'):
      shutil.rmtree(userDir)
      for f in os.listdir(newCertsDir):
        os.remove(os.path.join(newCertsDir, f))
      for f in os.listdir(caWorkingDirectory):
        if re.match("%s..*" % os.path.basename(indexTxt), f) or f.endswith('.old'):
          os.remove(os.path.join(caWorkingDirectory, f))
      with open(indexTxt, 'w') as indx:
        indx.write('')
      with open(serialLocation, 'w') as serialFile:
        serialFile.write('1000')
    else:
      shutil.rmtree(caWorkingDirectory)

    return result

  def getUserDN(self, userDict):
    """ Get DN of the user certificate that will be created

        :param dict userDict: dictionary with user information

        :return: S_OK(basestring)/S_ERROR() -- basestring is the DN string
    """

    if "DN" in userDict:
      # Get the DN info as a dictionary
      dnDict = dict([field.split('=') for field in userDict['DN'].lstrip('/').split('/')])
      # check that the DN corresponds to the template
      valid = True
      for field in ['C', 'O', 'OU']:
        if dnDict.get(field) != self.parameters.get(field):
          valid = False
      if not (dnDict.get('CN') and dnDict.get('emailAddress')):
        valid = False
      if valid:
        return S_OK(userDict['DN'])
      else:
        return S_ERROR('Invalid DN')

    dnParameters = dict(self.parameters)
    dnParameters.update(userDict)

    for field in ['C', 'O', 'OU', 'FullName', 'EMail']:
      if field not in dnParameters:
        return S_ERROR('Incomplete user information')

    dn = "/C=%(C)s/O=%(O)s/OU=%(OU)s/CN=%(FullName)s/emailAddress=%(EMail)s" % dnParameters
    return S_OK(dn)
