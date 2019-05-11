""" ProxyProvider implementation for the proxy generation using local (DIRAC)
    CA credentials
"""

import os
import commands
import glob
import shutil
import tempfile

from DIRAC import S_OK, S_ERROR
from DIRAC.Resources.ProxyProvider.ProxyProvider import ProxyProvider
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.ConfigurationSystem.Client.Helpers import Registry

__RCSID__ = "$Id$"


class DIRACCAProxyProvider(ProxyProvider):

  def __init__(self, parameters=None):

    super(DIRACCAProxyProvider, self).__init__(parameters)

  def getProxy(self, userDict):
    """ Generate user proxy

    :param dict userDict: user description dictionary with possible fields:
                          FullName, UserName, DN, EMail, DiracGroup

    :return: S_OK/S_ERROR, Value is a proxy string
    """

#################################################################
#   Prepare configuration files

    def writeUserConfigFile(fileName, fullName, eMail, dnFields):
      """ Write down the configuration file for the user certificate

          :param str fileName: output file name
          :param str fullName: user full name
          :param str eMail: user e-mail address
          :param dict dnFields: values for the DN fields C, O, OU

          :return: None
      """

      userConf = """[ req ]
default_bits           = 2048
encrypt_key            = yes
distinguished_name     = req_dn
prompt                 = no
req_extensions        = v3_req

[ req_dn ]
C                      = %s
O                      = %s
OU                     = %s
CN                     = %s
emailAddress           = %s

[ v3_req ]
# Extensions for client certificates (`man x509v3_config`).
nsComment = "OpenSSL Generated Client Certificate"
keyUsage = critical, nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
""" % (dnFields['C'], dnFields['O'], dnFields['OU'], fullName, eMail)

      with open(fileName, "w") as userConfigFile:
        userConfigFile.write(userConf)

    def writeCAConfigFile(fileName, caDirName, certLocation, keyLocation):

      newCertsDir = os.path.join(caDirName, 'newcerts')
      if not os.path.exists(newCertsDir):
        os.makedirs(newCertsDir)

      # Empty the cert database
      ffs = glob.glob(caDirName + '/index.txt*')
      for ff in ffs:
        os.unlink(ff)
      with open(caDirName + '/index.txt', 'w') as ind:
        ind.write('')

      # Write down serial
      serialLocation = '%s/serial' % caDirName
      if not os.path.exists(serialLocation):
        with open('%s/serial' % caDirName, 'w') as serialFile:
          serialFile.write('1000')

      caConf = """[ ca ]
default_ca = CA_default

[ CA_default ]
dir               = %s
database          = $dir/index.txt
serial            = $dir/serial
new_certs_dir     = $dir/newcerts
default_md        = sha256
private_key       = %s
certificate       = %s
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
""" % (caDirName, keyLocation, certLocation)

      with open(fileName, "w") as caConfigFile:
        caConfigFile.write(caConf)

# End writing config files
######################################################################

    workingDirectory = self.parameters.get('WorkingDirectory')
    caWorkingDirectory = tempfile.mkdtemp(dir=workingDirectory)

    caConfigFile = os.path.join(caWorkingDirectory, 'CA.cnf')
    certLocation = self.parameters.get('CertFile')
    keyLocation = self.parameters.get('KeyFile')
    writeCAConfigFile(caConfigFile, caWorkingDirectory, certLocation, keyLocation)

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

    userConfFile = os.path.join(caWorkingDirectory, fullName.replace(' ', '_') + '.cnf')
    userReqFile = os.path.join(caWorkingDirectory, fullName.replace(' ', '_') + '.req')
    userKeyFile = os.path.join(caWorkingDirectory, fullName.replace(' ', '_') + '.key.pem')
    userCertFile = os.path.join(caWorkingDirectory, fullName.replace(' ', '_') + '.cert.pem')

    dnFields = {}
    for field in ['C', 'O', 'OU']:
      dnFields[field] = self.parameters.get(field)

    writeUserConfigFile(userConfFile, fullName, eMail, dnFields)

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

    result = chain.generateProxyToString(365 * 24 * 3600, diracGroup=diracGroup, rfc=True)

    # Clean up temporary files
    shutil.rmtree(caWorkingDirectory)

    return result

  def getUserDN(self, userDict):
    """ Get DN of the user certificate that will be created

    :param dict userDict:
    :return: S_OK/S_ERROR, Value is the DN string
    """

    if "DN" in userDict:
      return S_OK(userDict['DN'])

    dnParameters = dict(self.parameters)
    dnParameters.update(userDict)

    for field in ['C', 'O', 'OU', 'FullName', 'EMail']:
      if field not in dnParameters:
        return S_ERROR('Incomplete user information')

    dn = "/C=%(C)s/O=%(O)s/OU=%(OU)s/CN=%(FullName)s/emailAddress=%(EMail)s" % dnParameters
    return S_OK(dn)
