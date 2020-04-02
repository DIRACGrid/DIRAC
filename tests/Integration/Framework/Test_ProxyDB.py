""" This is a test of the ProxyDB
    It supposes that the DB is present and installed in DIRAC
"""

# pylint: disable=invalid-name,wrong-import-position,protected-access
import os
import re
import sys
import stat
import shutil
import tempfile
import commands
import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.CFG import CFG
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.FrameworkSystem.DB.ProxyDB import ProxyDB
from DIRAC.Resources.ProxyProvider.DIRACCAProxyProvider import DIRACCAProxyProvider

certsPath = os.path.join(os.environ['DIRAC'], 'DIRAC/Core/Security/test/certs')
ca = DIRACCAProxyProvider()
ca.setParameters({'CertFile': os.path.join(certsPath, 'ca/ca.cert.pem'),
                  'KeyFile': os.path.join(certsPath, 'ca/ca.key.pem')})

diracTestCACFG = """
Resources
{
  ProxyProviders
  {
    DIRAC_CA
    {
      ProviderType = DIRACCA
      CertFile = %s
      KeyFile = %s
      Supplied = C, O, OU, CN
      Optional = emailAddress
      DNOrder = C, O, OU, CN, emailAddress
      OU = None
      C = DN
      O = DIRACCA
    }
  }
}
""" % (os.path.join(certsPath, 'ca/ca.cert.pem'), os.path.join(certsPath, 'ca/ca.key.pem'))

userCFG = """
Registry
{
  Users
  {
    # In dirac_user group
    user_ca
    {
      DN = /C=DN/O=DIRACCA/OU=None/CN=user_ca/emailAddress=user_ca@diracgrid.org
      DNProperties
      {
        -C_DN-O_DIRACCA-OU_None-CN_user_ca-emailAddress_user_ca@diracgrid.org
        {
          ProxyProviders = DIRAC_CA
          Groups = dirac_user
        }
      }
    }
    user
    {
      DN = /C=CC/O=DN/O=DIRAC/CN=user
      DNProperties
      {
        -C_CC-O_DN-O_DIRAC-CN_user
        {
          ProxyProviders =
          Groups = dirac_user
        }
      }
    }
    user_1
    {
      DN = /C=CC/O=DN/O=DIRAC/CN=user_1
      DNProperties
      {
        -C_CC-O_DN-O_DIRAC-CN_user_1
        {
          ProxyProviders =
          Groups = dirac_user
        }
      }
    }
    user_2
    {
      DN = /C=CC/O=DN/O=DIRAC/CN=user_2
      DNProperties
      {
        -C_CC-O_DN-O_DIRAC-CN_user_2
        {
        }
      }
    }
    user_3
    {
      DN = /C=CC/O=DN/O=DIRAC/CN=user_3
    }
    # Not in dirac_user group
    user_4
    {
      DN = /C=CC/O=DN/O=DIRAC/CN=user_4
    }
  }
  Groups
  {
    group_1
    {
      Users = user_ca, user, user_1, user_2, user_3
      VO = vo_1
    }
    group_2
    {
      Users = user_4
      enableToDownload = False
    }
  }
  VO
  {
    vo_1
    {
      VOMSName = vo_1
      VOMSServers
      {
      }
    }
  }
}
"""

db = ProxyDB()


class ProxyDBTestCase(unittest.TestCase):

  @classmethod
  def createProxy(self, userName, group, time, vo=None, role=None):
    """ Create user proxy

        :param str userName: user name
        :param str group: group name
        :param int time: proxy expired time
        :param str vo: VOMS VO name
        :param str role: VOMS Role

        :return: S_OK(tuple)/S_ERROR() -- contain proxy as and as string
    """
    userCertFile = os.path.join(self.userDir, userName + '.cert.pem')
    userKeyFile = os.path.join(self.userDir, userName + '.key.pem')
    self.proxyPath = os.path.join(self.userDir, userName + '.pem')
    if not vo:
      chain = X509Chain()
      # Load user cert and key
      retVal = chain.loadChainFromFile(userCertFile)
      if not retVal['OK']:
        gLogger.warn(retVal['Message'])
        return S_ERROR("Can't load %s" % userCertFile)
      retVal = chain.loadKeyFromFile(userKeyFile)
      if not retVal['OK']:
        gLogger.warn(retVal['Message'])
        if 'bad decrypt' in retVal['Message']:
          return S_ERROR("Bad passphrase")
        return S_ERROR("Can't load %s" % userKeyFile)
      result = chain.generateProxyToFile(self.proxyPath, time * 3600, diracGroup=group)
      if not result['OK']:
        return result
    else:
      cmd = 'voms-proxy-fake --cert %s --key %s -q' % (userCertFile, userKeyFile)
      cmd += ' -hostcert %s -hostkey %s' % (self.hostCert, self.hostKey)
      cmd += ' -uri fakeserver.cern.ch:15000'
      cmd += ' -voms "%s"' % vo
      cmd += ' -fqan "/%s/Role=%s/Capability=NULL"' % (vo, role)
      cmd += ' -hours %s -out %s -rfc' % (time, self.proxyPath)
      status, output = commands.getstatusoutput(cmd)
      if status:
        return S_ERROR(output)
    chain = X509Chain()
    result = chain.loadProxyFromFile(self.proxyPath)
    if not result['OK']:
      return result
    result = chain.generateProxyToString(12 * 3600, diracGroup=group)
    if not result['OK']:
      return result
    return S_OK((chain, result['Value']))

  @classmethod
  def setUpClass(cls):
    cls.failed = False

    # Add configuration
    cfg = CFG()
    cfg.loadFromBuffer(diracTestCACFG)
    gConfig.loadCFG(cfg)
    cfg.loadFromBuffer(userCFG)
    gConfig.loadCFG(cfg)

    # Prepare CA
    lines = []
    cfgDict = {}
    cls.caPath = os.path.join(certsPath, 'ca')
    cls.caConfigFile = os.path.join(cls.caPath, 'openssl_config_ca.cnf')
    # Save original configuration file
    shutil.copyfile(cls.caConfigFile, cls.caConfigFile + 'bak')
    # Parse
    fields = ['dir', 'database', 'serial', 'new_certs_dir', 'private_key', 'certificate']
    with open(cls.caConfigFile, "r") as caCFG:
      for line in caCFG:
        if re.findall('=', re.sub(r'#.*', '', line)):
          field = re.sub(r'#.*', '', line).replace(' ', '').rstrip().split('=')[0]
          line = 'dir = %s #PUT THE RIGHT DIR HERE!\n' % (cls.caPath) if field == 'dir' else line
          val = re.sub(r'#.*', '', line).replace(' ', '').rstrip().split('=')[1]
          if field in fields:
            for i in fields:
              if cfgDict.get(i):
                val = val.replace('$%s' % i, cfgDict[i])
            cfgDict[field] = val
            if not cfgDict[field]:
              cls.failed = '%s have empty value in %s' % (field, cls.caConfigFile)
        lines.append(line)
    with open(cls.caConfigFile, "w") as caCFG:
      caCFG.writelines(lines)
    for field in fields:
      if field not in cfgDict.keys():
        cls.failed = '%s value is absent in %s' % (field, cls.caConfigFile)
    cls.hostCert = os.path.join(certsPath, 'host/hostcert.pem')
    cls.hostKey = os.path.join(certsPath, 'host/hostkey.pem')
    cls.caCert = cfgDict['certificate']
    cls.caKey = cfgDict['private_key']
    os.chmod(cls.caKey, stat.S_IREAD)
    # Check directory for new certificates
    cls.newCertDir = cfgDict['new_certs_dir']
    if not os.path.exists(cls.newCertDir):
      os.makedirs(cls.newCertDir)
    for f in os.listdir(cls.newCertDir):
      os.remove(os.path.join(cls.newCertDir, f))
    # Empty the certificate database
    cls.index = cfgDict['database']
    with open(cls.index, 'w') as indx:
      indx.write('')
    # Write down serial
    cls.serial = cfgDict['serial']
    with open(cls.serial, 'w') as serialFile:
      serialFile.write('1000')

    # Create temporaly directory for users certificates
    cls.userDir = tempfile.mkdtemp(dir=certsPath)

    # Create user certificates
    for userName in ['no_user', 'user', 'user_1', 'user_2', 'user_3']:
      userConf = """[ req ]
        default_bits           = 4096
        encrypt_key            = yes
        distinguished_name     = req_dn
        prompt                 = no
        req_extensions         = v3_req
        [ req_dn ]
        C                      = CC
        O                      = DN
        0.O                    = DIRAC
        CN                     = %s
        [ v3_req ]
        # Extensions for client certificates (`man x509v3_config`).
        nsComment = "OpenSSL Generated Client Certificate"
        keyUsage = critical, nonRepudiation, digitalSignature, keyEncipherment
        extendedKeyUsage = clientAuth
        """ % (userName)
      userConfFile = os.path.join(cls.userDir, userName + '.cnf')
      userReqFile = os.path.join(cls.userDir, userName + '.req')
      userKeyFile = os.path.join(cls.userDir, userName + '.key.pem')
      userCertFile = os.path.join(cls.userDir, userName + '.cert.pem')
      with open(userConfFile, "w") as f:
        f.write(userConf)
      status, output = commands.getstatusoutput('openssl genrsa -out %s' % userKeyFile)
      if status:
        gLogger.error(output)
        exit()
      gLogger.debug(output)
      os.chmod(userKeyFile, stat.S_IREAD)
      status, output = commands.getstatusoutput('openssl req -config %s -key %s -new -out %s' %
                                                (userConfFile, userKeyFile, userReqFile))
      if status:
        gLogger.error(output)
        exit()
      gLogger.debug(output)
      cmd = 'openssl ca -config %s -extensions usr_cert -batch -days 375 -in %s -out %s'
      cmd = cmd % (cls.caConfigFile, userReqFile, userCertFile)
      status, output = commands.getstatusoutput(cmd)
      if status:
        gLogger.error(output)
        exit()
      gLogger.debug(output)

    # Result
    status, output = commands.getstatusoutput('ls -al %s' % cls.userDir)
    if status:
      gLogger.error(output)
      exit()
    gLogger.debug('User certificates:\n', output)

  def setUp(self):
    gLogger.debug('\n')
    if self.failed:
       self.fail(self.failed)
    db._update('DELETE FROM ProxyDB_Proxies WHERE UserName IN ("user_ca", "user", "user_1", "user_2", "user_3")')
    db._update('DELETE FROM ProxyDB_CleanProxies WHERE UserName IN ("user_ca", "user", "user_1", "user_2", "user_3")')

  def tearDown(self):
    db._update('DELETE FROM ProxyDB_Proxies WHERE UserName IN ("user_ca", "user", "user_1", "user_2", "user_3")')
    db._update('DELETE FROM ProxyDB_CleanProxies WHERE UserName IN ("user_ca", "user", "user_1", "user_2", "user_3")')

  @classmethod
  def tearDownClass(cls):
    shutil.move(cls.caConfigFile + 'bak', cls.caConfigFile)
    if os.path.exists(cls.newCertDir):
      for f in os.listdir(cls.newCertDir):
        os.remove(os.path.join(cls.newCertDir, f))
    for f in os.listdir(cls.caPath):
      if re.match("%s..*" % cls.index, f) or f.endswith('.old'):
        os.remove(os.path.join(cls.caPath, f))
    if os.path.exists(cls.userDir):
      shutil.rmtree(cls.userDir)
    # Empty the certificate database
    with open(cls.index, 'w') as index:
      index.write('')
    # Write down serial
    with open(cls.serial, 'w') as serialFile:
      serialFile.write('1000')


class testDB(ProxyDBTestCase):

  def test_connectDB(self):
    """ Try to connect to the ProxyDB
    """
    res = db._connect()
    self.assertTrue(res['OK'])

  def test_getUsers(self):
    """ Test 'getUsers' - try to get users from DB
    """
    field = '("%%s", "/C=CC/O=DN/O=DIRAC/CN=%%s", %%s "PEM", TIMESTAMPADD(SECOND, %%s, UTC_TIMESTAMP()))%s' % ''
    # Fill table for test
    gLogger.info('\n* Fill tables for test..')
    for table, values, fields in [('ProxyDB_Proxies',
                                  [field % ('user', 'user', '"group_1",', '800'),
                                   field % ('user_2', 'user_2', '"group_1",', '-1')],
                                  '(UserName, UserDN, UserGroup, Pem, ExpirationTime)'),
                                  ('ProxyDB_CleanProxies',
                                  [field % ('user_3', 'user_3', '', '43200')],
                                  '(UserName, UserDN, Pem, ExpirationTime)')]:
      result = db._update('INSERT INTO %s%s VALUES %s ;' % (table, fields, ', '.join(values)))
      self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
    # Testing 'getUsers'
    gLogger.info('\n* Run `purgeExpiredProxies()`..')
    for user, exp, expect, log in [(False, 0, ['user', 'user_2', 'user_3'], '\n* Without arguments'),
                                   (False, 1200, ['user_3'], '* Request proxy live time'),
                                   ('user_2', 0, ['user_2'], '* Request user name'),
                                   ('no_user', 0, [], '* Request not exist user name')]:
      gLogger.info('%s..' % log)
      result = db.getUsers(validSecondsLeft=exp, userMask=user)
      self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
      usersList = []
      for line in result['Value']:
        if line['Name'] in ['user', 'user_2', 'user_3']:
          usersList.append(line['Name'])
      self.assertEqual(set(expect), set(usersList), str(usersList) + ', when expected ' + str(expect))

  def test_purgeExpiredProxies(self):
    """ Test 'purgeExpiredProxies' - try to purge expired proxies
    """
    # Purge existed proxies
    gLogger.info('\n* First cleaning..')
    cmd = 'INSERT INTO ProxyDB_Proxies(UserName, UserDN, UserGroup, Pem, ExpirationTime) VALUES '
    cmd += '("user", "/C=CC/O=DN/O=DIRAC/CN=user", "group_1", "PEM", '
    cmd += 'TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP()));'
    result = db._query(cmd)
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
    cmd = 'SELECT COUNT( * ) FROM ProxyDB_Proxies WHERE ExpirationTime < UTC_TIMESTAMP()'
    self.assertTrue(bool(db._query(cmd)['Value'][0][0] > 0))
    result = db.purgeExpiredProxies()
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
    self.assertTrue(result['Value'] > 0, 'Must be more then null')
    self.assertFalse(bool(db._query(cmd)['Value'][0][0] > 0), "Must be null")

  def test_getRemoveProxy(self):
    """ Testing get, store proxy
    """
    gLogger.info('\n* Check that DB is clean..')
    result = db.getProxiesContent({'UserName': ['user_ca', 'user', 'user_1' 'user_2', 'user_3']}, {})
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
    self.assertTrue(bool(int(result['Value']['TotalRecords']) == 0), 'In DB present proxies.')

    gLogger.info('* Check posible crashes when get proxy..')
    # Make record with not valid proxy, valid group, user and short expired time
    cmd = 'INSERT INTO ProxyDB_Proxies(UserName, UserDN, UserGroup, Pem, ExpirationTime) VALUES '
    cmd += '("user", "/C=CC/O=DN/O=DIRAC/CN=user", "group_1", "PEM", '
    cmd += 'TIMESTAMPADD(SECOND, 1800, UTC_TIMESTAMP()));'
    result = db._update(cmd)
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
    # Try to no correct getProxy requests
    for dn, group, reqtime, log in [('/C=CC/O=DN/O=DIRAC/CN=user', 'group_1', 9999,
                                     'No proxy provider, set request time, not valid proxy in ProxyDB_Proxies'),
                                    ('/C=CC/O=DN/O=DIRAC/CN=user', 'group_1', 0,
                                     'Not valid proxy in ProxyDB_Proxies'),
                                    ('/C=CC/O=DN/O=DIRAC/CN=no_user', 'no_valid_group', 0,
                                     'User not exist, proxy not in DB tables'),
                                    ('/C=CC/O=DN/O=DIRAC/CN=user', 'no_valid_group', 0,
                                     'Group not valid, proxy not in DB tables'),
                                    ('/C=CC/O=DN/O=DIRAC/CN=user', 'group_1', 0,
                                     'No proxy provider for user, proxy not in DB tables'),
                                    ('/C=CC/O=DN/O=DIRAC/CN=user_4', 'group_2', 0,
                                     'Group has option enableToDownload = False in CS')]:
      gLogger.info('== > %s:' % log)
      result = db.getProxy(dn, group, reqtime)
      self.assertFalse(result['OK'], 'Must be fail.')
      gLogger.info('Msg: %s' % result['Message'])
    # In the last case method found proxy and must to delete it as not valid
    cmd = 'SELECT COUNT( * ) FROM ProxyDB_Proxies WHERE UserName="user"'
    self.assertTrue(bool(db._query(cmd)['Value'][0][0] == 0), "GetProxy method didn't delete the last proxy.")

    gLogger.info('* Check that DB is clean..')
    result = db.getProxiesContent({'UserName': ['user_ca', 'user', 'user_1', 'user_2', 'user_3']}, {})
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
    self.assertTrue(bool(int(result['Value']['TotalRecords']) == 0), 'In DB present proxies.')

    gLogger.info('* Generate proxy on the fly..')
    result = db.getProxy('/C=DN/O=DIRACCA/OU=None/CN=user_ca/emailAddress=user_ca@diracgrid.org',
                         'group_1', 1800)
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))

    gLogger.info('* Check that ProxyDB_CleanProxy contain generated proxy..')
    result = db.getProxiesContent({'UserName': 'user_ca'}, {})
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
    self.assertTrue(bool(int(result['Value']['TotalRecords']) == 1), 'Generated proxy must be one.')
    for table, count in [('ProxyDB_Proxies', 0), ('ProxyDB_CleanProxies', 1)]:
      cmd = 'SELECT COUNT( * ) FROM %s WHERE UserName="user_ca"' % table
      self.assertTrue(bool(db._query(cmd)['Value'][0][0] == count),
                      table + ' must ' + (count and 'contain proxy' or 'be empty'))

    gLogger.info('* Check that DB is clean..')
    result = db.deleteProxy('/C=DN/O=DIRACCA/OU=None/CN=user_ca/emailAddress=user_ca@diracgrid.org',
                            proxyProvider='DIRAC_CA')
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
    result = db.getProxiesContent({'UserName': ['user_ca', 'user', 'user_1', 'user_2', 'user_3']}, {})
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
    self.assertTrue(bool(int(result['Value']['TotalRecords']) == 0), 'In DB present proxies.')

    gLogger.info('* Upload proxy..')
    for user, dn, group, vo, time, res, log in [("user", '/C=CC/O=DN/O=DIRAC/CN=user', "group_1", False, 12,
                                                 True, 'With group extension'),
                                                ("user", '/C=CC/O=DN/O=DIRAC/CN=user', False, "vo_1", 12,
                                                 False, 'With voms extension'),
                                                ("user_1", '/C=CC/O=DN/O=DIRAC/CN=user_1', False, "vo_1", 12,
                                                 False, 'With voms extension'),
                                                ("user", '/C=CC/O=DN/O=DIRAC/CN=user', False, False, 0,
                                                 False, 'Expired proxy'),
                                                ("no_user", '/C=CC/O=DN/O=DIRAC/CN=no_user', False, False, 12,
                                                 False, 'Not exist user'),
                                                ("user", '/C=CC/O=DN/O=DIRAC/CN=user', False, False, 12,
                                                 True, 'Valid proxy')]:
      for table in ['ProxyDB_Proxies', 'ProxyDB_CleanProxies']:
        result = db._update('DELETE FROM %s WHERE UserName = "user"' % table)
        self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
        result = db._update('DELETE FROM %s WHERE UserName = "user_1"' % table)
        self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
      self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
      gLogger.info('== > %s:' % log)

      result = self.createProxy(user, group, time, vo=vo)
      self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
      chain = result['Value'][0]

      # Assert VOMSProxy
      if vo:
        self.assertTrue(bool(chain.isVOMS().get('Value')), 'Cannot create proxy with VOMS extension')

      result = db.generateDelegationRequest(chain, dn)
      self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
      resDict = result['Value']
      result = chain.generateChainFromRequestString(resDict['request'], time * 3500)
      self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
      if not chain.isVOMS().get('Value') and vo:
        gLogger.info('voms-proxy-fake command not working as expected, so proxy have no VOMS extention')
        res = not res
      result = db.completeDelegation(resDict['id'], dn, result['Value'])
      text = 'Must be ended %s%s' % (res and 'successful' or 'with error',
                                     ': %s' % result.get('Message', 'Error message is absent.'))
      self.assertEqual(result['OK'], res, text)
      if not res:
        gLogger.info('Msg: %s' % (result['Message']))
      cmd = 'SELECT COUNT( * ) FROM ProxyDB_Proxies WHERE UserName="%s"' % user
      self.assertTrue(bool(db._query(cmd)['Value'][0][0] == (res and group and 1) or 0),
                      'ProxyDB_Proxies must ' + (res and 'contain proxy' or 'be empty'))
      cmd = 'SELECT COUNT( * ) FROM ProxyDB_CleanProxies WHERE UserName="%s"' % user
      self.assertTrue(bool(db._query(cmd)['Value'][0][0] == (res and not group and 1) or 0),
                      'ProxyDB_CleanProxies must ' + (res and 'contain proxy' or 'be empty'))

    gLogger.info('* Check that ProxyDB_CleanProxy contain generated proxy..')
    result = db.getProxiesContent({'UserName': 'user'}, {})
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
    self.assertTrue(bool(int(result['Value']['TotalRecords']) == 1), 'Generated proxy must be one.')
    cmd = 'SELECT COUNT( * ) FROM ProxyDB_CleanProxies WHERE UserName="user"'
    self.assertTrue(bool(db._query(cmd)['Value'][0][0] == 1), 'ProxyDB_CleanProxies must contain proxy')

    gLogger.info('* Get proxy that store only in ProxyDB_CleanProxies..')
    # Try to get proxy that was stored to ProxyDB_CleanProxies in previous step
    for res, group, reqtime, log in [(False, 'group_1', 24 * 3600, 'Request time more that in stored proxy'),
                                     (False, 'group_2', 0, 'Request group not contain user'),
                                     (True, 'group_1', 0, 'Request time less that in stored proxy')]:
      gLogger.info('== > %s:' % log)
      result = db.getProxy('/C=CC/O=DN/O=DIRAC/CN=user', group, reqtime)
      text = 'Must be ended %s%s' % (res and 'successful' or 'with error',
                                     ': %s' % result.get('Message', 'Error message is absent.'))
      self.assertEqual(result['OK'], res, text)
      if res:
        chain = result['Value'][0]
        self.assertTrue(chain.isValidProxy()['OK'], '\n' + result.get('Message', 'Error message is absent.'))
        result = chain.getDIRACGroup()
        self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
        self.assertEqual('group_1', result['Value'], 'Group must be group_1, not ' + result['Value'])
      else:
        gLogger.info('Msg: %s' % (result['Message']))

    gLogger.info('* Check that DB is clean..')
    result = db.deleteProxy('/C=CC/O=DN/O=DIRAC/CN=user', proxyProvider='Certificate')
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
    result = db.getProxiesContent({'UserName': ['user_ca', 'user', 'user_2', 'user_3']}, {})
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
    self.assertTrue(bool(int(result['Value']['TotalRecords']) == 0), 'In DB present proxies.')

    gLogger.info('* Get proxy when it store only in ProxyDB_Proxies..')
    # Make record with proxy that contain group
    result = ca._forceGenerateProxyForDN('/C=CC/O=DN/O=DIRAC/CN=user', 12 * 3600, group='group_1')
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
    proxyStr = result['Value'][1]
    cmd = 'INSERT INTO ProxyDB_Proxies(UserName, UserDN, UserGroup, Pem, ExpirationTime) VALUES '
    cmd += '("user", "%s", "%s", "%s", TIMESTAMPADD(SECOND, 43200, UTC_TIMESTAMP()))' % (dn, group,
                                                                                           proxyStr)
    result = db._update(cmd)
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
    # Try to get it
    result = db.getProxy(dn, group, 1800)
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
    # Check that proxy contain group
    chain = result['Value'][0]
    self.assertTrue(chain.isValidProxy()['OK'], '\n' + result.get('Message', 'Error message is absent.'))
    result = chain.getDIRACGroup()
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
    self.assertEqual('group_1', result['Value'], 'Group must be group_1, not ' + result['Value'])

    gLogger.info('* Check that DB is clean..')
    result = db.deleteProxy('/C=CC/O=DN/O=DIRAC/CN=user')
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
    result = db.getProxiesContent({'UserName': ['user_ca', 'user', 'user_1', 'user_2', 'user_3']}, {})
    self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
    self.assertTrue(bool(int(result['Value']['TotalRecords']) == 0), 'In DB present proxies.')

    gLogger.info('* Get VOMS proxy..')
    for vomsuser in ['user', 'user_1']:
      # Create proxy with VOMS extension
      result = self.createProxy(vomsuser, 'group_1', 12, vo='vo_1', role='role_2')
      self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
      chain, proxyStr = result['Value']

      # Assert VOMSProxy
      self.assertTrue(bool(chain.isVOMS().get('Value')), 'Cannot create proxy with VOMS extension')

      cmd = 'INSERT INTO ProxyDB_Proxies(UserName, UserDN, UserGroup, Pem, ExpirationTime) VALUES '
      cmd += '("%s", "/C=CC/O=DN/O=DIRAC/CN=%s", "group_1", "%s", ' % (vomsuser, vomsuser, proxyStr)
      cmd += 'TIMESTAMPADD(SECOND, 43200, UTC_TIMESTAMP()))'
      result = db._update(cmd)
      self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))

    # Try to get proxy with VOMS extension
    for dn, group, role, time, log in [('/C=CC/O=DN/O=DIRAC/CN=user_4', 'group_2', False, 9999,
                                        'Not exist VO for current group'),
                                       ('/C=CC/O=DN/O=DIRAC/CN=user', 'group_1', 'role_1', 9999,
                                        'Stored proxy already have different VOMS extension'),
                                       ('/C=CC/O=DN/O=DIRAC/CN=user_1', 'group_1', 'role_1', 9999,
                                        'Stored proxy already have different VOMS extension'),
                                       ('/C=DN/O=DIRACCA/OU=None/CN=user_ca/emailAddress=user_ca@diracgrid.org',
                                        'group_1', 'role_1', 9999, 'Not correct VO configuration')]:
      gLogger.info('== > %s(DN: %s):' % (log, dn))
      if not any([dn, group, role, time, log]):
        gLogger.info('voms-proxy-fake command not working as expected, proxy have no VOMS extention, go to the next..')
        continue
      result = db.getVOMSProxy(dn, group, time, role)
      self.assertFalse(result['OK'], 'Must be fail.')
      gLogger.info('Msg: %s' % result['Message'])
    # Check stored proxies
    for table, user, count in [('ProxyDB_Proxies', 'user', 1), ('ProxyDB_CleanProxies', 'user_ca', 1)]:
      cmd = 'SELECT COUNT( * ) FROM %s WHERE UserName="%s"' % (table, user)
      self.assertTrue(bool(db._query(cmd)['Value'][0][0] == count))

    gLogger.info('* Delete proxies..')
    for dn, table in [('/C=CC/O=DN/O=DIRAC/CN=user', 'ProxyDB_Proxies'),
                      ('/C=DN/O=DIRACCA/OU=None/CN=user_ca/emailAddress=user_ca@diracgrid.org',
                       'ProxyDB_CleanProxies')]:
      result = db.deleteProxy(dn)
      self.assertTrue(result['OK'], '\n' + result.get('Message', 'Error message is absent.'))
      cmd = 'SELECT COUNT( * ) FROM %s WHERE UserName="user_ca"' % table
      self.assertTrue(bool(db._query(cmd)['Value'][0][0] == 0))


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ProxyDBTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(testDB))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
