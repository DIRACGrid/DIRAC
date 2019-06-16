""" This is a test of the ProxyDB
    It supposes that the DB is present and installed in DIRAC
"""

# pylint: disable=invalid-name,wrong-import-position,protected-access
import os
import sys
import stat
import shutil
import commands
import unittest

from tempfile import mkstemp

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.CFG import CFG
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.FrameworkSystem.DB.ProxyDB import ProxyDB

diracTestCACFG = """
Resources
{
  ProxyProviders
  {
    DIRAC_CA
    {
      ProxyProviderType = DIRACCA
      CertFile = %%s
      KeyFile = %%s
      C = DN
      O = DIRACCA
    }
  }
}
%s""" % ''
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
    user_1
    {
      DN = /C=DN/O=DIRAC/CN=user_1
      DNProperties
      {
        -C_DN-O_DIRAC-OU_user_1
        {
          ProxyProviders =
          Groups = dirac_user
        }
      }
    }
    user_2
    {
      DN = /C=DN/O=DIRAC/CN=user_2
      DNProperties
      {
        -C_DN-O_DIRAC-OU_user_2
        {
        }
      }
    }
    user_3
    {
      DN = /C=DN/O=DIRAC/CN=user_3
    }
    # Not in dirac_user group
    user_4
    {
      DN = /C=DN/O=DIRAC/CN=user_4
    }
  }
  Groups
  {
    group_1
    {
      Users = user_ca, user_1, user_2, user_3
      VO = vo_1
    }
    group_2
    {
      Users = user_4
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


class ProxyDBTestCase(unittest.TestCase):

  @classmethod
  def createProxy(self, userName, group, time, rfc=True, limit=False, vo=False, role=None, path=None):
    """ Create proxy
    """
    userCertFile = os.path.join(self.tmpDir, userName + '.cert.pem')
    userKeyFile = os.path.join(self.tmpDir, userName + '.key.pem')
    self.proxyPath = path or os.path.join(self.tmpDir, userName + '.pem')
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
      result = chain.generateProxyToFile(self.proxyPath, time * 3600,
                                         limited=limit, diracGroup=group,
                                         rfc=rfc)
      if not result['OK']:
        return result
    else:
      cmd = 'voms-proxy-fake --cert %s --key %s -q' % (userCertFile, userKeyFile)
      cmd += ' -hostcert %s -hostkey %s' % (self.hostCert, self.hostKey)
      cmd += ' -uri fakeserver.cern.ch:15000'
      cmd += ' -voms "%s%s"' % (vo, role and ':%s' % role or '')
      cmd += ' -fqan "/%s/Role=%s/Capability=NULL"' % (vo, role)
      cmd += ' -hours %s -out %s' % (time, self.proxyPath)
      if limit:
        cmd += ' -limited'
      if rfc:
        cmd += ' -rfc'
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
    cls.db = ProxyDB()

    # Create tmp dir
    cls.tmpDir = os.path.join(os.environ['HOME'], 'tmpDirForTesting')
    if os.path.exists(cls.tmpDir):
      shutil.rmtree(cls.tmpDir)
    os.makedirs(cls.tmpDir)

    # Prepare CA
    certsPath = os.path.join(os.environ['DIRAC'], 'DIRAC/Core/Security/test/certs')
    cls.caWorkingDirectory = os.path.join(cls.tmpDir, 'ca')
    if os.path.exists(cls.caWorkingDirectory):
      shutil.rmtree(cls.caWorkingDirectory)
    # Copy CA files to temporaly directory
    shutil.copytree(os.path.join(certsPath, 'ca'), cls.caWorkingDirectory)
    cls.hostCert = os.path.join(certsPath, 'host/hostcert.pem')
    cls.hostKey = os.path.join(certsPath, 'host/hostkey.pem')
    cls.caCert = os.path.join(cls.caWorkingDirectory, 'ca.cert.pem')
    cls.caKey = os.path.join(cls.caWorkingDirectory, 'ca.key.pem')
    os.chmod(cls.caKey, stat.S_IREAD)
    cls.caConfigFile = os.path.join(cls.caWorkingDirectory, 'openssl_config_ca.cnf')
    # Fix CA configuration file
    fh, absPath = mkstemp()
    newFile = open(absPath, 'w')
    oldFile = open(cls.caConfigFile)
    for line in oldFile:
      newFile.write('dir = %s' % (cls.caWorkingDirectory) if '#PUT THE RIGHT DIR HERE!' in line else line)
    newFile.close()
    os.close(fh)
    oldFile.close()
    os.remove(cls.caConfigFile)
    shutil.move(absPath, cls.caConfigFile)
    # Erase index
    open(cls.caWorkingDirectory + '/index.txt', 'w').close()

    # Add configuration
    cfg = CFG()
    cfg.loadFromBuffer(diracTestCACFG % (cls.caCert, cls.caKey))
    gConfig.loadCFG(cfg)
    cfg.loadFromBuffer(userCFG)
    gConfig.loadCFG(cfg)

    # Create user certificates
    for userName in ['no_user', 'user_1', 'user_2', 'user_3']:
      userConf = """[ req ]
        default_bits           = 2048
        encrypt_key            = yes
        distinguished_name     = req_dn
        prompt                 = no
        req_extensions         = v3_req
        [ req_dn ]
        C                      = DN
        O                      = DIRAC
        CN                     = %s
        [ v3_req ]
        # Extensions for client certificates (`man x509v3_config`).
        nsComment = "OpenSSL Generated Client Certificate"
        keyUsage = critical, nonRepudiation, digitalSignature, keyEncipherment
        extendedKeyUsage = clientAuth
        """ % (userName)
      userConfFile = os.path.join(cls.tmpDir, userName + '.cnf')
      userReqFile = os.path.join(cls.tmpDir, userName + '.req')
      userKeyFile = os.path.join(cls.tmpDir, userName + '.key.pem')
      userCertFile = os.path.join(cls.tmpDir, userName + '.cert.pem')
      with open(userConfFile, "w") as f:
        f.write(userConf)
      status, output = commands.getstatusoutput('openssl genrsa -out %s 2048' % userKeyFile)
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

  def setUp(self):
    self.db._update('DELETE FROM ProxyDB_Proxies WHERE UserName IN ("user_ca", "user_1", "user_2", "user_3")')
    self.db._update('DELETE FROM ProxyDB_CleanProxies WHERE UserName IN ("user_ca", "user_1", "user_2", "user_3")')

  def tearDown(self):
    self.db._update('DELETE FROM ProxyDB_Proxies WHERE UserName IN ("user_ca", "user_1", "user_2", "user_3")')
    self.db._update('DELETE FROM ProxyDB_CleanProxies WHERE UserName IN ("user_ca", "user_1", "user_2", "user_3")')

  @classmethod
  def tearDownClass(cls):
    if os.path.exists(cls.tmpDir):
      shutil.rmtree(cls.tmpDir)


class testDB(ProxyDBTestCase):

  def test_connectDB(self):
    """ Try to connect to the ProxyDB
    """
    res = self.db._connect()
    self.assertTrue(res['OK'])

  def test_getUsers(self):
    """ Try to get users from DB
    """
    # Fill table for test
    for table, values, fields in [('ProxyDB_Proxies',
                                  ['("user_1", "/C=DN/O=DIRAC/CN=user_1", "group_1",' +
                                   ' "PEM", TIMESTAMPADD(SECOND, 800, UTC_TIMESTAMP()))',
                                   '("user_2", "/C=DN/O=DIRAC/CN=user_2", "group_1",' +
                                   ' "PEM", TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP()))'],
                                  '(UserName, UserDN, UserGroup, Pem, ExpirationTime)'),
                                  ('ProxyDB_CleanProxies',
                                  ['("user_3", "/C=DN/O=DIRAC/CN=user_3", "PEM",' +
                                   ' TIMESTAMPADD(SECOND, 43200, UTC_TIMESTAMP()))'],
                                  '(UserName, UserDN, Pem, ExpirationTime)')]:
      fields = '("%s", "%s", %s "PEM", TIMESTAMPADD(SECOND, 800, UTC_TIMESTAMP()))'
      result = self.db._update('INSERT INTO %s%s VALUES %s ;' % (table, fields, ', '.join(values)))
      self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    gLogger.info('\n Causes:')
    for user, exp, expect, log in [(False, 0, ['user_1', 'user_2', 'user_3'], 'Without arguments'),
                                   (False, 1200, ['user_3'], 'Request proxy live time'),
                                   ('user_2', 0, ['user_2'], 'Request user name'),
                                   ('no_user', 0, [], 'Request not exist user name')]:
      gLogger.info(' %s..' % log)
      result = self.db.getUsers(validSecondsLeft=exp, userName=user)
      self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
      res = []
      for line in result['Value']:
        if line['Name'] in ['user_1', 'user_2', 'user_3']:
          res.append(line['Name'])
      self.assertEqual(set(expect), set(res), '%s, when expected %s' % (res, expect))

  def test_purgeExpiredProxies(self):
    """ Try to purge expired proxies
    """
    cmd = 'INSERT INTO ProxyDB_Proxies(UserName, UserDN, UserGroup, Pem, ExpirationTime) VALUES '
    cmd += '("user_1", "/C=DN/O=DIRAC/CN=user_1", "group_1", "PEM", '
    cmd += 'TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP()));'
    result = self.db._query(cmd)
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    cmd = 'SELECT COUNT( * ) FROM ProxyDB_Proxies WHERE ExpirationTime < UTC_TIMESTAMP()'
    self.assertTrue(bool(self.db._query(cmd)['Value'][0][0] > 0))
    result = self.db.purgeExpiredProxies()
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    self.assertTrue(result['Value'] > 0, 'Must be more then null')
    self.assertFalse(bool(self.db._query(cmd)['Value'][0][0] > 0), "Must be null")

  def test_getRemoveProxy(self):
    """ Testing get, store proxy
    """
    gLogger.info('\n Check that DB is clean..')
    result = self.db.getProxiesContent({'UserName': ['user_ca', 'user_1', 'user_2', 'user_3']}, {})
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    self.assertTrue(bool(int(result['Value']['TotalRecords']) == 0), 'In DB present proxies.')

    gLogger.info(' Check posible crashes when get proxy..')
    # Make record with not valid proxy, valid group, user and short expired time
    cmd = 'INSERT INTO ProxyDB_Proxies(UserName, UserDN, UserGroup, Pem, ExpirationTime) VALUES '
    cmd += '("user_1", "/C=DN/O=DIRAC/CN=user_1", "group_1", "PEM", '
    cmd += 'TIMESTAMPADD(SECOND, 1800, UTC_TIMESTAMP()));'
    result = self.db._update(cmd)
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    # Try to no correct getProxy requests
    for dn, group, reqtime, log in [('/C=DN/O=DIRAC/CN=user_1', 'group_1', 9999,
                                     'No proxy provider, set request time, not valid proxy in ProxyDB_Proxies'),
                                    ('/C=DN/O=DIRAC/CN=user_1', 'group_1', 0,
                                     'Not valid proxy in ProxyDB_Proxies'),
                                    ('/C=DN/O=DIRAC/CN=no_user', 'group', 0,
                                     'User no exist, proxy not in DB tables'),
                                    ('/C=DN/O=DIRAC/CN=user_1', 'group', 0,
                                     'Group not valid, proxy not in DB tables'),
                                    ('/C=DN/O=DIRAC/CN=user_1', 'group_1', 0,
                                     'No proxy provider for user, proxy not in DB tables')]:
      result = self.db.getProxy(dn, group, reqtime)
      self.assertFalse(result['OK'], 'Must be fail.')
      gLogger.info('%s:\nError: %s' % (log, result['Message']))
    # In the last case method found proxy and must to delete it as not valid
    cmd = 'SELECT COUNT( * ) FROM ProxyDB_Proxies WHERE UserName="user_1"'
    self.assertTrue(bool(self.db._query(cmd)['Value'][0][0] == 0), 'GetProxy method was not delete proxy.')

    gLogger.info(' Check that DB is clean..')
    result = self.db.getProxiesContent({'UserName': ['user_ca', 'user_1', 'user_2', 'user_3']}, {})
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    self.assertTrue(bool(int(result['Value']['TotalRecords']) == 0), 'In DB present proxies.')

    gLogger.info(' Generate proxy on the fly..')
    result = self.db.getProxy('/C=DN/O=DIRACCA/OU=None/CN=user_ca/emailAddress=user_ca@diracgrid.org',
                              'group_1', 1800)
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')

    gLogger.info(' Check that ProxyDB_CleanProxy contain generated proxy..')
    result = self.db.getProxiesContent({'UserName': 'user_ca'}, {})
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    self.assertTrue(bool(int(result['Value']['TotalRecords']) == 1), 'Generated proxy must be one.')
    for table, count in [('ProxyDB_Proxies', 0), ('ProxyDB_CleanProxies', 1)]:
      cmd = 'SELECT COUNT( * ) FROM %s WHERE UserName="user_ca"' % table
      self.assertTrue(bool(self.db._query(cmd)['Value'][0][0] == count))

    gLogger.info('\n Check that DB is clean..')
    result = self.db.deleteProxy('/C=DN/O=DIRACCA/OU=None/CN=user_ca/emailAddress=user_ca@diracgrid.org',
                                 proxyProvider='DIRAC_CA')
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    result = self.db.getProxiesContent({'UserName': ['user_ca', 'user_1', 'user_2', 'user_3']}, {})
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    self.assertTrue(bool(int(result['Value']['TotalRecords']) == 0), 'In DB present proxies.')

    gLogger.info(' Upload proxy..')
    for user, dn, group, vo, time, res, log in [("user_1", '/C=DN/O=DIRAC/CN=user_1', "group_1", False, 12,
                                                 False, 'With group extansion'),
                                                ("user_1", '/C=DN/O=DIRAC/CN=user_1', False, "vo_1", 12,
                                                 False, 'With voms extansion'),
                                                ("user_1", '/C=DN/O=DIRAC/CN=user_1', False, False, 0,
                                                 False, 'Expired proxy'),
                                                ("no_user", '/C=DN/O=DIRAC/CN=no_user', False, False, 12,
                                                 False, 'Not exist user'),
                                                ("user_1", '/C=DN/O=DIRAC/CN=user_1', False, False, 12,
                                                 True, 'Valid proxy')]:
      gLogger.info(' %s:' % log)
      result = self.createProxy(user, group, time, vo=vo)
      self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
      chain = result['Value'][0]
      self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
      result = self.db.generateDelegationRequest(chain, dn)
      self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
      resDict = result['Value']
      result = chain.generateChainFromRequestString(resDict['request'], time * 3500)
      self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
      result = self.db.completeDelegation(resDict['id'], dn, result['Value'])
      self.assertEqual(result['OK'], res, 'Must be ended %s%s' %
                                          (res and 'successful' or 'with error',
                                           ': %s' % result.get('Message') or 'Error message is absent.'))
      if not res:
        gLogger.info('Error: %s' % (result['Message']))
      cmd = 'SELECT COUNT( * ) FROM ProxyDB_Proxies WHERE UserName="%s"' % user
      self.assertTrue(bool(self.db._query(cmd)['Value'][0][0] == 0))
      cmd = 'SELECT COUNT( * ) FROM ProxyDB_CleanProxies WHERE UserName="%s"' % user
      self.assertTrue(bool(self.db._query(cmd)['Value'][0][0] == res and 1 or 0))

    gLogger.info(' Get proxy when it store only in ProxyDB_CleanProxies..')
    # Try to get proxy that was stored in previous step
    for res, group, reqtime, log in [(False, 'group_1', 24 * 3600, 'Request time more that in stored proxy'),
                                     (False, 'group_2', 0, 'Request group not contain user'),
                                     (True, 'group_1', 0, 'Request time less that in stored proxy')]:
      gLogger.info(' %s:' % log)
      result = self.db.getProxy('/C=DN/O=DIRAC/CN=user_1', group, reqtime)
      self.assertEqual(result['OK'], res, 'Must be ended %s%s' %
                                          (res and 'successful' or 'with error',
                                           ': %s' % result.get('Message') or 'Error message is absent.'))
      if res:
        chain = result['Value'][0]
        self.assertTrue(chain.isValidProxy()['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
        result = chain.getDIRACGroup()
        self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
        self.assertEqual('group_1', result['Value'], 'Group must be group_1, not %s' % result['Value'])
      else:
        gLogger.info('Error: %s' % (result['Message']))

    gLogger.info('\n Check that DB is clean..')
    result = self.db.deleteProxy('/C=DN/O=DIRAC/CN=user_1', proxyProvider='Certificate')
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    result = self.db.getProxiesContent({'UserName': ['user_ca', 'user_1', 'user_2', 'user_3']}, {})
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    self.assertTrue(bool(int(result['Value']['TotalRecords']) == 0), 'In DB present proxies.')

    gLogger.info(' Get proxy when it store only in ProxyDB_Proxies..')
    # Make record with proxy that contain group
    result = self.createProxy('user_1', group, 12)
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    proxyStr = result['Value'][1]
    cmd = 'INSERT INTO ProxyDB_Proxies(UserName, UserDN, UserGroup, Pem, ExpirationTime) VALUES '
    cmd += '("user_1", "%s", "%s", "%s", TIMESTAMPADD(SECOND, 43200, UTC_TIMESTAMP()))' % (dn, group,
                                                                                           proxyStr)
    result = self.db._update(cmd)
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    # Try to get it
    result = self.db.getProxy(dn, group, 1800)
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    # Check that proxy contain group
    chain = result['Value'][0]
    self.assertTrue(chain.isValidProxy()['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    result = chain.getDIRACGroup()
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    self.assertEqual('group_1', result['Value'], 'Group must be group_1, not %s' % result['Value'])

    gLogger.info('\n Check that DB is clean..')
    result = self.db.deleteProxy('/C=DN/O=DIRAC/CN=user_1')
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    result = self.db.getProxiesContent({'UserName': ['user_ca', 'user_1', 'user_2', 'user_3']}, {})
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    self.assertTrue(bool(int(result['Value']['TotalRecords']) == 0), 'In DB present proxies.')

    gLogger.info(' Get VOMS proxy..')
    # Create proxy with VOMS extansion
    result = self.createProxy('user_1', 'group_1', 12, vo='vo_1', role='role_2')
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')

    proxyStr = result['Value'][1]
    cmd = 'INSERT INTO ProxyDB_Proxies(UserName, UserDN, UserGroup, Pem, ExpirationTime) VALUES '
    cmd += '("user_1", "/C=DN/O=DIRAC/CN=user_1", "group_1", "%s", ' % proxyStr
    cmd += 'TIMESTAMPADD(SECOND, 43200, UTC_TIMESTAMP()))'
    result = self.db._update(cmd)
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    # Try to get proxy with VOMS extansion
    gLogger.info(' Error casuses:')
    for dn, group, role, time, log in [('/C=DN/O=DIRAC/CN=user_4', 'group_2', False, 9999,
                                        'Not exist VO for current group'),
                                       ('/C=DN/O=DIRAC/CN=user_1', 'group_1', 'role_1', 9999,
                                        'Stored proxy already have different VOMS extansion'),
                                       ('/C=DN/O=DIRACCA/OU=None/CN=user_ca/emailAddress=user_ca@diracgrid.org',
                                        'group_1', 'role_1', 9999, 'Not correct VO configuration')]:
      result = self.db.getVOMSProxy(dn, group, time, role)
      self.assertFalse(result['OK'], 'Must be fail.')
      gLogger.info(' %s:\nError: %s' % (log, result['Message']))
    # Check stored proxies
    for table, user, count in [('ProxyDB_Proxies', 'user_1', 1), ('ProxyDB_CleanProxies', 'user_ca', 1)]:
      cmd = 'SELECT COUNT( * ) FROM %s WHERE UserName="%s"' % (table, user)
      self.assertTrue(bool(self.db._query(cmd)['Value'][0][0] == count))

    gLogger.info(' Delete proxies..')
    for dn, table in [('/C=DN/O=DIRAC/CN=user_1', 'ProxyDB_Proxies'),
                      ('/C=DN/O=DIRACCA/OU=None/CN=user_ca/emailAddress=user_ca@diracgrid.org',
                       'ProxyDB_CleanProxies')]:
      result = self.db.deleteProxy(dn)
      self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
      cmd = 'SELECT COUNT( * ) FROM %s WHERE UserName="user_ca"' % table
      self.assertTrue(bool(self.db._query(cmd)['Value'][0][0] == 0))


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ProxyDBTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(testDB))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
