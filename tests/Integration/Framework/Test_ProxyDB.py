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

# For Jenkins
for f in ['', 'TestCode', os.environ['DIRAC']]:
  certsPath = os.path.join(f, 'DIRAC/Core/Security/test/certs')
  if os.path.exists(certsPath):
    break

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
      OU = None
      C = DN
      O = DIRACCA
    }
  }
}
""" % (os.path.join(certsPath, 'ca/ca.cert.pem'), os.path.join(certsPath, 'ca/ca.key.pem'))

DNs = ['/C=DN/O=DIRACCA/OU=None/CN=user_ca/emailAddress=user_ca@diracgrid.org',
       '/C=DN/O=DIRAC/CN=user_1', '/C=DN/O=DIRAC/CN=user_2', '/C=DN/O=DIRAC/CN=user_3', '/C=DN/O=DIRAC/CN=user_4']

userCFG = """
Registry
{
  Users
  {
    # In dirac_user group
    user_ca
    {
      DN = /C=DN/O=DIRACCA/OU=None/CN=user_ca/emailAddress=user_ca@diracgrid.org
    }
    user_1
    {
      DN = /C=DN/O=DIRAC/CN=user_1
    }
    user_2
    {
      DN = /C=DN/O=DIRAC/CN=user_2
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
  def createProxy(self, userName, group, time, rfc=True, limit=False, vo=None, role=None, path=None):
    """ Create user proxy
    """
    userCertFile = os.path.join(self.userDir, userName + '.cert.pem')
    userKeyFile = os.path.join(self.userDir, userName + '.key.pem')
    self.proxyPath = path or os.path.join(self.userDir, userName + '.pem')
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
      userConfFile = os.path.join(cls.userDir, userName + '.cnf')
      userReqFile = os.path.join(cls.userDir, userName + '.req')
      userKeyFile = os.path.join(cls.userDir, userName + '.key.pem')
      userCertFile = os.path.join(cls.userDir, userName + '.cert.pem')
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
    if self.failed:
       self.fail(self.failed)
    for table in ['ProxyDB_Proxies', 'ProxyDB_CleanProxies', 'ProxyDB_VOMSProxies']:
      db._update('DELETE FROM %s WHERE UserDN IN ("%s")' % (table, '", "'.join(DNs)))

  def tearDown(self):
    for table in ['ProxyDB_Proxies', 'ProxyDB_CleanProxies', 'ProxyDB_VOMSProxies']:
      db._update('DELETE FROM %s WHERE UserDN IN ("%s")' % (table, '", "'.join(DNs)))

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

  def __isProxiesTablesClean(self):
    """ Helper method to check if DB is clean
    """
    for table in ['ProxyDB_Proxies', 'ProxyDB_CleanProxies', 'ProxyDB_VOMSProxies']:
      self.assertTrue(bool(db._query('SELECT COUNT( * ) FROM %%s WHERE UserDN in (%s)' % table)['Value'][0][0] > 0),
                      '%s table contain proxies.')

  def test_connectDB(self):
    """ Try to connect to the ProxyDB
    """
    gLogger.info('\n=== Testing conecting to ProxyDB')
    res = db._connect()
    self.assertTrue(res['OK'])

  def test_getUsers(self):
    """ Test 'getUsers' - try to get users from DB
    """
    gLogger.info('\n=== Testing `getUsers`')

    # Fill table for test
    gLogger.info('\n* Fill tables for test..')
    field = '(%%s "/C=DN/O=DIRAC/CN=%%s", %%s "PEM", TIMESTAMPADD(SECOND, %%s, UTC_TIMESTAMP()))%s' % ''
    for table, values, fields in [('ProxyDB_Proxies',
                                   [field % ('"user_1",', 'user_1', '"group_1",', '800'),
                                    field % ('"user_2",', 'user_2', '"group_1",', '-1')],
                                   '(UserName, UserDN, UserGroup, Pem, ExpirationTime)'),
                                  ('ProxyDB_CleanProxies',
                                   [field % ('', 'user_3', '', '43200')],
                                   '(UserDN, Pem, ExpirationTime)')]:
      result = db._update('INSERT INTO %s%s VALUES %s ;' % (table, fields, ', '.join(values)))
      self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    
    # Testing 'getUsers'
    gLogger.info('\n* Run `purgeExpiredProxies()`..')
    for log, user, exp, expect in [(' - Without arguments     ', False, 0, ['user_1', 'user_2', 'user_3']),
                                   (' - Request proxy livetime', False, 1200, ['user_3']),
                                   (' - Request user name     ', 'user_2', 0, ['user_2']),
                                   (' - Request not exist user', 'no_user', 0, [])]:
      gLogger.info('%s..' % log)
      result = db.getUsers(validSecondsLeft=exp, userMask=user)
      self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
      usersList = []
      for line in result['Value']:
        if line['Name'] in ['user_1', 'user_2', 'user_3']:
          usersList.append(line['Name'])
      self.assertEqual(set(expect), set(usersList), '%s, when expected %s' % (usersList, expect))

  def test_purgeExpiredProxies(self):
    """ Test 'purgeExpiredProxies' - try to purge expired proxies
    """
    gLogger.info('\n=== Testing `purgeExpiredProxies`')

    # Purge existed proxies
    gLogger.info('\n* First cleaning..')
    isDBContainExp = 'SELECT COUNT( * ) FROM %%s WHERE ExpirationTime < UTC_TIMESTAMP()%s' % ''
    result = db.purgeExpiredProxies()
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    for table in ['ProxyDB_Proxies', 'ProxyDB_CleanProxies', 'ProxyDB_VOMSProxies']:
      self.assertTrue(bool(db._query(isDBContainExp % table)['Value'][0][0] > 0),
                      '%s table contain expired proxies after first cleaning.')

    # Fill table for test
    gLogger.info('\n* Fill tables for test..')
    field = '(%%s "/C=DN/O=DIRAC/CN=%%s", %%s "PEM", TIMESTAMPADD(SECOND, %%s, UTC_TIMESTAMP()))%s' % ''
    for table, values, fields in [('ProxyDB_Proxies',
                                   [field % ('"user_1",', 'user_1', '"group_1",', '-1')],
                                   '(UserName, UserDN, UserGroup, Pem, ExpirationTime)'),
                                  ('ProxyDB_CleanProxies',
                                   [field % ('', 'user_1', '', '-1')],
                                   '(UserDN, Pem, ExpirationTime)'),
                                  ('ProxyDB_VOMSProxies',
                                   [field % ('"user_1",', 'user_1', '"group_1", "/vo_1",', '-1')],
                                   '(UserName, UserDN, UserGroup, VOMSAttr, Pem, ExpirationTime)')]:
      result = db._update('INSERT INTO %s%s VALUES %s ;' % (table, fields, ', '.join(values)))
      self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    
    # Testing 'purgeExpiredProxies'
    gLogger.info('\n* Run `purgeExpiredProxies()`..')
    result = db.purgeExpiredProxies()
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    for table in ['ProxyDB_Proxies', 'ProxyDB_CleanProxies', 'ProxyDB_VOMSProxies']:
      self.assertTrue(bool(db._query(isDBContainExp % table)['Value'][0][0] > 0),
                      '%s table contain expired proxies after cleaning.')
    self.assertTrue(result['Value'] == 3, '"%s" proxies cleaned instead "3"' % result['Value'])
  
  def test_getProxiesContent(self):
    """ Test 'getProxiesContent' - Try to get proxies contant from DB
    """
    gLogger.info('\n=== Testing `getProxiesContent`')

    # Checking clean DB
    gLogger.info('\n* Check if tables is clean..')
    self.__isProxiesTablesClean()

    # Testing 'getProxiesContent'
    gLogger.info("\n* Run `getProxiesContent({'UserDN': [<all DNs described in test>]})`..")
    result = db.getProxiesContent({'UserDN': [DNs]})
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    self.assertTrue(bool(int(result['Value']['TotalRecords']) == 0), 'Found not existed proxies.')

    # Fill table for test
    gLogger.info('\n* Fill tables for test..')
    field = '(%%s "/C=DN/O=DIRAC/CN=%%s", %%s "PEM", TIMESTAMPADD(SECOND, %%s, UTC_TIMESTAMP()))%s' % ''
    for table, values, fields in [('ProxyDB_Proxies',
                                   [field % ('"user_1",', 'user_1', '"group_1",', '9999')],
                                   '(UserName, UserDN, UserGroup, Pem, ExpirationTime)'),
                                  ('ProxyDB_CleanProxies',
                                   [field % ('', 'user_2', '', '9999'),
                                    field % ('', 'user_3', '', '-1'),
                                    field % ('', 'user_4', '', '9999')],
                                   '(UserDN, Pem, ExpirationTime)')]:
      result = db._update('INSERT INTO %s%s VALUES %s ;' % (table, fields, ', '.join(values)))
      self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')

    # Testing 'getProxiesContent'
    gLogger.info("\n* Run `getProxiesContent(<selection dictionary>)`..")
    for log, res, sel in [(' - Look two users where one is not in requested group', 1,
                           {'UserName': ['user_1', 'user_4'], 'UserGroup': 'group_1'}),
                          (' - Look group where 3 users and one have expired proxy', 2,
                           {'UserGroup': 'group_1'}),
                          (' - Look all DNs where one is expired', 3,
                           {'UserDN': DNs})]:
      gLogger.info('%s..' % log)
      result = db.getProxiesContent(sel)
      self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
      self.assertTrue(bool(int(result['Value']['TotalRecords']) == res),
                      'Found %s proxies instead %s.' % (result['Value']['TotalRecords']), res)
  
  def test_deleteProxy(self):
    """ Test 'deleteProxy' - Try to delete proxy
    """
    gLogger.info('\n=== Testing `deleteProxy`')

    # Checking clean DB
    gLogger.info('\n* Check if tables is clean..')
    self.__isProxiesTablesClean()

    # Fill table for test
    gLogger.info('\n* Fill tables for test..')
    field = '(%%s "/C=DN/O=DIRAC/CN=%%s", %%s "PEM", TIMESTAMPADD(SECOND, %%s, UTC_TIMESTAMP()))%s' % ''
    for table, values, fields in [('ProxyDB_Proxies',
                                   [field % ('"user_1",', 'user_1', '"group_1",', '9999')],
                                   '(UserName, UserDN, UserGroup, Pem, ExpirationTime)'),
                                  ('ProxyDB_CleanProxies',
                                   [field % ('', 'user_1', '', '9999')],
                                   '(UserDN, Pem, ExpirationTime)'),
                                  ('ProxyDB_VOMSProxies',
                                   [field % ('"user_1",', 'user_1', '"group_1", "/vo_1",', '9999')],
                                   '(UserName, UserDN, UserGroup, VOMSAttr, Pem, ExpirationTime)')]:
      result = db._update('INSERT INTO %s%s VALUES %s ;' % (table, fields, ', '.join(values)))
      self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')

    # Testing 'deleteProxy'
    gLogger.info("\n* Run `deleteProxy('/C=DN/O=DIRAC/CN=user_1')`..")
    result = db.deleteProxy('/C=DN/O=DIRAC/CN=user_1')
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    self.assertTrue(bool(int(result['Value']['TotalRecords']) == res),
                    'Found %s proxies instead %s.' % (result['Value']['TotalRecords']), res))
    self.__isProxiesTablesClean()

  def test_getProxy(self):
    """ Test 'getProxy' - Testing get, store proxy
    """
    gLogger.info('\n=== Testing `getProxy`')

    # Checking clean DB
    gLogger.info('\n* Check if tables is clean..')
    self.__isProxiesTablesClean()
    
    gLogger.info('* Check posible crashes when get proxy..')
    # Make record with not valid proxy, valid group, user and short expired time
    cmd = 'INSERT INTO ProxyDB_Proxies(UserName, UserDN, UserGroup, Pem, ExpirationTime) VALUES '
    cmd += '("user_1", "/C=DN/O=DIRAC/CN=user_1", "group_1", "PEM", '
    cmd += 'TIMESTAMPADD(SECOND, 1800, UTC_TIMESTAMP()));'
    result = db._update(cmd)
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    # Try to no correct getProxy requests
    for user, group, reqtime, voms, log in [('user_1', 'group_1', 9999, False,
                                           'No proxy provider, set request time, not valid proxy in ProxyDB_Proxies'),
                                          ('user_1', 'group_1', 0, False,
                                           'Not valid proxy in ProxyDB_Proxies'),
                                          ('no_user', 'no_valid_group', 0, False,
                                           'User not exist, proxy not in DB tables'),
                                          ('user_1', 'no_valid_group', 0, False,
                                           'Group not valid, proxy not in DB tables'),
                                          ('user_1', 'group_1', 0, False,
                                           'No proxy provider for user, proxy not in DB tables'),
                                          ('user_4', 'group_2', 0, False,
                                           'Group has option enableToDownload = False in CS')]:
      gLogger.info('== > %s:' % log)
      result = db.getProxy(user, group, reqtime, voms)
      self.assertFalse(result['OK'], 'Must be fail.')
      gLogger.info('Msg: %s' % result['Message'])
    # In the last case method found proxy and must to delete it as not valid
    cmd = 'SELECT COUNT( * ) FROM ProxyDB_Proxies WHERE UserName="user_1"'
    self.assertTrue(bool(db._query(cmd)['Value'][0][0] == 0), "GetProxy method didn't delete the last proxy.")

    gLogger.info('* Check that DB is clean..')
    result = db.getProxiesContent({'UserName': ['user_ca', 'user_1', 'user_2', 'user_3']})
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    self.assertTrue(bool(int(result['Value']['TotalRecords']) == 0), 'In DB present proxies.')

    gLogger.info('* Generate proxy on the fly..')
    result = db.getProxy('user_ca', 'group_1', 1800)
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')

    gLogger.info('* Check that ProxyDB_CleanProxy contain generated proxy..')
    result = db.getProxiesContent({'UserName': 'user_ca'})
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    self.assertTrue(bool(int(result['Value']['TotalRecords']) == 1), 'Generated proxy must be one.')
    for table, count in [('ProxyDB_Proxies', 0), ('ProxyDB_CleanProxies', 1)]:
      cmd = 'SELECT COUNT( * ) FROM %s WHERE UserName="user_ca"' % table
      self.assertTrue(bool(db._query(cmd)['Value'][0][0] == count),
                      '%s must %s' % (table, count and 'contain proxy' or 'be empty'))

    gLogger.info('* Check that DB is clean..')
    result = db.deleteProxy('/C=DN/O=DIRACCA/OU=None/CN=user_ca/emailAddress=user_ca@diracgrid.org')
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    result = db.getProxiesContent({'UserName': ['user_ca', 'user_1', 'user_2', 'user_3']})
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    self.assertTrue(bool(int(result['Value']['TotalRecords']) == 0), 'In DB present proxies.')

    gLogger.info('* Upload proxy..')
    for user, dn, group, vo, time, res, log in [("user_1", '/C=DN/O=DIRAC/CN=user_1', "group_1", False, 12,
                                                 True, 'With group extension'),
                                                ("user_1", '/C=DN/O=DIRAC/CN=user_1', False, "vo_1", 12,
                                                 False, 'With voms extension'),
                                                ("user_1", '/C=DN/O=DIRAC/CN=user_1', False, False, 0,
                                                 False, 'Expired proxy'),
                                                ("no_user", '/C=DN/O=DIRAC/CN=no_user', False, False, 12,
                                                 False, 'Not exist user'),
                                                ("user_1", '/C=DN/O=DIRAC/CN=user_1', False, False, 12,
                                                 True, 'Valid proxy')]:
      result = db._update('DELETE FROM ProxyDB_Proxies WHERE UserName = "user_1"')
      self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
      gLogger.info('== > %s:' % log)
      result = self.createProxy(user, group, time, vo=vo)
      self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
      chain = result['Value'][0]
      result = db.generateDelegationRequest({'x509Chain': chain, 'DN': dn})
      self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
      resDict = result['Value']
      result = chain.generateChainFromRequestString(resDict['request'], time * 3500)
      self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
      result = db.completeDelegation(resDict['id'], dn, result['Value'])
      self.assertEqual(result['OK'], res, 'Must be ended %s%s' %
                                          (res and 'successful' or 'with error',
                                           ': %s' % result.get('Message') or 'Error message is absent.'))
      if not res:
        gLogger.info('Msg: %s' % (result['Message']))
      cmd = 'SELECT COUNT( * ) FROM ProxyDB_Proxies WHERE UserName="%s"' % user
      self.assertTrue(bool(db._query(cmd)['Value'][0][0] == (res and group and 1) or 0),
                      'ProxyDB_Proxies must %s' % (res and 'contain proxy' or 'be empty'))
      cmd = 'SELECT COUNT( * ) FROM ProxyDB_CleanProxies WHERE UserName="%s"' % user
      self.assertTrue(bool(db._query(cmd)['Value'][0][0] == (res and not group and 1) or 0),
                      'ProxyDB_CleanProxies must %s' % (res and 'contain proxy' or 'be empty'))

    gLogger.info('* Check that ProxyDB_CleanProxy contain generated proxy..')
    result = db.getProxiesContent({'UserName': 'user_1'})
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    self.assertTrue(bool(int(result['Value']['TotalRecords']) == 1), 'Generated proxy must be one.')
    cmd = 'SELECT COUNT( * ) FROM ProxyDB_CleanProxies WHERE UserName="user_1"'
    self.assertTrue(bool(db._query(cmd)['Value'][0][0] == 1), 'ProxyDB_CleanProxies must contain proxy')

    gLogger.info('* Get proxy that store only in ProxyDB_CleanProxies..')
    # Try to get proxy that was stored to ProxyDB_CleanProxies in previous step
    for res, group, reqtime, log in [(False, 'group_1', 24 * 3600, 'Request time more that in stored proxy'),
                                     (False, 'group_2', 0, 'Request group not contain user'),
                                     (True, 'group_1', 0, 'Request time less that in stored proxy')]:
      gLogger.info('== > %s:' % log)
      result = db.getProxy('user_1', group, reqtime)
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
        gLogger.info('Msg: %s' % (result['Message']))

    gLogger.info('* Check that DB is clean..')
    result = db.deleteProxy('/C=DN/O=DIRAC/CN=user_1')
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    result = db.getProxiesContent({'UserName': ['user_ca', 'user_1', 'user_2', 'user_3']})
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    self.assertTrue(bool(int(result['Value']['TotalRecords']) == 0), 'In DB present proxies.')

    gLogger.info('* Get proxy when it store only in ProxyDB_Proxies..')
    # Make record with proxy that contain group
    result = self.createProxy('user_1', 'group_1', 12)
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    cmd = 'INSERT INTO ProxyDB_Proxies(UserName, UserDN, UserGroup, ExpirationTime, Pem) VALUES '
    cmd += '("user_1", "/C=DN/O=DIRAC/CN=user_1", "group_1", TIMESTAMPADD(SECOND, 43200, UTC_TIMESTAMP()), '
    cmd += '"%s")' % result['Value'][1]
    result = db._update(cmd)
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    # Try to get it
    result = db.getProxy('user_1', 'group_1', 1800)
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    # Check that proxy contain group
    chain = result['Value'][0]
    self.assertTrue(chain.isValidProxy()['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    result = chain.getDIRACGroup()
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    self.assertEqual('group_1', result['Value'], 'Group must be group_1, not %s' % result['Value'])

    gLogger.info('* Check that DB is clean..')
    result = db.deleteProxy('/C=DN/O=DIRAC/CN=user_1')
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    result = db.getProxiesContent({'UserName': ['user_ca', 'user_1', 'user_2', 'user_3']})
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    self.assertTrue(bool(int(result['Value']['TotalRecords']) == 0), 'In DB present proxies.')

    gLogger.info('* Get VOMS proxy..')
    # Create proxy with VOMS extension
    result = self.createProxy('user_1', 'group_1', 12, vo='vo_1', role='role_2')
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')

    proxyStr = result['Value'][1]
    cmd = 'INSERT INTO ProxyDB_Proxies(UserName, UserDN, UserGroup, Pem, ExpirationTime) VALUES '
    cmd += '("user_1", "/C=DN/O=DIRAC/CN=user_1", "group_1", "%s", ' % proxyStr
    cmd += 'TIMESTAMPADD(SECOND, 43200, UTC_TIMESTAMP()))'
    result = db._update(cmd)
    self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
    # Try to get proxy with VOMS extension
    for user, group, role, time, log in [('user_4', 'group_2', False, 9999,
                                          'Not exist VO for current group'),
                                         ('user_1', 'group_1', True, 9999,
                                          'Stored proxy already have different VOMS extension'),
                                         ('user_ca', 'group_1', True, 9999,
                                          'Not correct VO configuration')]:
      gLogger.info('== > %s:' % log)
      result = db.getProxy(user, group, time, role)
      self.assertFalse(result['OK'], 'Must be fail.')
      gLogger.info('Msg: %s' % result['Message'])
    # Check stored proxies
    for table, user, count in [('ProxyDB_Proxies', 'user_1', 1), ('ProxyDB_CleanProxies', 'user_ca', 1)]:
      cmd = 'SELECT COUNT( * ) FROM %s WHERE UserName="%s"' % (table, user)
      self.assertTrue(bool(db._query(cmd)['Value'][0][0] == count))

    gLogger.info('* Delete proxies..')
    for dn, table in [('/C=DN/O=DIRAC/CN=user_1', 'ProxyDB_Proxies'),
                      ('/C=DN/O=DIRACCA/OU=None/CN=user_ca/emailAddress=user_ca@diracgrid.org',
                       'ProxyDB_CleanProxies')]:
      result = db.deleteProxy(dn)
      self.assertTrue(result['OK'], '\n%s' % result.get('Message') or 'Error message is absent.')
      cmd = 'SELECT COUNT( * ) FROM %s WHERE UserName="user_ca"' % table
      self.assertTrue(bool(db._query(cmd)['Value'][0][0] == 0))


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ProxyDBTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(testDB))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
