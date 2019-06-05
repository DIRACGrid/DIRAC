""" This is a ProxyMannager test

    It supposes that all ProxyManager methods is present and working in DIRAC
"""

# pylint: disable=invalid-name,wrong-import-position,protected-access
import os
import sys
import stat
import shutil
import unittest
import commands

from tempfile import mkstemp
from sqlalchemy import create_engine

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.CFG import CFG
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.ConfigurationSystem.Client.Utilities import getDBParameters
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import ProxyManagerClient


class ProxyManagerTestCase(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    cls.proxyMgr = ProxyManagerClient()
    
    # Dublicate original proxy
    cls.oldProxy = '/tmp/x509up_u%s' % os.geteuid()
    if os.path.isfile(cls.oldProxy):
      shutil.copy(cls.oldProxy, '%s.save' % cls.oldProxy)
    
    # Prepare CA
    certsPath = os.path.join(os.environ['DIRAC'], 'DIRAC/Core/Security/test/certs')
    cls.caWorkingDirectory = os.path.join(certsPath, 'ca')
    # Dublicate original files
    if os.path.exists('%s.save' % cls.caWorkingDirectory):
      shutil.rmtree('%s.save' % cls.caWorkingDirectory)
    shutil.copytree(cls.caWorkingDirectory, '%s.save' % cls.caWorkingDirectory)
    cls.hostCert = os.path.join(certsPath, 'host/hostcert.pem')
    cls.hostKey = os.path.join(certsPath, 'host/hostkey.pem')
    cls.caConfigFile = os.path.join(cls.caWorkingDirectory, 'openssl_config_ca.cnf')
    #Create temp file
    fh, absPath = mkstemp()
    newFile = open(absPath,'w')
    oldFile = open(cls.caConfigFile)
    for line in oldFile:
      newFile.write('dir = %s' % (cls.caWorkingDirectory) if 'dir = ' in line else line)
    #close temp file
    newFile.close()
    os.close(fh)
    oldFile.close()
    # Remove original file
    os.remove(cls.caConfigFile)
    #Move new file
    shutil.move(absPath, cls.caConfigFile)
    # Erase index
    open(cls.caWorkingDirectory + '/index.txt', 'w').close()
    
    # Create tmp dir
    cls.tmpDir = os.path.join(os.environ['HOME'], 'tmpDirForTesting')
    if not os.path.exists(cls.tmpDir):
      os.makedirs(cls.tmpDir)

    # # Create needed for tests rows
    # result = getDBParameters('Framework/ProxyDB')
    # if not result['OK']:
    #   raise RuntimeError('Cannot get database parameters: %s' % result['Message'])    
    # dbParam = result['Value']
    # engine = create_engine('mysql://%s:%s@%s:%s/%s' % (dbParam['User'],
    #                                                    dbParam['Password'],
    #                                                    dbParam['Host'],
    #                                                    dbParam['Port'],
    #                                                    dbParam['DBName']))
    # cls.connection = engine.connect()

  def setUp(self):
    pass

  def tearDown(self):
    pass

  @classmethod
  def tearDownClass(cls):
    print('Clean DB!!!!!!!!')
    # cls.connection.execute('DELETE FROM ProxyDB_Proxies WHERE UserName IN ("user_11", "user_12")')
    # cls.connection.close()
    # Move original dirs back
    if os.path.exists('%s.save' % cls.caWorkingDirectory):
      if os.path.exists(cls.caWorkingDirectory):
        shutil.rmtree(cls.caWorkingDirectory)
      shutil.move('%s.save' % cls.caWorkingDirectory, cls.caWorkingDirectory)
    if os.path.exists(cls.tmpDir):
      shutil.rmtree(cls.tmpDir)
    if os.path.isfile(cls.oldProxy + '.save'):
      shutil.move('%s.save' % cls.oldProxy, cls.oldProxy)


class ProxyManagerTest(ProxyManagerTestCase):

  # def test_getRegisteredUsers(self):
  #   """ Testing userHasProxy, setPersistency, getUserPersistence methods
  #   """
  #   for userDB,pers in [('user_11', True),
  #                       ('user_21', False)]:
  #     for expDB,exp,res in [("UTC_TIMESTAMP()", 0,    False),
  #                           ("UTC_TIMESTAMP()", 1000, False),
  #                           ("TIMESTAMPADD( SECOND, 200, UTC_TIMESTAMP() )",  0,    True),
  #                           ("TIMESTAMPADD( SECOND, 200, UTC_TIMESTAMP() )",  1000, False),
  #                           ("TIMESTAMPADD( SECOND, 43200, UTC_TIMESTAMP() )",0,    True),
  #                           ("TIMESTAMPADD( SECOND, 43200, UTC_TIMESTAMP() )",1000, True)]:
  #       dnDB = "/C=FR/O=DIRAC/OU=DIRAC_1/CN=DIRAC %s" % userDB
  #       groupDB = "dirac_user_1"
  #       cmd = "REPLACE INTO ProxyDB_Proxies(UserName, UserDN, UserGroup, Pem, ExpirationTime, PersistentFlag) "
  #       cmd += 'VALUES ( "%s", "%s", "%s", "%s", %s, "%s" ) ;' % (userDB, dnDB, groupDB, 'PEM', expDB, str(pers))
  #       self.connection.execute(cmd)
  #       print("\n==>" + "".join(word.ljust(50) for word in [dnDB, groupDB, str(expDB), str(pers)]))
  #       for dn,group,res in [(dnDB, groupDB, res),
  #                            (dnDB, 'no',    False),
  #                            ('no', 'no',    False),
  #                            ('no', groupDB, False)]:
  #         print("   " + "".join(word.ljust(50) for word in [dnDB, groupDB, str(expDB), str(pers)]))
  #         self.proxyMgr.clearCaches()
  #         result = self.proxyMgr.userHasProxy(dn, group, exp)
  #         self.assertTrue(result['OK'])
  #         self.assertEqual(result['Value'], res, 'No match!')
  #         result = self.proxyMgr.getUserPersistence(dn, group, exp)
  #         self.assertTrue(result['OK'])
  #         self.assertEqual(result['Value'], True if res and pers else False, 'No match!')
  #         newpers = False and True or True
  #         result = self.proxyMgr.setPersistency(dn, group, newpers)
  #         self.assertTrue(result['OK'])
  #         if res:
  #           result = self.proxyMgr.getUserPersistence(dn, group, exp)
  #           self.assertTrue(result['OK'])
  #           self.assertEqual(result['Value'], True if res and newpers else False, 'No match!')

  def test_uploadProxy(self):
    """ Testing uploadProxy method
    """
    def createCert(userName):
      """ Create certificate
      """
      userConf = """[ req ]
        default_bits           = 2048
        encrypt_key            = yes
        distinguished_name     = req_dn
        prompt                 = no
        req_extensions         = v3_req
        [ req_dn ]
        C                      = FR
        O                      = DIRAC
        OU                     = DIRAC TEST
        CN                     = DIRAC test %s
        emailAddress           = %s@diracgrid.org
        [ v3_req ]
        # Extensions for client certificates (`man x509v3_config`).
        nsComment = "OpenSSL Generated Client Certificate"
        keyUsage = critical, nonRepudiation, digitalSignature, keyEncipherment
        extendedKeyUsage = clientAuth
        """ % (userName, userName)

      userConfFile = os.path.join(self.tmpDir, userName + '.cnf')
      userReqFile = os.path.join(self.tmpDir, userName + '.req')
      userKeyFile = os.path.join(self.tmpDir, userName + '.key.pem')
      userCertFile = os.path.join(self.tmpDir, userName + '.cert.pem')
      with open(userConfFile, "w") as f:
        f.write(userConf)

      status, output = commands.getstatusoutput('openssl genrsa -out %s 2048' % userKeyFile)
      if status:
        return S_ERROR(output)
      os.chmod(userKeyFile, stat.S_IREAD)
      status, output = commands.getstatusoutput('openssl req -config %s -key %s -new -out %s' %
                                                (userConfFile, userKeyFile, userReqFile))
      if status:
        return S_ERROR(output)
      cmd = 'openssl ca -config %s -extensions usr_cert -batch -days 375 -in %s -out %s'
      cmd = cmd % (self.caConfigFile, userReqFile, userCertFile)
      status, output = commands.getstatusoutput(cmd)
      if status:
        return S_ERROR(output)
      return S_OK()

    def createProxyToFile(userName, group, rfc, limit, time, vo, path=None):
      """ Create proxy
      """
      userCertFile = os.path.join(self.tmpDir, userName + '.cert.pem')
      userKeyFile = os.path.join(self.tmpDir, userName + '.key.pem')
      self.proxyPath = path or os.path.join(self.tmpDir, userName + '.pem')
      if not voProxy:
        chain = X509Chain()
        #Load user cert and key
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
        return chain.generateProxyToFile(self.proxyPath, time * 3600,
                                         limited=limit, diracGroup=group,
                                         rfc=rfc)
      else:
        cmd = 'voms-proxy-fake --cert %s --key %s -q' % (userCertFile, userKeyFile)
        cmd += ' -hostcert %s -hostkey %s' % (self.hostCert, self.hostKey)
        cmd += ' -uri fakeserver.cern.ch:15000'
        cmd += ' -fqan "/%s/Role=user/Capability=NULL" -voms %s ' % (vo, vo)
        cmd += ' -hours %s -out %s' % (time, self.proxyPath)
        if limit:
          cmd += ' -limited'
        if rfc:
          cmd += ' -rfc'
        status, output = commands.getstatusoutput(cmd)
        if status:
          return S_ERROR(output)
        return S_OK()

    # Create certificates
    for user in ['no_user', 'user_11', 'user_21']:
      result = createCert(user)
      self.assertTrue(result['OK'], 'Cannot create certificate for "%s": %s' % (user, result.get('Message')))
    
    userUpload = 'user_11'
    userProxy = 'user_11'
    groupUpload = 'dirac_user_1'
    groupProxy = False
    rfcUpload = True
    rfcProxy = True
    limitUpload = False
    limitProxy = False
    timeUpload = 12
    timeProxy = 12
    voUpload = 'vo_1'
    voProxy = False

    timeReq = 12 * 3600
    rfcReq = True


    result = createProxyToFile(userUpload, groupUpload, rfcUpload, limitUpload, timeUpload, voUpload, '/tmp/x509up_u%s' % os.geteuid())
    uploadParams = 'User: %s\nGroup: %s\nRFC: %s\nLimited: %s\nLivetime: %s\nVO: %s' % (userUpload, groupUpload, rfcUpload, limitUpload, timeUpload, voUpload)
    self.assertTrue(result['OK'], 'Cannot create proxy with next params:\n %s\nERROR: %s' % (uploadParams, result.get('Message')))
    
    result = createProxyToFile(userProxy, groupProxy, rfcProxy, limitProxy, timeProxy, voProxy)
    proxyParams = 'User: %s\nGroup: %s\nRFC: %s\nLimited: %s\nLivetime: %s\nVO: %s' % (userProxy, groupProxy, rfcProxy, limitProxy, timeProxy, voProxy)
    self.assertTrue(result['OK'], 'Cannot create proxy that need to attach with next params:\n %s\nERROR: %s' % (proxyParams, result.get('Message')))
    
    chain = X509Chain()
    result = chain.loadProxyFromFile(self.proxyPath)
    self.assertTrue(result['OK'], 'Cannot load proxy from file "%s": %s' % (self.proxyPath, result.get('Message')))

    proxy = chain

    print('Try to upload proxy with next params:\n Proxy: %s\nRFC: %s\nLivetime: %s\nUploadParams:\n%s\nProxyParams:\n%s\n' %
          (proxy, rfcReq, timeReq, uploadParams, proxyParams))
    result = self.proxyMgr.uploadProxy(proxy=proxy, restrictLifeTime=timeReq, rfcIfPossible=rfcReq)
    print('\nRESULT: %s' % result)


    # for userUpload, userProxy in [('user_11', 'user_11'), ('user_11', 'user_21'),
    #                               ('user_11', 'no_user'), ('no_user', 'user_11')]:
    #   for rfcUpload, rfcProxy, rfcReq  in [(True, True, True), (False, False, False),
    #                                        (True, True, False), (False, False, True),
    #                                        (True, False, True), (False, True, False),
    #                                        (True, False, False), (False, True, True)]:
    #     for limitUpload, limitProxy in [(True, True), (False, False),
    #                                     (True, False), (False, True)]:
    #       for timeUpload, timeProxy, timeReq  in [(0, 12, 12 * 3600), (48, 0, 0),
    #                                               (48, 12, 0), (48, 12, 48 * 3600)]:
    #         for groupUpload, groupProxy in [('dirac_user_1', 'dirac_user_1'), 
    #                                         ('dirac_user_1', 'dirac_user_2'),
    #                                         ('dirac_user_1', 'noexistgroup'),
    #                                         ('dirac_user_1', False),
    #                                         ('noexistgroup', False),
    #                                         (False, False)]:
    #             for voProxy, voUpload in [('vo_1', 'vo_1'), (False, False),
    #                                       ('vo_1', False), (False, 'vo_1')]:
    #               result = createProxyToFile(userUpload, groupUpload, rfcUpload, limitUpload, timeUpload, voUpload, '/tmp/x509up_u%s' % os.geteuid())
    #               uploadParams = 'User: %s\nGroup: %s\nRFC: %s\nLimited: %s\nLivetime: %s\nVO: %s' % (userUpload, groupUpload, rfcUpload, limitUpload, timeUpload, voUpload)
    #               self.assertTrue(result['OK'], 'Cannot create proxy with next params:\n %s\nERROR: %s' % (uploadParams, result.get('Message')))
    #               result = createProxyToFile(userProxy, groupProxy, rfcProxy, limitProxy, timeProxy, voProxy)
    #               proxyParams = 'User: %s\nGroup: %s\nRFC: %s\nLimited: %s\nLivetime: %s\nVO: %s' % (userProxy, groupProxy, rfcProxy, limitProxy, timeProxy, voProxy)
    #               self.assertTrue(result['OK'], 'Cannot create proxy that need to attach with next params:\n %s\nERROR: %s' % (proxyParams, result.get('Message')))
    #               chain = X509Chain()
    #               result = chain.loadProxyFromFile(self.proxyPath)
    #               self.assertTrue(result['OK'], 'Cannot load proxy from file "%s": %s' % (self.proxyPath, result.get('Message')))

    #               for proxy in [chain, self.proxyPath, '/no/exist', False]:
    #                 result = self.proxyMgr.uploadProxy(proxy=proxy, restrictLifeTime=timeReq, rfcIfPossible=rfcReq)
    #                 # self.assertTrue(result['OK'], 'Cannot upload proxy with next params:\n Proxy: %s\nGroup: %s\nRFC: %s\nLivetime: %s\nERROR: %s\nUploadParams:\n%s\nProxyParams:\n%s\n' %
    #                 #                             (proxy, groupReq, rfcReq, timeReq, result.get('Message'), uploadParams, proxyParams))
                    
    #                 print('Try to upload proxy with next params:\n Proxy: %s\nRFC: %s\nLivetime: %s\nRESULT: %s\nUploadParams:\n%s\nProxyParams:\n%s\n' %
    #                                              (proxy, rfcReq, timeReq, result, uploadParams, proxyParams))
                    # for dn, gorup in [('DN',group), ('no',group), (dn,'no'), ('no','no')]:
                    #   for limited in [False, True]:
                    #     for reqtime in [0,200,999999999999]:
                    #       for cachtime in [0,200,999999999999]:
                    #         for token in [False, True]:
                    #           pass
                              #downloadProxy                      


    # result = downloadProxy                (userDN, userGroup, limited=False, requiredTimeLeft=1200, cacheTime=14400, proxyToConnect=False, token=False)
    #          downloadProxyToFile
    # result = downloadVOMSProxy            (userDN, userGroup, limited=False, requiredTimeLeft=1200, cacheTime=14400, proxyToConnect=False, token=False, requiredVOMSAttribute=False)
    #          downloadVOMSProxyToFile
    #          getPilotProxyFromDIRACGroup  (userDN, userGroup,                requiredTimeLeft=43200,                 proxyToConnect=False)
    #          getPilotProxyFromVOMSGroup   (userDN, vomsAttr,                 requiredTimeLeft=43200,                 proxyToConnect=False)
    #          getPayloadProxyFromDIRACGroup(userDN, userGroup,                requiredTimeLeft,                       proxyToConnect=False, token=False)
    #          getPayloadProxyFromVOMSGroup (userDN, vomsAttr, token,          requiredTimeLeft,                       proxyToConnect=False)
    #          renewProxy                   (proxyToBeRenewed=False, minLifeTime=3600, newProxyLifeTime=43200,         proxyToConnect=False)

    #     dumpProxyToFile(chain, destinationFile=False, requiredTimeLeft=600)

    # result = requestToken(requesterDN, requesterGroup, numUses=1)

    # result = getDBContents(condDict={})
    #          getUploadedProxyLifeTime(DN, group)

    # result = getUserProxiesInfo()




if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ProxyManagerTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ProxyManagerTest))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
