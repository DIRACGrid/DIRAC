""" Class for the LCG File Catalog Client

"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
__RCSID__ = "$Id"

from past.builtins import long
import six
from stat import *
import os
import re
import time

import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Resources.Catalog.Utilities import checkCatalogArguments
from DIRAC.Core.Utilities.Time import fromEpoch
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Security.ProxyInfo import getProxyInfo, formatProxyInfoAsString
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForUsername, getVOMSAttributeForGroup, \
    getVOForGroup, getVOOption
from DIRAC.Resources.Catalog.FileCatalogClientBase import FileCatalogClientBase

lfc = None
importedLFC = None

####################################################################
#
# These are some functions used by all methods in the class
#


def setLfnReplicas(lfn, replicas, successful, failed):
  if not lfn:
    return
  if replicas:
    successful[lfn] = replicas.copy()
    replicas.clear()
  elif lfn not in failed:
    failed[lfn] = 'No active replica'


def getClientCertInfo():
  res = getProxyInfo(False, False)
  if not res['OK']:
    gLogger.error("getClientCertInfo: Failed to get client proxy information.",
                  res['Message'])
    return res
  proxyInfo = res['Value']
  gLogger.debug(formatProxyInfoAsString(proxyInfo))
  if 'group' not in proxyInfo:
    errStr = "getClientCertInfo: Proxy information does not contain the group."
    gLogger.error(errStr)
    return S_ERROR(errStr)
  if 'VOMS' not in proxyInfo:
    proxyInfo['VOMS'] = getVOMSAttributeForGroup(proxyInfo['group'])
    errStr = "getClientCertInfo: Proxy information does not contain the VOMs information."
    gLogger.warn(errStr)
  res = getDNForUsername(proxyInfo['username'])
  if not res['OK']:
    errStr = "getClientCertInfo: Error getting known proxies for user."
    gLogger.error(errStr, res['Message'])
    return S_ERROR(errStr)
  diracGroup = proxyInfo.get('group', 'Unknown')
  vo = getVOForGroup(diracGroup)
  vomsVO = getVOOption(vo, 'VOMSVO', '')
  resDict = {'DN': proxyInfo['identity'],
             'Role': proxyInfo['VOMS'],
             'User': proxyInfo['username'],
             'AllDNs': res['Value'],
             'Group': diracGroup,
             'VO': vo,
             'VOMSVO': vomsVO
             }
  return S_OK(resDict)


def existsGuid(guid):
  """ Check if the guid exists
  """
  fstat = lfc.lfc_filestatg()
  error = lfc.lfc_statg('', guid, fstat)
  return returnCode(error and lfc.cvar.serrno != 2, not error)


def getDNFromUID(userID):
  buff = " " * (lfc.CA_MAXNAMELEN + 1)
  res = lfc.lfc_getusrbyuid(userID, buff)
  if res == 0:
    dn = buff[:buff.find('\x00')]
    gLogger.debug("LcgFileCatalogClient.getDNFromUID: UID %s maps to %s." % (userID, dn))
    return S_OK(dn)
  else:
    errStr = "LcgFileCatalogClient.getDNFromUID: Failed to get DN from UID"
    gLogger.error(errStr, "%s %s" % (userID, lfc.sstrerror(lfc.cvar.serrno)))
    return S_ERROR(errStr)


def getRoleFromGID(groupID, path=None):
  buff = " " * (lfc.CA_MAXNAMELEN + 1)
  res = lfc.lfc_getgrpbygid(groupID, buff)
  if res == 0:
    role = buff[:buff.find('\x00')]
    if role == 'lhcb':
      role = 'lhcb/Role=user'
    gLogger.debug("LcgFileCatalogClient.getRoleFromGID: GID %s maps to %s." % (groupID, role))
    return S_OK(role)
  else:
    errStr = "LcgFileCatalogClient:getRoleFromGID: Failed to get role from GID"
    gLogger.error(errStr, "%s %s%s" % (groupID, ('(%s) ' % path) if path else '', lfc.sstrerror(lfc.cvar.serrno)))
    return S_ERROR()


def addReplica(guid, pfn, se, master):
  fid = lfc.lfc_fileid()
  status = 'U'
  f_type = 'D'
  poolname = ''
  fs = ''
  error = lfc.lfc_addreplica(guid, fid, se, pfn, status, f_type, poolname, fs)
  # If replica exists, re-register it as one may have changed some parameters (status, se)
  if lfc.sstrerror(lfc.cvar.serrno) == "File exists":
    retCode = removeReplica(pfn)
    if not retCode['OK']:
      return retCode
    error = lfc.lfc_addreplica(guid, fid, se, pfn, status, f_type, poolname, fs)
  return returnCode(error)


def removeReplica(pfn):
  fid = lfc.lfc_fileid()
  error = lfc.lfc_delreplica('', fid, pfn)
  return returnCode(error and error != 2)


def setReplicaStatus(pfn, status):
  return returnCode(lfc.lfc_setrstatus(pfn, status))


def modReplica(pfn, newse):
  return returnCode(lfc.lfc_modreplica(pfn, '', '', newse))


def closeDirectory(oDirectory):
  return returnCode(lfc.lfc_closedir(oDirectory))


def getDNUserID(dn):
  error, users = lfc.lfc_getusrmap()
  userid = None
  for userMap in users if not error else []:
    if userMap.username == dn:
      userid = userMap.userid
      break
  return returnCode(userid is None, userid, errMsg="DN does not exist" if not error else '')


def addUserDN(userID, dn):
  error = lfc.lfc_enterusrmap(userID, dn)
  # 17 is if dn already exists, then OK
  return returnCode(error and lfc.cvar.serrno != 17)


def returnCode(error, value='', errMsg=''):
  if not error:
    return S_OK(value)
  elif errMsg:
    return S_ERROR(errMsg)
  else:
    return S_ERROR(lfc.sstrerror(lfc.cvar.serrno))

#####################################################
#
# LFC catalog client class
#
#####################################################


class LcgFileCatalogClient(FileCatalogClientBase):

  READ_METHODS = FileCatalogClientBase.READ_METHODS + ['isLink',
                                                       'readLink',
                                                       'isFile',
                                                       'getFileMetadata',
                                                       'getReplicas',
                                                       'getReplicaStatus',
                                                       'getFileSize',
                                                       'isDirectory',
                                                       'getDirectoryReplicas',
                                                       'listDirectory',
                                                       'getDirectoryMetadata',
                                                       'getDirectorySize',
                                                       'getDirectoryContents',
                                                       'resolveDataset',
                                                       'getLFNForPFN',
                                                       'getUserDirectory']

  WRITE_METHODS = FileCatalogClientBase.WRITE_METHODS + ['createLink',
                                                         'removeLink',
                                                         'addFile',
                                                         'addReplica',
                                                         'removeReplica',
                                                         'removeFile',
                                                         'setReplicaStatus',
                                                         'setReplicaHost',
                                                         'createDirectory',
                                                         'removeDirectory',
                                                         'removeDataset',
                                                         'removeFileFromDataset',
                                                         'createDataset',
                                                         'changePathOwner',
                                                         'changePathMode']

  NO_LFN_METHODS = FileCatalogClientBase.NO_LFN_METHODS + ['getUserDirectory', 'createUserDirectory',
                                                           'createUserMapping', 'removeUserDirectory']

  ADMIN_METHODS = FileCatalogClientBase.ADMIN_METHODS + ['getUserDirectory', 'createUserDirectory',
                                                         'createUserMapping',
                                                         'removeUserDirectory']

  def __init__(self, **options):
    global lfc, importedLFC

    if importedLFC is None:
      try:
        import lfcthr as lfc
        # This is necessary to make the LFC client thread safe.
        lfc.init()
        importedLFC = True
        gLogger.debug("LcgFileCatalogClient.__init__: Successfully imported lfc module.")

      except ImportError:
        importedLFC = False
        gLogger.exception("LcgFileCatalogClient.__init__: Failed to import lfc module.")

    self.valid = importedLFC

    infosys = options.get('LcgGfalInfosys')
    if not infosys and 'LCG_GFAL_INFOSYS' in os.environ:
      # if not in CS take from environ
      infosys = os.environ['LCG_GFAL_INFOSYS']

    host = options.get('MasterHost')
    if not host and 'LFC_HOST' in os.environ:
      # if not in CS take from environ
      host = os.environ['LFC_HOST']

    self.host = host

    if host:
      os.environ['LFC_HOST'] = host
    if infosys:
      os.environ['LCG_GFAL_INFOSYS'] = infosys

    if 'LFC_CONRETRYINT' not in os.environ:
      os.environ['LFC_CONRETRYINT'] = '5'
    if 'LFC_CONNTIMEOUT' not in os.environ:
      os.environ['LFC_CONNTIMEOUT'] = '5'
    if 'LFC_CONRETRY' not in os.environ:
      os.environ['LFC_CONRETRY'] = '5'

    self.prefix = '/grid'
    self.session = False
    self.transaction = False

  def isOK(self):
    return self.valid

  def getName(self):
    return S_OK(self.name)

  ####################################################################
  #
  # These are the methods for session/transaction manipulation
  #

  def __openSession(self):
    """Open the LFC client/server session"""
    if self.session:
      # Another thread is holding a session, can't create it
      return 0
    else:
      sessionName = 'DIRAC_%s.%s at %s at time %s' % (DIRAC.majorVersion,
                                                      DIRAC.minorVersion,
                                                      DIRAC.siteName(),
                                                      time.time())
      rc = lfc.lfc_startsess(self.host, sessionName)
      self.session = (rc == 0)
      # if there was an error, return -1, to be tested just after the call...
      return 1 if self.session else -1

  def __closeSession(self):
    """Close the LFC client/server session"""
    if self.session:
      lfc.lfc_endsess()
      self.session = False

  def __startTransaction(self):
    """ Begin transaction for one time commit """
    if not self.transaction:
      transactionName = 'Transaction: DIRAC_%s.%s at %s at time %s' % (DIRAC.majorVersion,
                                                                       DIRAC.minorVersion,
                                                                       DIRAC.siteName(),
                                                                       time.time())
      lfc.lfc_starttrans(self.host, transactionName)
      self.transaction = True

  def __abortTransaction(self):
    """ Abort transaction """
    if self.transaction:
      lfc.lfc_aborttrans()
      self.transaction = False

  def __endTransaction(self):
    """ End transaction gracefully """
    if self.transaction:
      lfc.lfc_endtrans()
      self.transaction = False

  def setAuthorizationId(self, dn):
    """ Set authorization id for the proxy-less LFC communication """
    lfc.lfc_client_setAuthorizationId(0, 0, 'GSI', dn)

  ####################################################################
  #
  # The following are read methods for paths
  #

  @checkCatalogArguments
  def exists(self, lfns):
    """ Check if the path exists
    """
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    failed = {}
    successful = {}
    for lfn, guid in lfns.items():
      res = self.__existsLfn(lfn)
      if not res['OK']:
        failed[lfn] = res['Message']
      elif res['Value']:
        successful[lfn] = lfn
      elif not isinstance(guid, six.string_types):
        successful[lfn] = False
      else:
        res = existsGuid(guid)
        if not res['OK']:
          failed[lfn] = res['Message']
        elif not res['Value']:
          successful[lfn] = False
        else:
          successful[lfn] = self.__getLfnForGUID(guid)['Value']
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  def __getPathAccess(self, path):
    """ Determine the permissions using the lfc function lfc_access """
    permDict = {'Read': 1,
                'Write': 2,
                'Execute': 4}
    resDict = {}
    for p in permDict:

      code = permDict[p]
      value = lfc.lfc_access(self.__fullLfn(path), code)
      if value == 0:
        resDict[p] = True
      else:
        resDict[p] = False
    return S_OK(resDict)

  @checkCatalogArguments
  def hasAccess(self, lfns, opType):

    if opType in LcgFileCatalogClient.READ_METHODS:
      opType = 'Read'
    elif opType in LcgFileCatalogClient.WRITE_METHODS:
      opType = 'Write'

    res = self.getPathPermissions(lfns)

    if not res['OK']:
      return res

    perms = res['Value']
    failed = perms['Failed']
    successful = dict((path, perms['Successful'][path].get(opType, False)) for path in perms['Successful'])

    return S_OK({'Successful': successful, 'Failed': failed})

  @checkCatalogArguments
  def getPathPermissions(self, lfns):
    """ Determine the VOMs based ACL information for a supplied path
    """
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    failed = {}
    successful = {}
    for path in lfns:
      res = self.__getBasePath(path)
      if not res['OK']:
        failed[path] = res['Message']
      else:
        basePath = res['Value']
        res = self.__getPathAccess(basePath)
        if not res['OK']:
          failed[path] = res['Message']
        else:
          lfcPerm = res['Value']
          res = self.__getACLInformation(basePath)
          if not res['OK']:
            failed[path] = res['Message']
          else:
            # Evaluate access rights
            val = res['Value']
            try:
              lfcPerm['user'] = val['user']
              lfcPerm['group'] = val['group']
              lfcPerm['world'] = val['world']
              lfcPerm['DN'] = val['DN']
              lfcPerm['Role'] = val['Role']
            except KeyError:
              print('key not found: __getACLInformation returned incomplete dictionary', KeyError)
              failed[path] = lfcPerm
              continue
          # ACLs are just an additional information, therefore here it is successful
          successful[path] = lfcPerm

    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  ####################################################################
  #
  # The following are read methods for files
  #

  @checkCatalogArguments
  def isFile(self, lfns):

    # If we have less than three lfns to query a session doesn't make sense
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    failed = {}
    successful = {}
    for lfn in lfns:
      res = self.__getPathStat(lfn)
      if not res['OK']:
        failed[lfn] = res['Message']
      elif S_ISREG(res['Value'].filemode):
        successful[lfn] = True
      else:
        successful[lfn] = False
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def getFileMetadata(self, lfns, ownership=False):
    """ Returns the file metadata associated to a supplied LFN
    """
    # If we have less than three lfns to query a session doesn't make sense
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    failed = {}
    successful = {}
    for lfn in lfns:
      res = self.__getPathStat(lfn)
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        fstat = res['Value']
        successful[lfn] = {}
        successful[lfn]['Size'] = fstat.filesize
        successful[lfn]['ChecksumType'] = fstat.csumtype
        successful[lfn]['Checksum'] = fstat.csumvalue
        successful[lfn]['GUID'] = fstat.guid
        successful[lfn]['Status'] = fstat.status
        successful[lfn]['CreationDate'] = fromEpoch(fstat.ctime)
        successful[lfn]['ModificationDate'] = fromEpoch(fstat.mtime)
        successful[lfn]['NumberOfLinks'] = fstat.nlink
        successful[lfn]['Mode'] = S_IMODE(fstat.filemode)
        if ownership:
          res = getDNFromUID(fstat.uid)
          if res['OK']:
            successful[lfn]['OwnerDN'] = res['Value']
          else:
            successful[lfn]['OwnerDN'] = None
          res = getRoleFromGID(fstat.gid, path=lfn)
          if res['OK']:
            successful[lfn]['OwnerRole'] = res['Value']
          else:
            successful[lfn]['OwnerRole'] = None
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def getFileSize(self, lfns):
    """ Get the size of a supplied file
    """
    # If we have less than three lfns to query a session doesn't make sense
    created = False
    if len(lfns) > 2:
      created = self.__openSession()
      if created < 0:
        return S_ERROR("Error opening LFC session")
    failed = {}
    successful = {}
    for lfn in lfns:
      res = self.__getPathStat(lfn)
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        successful[lfn] = res['Value'].filesize
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def getReplicas(self, lfns, allStatus=False):
    """ Returns replicas for an LFN or list of LFNs
    """
    lfnChunks = breakListIntoChunks(lfns, 1000)
    # If we have less than three groups to query a session doesn't make sense
    created = False
    if len(lfnChunks) > 2:
      created = self.__openSession()
      if created < 0:
        return S_ERROR("Error opening LFC session")
    failed = {}
    successful = {}
    for lfnList in lfnChunks:
      fullLfnList = [self.__fullLfn(lfn) for lfn in lfnList if lfn]
      value, replicaList = lfc.lfc_getreplicasl(fullLfnList, '')
      if value != 0:
        for lfn in lfnList:
          reason = lfc.sstrerror(lfc.cvar.serrno)
          if 'Could not secure the connection' in reason:
            # This is a fatal error
            return S_ERROR('Could not secure the connection')
          elif 'Bad credentials' in reason:
            return S_ERROR("Bad Credentials")
        continue
      guid = ''
      it = iter(lfnList)
      replicas = {}
      # This is useless but makes pylinit happy as lfn is defined in the loop when the guid changes
      lfn = None
      for oReplica in replicaList:
        if oReplica.errcode != 0:
          if (oReplica.guid == '') or (oReplica.guid != guid):
            setLfnReplicas(lfn, replicas, successful, failed)
            lfn = it.next()
            failed[lfn] = lfc.sstrerror(oReplica.errcode)
            guid = oReplica.guid
        elif oReplica.sfn == '':
          setLfnReplicas(lfn, replicas, successful, failed)
          lfn = it.next()
          failed[lfn] = 'File has zero replicas'
          guid = oReplica.guid
        else:
          # This is where we change lfn for good!
          if oReplica.guid != guid:
            setLfnReplicas(lfn, replicas, successful, failed)
            lfn = it.next()
            guid = oReplica.guid
          if (oReplica.status != 'P') or allStatus:
            se = oReplica.host
            pfn = oReplica.sfn  # .strip()
            replicas[se] = pfn
      # This is for the last file in the list
      setLfnReplicas(lfn, replicas, successful, failed)
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def getReplicaStatus(self, lfns):
    # If we have less than three lfns to query a session doesn't make sense
    created = False
    if len(lfns) > 2:
      created = self.__openSession()
      if created < 0:
        return S_ERROR("Error opening LFC session")
    failed = {}
    successful = {}
    for lfn, se in lfns.items():
      res = self.__getFileReplicaStatus(lfn, se)
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        successful[lfn] = res['Value']
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def getLFNForPFN(self, pfns):
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    failed = {}
    successful = {}
    for pfn in pfns:
      res = self.__getLFNForPFN(pfn)
      if not res['OK']:
        failed[pfn] = res['Message']
      else:
        successful[pfn] = res['Value']
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  ####################################################################
  #
  # The following a read methods for directories
  #

  @checkCatalogArguments
  def isDirectory(self, lfns):
    """ Determine whether the path is a directory
    """
    # If we have less than three lfns to query a session doesn't make sense
    created = False
    if len(lfns) > 2:
      created = self.__openSession()
      if created < 0:
        return S_ERROR("Error opening LFC session")
    failed = {}
    successful = {}
    for lfn in lfns:
      res = self.__getPathStat(lfn)
      if not res['OK']:
        failed[lfn] = res['Message']
      elif S_ISDIR(res['Value'].filemode):
        successful[lfn] = True
      else:
        successful[lfn] = False
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def getDirectoryMetadata(self, lfns):
    # If we have less than three lfns to query a session doesn't make sense
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    failed = {}
    successful = {}
    for lfn in lfns:
      res = self.__getPathStat(lfn)
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        fstat = res['Value']
        successful[lfn] = {}
        successful[lfn]['Size'] = fstat.filesize
        successful[lfn]['ChecksumType'] = fstat.csumtype
        successful[lfn]['Checksum'] = fstat.csumvalue
        successful[lfn]['GUID'] = fstat.guid
        successful[lfn]['Status'] = fstat.status
        successful[lfn]['CreationDate'] = fromEpoch(fstat.ctime)
        successful[lfn]['ModificationDate'] = fromEpoch(fstat.mtime)
        successful[lfn]['NumberOfSubPaths'] = fstat.nlink
        res = getDNFromUID(fstat.uid)
        if res['OK']:
          successful[lfn]['OwnerDN'] = res['Value']
        else:
          successful[lfn]['OwnerDN'] = None
        res = getRoleFromGID(fstat.gid, path=lfn)
        if res['OK']:
          successful[lfn]['OwnerRole'] = res['Value']
        else:
          successful[lfn]['OwnerRole'] = None
        successful[lfn]['Mode'] = S_IMODE(fstat.filemode)
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def getDirectoryReplicas(self, lfns, allStatus=False):
    """ This method gets all of the pfns in the directory
    """
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    failed = {}
    successful = {}
    for path in lfns:
      res = self.__getDirectoryContents(path)
      if not res['OK']:
        failed[path] = res['Message']
      else:
        pathReplicas = {}
        files = res['Value']['Files']
        for lfn, fileDict in files.items():
          pathReplicas[lfn] = {}
          for se, seDict in fileDict['Replicas'].items():
            pfn = seDict['PFN']
            status = seDict['Status']
            if (status != 'P') or allStatus:
              pathReplicas[lfn][se] = pfn
        successful[path] = pathReplicas
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def listDirectory(self, lfns, verbose=False):
    """ Returns the result of __getDirectoryContents for multiple supplied paths
    """
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    failed = {}
    successful = {}
    for path in lfns:
      res = self.__getDirectoryContents(path, verbose)
      if res['OK']:
        successful[path] = res['Value']
      else:
        failed[path] = res['Message']
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def getDirectorySize(self, lfns, longOutput=False, rawFiles=False):
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    failed = {}
    successful = {}
    for path in lfns.keys():
      res = self.__getDirectorySize(path, longOutput=longOutput)
      if res['OK']:
        successful[path] = res['Value']
      else:
        failed[path] = res['Message']
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  ####################################################################
  #
  # The following are read methods for links
  #

  @checkCatalogArguments
  def isLink(self, links):
    # If we have less than three lfns to query a session doesn't make sense
    failed = {}
    successful = {}
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    for link in links:
      res = self.__getLinkStat(link)
      if not res['OK']:
        failed[link] = res['Message']
      elif S_ISLNK(res['Value'].filemode):
        successful[link] = True
      else:
        successful[link] = False
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def readLink(self, links):
    # If we have less than three lfns to query a session doesn't make sense
    created = False
    if len(links) > 2:
      created = self.__openSession()
      if created < 0:
        return S_ERROR("Error opening LFC session")
    failed = {}
    successful = {}
    for link in links:
      res = self.__readLink(link)
      if res['OK']:
        successful[link] = res['Value']
      else:
        failed[link] = res['Message']
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  ####################################################################
  #
  # The following are read methods for datasets
  #

  @checkCatalogArguments
  def resolveDataset(self, datasets, allStatus=False):
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    successful = {}
    failed = {}
    for datasetName in datasets:
      res = self.__getDirectoryContents(datasetName)
      if not res['OK']:
        failed[datasetName] = res['Message']
      else:
        # linkDict = res['Value']['Links']
        linkDict = res['Value']['Files']
        datasetFiles = {}
        for link, fileMetadata in linkDict.items():
          # target = fileMetadata[link]['MetaData']['Target']
          target = link
          datasetFiles[target] = fileMetadata['Replicas']
        successful[datasetName] = datasetFiles
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  ####################################################################
  #
  # The following a write methods for files
  #

  @checkCatalogArguments
  def addFile(self, lfns):
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    failed = {}
    successful = {}
    baseDirs = []
    for lfn in lfns:
      baseDir = os.path.dirname(lfn)
      if baseDir in baseDirs:
        continue
      baseDirs.append(baseDir)
      res = self.__executeOperation(baseDir, 'exists')
      # If we failed to find out whether the directory exists
      if not res['OK']:
        continue
      # If the directory exists
      if res['Value']:
        continue
      # Make the directories recursively if needed
      res = self.__makeDirs(baseDir)
      # If we failed to make the directory for the file
      if not res['OK']:
        continue
    lfc.lfc_umask(0000)
    for lfnList in breakListIntoChunks(lfns, 1000):
      fileChunk = []
      for lfn in list(lfnList):
        lfnInfo = lfns[lfn]
        pfn = lfnInfo['PFN']
        size = lfnInfo['Size']
        se = lfnInfo['SE']
        guid = lfnInfo['GUID']
        checksum = lfnInfo['Checksum']
        res = self.__checkAddFile(lfn, pfn, size, se, guid, checksum)
        if not res['OK']:
          # Error
          failed[lfn] = res['Message']
          lfnList.remove(lfn)
        elif not res['Value']:
          # File already exists adn is consistent
          successful[lfn] = True
          lfnList.remove(lfn)
        else:
          # File doesn't exist, create it
          oFile = lfc.lfc_filereg()
          oFile.lfn = self.__fullLfn(lfn)
          oFile.sfn = pfn
          oFile.size = size
          oFile.mode = 0o664
          oFile.server = se
          oFile.guid = guid
          oFile.csumtype = 'AD'
          oFile.status = 'U'
          oFile.csumvalue = lfnInfo['Checksum']
          fileChunk.append(oFile)
      if not lfnList:
        continue
      error, errCodes = lfc.lfc_registerfiles(fileChunk)
      if error or (len(errCodes) != len(lfnList)):
        for lfn in lfnList:
          failed[lfn] = lfc.sstrerror(lfc.cvar.serrno)
        continue
      for index in range(len(errCodes)):
        lfn = lfnList[index]
        errCode = errCodes[index]
        if errCode == 0:
          successful[lfn] = True
        elif errCode == 17:
          failed[lfn] = "The supplied GUID is already used"
          res = self.__getLfnForGUID(guid)
          if res['OK']:
            failed[lfn] = "The supplied GUID is already used by %s" % res['Value']
        else:
          failed[lfn] = lfc.sstrerror(errCode)
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def addReplica(self, lfns):
    """ This adds a replica to the catalogue.
    """
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    failed = {}
    successful = {}
    for lfn, info in lfns.items():
      pfn = info['PFN']
      se = info['SE']
      if 'Master' not in info:
        master = False
      else:
        master = info['Master']
      res = self.__getLFNGuid(lfn)
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        guid = res['Value']
        res = addReplica(guid, pfn, se, master)
        if res['OK']:
          successful[lfn] = True
        else:
          failed[lfn] = res['Message']
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def removeFile(self, lfns):
    """ Remove the supplied path
    """
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    res = self.exists(lfns)
    if not res['OK']:
      if created:
        self.__closeSession()
      return res
    failed = res['Value']['Failed']
    successful = {}
    for lfn, exists in res['Value']['Successful'].items():
      if not exists:
        successful[lfn] = True
      else:
        res = self.__unlinkPath(lfn)
        if res['OK']:
          successful[lfn] = True
        else:
          failed[lfn] = res['Message']
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def removeReplica(self, lfns):
    created = False
    if len(lfns) > 2:
      created = self.__openSession()
      if created < 0:
        return S_ERROR("Error opening LFC session")
    res = self.getReplicas(lfns)  # We need the PFNs of the input lfn (list)
    if not res['OK']:
      return res
    for lfn, lfnrep in res['Value']['Successful'].items():
      if "PFN" not in lfns[lfn]:  # Update only if the PFN was not supplied
        lfns[lfn]["PFN"] = lfnrep[lfns[lfn]['SE']]
    failed = {}
    for lfn, message in res['Value']['Failed'].items():
      if "PFN" not in lfns[lfn]:  # Change only if PFN is not there
        failed[lfn] = message  # The replicas are not available, mark the lfn as failed
        lfns.pop(lfn)  # and remove them
    successful = {}
    for lfn, info in lfns.items():
      if ('PFN' not in info) or ('SE' not in info):
        failed[lfn] = "Required parameters not supplied"
      else:
        pfn = info['PFN']
        se = info['SE']
        res = removeReplica(pfn)
        if res['OK']:
          successful[lfn] = True
        else:
          if res['Message'] == 'No such file or directory':
            # The PFN didn't exist, but maybe it wsa changed...
            res1 = self.getReplicas(lfn)
            if res1['OK']:
              pfn1 = res1['Value']['Successful'].get(lfn, {}).get(se)
              if pfn1 and pfn1 != pfn:
                res = removeReplica(pfn1)
          if res['OK']:
            successful[lfn] = True
          else:
            failed[lfn] = res['Message']
    lfnRemoved = list(successful)
    if len(lfnRemoved) > 0:
      res = self.getReplicas(lfnRemoved, True)
      zeroReplicaFiles = []
      if not res['OK']:
        if created:
          self.__closeSession()
        return res
      else:
        for lfn, repDict in res['Value']['Successful'].items():
          if len(repDict) == 0:
            zeroReplicaFiles.append(lfn)
      if len(zeroReplicaFiles) > 0:
        res = self.removeFile(zeroReplicaFiles)
        if not res['OK']:
          if created:
            self.__closeSession()
          return res
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def setReplicaStatus(self, lfns):
    created = False
    if len(lfns) > 2:
      created = self.__openSession()
      if created < 0:
        return S_ERROR("Error opening LFC session")
    failed = {}
    successful = {}
    for lfn, info in lfns.items():
      pfn = info['PFN']
      status = info['Status']
      res = setReplicaStatus(pfn, status[0])
      if res['OK']:
        successful[lfn] = True
      else:
        failed[lfn] = res['Message']
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def setReplicaProblematic(self, lfns, revert=False):
    """
      Set replicas to problematic.

      :param lfn lfns: has to be formated this way :
                  { lfn : { se1 : pfn1, se2 : pfn2, ...}, ...}
      :param revert: If True, remove the problematic flag

      :return: { Successful : { lfn : [ ses ] }, Failed : { lfn : { se : msg } } }
    """

    # This method does a batch treatment because the setReplicaStatus can only take one replica per lfn at once
    #
    # Illustration :
    #
    # lfns {'L2': {'S1': 'P3'}, 'L3': {'S3': 'P5', 'S2': 'P4', 'S4': 'P6'}, 'L1': {'S2': 'P2', 'S1': 'P1'}}
    #
    # loop1: lfnSEs {'L2': ['S1'], 'L3': ['S3', 'S2', 'S4'], 'L1': ['S2', 'S1']}
    # loop1 : batch {'L2': {'Status': 'P', 'SE': 'S1', 'PFN': 'P3'}, 'L3': {'Status': 'P', 'SE': 'S4', 'PFN': 'P6'},
    #                'L1': {'Status': 'P', 'SE': 'S1', 'PFN': 'P1'}}
    #
    # loop2: lfnSEs {'L2': [], 'L3': ['S3', 'S2'], 'L1': ['S2']}
    # loop2 : batch {'L3': {'Status': 'P', 'SE': 'S2', 'PFN': 'P4'}, 'L1': {'Status': 'P', 'SE': 'S2', 'PFN': 'P2'}}
    #
    # loop3: lfnSEs {'L3': ['S3'], 'L1': []}
    # loop3 : batch {'L3': {'Status': 'P', 'SE': 'S3', 'PFN': 'P5'}}
    #
    # loop4: lfnSEs {'L3': []}
    # loop4 : batch {}

    successful = {}
    failed = {}

    status = '-' if revert else 'P'

    # { lfn : [ se1, se2, ...], ...}
    lfnsSEs = dict((lfn, [se for se in lfns[lfn]]) for lfn in lfns)

    while lfnsSEs:

      # { lfn : { 'SE' : se1, 'PFN' : pfn1, 'Status' : status }, ... }
      batch = {}

      for lfn in list(lfnsSEs):
        # If there are still some Replicas (SE) for the given LFN, we put it in the next batch
        # else we remove the entry from the lfnsSEs dict
        if lfnsSEs[lfn]:
          se = lfnsSEs[lfn].pop()
          batch[lfn] = {'SE': se, 'PFN': lfns[lfn][se], 'Status': status}
        else:
          del lfnsSEs[lfn]

      # Happens when there is nothing to treat anymore
      if not batch:
        break

      res = self.setReplicaStatus(batch)
      if not res['OK']:
        for lfn in batch:
          failed.setdefault(lfn, {})[batch[lfn]['SE']] = res['Message']
        continue

      for lfn in res['Value']['Failed']:
        failed.setdefault(lfn, {})[batch[lfn]['SE']] = res['Value']['Failed'][lfn]

      for lfn in res['Value']['Successful']:
        successful.setdefault(lfn, []).append(batch[lfn]['SE'])

    return S_OK({'Successful': successful, 'Failed': failed})

  @checkCatalogArguments
  def setReplicaHost(self, lfns):
    """ This modifies the replica metadata for the SE.
    """
    created = False
    if len(lfns) > 2:
      created = self.__openSession()
      if created < 0:
        return S_ERROR("Error opening LFC session")
    failed = {}
    successful = {}
    for lfn, info in lfns.items():
      pfn = info['PFN']
      newse = info['NewSE']
      res = modReplica(pfn, newse)
      if res['OK']:
        successful[lfn] = True
      else:
        failed[lfn] = res['Message']
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  ####################################################################
  #
  # The following are write methods for directories
  #

  @checkCatalogArguments
  def removeDirectory(self, lfns, recursive=False):
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    res = self.exists(lfns)
    if not res['OK']:
      if created:
        self.__closeSession()
      return res
    failed = res['Value']['Failed']
    successful = {}
    for lfn, exists in res['Value']['Successful'].items():
      if not exists:
        successful[lfn] = True
        continue
      if recursive:
        res = self.__removeDirs(lfn)
      else:
        res = self.__removeDirectory(lfn)
      if res['OK']:
        successful[lfn] = True
      else:
        failed[lfn] = res['Message']
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def createDirectory(self, lfns):
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    failed = {}
    successful = {}
    for path in lfns:
      res = self.__makeDirs(path)
      if res['OK']:
        successful[path] = True
      else:
        failed[path] = res['Message']
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  ####################################################################
  #
  # The following are write methods for links
  #

  @checkCatalogArguments
  def createLink(self, links):
    # If we have less than three lfns to query a session doesn't make sense
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    failed = {}
    successful = {}
    for link, target in links.items():
      res = self.__makeDirs(os.path.dirname(link))
      if not res['OK']:
        failed[link] = res['Message']
      else:
        res = self.__makeLink(link, target)
        if not res['OK']:
          failed[link] = res['Message']
        else:
          successful[link] = target
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def removeLink(self, links):
    # If we have less than three lfns to query a session doesn't make sense
    created = False
    if len(links) > 2:
      created = self.__openSession()
      if created < 0:
        return S_ERROR("Error opening LFC session")
    failed = {}
    successful = {}
    for link in links:
      res = self.__unlinkPath(link)
      if not res['OK']:
        failed[link] = res['Message']
      else:
        successful[link] = True
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  ####################################################################
  #
  # The following are write methods for datasets
  #

  @checkCatalogArguments
  def createDataset(self, datasets):
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    successful = {}
    failed = {}
    for datasetName, lfns in datasets.items():
      res = self.__executeOperation(datasetName, 'exists')
      if not res['OK']:
        return res
      elif res['Value']:
        return S_ERROR("LcgFileCatalogClient.createDataset: This dataset already exists.")
      res = self.__createDataset(datasetName, lfns)
      if res['OK']:
        successful[datasetName] = True
      else:
        self.__executeOperation(datasetName, 'removeDataset')
        failed[datasetName] = res['Message']
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def removeDataset(self, datasets):
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    successful = {}
    failed = {}
    for datasetName in datasets:
      res = self.__removeDataset(datasetName)
      if not res['OK']:
        failed[datasetName] = res['Message']
      else:
        successful[datasetName] = True
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def removeFileFromDataset(self, datasets):
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    successful = {}
    failed = {}
    for datasetName, lfns in datasets.items():
      res = self.__removeFilesFromDataset(datasetName, lfns)
      if not res['OK']:
        failed[datasetName] = res['Message']
      else:
        successful[datasetName] = True
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  ####################################################################
  #
  # These are the internal methods to be used by all methods
  #

  def __executeOperation(self, path, method):
    """ Executes the requested functionality with the supplied path
    """
    fcn = None
    if hasattr(self, method) and callable(getattr(self, method)):
      fcn = getattr(self, method)
    if not fcn:
      return S_ERROR("Unable to invoke %s, it isn't a member function of LcgFileCatalogClient" % method)
    res = fcn(path)
    if isinstance(path, dict):
      path = list(path)[0]
    if not res['OK']:
      return res
    elif path not in res['Value']['Successful']:
      return S_ERROR(res['Value']['Failed'][path])
    return S_OK(res['Value']['Successful'][path])

  def __getLFNForPFN(self, pfn):
    fstat = lfc.lfc_filestatg()
    error = lfc.lfc_statr(pfn, fstat)
    return returnCode(error, self.__getLfnForGUID(fstat.guid) if not error else None)

  def __existsLfn(self, lfn):
    """ Check whether the supplied LFN exists
    """
    error = lfc.lfc_access(self.__fullLfn(lfn), 0)
    return returnCode(error and lfc.cvar.serrno != 2, error == 0)

  @checkCatalogArguments
  def getLFNForGUID(self, guids):
    guidLFN = {}
    failed = {}

    for guid in guids:
      # I somehow have the feeling that lfnlist[0] of __getLfnForGUID
      # could throw an exception if the guid does not exist in the DB
      # but... not touching this black magic
      try:
        guidLFN[guid] = self.__getLfnForGUID(guid)['Value']
      except Exception as _e:
        failed[guid] = "GUID does not exist"
    return S_OK({"Successful": guidLFN, "Failed": failed})

  def __getLfnForGUID(self, guid):
    """ Resolve the LFN for a supplied GUID
    """
    if not guid:
      return S_OK()
    linkList = lfc.lfc_list()
    lfnlist = []
    listlinks = lfc.lfc_listlinks('', guid, lfc.CNS_LIST_BEGIN, linkList)
    while listlinks:
      ll = listlinks.path
      if re.search('^' + self.prefix, ll):
        ll = listlinks.path.replace(self.prefix, "", 1)
      lfnlist.append(ll)
      listlinks = lfc.lfc_listlinks('', guid, lfc.CNS_LIST_CONTINUE, linkList)
    else:
      lfc.lfc_listlinks('', guid, lfc.CNS_LIST_END, linkList)
    return S_OK(lfnlist[0])

  def __getBasePath(self, path):
    exists = False
    while not exists:
      res = self.__executeOperation(path, 'exists')
      if not res['OK']:
        return res
      else:
        exists = res['Value']
        if not exists:
          path = os.path.dirname(path)
    return S_OK(path)

  def __getACLInformation(self, path):
    results, objects = lfc.lfc_getacl(self.__fullLfn(path), 256)  # lfc.CNS_ACL_GROUP_OBJ)
    if results == -1:
      errStr = "LcgFileCatalogClient.__getACLInformation: Failed to obtain all path ACLs."
      gLogger.error(errStr, "%s %s" % (path, lfc.sstrerror(lfc.cvar.serrno)))
      return S_ERROR(errStr)
    permissionsDict = {}
    for obj in objects:
      if obj.a_type == lfc.CNS_ACL_USER_OBJ:
        res = getDNFromUID(obj.a_id)
        if not res['OK']:
          return res
        permissionsDict['DN'] = res['Value']
        permissionsDict['user'] = obj.a_perm
      elif obj.a_type == lfc.CNS_ACL_GROUP_OBJ:
        res = getRoleFromGID(obj.a_id, path=path)
        if not res['OK']:
          return res
        role = res['Value']
        permissionsDict['Role'] = role
        permissionsDict['group'] = obj.a_perm
      elif obj.a_type == lfc.CNS_ACL_OTHER:
        permissionsDict['world'] = obj.a_perm
      else:
        errStr = "LcgFileCatalogClient.__getACLInformation: ACL type not considered."
        gLogger.debug(errStr, obj.a_type)
    gLogger.verbose("LcgFileCatalogClient.__getACLInformation: %s owned by %s:%s." % (path,
                                                                                      permissionsDict['DN'],
                                                                                      permissionsDict['Role']))
    return S_OK(permissionsDict)

  def __getPathStat(self, path='', guid=''):
    if path:
      path = self.__fullLfn(path)
    fstat = lfc.lfc_filestatg()
    error = lfc.lfc_statg(path, guid, fstat)
    return returnCode(error, fstat)

  def __getFileReplicas(self, lfn, allStatus):
    error, replicaObjects = lfc.lfc_getreplica(self.__fullLfn(lfn), '', '')
    return returnCode(error or not replicaObjects,
                      dict([(replica.host,
                             replica.sfn) for replica in replicaObjects if allStatus or replica.status != 'P'])
                      if not error else None,
                      errMsg='File has zero replicas' if not error else '')

  def __getFileReplicaStatus(self, lfn, se):
    error, replicaObjects = lfc.lfc_getreplica(self.__fullLfn(lfn), '', '')
    status = None
    for replica in replicaObjects if not error else []:
      if se == replica.host:
        status = replica.status
        break
    return returnCode(status is None, status, errMsg="No replica at supplied site" if not error else '')

  def __checkAddFile(self, lfn, pfn, size, se, guid, checksum):
    res = self.__getPathStat(lfn)
    if not res['OK']:
      if res['Message'] != 'No such file or directory':
        return S_ERROR("Failed to find pre-existance of LFN")
    else:
      # File exists, check if consistent with supplied parameters
      fstat = res['Value']
      errStr = ''
      if fstat.guid != guid:
        errStr = "This LFN %s is already registered with another GUID" % lfn
      elif fstat.filesize != size:
        errStr = "This LFN %s is already registered with another size" % lfn
      elif fstat.csumvalue.upper() != checksum.upper():
        errStr = "This LFN %s is already registered with another adler32" % lfn
      if errStr:
        return S_ERROR(errStr)
      res = self.__getFileReplicas(lfn, True)
      if not res['OK']:
        return S_ERROR("Failed to obtain replicas for existing LFN %s" % lfn)
      replicas = res['Value']
      if replicas.get(se) != pfn:
        return S_ERROR("This LFN %s is already registered with another SE/PFN" % lfn)
      return S_OK(False)
    # We reach here only if the file doesn't exist, which is what we look for!!
    # Now we check the arguments
    try:
      errStr = ''
      size = long(size)
    except Exception:
      errStr = "The size of the file must be an 'int','long' or 'string'"
    if not se:
      errStr = "The SE for the file was not supplied."
    elif not pfn:
      errStr = "The PFN for the file was not supplied."
    elif not lfn:
      errStr = "The LFN for the file was not supplied."
    elif not guid:
      errStr = "The GUID for the file was not supplied."
    elif not checksum:
      errStr = "The adler32 for the file was not supplied."
    if errStr:
      return S_ERROR(errStr)
    return S_OK(True)

  def __unlinkPath(self, lfn):
    return returnCode(lfc.lfc_unlink(self.__fullLfn(lfn)))

  def __removeDirectory(self, path):
    return returnCode(lfc.lfc_rmdir(self.__fullLfn(path)))

  def __removeDirs(self, path):
    """ Black magic contained within...
    """
    res = self.__getDirectoryContents(path)
    if not res['OK']:
      return res
    subDirs = res['Value']['SubDirs']
    files = res['Value']['Files']
    for subDir in subDirs:
      res = self.__removeDirs(subDir)
      if not res['OK']:
        return res
    if files:
      return S_ERROR("Directory not empty")
    return self.__removeDirectory(path)

  def __makeDirs(self, path, mode=0o775):
    """  Black magic contained within....
    """
    dirName = os.path.dirname(path)
    res = self.__executeOperation(path, 'exists')
    if not res['OK']:
      return res
    if res['Value']:
      return S_OK()
    res = self.__executeOperation(dirName, 'exists')
    if not res['OK']:
      return res
    if res['Value']:
      res = self.__makeDirectory(path, mode)
    else:
      res = self.__makeDirs(dirName, mode)
      res = self.__makeDirectory(path, mode)
    return res

  def __makeDirectory(self, path, mode):
    lfc.lfc_umask(0000)
    return returnCode(lfc.lfc_mkdir(self.__fullLfn(path), mode))

  def __openDirectory(self, path):
    value = lfc.lfc_opendirg(self.__fullLfn(path), '')
    return returnCode(not value, value)

  def __getDirectoryContents(self, path, verbose=False):
    """ Returns a dictionary containing all of the contents of a directory.
        This includes the metadata associated to files (replicas, size, guid, status) and the subdirectories found.
    """
    # First check that the directory exists
    res = self.__executeOperation(path, 'exists')
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR('No such file or directory')

    res = self.__getPathStat(path)
    if not res['OK']:
      return res
    nbfiles = res['Value'].nlink
    res = self.__openDirectory(path)
    if not res['OK']:
      return res
    oDirectory = res['Value']
    subDirs = {}
    links = {}
    files = {}
    loop = list(range(nbfiles + 1))
    while loop.pop():
      result = lfc.lfc_readdirxr(oDirectory, "")
      if not result:
        # In some rare cases we reach the end of oDirectory, before nbfiles iterations (!!!)
        break
      entry, fileInfo = result
      pathMetadata = {}
      pathMetadata['Mode'] = S_IMODE(entry.filemode)
      subPath = '%s/%s' % (path, entry.d_name)
      if verbose:
        statRes = self.__getPathStat(subPath)
        if statRes['OK']:
          oPath = statRes['Value']
          pathMetadata['Size'] = oPath.filesize
          pathMetadata['ChecksumType'] = oPath.csumtype
          pathMetadata['Checksum'] = oPath.csumvalue
          pathMetadata['GUID'] = oPath.guid
          pathMetadata['Status'] = oPath.status
          pathMetadata['CreationDate'] = fromEpoch(oPath.ctime)
          pathMetadata['ModificationDate'] = fromEpoch(oPath.mtime)
          pathMetadata['NumberOfLinks'] = oPath.nlink
          pathMetadata['LastAccess'] = oPath.atime
          res = getDNFromUID(oPath.uid)
          if res['OK']:
            pathMetadata['OwnerDN'] = res['Value']
          else:
            pathMetadata['OwnerDN'] = None
          res = getRoleFromGID(oPath.gid, path=subPath)
          if res['OK']:
            pathMetadata['OwnerRole'] = res['Value']
          else:
            pathMetadata['OwnerRole'] = None
      if S_ISDIR(entry.filemode):
        subDirs[subPath] = pathMetadata
      else:
        replicaDict = {}
        if fileInfo:
          for replica in fileInfo:
            replicaDict[replica.host] = {'PFN': replica.sfn, 'Status': replica.status}
        pathMetadata['Size'] = entry.filesize
        pathMetadata['GUID'] = entry.guid
        if S_ISLNK(entry.filemode):
          res = self.__executeOperation(subPath, 'readLink')
          if res['OK']:
            pathMetadata['Target'] = res['Value']
          links[subPath] = {}
          links[subPath]['MetaData'] = pathMetadata
          links[subPath]['Replicas'] = replicaDict
        elif S_ISREG(entry.filemode):
          files[subPath] = {}
          files[subPath]['Replicas'] = replicaDict
          files[subPath]['MetaData'] = pathMetadata
    pathDict = {}
    res = closeDirectory(oDirectory)
    pathDict = {'Files': files, 'SubDirs': subDirs, 'Links': links}
    return S_OK(pathDict)

  def __getDirectorySize(self, path, longOutput=False):
    res = self.__executeOperation(path, 'exists')
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR('No such file or directory')

    res = self.__getPathStat(path)
    if not res['OK']:
      return res
    nbfiles = res['Value'].nlink
    res = self.__openDirectory(path)
    if not res['OK']:
      return res
    oDirectory = res['Value']
    pathDict = {'SubDirs': {}, 'ClosedDirs': [], 'Files': 0, 'TotalSize': 0, 'SiteUsage': {}}
    loop = list(range(nbfiles + 1))
    while loop.pop():
      entry, fileInfo = lfc.lfc_readdirxr(oDirectory, "")
      if S_ISDIR(entry.filemode):
        subDir = '%s/%s' % (path, entry.d_name)
        permissions = S_IMODE(entry.filemode)
        if (not permissions & S_IWUSR) and (not permissions & S_IWGRP) and (not permissions & S_IWOTH):
          pathDict['ClosedDirs'].append(subDir)
        modTime = time.ctime()
        statRes = self.__getPathStat(subDir)
        if statRes['OK']:
          modTime = fromEpoch(statRes['Value'].mtime)
        pathDict['SubDirs'][subDir] = modTime
      else:
        fileSize = entry.filesize
        pathDict['TotalSize'] += fileSize
        pathDict['Files'] += 1
        if not fileInfo:
          gLogger.error("LcgFileCatalogClient.__getDirectorySize: File found with no replicas",
                        "%s/%s" % (path, entry.d_name))
        else:
          for replica in fileInfo:
            if replica.host not in pathDict['SiteUsage']:
              pathDict['SiteUsage'][replica.host] = {'Files': 0, 'Size': 0}
            pathDict['SiteUsage'][replica.host]['Size'] += fileSize
            pathDict['SiteUsage'][replica.host]['Files'] += 1
    res = closeDirectory(oDirectory)
    return S_OK(pathDict)

  def __getLinkStat(self, link):
    lstat = lfc.lfc_filestat()
    return returnCode(lfc.lfc_lstat(self.__fullLfn(link), lstat), lstat)

  def __readLink(self, link):
    buff = " " * (lfc.CA_MAXPATHLEN + 1)
    chars = lfc.lfc_readlink(self.__fullLfn(link), buff, lfc.CA_MAXPATHLEN)
    if chars > 0:
      error = 0
      chars = buff[:chars].replace(self.prefix, '', 1).replace('\x00', '')
    else:
      error = 1
    return returnCode(error, chars)

  def __makeLink(self, source, target):
    return returnCode(lfc.lfc_symlink(self.__fullLfn(target), self.__fullLfn(source)))

  def __getLFNGuid(self, lfn):
    """Get the GUID for the given lfn"""
    fstat = lfc.lfc_filestatg()
    return returnCode(lfc.lfc_statg(self.__fullLfn(lfn), '', fstat), fstat.guid)

  def __createDataset(self, datasetName, lfns):
    res = self.__makeDirs(datasetName)
    if not res['OK']:
      return res
    links = {}
    for lfn in lfns:
      res = self.__getLFNGuid(lfn)
      if not res['OK']:
        return res
      else:
        link = "%s/%s" % (datasetName, res['Value'])
        links[link] = lfn
    res = self.createLink(links)
    if len(res['Value']['Successful']) == len(links):
      return S_OK()
    totalError = ""
    for link, error in res['Value']['Failed'].items():
      gLogger.error("LcgFileCatalogClient.__createDataset: Failed to create link",
                    "for %s: %s" % (link, error))
      totalError = "%s\n %s : %s" % (totalError, link, error)
    return S_ERROR(totalError)

  def __removeDataset(self, datasetName):
    res = self.__getDirectoryContents(datasetName)
    if not res['OK']:
      return res
    links = list(res['Value']['Files'])
    res = self.removeLink(links)
    if not res['OK']:
      return res
    elif len(res['Value']['Failed']):
      return S_ERROR("Failed to remove all links")
    else:
      res = self.__executeOperation(datasetName, 'removeDirectory')
      return res

  def __removeFilesFromDataset(self, datasetName, lfns):
    links = []
    for lfn in lfns:
      res = self.__getLFNGuid(lfn)
      if not res['OK']:
        return res
      guid = res['Value']
      linkPath = "%s/%s" % (datasetName, guid)
      links.append(linkPath)
    res = self.removeLink(links)
    if not res['OK']:
      return res
    if len(res['Value']['Successful']) == len(links):
      return S_OK()
    totalError = ""
    for link, error in res['Value']['Failed'].items():
      gLogger.error("LcgFileCatalogClient.__removeFilesFromDataset: Failed to remove link",
                    "%s: %s" % (link, error))
      totalError = "%s %s : %s" % (totalError, link, error)
    return S_ERROR(totalError)

  ####################################################################
  #
  # These are the methods required for the admin interface
  #

  def getUserDirectory(self, usernames):
    """ Takes a list of users and determines whether their directories already exist
    """
    result = getClientCertInfo()
    if not result['OK']:
      return result
    vo = result['Value']['VO']
    usernameDict = {}
    for username in usernames:
      userDirectory = "/%s/user/%s/%s" % (vo, username[0], username)
      usernameDict[userDirectory] = username
    res = self.exists(list(usernameDict))
    if not res['OK']:
      return res
    failed = {}
    for directory, reason in res['Value']['Failed'].items():
      failed[usernameDict[directory]] = reason
    successful = {}
    for directory, exists in res['Value']['Successful'].items():
      successful[usernameDict[directory]] = exists
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  def createUserDirectory(self, usernames):
    """ Creates the user directory
    """
    result = getClientCertInfo()
    if not result['OK']:
      return result
    vo = result['Value']['VO']
    successful = {}
    failed = {}
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    for username in usernames:
      userDirectory = "/%s/user/%s/%s" % (vo, username[0], username)
      res = self.__makeDirs(userDirectory, 0o755)
      if res['OK']:
        successful[username] = userDirectory
      else:
        failed[username] = res['Message']
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  def removeUserDirectory(self, username):
    """ Remove the user directory and remove the user mapping
    """
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    res = self.getUserDirectory(username)
    if not res['OK']:
      return res
    failed = {}
    for username, error in res['Value']['Failed'].items():
      failed[username] = error
    directoriesToRemove = {}
    successful = {}
    for username, directory in res['Value']['Successful'].items():
      if not directory:
        successful[username] = True
      else:
        directoriesToRemove[directory] = username
    res = self.removeDirectory(list(directoriesToRemove))
    if not res['OK']:
      return res
    for directory, error in res['Value']['Failed'].items():
      failed[directoriesToRemove[directory]] = error
    for directory in res['Value']['Successful']:
      successful[directoriesToRemove[directory]] = True
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def changePathOwner(self, paths):
    """ Change the ownership of the directory to the user associated to the supplied DN
    """
    successful = {}
    failed = {}
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    for dirPath, dn in paths.items():
      res = getDNUserID(dn)
      if not res['OK']:
        failed[dirPath] = res['Message']
      else:
        userID = res['Value']
        res = self.__changeOwner(dirPath, userID)
        if not res['OK']:
          failed[dirPath] = res['Message']
        else:
          successful[dirPath] = True
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def changePathMode(self, paths):
    """ Change the ownership of the directory to the user associated to the supplied DN
    """
    successful = {}
    failed = {}
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    for dirPath, mode in paths.items():
      res = self.__changeMode(dirPath, mode)
      if not res['OK']:
        failed[dirPath] = res['Message']
      else:
        successful[dirPath] = True
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  def createUserMapping(self, userDNs):
    """ Create a user with the supplied DN and return the userID
    """
    successful = {}
    failed = {}
    created = self.__openSession()
    if created < 0:
      return S_ERROR("Error opening LFC session")
    for userDN, uid in userDNs.items():
      if not uid:
        uid = -1
      res = addUserDN(uid, userDN)
      if not res['OK']:
        failed[userDN] = res['Message']
      else:
        res = getDNUserID(userDN)
        if not res['OK']:
          failed[userDN] = res['Message']
        else:
          successful[userDN] = res['Value']
    if created:
      self.__closeSession()
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  ####################################################################
  #
  # These are the internal methods used for the admin interface
  #

  def __changeOwner(self, lfn, userID):
    return returnCode(lfc.lfc_chown(self.__fullLfn(lfn), userID, -1))

  def __changeMod(self, lfn, mode):
    return returnCode(lfc.lfc_chmod(self.__fullLfn(lfn), mode))

  def __fullLfn(self, lfn):
    return str(self.prefix + lfn)

  # THIS IS NOT YET WORKING
