""" The RucioFileCatalogClient is a class that allows to interface Dirac with Rucio
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
__RCSID__ = "$Id"

import os
import uuid
import datetime
from copy import deepcopy

import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Resources.Catalog.Utilities import checkCatalogArguments
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Resources.Catalog.FileCatalogClientBase import FileCatalogClientBase

from rucio.client import Client
from rucio.common.exception import DataIdentifierNotFound, FileReplicaAlreadyExists, \
    CannotAuthenticate, MissingClientParameter
from rucio.common.utils import chunks, extract_scope
sLog = gLogger.getSubLogger(__name__)


def get_scope(lfn, scopes=None):
  """Helper function that extract the scope from the LFN
  """
  if scopes is None:
    scopes = []
  scope, _ = extract_scope(did=lfn, scopes=scopes)
  return scope


class RucioFileCatalogClient(FileCatalogClientBase):

  READ_METHODS = FileCatalogClientBase.READ_METHODS + ['isLink', 'readLink', 'isFile', 'getFileMetadata',
                                                       'getReplicas', 'getReplicaStatus', 'getFileSize',
                                                       'isDirectory', 'getDirectoryReplicas',
                                                       'listDirectory', 'getDirectoryMetadata',
                                                       'getDirectorySize', 'getDirectoryContents',
                                                       'resolveDataset', 'getLFNForPFN', 'getUserDirectory']

  WRITE_METHODS = FileCatalogClientBase.WRITE_METHODS + ['createLink', 'removeLink', 'addFile', 'addReplica',
                                                         'removeReplica', 'removeFile', 'setReplicaStatus',
                                                         'setReplicaHost', 'createDirectory', 'removeDirectory',
                                                         'removeDataset', 'removeFileFromDataset', 'createDataset',
                                                         'changePathOwner', 'changePathMode']

  NO_LFN_METHODS = FileCatalogClientBase.NO_LFN_METHODS + ['getUserDirectory', 'createUserDirectory',
                                                           'createUserMapping', 'removeUserDirectory']

  ADMIN_METHODS = FileCatalogClientBase.ADMIN_METHODS + ['getUserDirectory', 'createUserDirectory',
                                                         'createUserMapping', 'removeUserDirectory']

  def __init__(self, **options):
    """ Constructor
    """
    self.convertUnicode = True
    proxyInfo = {'OK': False}
    if os.getenv('RUCIO_AUTH_TYPE') == 'x509_proxy' and not os.getenv('X509_USER_PROXY'):
      proxyInfo = getProxyInfo(disableVOMS=True)
      if proxyInfo['OK']:
        os.environ['X509_USER_PROXY'] = proxyInfo['Value']['path']
        sLog.debug('X509_USER_PROXY not defined. Using %s' % proxyInfo['Value']['path'])
    try:
      self._client = Client()
      self.account = self._client.account
    except (CannotAuthenticate, MissingClientParameter):
      if os.getenv('RUCIO_AUTH_TYPE') == 'x509_proxy':
        if not proxyInfo['OK']:
          proxyInfo = getProxyInfo(disableVOMS=True)
        if proxyInfo['OK']:
          dn = proxyInfo['Value']['identity']
          username = proxyInfo['Value']['username']
          self.account = username
          sLog.debug('Switching to account %s mapped to proxy %s' % (username, dn))

    try:
      self._client = Client(account=self.account)
      self.scopes = self._client.list_scopes()
    except Exception as err:
      sLog.error('Cannot instantiate RucioFileCatalog interface', 'error : %s' % repr(err))

  @property
  def client(self):
    """ Check if the session to the server is still active and return an instance of RucioClient
    """
    try:
      self._client.ping()
      return self._client
    except Exception:
      self._client = Client(account=self.account)
      self.scopes = self._client.list_scopes()
      return self._client

  def __getDidsFromLfn(self, lfn):
    """ Helper function to convert LFNs into DIDs
    """
    if lfn.find(':') > -1:
      scope, name = lfn.split(':')
    else:
      scope = get_scope(lfn, scopes=self.scopes)
      name = lfn
    return {'scope': scope, 'name': name}

  @checkCatalogArguments
  def getCompatibleMetadata(self, queryDict, path, credDict):
    """ Get distinct metadata values compatible with the given already defined metadata
    """
    if path != '/':
      result = self.exists(path)
      if not result['OK']:
        return result
      if not result['Value']:
        return S_ERROR('Path not found: %s' % path)
    result = S_OK({})
    return result

  @checkCatalogArguments
  def getReplicas(self, lfns, allStatus=False):
    """ Returns replicas for an LFN or list of LFNs
    """
    result = {'OK': True, 'Value': {'Successful': {}, 'Failed': {}}}
    lfnChunks = breakListIntoChunks(lfns, 1000)

    for lfnList in lfnChunks:
      try:
        didList = [self.__getDidsFromLfn(lfn) for lfn in lfnList if lfn]
        for rep in self.client.list_replicas(didList):
          if rep:
            lfn = rep['name']
            if self.convertUnicode:
              lfn = str(lfn)
            if lfn not in result['Value']['Successful']:
              result['Value']['Successful'][lfn] = {}
            for rse in rep['rses']:
              if self.convertUnicode:
                result['Value']['Successful'][lfn][str(rse)] = str(rep['rses'][rse][0])
              else:
                result['Value']['Successful'][lfn][rse] = rep['rses'][rse][0]
          else:
            for did in didList:
              result['Value']['Failed'][did['name']] = 'Error'
      except Exception as err:
        return S_ERROR(str(err))
    return result

  @checkCatalogArguments
  def listDirectory(self, lfns, verbose=False):
    """ Returns the result of __getDirectoryContents for multiple supplied paths
    """
    result = {'OK': True, 'Value': {'Successful': {}, 'Failed': {}}}

    for lfn in lfns:
      try:
        did = self.__getDidsFromLfn(lfn)
        # First need to check if it's a dataset or container
        meta = self.client.get_metadata(did['scope'], did['name'])
        if meta['did_type'] == 'CONTAINER':
          if lfn not in result['Value']['Successful']:
            result['Value']['Successful'][lfn] = {'Files': {}, 'Links': {}, 'SubDirs': {}}
          for child in self.client.list_content(scope=did['scope'], name=did['name']):
            childName = child['name']
            if self.convertUnicode:
              childName = str(childName)
            if child['type'] in ['DATASET', 'CONTAINER']:
              result['Value']['Successful'][lfn]['SubDirs'][childName] = {'Mode': 509}
              if verbose:
                result['Value']['Successful'][lfn]['SubDirs'][childName] = {
                    'Status': '-',
                    'CreationDate': datetime.datetime(
                        1970,
                        1,
                        1),
                    'ChecksumType': '',
                    'OwnerRole': 'None',
                    'Checksum': '',
                    'NumberOfLinks': -999,
                    'OwnerDN': 'None',
                    'Mode': 509,
                    'ModificationDate': datetime.datetime(
                        1970,
                        1,
                        1),
                    'GUID': '',
                    'LastAccess': 0,
                    'Size': 0}
            else:
              result['Value']['Successful'][lfn]['Files'][childName] = {'Mode': 509}
              if verbose:
                pass
        elif meta['did_type'] == 'DATASET':
          file_dict = {}
          for file_did in self.client.list_files(scope=did['scope'], name=did['name']):
            guid = file_did['guid']
            file_dict[file_did['name']] = guid
            if guid:
              file_dict[file_did['name']] = str(uuid.UUID(guid))
          if lfn not in result['Value']['Successful']:
            result['Value']['Successful'][lfn] = {'Files': {}, 'Links': {}, 'SubDirs': {}}
          for rep in self.client.list_replicas([did, ]):
            if rep:
              name = rep['name']
              if self.convertUnicode:
                name = str(name)
              result['Value']['Successful'][lfn]['Files'][name] = {'Metadata': {}, 'Replicas': {}}
              result['Value']['Successful'][lfn]['Files'][name]['Metadata'] = {
                  'GUID': str(file_dict.get(name, 'UNKNOWN')), 'Mode': 436, 'Size': rep['bytes']}
              if verbose:
                result['Value']['Successful'][lfn]['Files'][name]['Metadata'] = {
                    'Status': '-',
                    'CreationDate': datetime.datetime(
                        1970,
                        1,
                        1),
                    'ChecksumType': 'AD',
                    'OwnerRole': 'None',
                    'Checksum': str(
                        rep['adler32']),
                    'NumberOfLinks': 1,
                    'OwnerDN': 'None',
                    'Mode': 436,
                    'ModificationDate': datetime.datetime(
                        1970,
                        1,
                        1),
                    'GUID': str(
                        file_dict.get(
                            name,
                            'UNKNOWN')),
                    'LastAccess': 0,
                    'Size': rep['bytes']}
              for rse in rep['rses']:
                pfn = rep['rses'][rse][0]
                if self.convertUnicode:
                  pfn = str(pfn)
                result['Value']['Successful'][lfn]['Files'][name]['Replicas'][rse] = {'PFN': pfn, 'Status': 'U'}
      except DataIdentifierNotFound as err:
        result['Value']['Failed'][lfn] = 'No such file or directory'
      except Exception as err:
        return S_ERROR(str(err))
    return result

  @checkCatalogArguments
  def getFileMetadata(self, lfns, ownership=False):
    """ Returns the file metadata associated to a supplied LFN
    """
    successful, failed = {}, {}
    lfnChunks = breakListIntoChunks(lfns, 1000)
    listFiles = deepcopy(list(lfns))
    for chunk in lfnChunks:
      try:
        dids = [self.__getDidsFromLfn(lfn) for lfn in chunk]
        for meta in self.client.get_metadata_bulk(dids):
          lfn = str(meta['name'])
          if meta['did_type'] in ['DATASET', 'CONTAINER']:
            nlinks = len([child for child in self.client.list_content(meta['scope'], meta['name'])])
            successful[lfn] = {'Checksum': '',
                               'ChecksumType': '',
                               'CreationDate': meta['created_at'],
                               'GUID': '',
                               'Mode': 509,
                               'ModificationDate': meta['updated_at'],
                               'NumberOfLinks': nlinks,
                               'Size': 0,
                               'Status': '-'}
            try:
              listFiles.remove(lfn)
            except ValueError:
              pass
          else:
            guid = meta['guid']
            if guid:
              guid = str(uuid.UUID(guid))
            successful[lfn] = {'Checksum': str(meta['adler32']),
                               'ChecksumType': 'AD',
                               'CreationDate': meta['created_at'],
                               'GUID': guid,
                               'Mode': 436,
                               'ModificationDate': meta['updated_at'],
                               'NumberOfLinks': 1,
                               'Size': meta['bytes'],
                               'Status': '-'}
            try:
              listFiles.remove(lfn)
            except ValueError:
              pass
      except DataIdentifierNotFound as err:
        failed[lfn] = str(err)
      except Exception as err:
        return S_ERROR(str(err))
    for lfn in listFiles:
      failed[lfn] = 'No such file or directory'
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def exists(self, lfns):
    """ Check if the path exists
    """
    result = {'OK': True, 'Value': {'Successful': {}, 'Failed': {}}}
    for lfn in lfns:
      try:
        did = self.__getDidsFromLfn(lfn)
        exists = True
        self.client.get_metadata(did['scope'], did['name'])
      except DataIdentifierNotFound:
        exists = False
      except Exception as err:
        return S_ERROR(str(err))
      result['Value']['Successful'][lfn] = exists
    return result

  @checkCatalogArguments
  def getFileSize(self, lfns):
    """ Get the size of a supplied file
    """
    result = {'OK': True, 'Value': {'Successful': {}, 'Failed': {}}}
    for lfn in lfns:
      try:
        did = self.__getDidsFromLfn(lfn)
        meta = self.client.get_metadata(did['scope'], did['name'])
        if meta['did_type'] == 'FILE':
          result['Value']['Successful'][lfn] = meta['bytes']
        else:
          result['Value']['Successful'][lfn] = 0
      except DataIdentifierNotFound:
        result['Value']['Failed'][lfn] = 'No such file or directory'
      except Exception as err:
        return S_ERROR(str(err))
    return result

  @checkCatalogArguments
  def isDirectory(self, lfns):
    """ Determine whether the path is a directory
    """
    result = {'Successful': {}, 'Failed': {}}
    dids = [self.__getDidsFromLfn(lfn) for lfn in lfns]
    try:
      for meta in self.client.get_metadata_bulk(dids):
        lfn = str(meta['name'])
        result['Successful'][lfn] = (meta['did_type'] in ['DATASET', 'CONTAINER'])
      for lfn in lfns:
        if lfn not in result['Successful'] and lfn not in result['Failed']:
          result['Failed'][lfn] = 'No such file or directory'
    except Exception as err:
      return S_ERROR(str(err))
    return S_OK(result)

  @checkCatalogArguments
  def isFile(self, lfns):
    """ Determine whether the path is a file
    """
    result = {'Successful': {}, 'Failed': {}}
    dids = []
    for lfn in lfns:
      dids.append(self.__getDidsFromLfn(lfn))
    try:
      for meta in self.client.get_metadata_bulk(dids):
        lfn = str(meta['name'])
        result['Successful'][lfn] = (meta['did_type'] in ['FILE'])
      for lfn in lfns:
        if lfn not in result['Successful'] and lfn not in result['Failed']:
          result['Failed'][lfn] = 'No such file or directory'
    except Exception as err:
      return S_ERROR(str(err))
    return S_OK(result)

  @checkCatalogArguments
  def addFile(self, lfns):
    """ Register supplied files
    """
    failed = {}
    successful = {}
    deterministicDictionary = {}
    for lfnList in breakListIntoChunks(lfns, 100):
      listLFNs = []
      for lfn in list(lfnList):
        lfnInfo = lfns[lfn]
        pfn = None
        se = lfnInfo['SE']
        if se not in deterministicDictionary:
          isDeterministic = self.client.get_rse(se)['deterministic']
          deterministicDictionary[se] = isDeterministic
        if not deterministicDictionary[se]:
          pfn = lfnInfo['PFN']
        size = lfnInfo['Size']
        guid = lfnInfo.get('GUID', None)
        checksum = lfnInfo['Checksum']
        rep = {'lfn': lfn, 'bytes': size, 'adler32': checksum, 'rse': se}
        if pfn:
          rep['pfn'] = pfn
        if guid:
          rep['guid'] = guid
        listLFNs.append(rep)
      try:
        self.client.add_files(lfns=listLFNs, ignore_availability=True)
        for lfn in list(lfnList):
          successful[lfn] = True
      except Exception as err:
        # Try inserting one by one
        sLog.warning('Cannot bulk insert files', 'error : %s' % repr(err))
        for lfn in list(lfnList):
          try:
            self.client.add_files(lfns=lfn, ignore_availability=True)
            successful[lfn] = True
          except FileReplicaAlreadyExists:
            successful[lfn] = True
          except Exception as err:
            failed[lfn] = str(err)
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def addReplica(self, lfns):
    """ This adds a replica to the catalogue.
    """
    failed = {}
    successful = {}
    deterministicDictionary = {}
    for lfn, info in lfns.items():
      pfn = None
      se = info['SE']
      if se not in deterministicDictionary:
        isDeterministic = self.client.get_rse(se)['deterministic']
        deterministicDictionary[se] = isDeterministic
      if not deterministicDictionary[se]:
        pfn = info['PFN']
      size = info.get('Size', None)
      checksum = info.get('Checksum', None)
      try:
        did = self.__getDidsFromLfn(lfn)
        if not size or not checksum:
          meta = self.client.get_metadata(did['scope'], did['name'])
          size = meta['bytes']
          checksum = meta['adler32']
        rep = {'scope': did['scope'], 'name': did['name'], 'bytes': size, 'adler32': checksum}
        if pfn:
          rep['pfn'] = pfn
        self.client.add_replicas(rse=se, files=[rep, ])
        self.client.add_replication_rule([{'scope': did['scope'],
                                           'name': did['name']}],
                                         copies=1,
                                         activity='User Transfers',
                                         rse_expression=se,
                                         weight=None,
                                         lifetime=None,
                                         grouping='NONE',
                                         account=self.account)
        successful[lfn] = True
      except FileReplicaAlreadyExists:
        successful[lfn] = True
      except Exception as err:
        failed[lfn] = str(err)
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def removeReplica(self, lfns):
    """ Remove the supplied replicas
    """
    failed = {}
    successful = {}
    for lfn, info in lfns.items():
      if 'SE' not in info:
        failed[lfn] = "Required parameters not supplied"
        continue
      se = info['SE']
      try:
        did = self.__getDidsFromLfn(lfn)
        meta = self.client.get_metadata(did['scope'], did['name'])
        if meta['did_type'] == 'FILE':
          # For file cannot use dataset_locks to identify the rule
          for rule in self.client.list_did_rules(did['scope'], did['name']):
            rid = rule['id']
            self.client.update_replication_rule(rid, options={'lifetime': -86400})
          successful[lfn] = True
        elif meta['did_type'] == 'DATASET':
          rules = {}
          for lock in self.client.get_dataset_locks(did['scope'], did['name']):
            rule_id = lock['rule_id']
            if rule_id not in rules:
              rules[rule_id] = []
            rse = lock['rse']
            if str(rse) not in rules[rule_id]:
              rules[rule_id].append(str(rse))

          for rid in rules:
            if len(rules[rid]) == 1 and rules[rid][0] == se:
              rule_info = self.client.get_replication_rule(rid)
              if str(rule_info['name']) == lfn:
                self.client.update_replication_rule(rid, options={'lifetime': -86400})
          successful[lfn] = True

      except Exception as err:
        # Always assumes that it succeeded
        failed[lfn] = str(err)
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)

  @checkCatalogArguments
  def removeFile(self, lfns):
    """ Remove the supplied path
    """
    resDict = {'Successful': {}, 'Failed': {}}
    for lfn in lfns:
      try:
        did = self.__getDidsFromLfn(lfn)
        meta = self.client.get_metadata(did['scope'], did['name'])
        if meta['did_type'] == 'FILE':
          parentLfn = "/".join(lfn.split('/')[:-1])
          parentDid = self.__getDidsFromLfn(parentLfn)
          dsnScope, dsnName = parentDid['scope'], parentDid['name']
          try:
            self.client.detach_dids(dsnScope, dsnName, [{'scope': did['scope'], 'name': did['name']}])
            resDict['Successful'][lfn] = True
          except Exception as err:
            resDict['Failed'][lfn] = str(err)
        else:
          resDict['Failed'][lfn] = 'Not a file'
      except DataIdentifierNotFound:
        resDict['Failed'][lfn] = 'No such file or directory'
      except Exception as err:
        return S_ERROR(str(err))
    return S_OK(resDict)

  @checkCatalogArguments
  def removeDirectory(self, lfns):
    """ Remove the supplied directory
    """
    resDict = {'Successful': {}, 'Failed': {}}
    for lfn in lfns:
      try:
        did = self.__getDidsFromLfn(lfn)
        meta = self.client.get_metadata(did['scope'], did['name'])
        if meta['did_type'] == 'DATASET':
          try:
            self.client.set_metadata(scope=did['scope'], name=did['name'], key='lifetime', value=1)
            resDict['Successful'][lfn] = True
          except Exception as err:
            resDict['Failed'][lfn] = str(err)
        else:
          resDict['Failed'][lfn] = 'Not a directory'
      except DataIdentifierNotFound:
        resDict['Failed'][lfn] = 'No such file or directory'
      except Exception as err:
        return S_ERROR(str(err))
    return S_OK(resDict)

  @checkCatalogArguments
  def getDirectorySize(self, lfns, longOutput=False, rawFiles=False):
    """ Get the directory size
    """
    resDict = {'Successful': {}, 'Failed': {}}
    for lfn in lfns:
      try:
        did = self.__getDidsFromLfn(lfn)
        meta = self.client.get_metadata(did['scope'], did['name'])
        if meta['did_type'] == 'FILE':
          resDict['Failed'][lfn] = 'Not a directory'

        elif meta['did_type'] == 'CONTAINER':
          resDict['Successful'][lfn] = {'ClosedDirs': [], 'Files': 0, 'SiteUsage': {}, 'SubDirs': {}, 'TotalSize': 0}
          for child in self.client.list_content(scope=did['scope'], name=did['name']):
            childName = child['name']
            if self.convertUnicode:
              childName = str(childName)
            resDict['Successful'][lfn]['SubDirs'][childName] = datetime.datetime.now()

        else:
          resDict['Successful'][lfn] = {'ClosedDirs': [], 'Files': 0, 'SiteUsage': {}, 'SubDirs': {}, 'TotalSize': 0}
          for child in self.client.list_files(scope=did['scope'], name=did['name']):
            resDict['Successful'][lfn]['Files'] += 1
            resDict['Successful'][lfn]['TotalSize'] += child['bytes']
          for rep in self.client.list_replicas([{'scope': did['scope'], 'name': did['name']}]):
            fileSize = rep['bytes']
            for rse in rep['rses']:
              if rse not in resDict['Successful'][lfn]['SiteUsage']:
                resDict['Successful'][lfn]['SiteUsage'][rse] = {'Files': 0, 'Size': 0}
              resDict['Successful'][lfn]['SiteUsage'][rse]['Files'] += 1
              resDict['Successful'][lfn]['SiteUsage'][rse]['Size'] += fileSize

      except DataIdentifierNotFound:
        resDict['Failed'][lfn] = 'No such file or directory'
      except Exception as err:
        return S_ERROR(str(err))
    return S_OK(resDict)
