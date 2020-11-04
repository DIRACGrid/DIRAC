from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
__RCSID__ = "$Id"

from stat import *
import os
import re
import time
import datetime
from copy import deepcopy

import DIRAC
from DIRAC                                              import gConfig
from DIRAC                                              import S_OK, S_ERROR, gLogger
from DIRAC.Resources.Catalog.Utilities                  import checkCatalogArguments
from DIRAC.Core.Utilities.Time                          import fromEpoch
from DIRAC.Core.Utilities.List                          import breakListIntoChunks
from DIRAC.Core.Security.ProxyInfo                      import getProxyInfo, formatProxyInfoAsString
from DIRAC.ConfigurationSystem.Client.Helpers.Registry  import getDNForUsername, getVOMSAttributeForGroup, \
                                                               getVOForGroup, getVOOption
from DIRAC.Resources.Catalog.FileCatalogClientBase import FileCatalogClientBase

#from DIRAC.Core.Base import Script
#Script.parseCommandLine()

from rucio.client import Client
from rucio.common.exception import DataIdentifierNotFound, DuplicateContent, FileReplicaAlreadyExists, \
                                   FileAlreadyExists, CannotAuthenticate, MissingClientParameter
from rucio.common.utils import chunks, extract_scope


def get_scope(lfn, scopes=[], client=None):
  scope, name = extract_scope(did=lfn, scopes=scopes)
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
    self.convertUnicode = True
    proxyInfo = {'OK': False}
    if not os.getenv('X509_USER_PROXY'):
      proxyInfo = getProxyInfo(disableVOMS=True)
      if proxyInfo['OK']:
        os.environ['X509_USER_PROXY'] = proxyInfo['Value']['path']
        gLogger.debug('X509_USER_PROXY not defined. Using %s' % proxyInfo['Value']['path'])
    try:
      self.client = Client()
    except (CannotAuthenticate, MissingClientParameter) as err:
      if not proxyInfo['OK']:
        proxyInfo = getProxyInfo(disableVOMS=True)
      if proxyInfo['OK']:
        dn = proxyInfo['Value']['identity']
        username = proxyInfo['Value']['username']
        os.environ['RUCIO_ACCOUNT'] = username
        gLogger.debug('Switching to account %s mapped to proxy %s' %(username, dn))

    try:
      self.client = Client()
      self.scopes = self.client.list_scopes()
      self.account = self.client.account
    except Exception as err:
      gLogger.error('Cannot instantiate RucioFileCatalog interface, error : %s' % str(err))


  def __getDidsFromLfn(self, lfn):
    if lfn.find(':') > -1:
      scope, name = lfn.split(':')
      return {'scope': scope, 'name': name}
    else:
      scope = get_scope(lfn, scopes=self.scopes, client=self.client)
    return {'scope': scope, 'name': lfn}


  @checkCatalogArguments
  def getCompatibleMetadata(self, queryDict, path, credDict):
    """ Get distinct metadata values compatible with the given already defined metadata
    """
    self.client = Client()
    if path != '/':
      result = self.exists(path)
      if not result['OK']:
        return result
      if not result['Value']:
        return S_ERROR('Path not found: %s' % path)
    result = S_OK({})
    return result


  @checkCatalogArguments
  def getReplicas(self, lfns, allStatus = False):
    """ Returns replicas for an LFN or list of LFNs
    """
    result = {'OK': True, 'Value': {'Successful': {}, 'Failed': {}}}
    lfnChunks = breakListIntoChunks( lfns, 1000 )

    self.client = Client()
    for lfnList in lfnChunks:
      try:
        DidList = [self.__getDidsFromLfn(lfn) for lfn in lfnList if lfn]
        for rep in self.client.list_replicas(DidList):
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
            for did in DidList:
              result['Value']['Failed'][did['name']] = 'Error'
      except Exception as err:
        return S_ERROR(str(err))
    return result


  @checkCatalogArguments
  def listDirectory(self, lfns, verbose = False):
    """ Returns the result of __getDirectoryContents for multiple supplied paths
    """
    result = {'OK': True, 'Value': {'Successful': {}, 'Failed': {}}}

    self.client = Client()
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
            else:
              result['Value']['Successful'][lfn]['Files'][childName] = {'Mode': 509}
        elif meta['did_type'] == 'DATASET':
          file_dict = {}
          for file_did in self.client.list_files(scope=did['scope'], name=did['name']):
            file_dict[file_did['name']] = file_did['guid']
          if lfn not in result['Value']['Successful']:
            result['Value']['Successful'][lfn] = {'Files': {}, 'Links': {}, 'SubDirs': {}}
          for rep in self.client.list_replicas([did, ]):
            if rep:
              name = rep['name']
              if self.convertUnicode:
                name = str(name)
              result['Value']['Successful'][lfn]['Files'][name] = {'Metadata': {}, 'Replicas': {}}
              result['Value']['Successful'][lfn]['Files'][name]['Metadata'] = {'GUID': str(file_dict.get(name, 'UNKNOWN')), 'Mode': 436, 'Size': rep['bytes']}
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
  def getFileMetadata(self, lfns, ownership = False):
    """ Returns the file metadata associated to a supplied LFN
    """
    successful, failed = {}, {}
    lfnChunks = breakListIntoChunks(lfns, 1000)
    listFiles = deepcopy(lfns.keys())
    self.client = Client()
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
            successful[lfn] = {'Checksum': str(meta['adler32']),
                               'ChecksumType': 'AD',
                               'CreationDate': meta['created_at'],
                               'GUID': str(meta['guid']),
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
    self.client = Client()
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
    self.client = Client()
    for lfn in lfns:
      size = 0
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
    result = {'OK': True, 'Value': {'Successful': {}, 'Failed': {}}}
    self.client = Client()
    for lfn in lfns:
      try:
        did = self.__getDidsFromLfn(lfn)
        meta = self.client.get_metadata(did['scope'], did['name'])
        if meta['did_type'] in ['DATASET', 'CONTAINER']:
          result['Value']['Successful'][lfn] = True
        else:
          result['Value']['Successful'][lfn] = False
      except DataIdentifierNotFound:
        result['Value']['Failed'][lfn] = 'No such file or directory'
      except Exception as err:
        return S_ERROR(str(err))
    return result



  @checkCatalogArguments
  def isFile(self, lfns):
    """ Determine whether the path is a directory
    """
    result = {'OK': True, 'Value': {'Successful': {}, 'Failed': {}}}
    self.client = Client()
    for lfn in lfns:
      try:
        did = self.__getDidsFromLfn(lfn)
        meta = self.client.get_metadata(did['scope'], did['name'])
        if meta['did_type'] == 'FILE':
          result['Value']['Successful'][lfn] = True
        else:
          result['Value']['Successful'][lfn] = False
      except DataIdentifierNotFound:
        result['Value']['Failed'][lfn] = 'No such file or directory'
      except Exception as err:
        return S_ERROR(str(err))
    return result



  @checkCatalogArguments
  def addFile(self, lfns):
    failed = {}
    successful = {}
    deterministicDictionary = {}
    self.client = Client()
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
    self.client = Client()
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
        self.client.add_replication_rule([{'scope': did['scope'], 'name': did['name']}], copies=1, rse_expression=se, weight=None, lifetime=None, grouping='NONE', account=self.account)
        successful[lfn] = True
      except FileReplicaAlreadyExists:
        successful[lfn] = True
      except Exception as err:
        failed[lfn] = str(err)
    resDict = {'Failed': failed, 'Successful': successful}
    return S_OK(resDict)


  @checkCatalogArguments
  def removeReplica(self, lfns):
    failed = {}
    successful = {}
    self.client = Client()
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
    self.client = Client()
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
    self.client = Client()
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
    self.client = Client()
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
