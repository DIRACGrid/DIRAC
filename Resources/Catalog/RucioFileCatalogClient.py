"""
Rucio File Catalog Client.
"""

from __future__ import division

import os
import os.path
import sys
import datetime

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Resources.Catalog.RucioRESTClientApi.BaseClient import BaseClient
from DIRAC.Resources.Catalog.RucioRESTClientApi.RSEClient import RSEClient
from DIRAC.Resources.Catalog.RucioRESTClientApi.DIDClient import DIDClient
from DIRAC.Resources.Catalog.RucioRESTClientApi.ScopeClient import ScopeClient
from DIRAC.Resources.Catalog.RucioRESTClientApi.ReplicaClient import ReplicaClient
from DIRAC.Resources.Catalog.RucioRESTClientApi import Utils
from DIRAC.Resources.Catalog.Utilities import checkCatalogArguments
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForUsername, getVOMSAttributeForGroup, \
    getVOForGroup, getVOOption
from DIRAC.Resources.Catalog.FileCatalogClientBase import FileCatalogClientBase
from DIRAC.Core.Base.Client import Client
from DIRAC.Resources.Catalog.LcgFileCatalogClient import getClientCertInfo
from DIRAC.Core.Utilities.List import breakListIntoChunks


class RucioFileCatalogClient(FileCatalogClientBase):
  """

  """

  READ_METHODS = FileCatalogClientBase.READ_METHODS + ['listDirectory', 'getUserDirectory', 'getPfnFromLfn',
                                                       'getReplicas', 'getFileMetadata', 'isFile']

  WRITE_METHODS = FileCatalogClientBase.WRITE_METHODS + ['addFile']

  NO_LFN_METHODS = FileCatalogClientBase.NO_LFN_METHODS + ['getUserDirectory', 'createUserDirectory',
                                                           'createUserMapping', 'removeUserDirectory']

  ADMIN_METHODS = FileCatalogClientBase.ADMIN_METHODS + ['getUserDirectory']

  def __init__(self, url=None, **options):

    super(RucioFileCatalogClient, self).__init__(url=url, **options)

    result = Operations(vo=getVOfromProxyGroup().get('Value', None)).\
        getOptionsDict('/Services/Catalogs/%s' % self.__class__.__name__.replace('Client', ''))
    if result['OK']:
      options.update(result['Value'])

    rucioHost = options.get('RucioHost', None)
    authHost = options.get('AuthHost', None)
    # giving ourselves a chance to have a VO-wide Rucio account, mainly for testing.
    account = options.get('RucioAccount', None)
    # ideally the Rucio account should match Dirac username, until we have a mapper
    if account is None:
      result = getProxyInfo()
      if result['OK']:
        account = result['Value'].get('username', 'root')

    clients = [ScopeClient, RSEClient, DIDClient, ReplicaClient]
    for client in clients:
      clientName = client.__name__.lower().replace("client", "Client")
      setattr(self, clientName, client(rucioHost, authHost, account=account))
      result = getattr(self, clientName).authenticate()
      if result['OK']:
        gLogger.info('Rucio %s authentication successful' % clientName)
        gLogger.debug("Rucio File Catalog %s client created with options: " % clientName, options)
      else:
        gLogger.error('Rucio client authentication failed', result)

  @checkCatalogArguments
  def listDirectory(self, lfns, verbose=False):
    gLogger.debug("Rucio list directory for lfns: ", lfns)
    failed = {}
    successful = {}
    for path in lfns:
      res = self.__getDirectoryContents(path, verbose)
      if res['OK']:
        successful[path] = res['Value']
      else:
        failed[path] = res['Message']
    resDict = {'Failed': failed, 'Successful': successful}
    gLogger.debug(resDict)
    return S_OK(resDict)

  @checkCatalogArguments
  def isFile(self, lfns, verbose=False):
    """
    Check whether the supplied lfns are files.

    :param lfns: List of LFNs
    :type lfns: list
    :param verbose: verbose flag
    :type verbose: bool
    :return: Dirac S_OK object
    :rtype: dict
    """

    failed = {}
    successful = {}

    for lfn in lfns:
      _, scope, didname = self.__getLfnElements(lfn)
      metaresult = self.didClient.getMetadata(scope, didname)
      if metaresult['OK']:
        meta = metaresult['Value']
        if meta['did_type'] == 'FILE':
          successful[lfn] = True
        else:
          successful[lfn] = False
      else:
        failed[lfn] = metaresult['Message']
    if verbose:
      gLogger("Rucio isFile: ", {'Failed': failed, 'Successful': successful})
    return S_OK({'Failed': failed, 'Successful': successful})

  def __getDirectoryContents(self, path, verbose):
    """
    Get files and directories for a given path.

    :param path: Dirac logical file name, including VO and scope
    :param verbose:
    :return: a dictionary with files and their attributes
    """
    subDirs = {}
    links = {}
    files = {}
    # get the scope as a second element of the path - this is a convention
    elements = os.path.normpath(path).split(os.sep)

    if len(elements) <= 2:
      return S_ERROR(" The Rucio lfn path should contain at least 2 elements:"
                     " the VO, the scope (and an optional path):\n%s" % path)
    scope = elements[2]
    result = self.scopeClient.listScopes()
    if result['OK']:
      if scope not in result['Value']:
        return S_ERROR("Rucio scope %s does not exist" % scope)
    else:
      return result

    result = self.didClient.scopeList(scope)
    if result['OK']:
      for elem in result['Value']:
        # file only for a time being:
        if elem['type'] == 'FILE':
          name = elem['name']
          replicasListing = self.replicaClient.listReplicas([{'scope': elem['scope'], 'name': name}])
          if replicasListing['OK']:
            replicas = replicasListing['Value']
            # only can get size and name, metadata rather limited
            size = replicas['bytes']
            mtime = datetime.datetime(1970, 1, 1, 0, 0, 0)
            metadata = {'MetaData': {'TYpe': 'File', 'Mode': 509, 'Size': size, 'ModificationDate': mtime}}
            if len(name.split(os.sep)) == 1:
              files[os.path.join(path, name)] = metadata
          else:
            return replicasListing
    else:
      return result
    pathDict = {'Files': files, 'SubDirs': subDirs, 'Links': links}
    return S_OK(pathDict)

  @checkCatalogArguments
  def addFile(self, lfns):
    """
    Upload and register a local file with Rucio file catalog.

    :param lfns: a list containing logical filenames
    :return: Dirac S_OK object
    """
    successful = {}
    failed = {}
    gLogger.debug("Rucio addFile (lfns): ", lfns)
    for lfn in lfns:
      lfnInfo = lfns[lfn]
      pfn = lfnInfo['PFN']
      size = lfnInfo['Size']
      se = lfnInfo['SE']
      guid = lfnInfo['GUID']
      checksum = lfnInfo['Checksum']
      VO, scope, name = self.__getLfnElements(lfn)
      rse = self.__dirac2RucioSE(se)
      res = self.replicaClient.addReplica(rse, scope, name, size, checksum)
      if res['OK']:
        gLogger.debug(" Rucio replica %s registered successfully " % name)
        successful[lfn] = True
      else:
        failed[lfn] = " Rucio replica %s registration failed " % name
    return S_OK({'Failed': failed, 'Successful': successful})

  @checkCatalogArguments
  def exists(self, lfns):
    """
    Check whether parts exists in Rucio.

    :param lfns: LFN list to check for existance
    :return: Dirac S_OK object
    """

    failed = {}
    successful = {}

    for lfn in lfns:
      _, scope, didname = self.__getLfnElements(lfn)
      metaresult = self.didClient.getMetadata(scope, didname)
      successful[lfn] = metaresult['OK']
    return S_OK({'Failed': failed, 'Successful': successful})

  @checkCatalogArguments
  def getReplicas(self, lfns, allStatus=False, active=True):
    """
    Get file replicas from Rucio.

    :param lfns: list of Dirac logical file names
    :type lfns: list
    :param allStatus:  currently unused
    :type allStatus: bool
    :param active: look only at active SEs (currently unusedO
    :type active: bool
    :return: Dirac S_OK object
    :rtype: dict
    """
    lfnChunks = breakListIntoChunks(lfns, 1000)
    failed = {}
    successful = {}
    fullDidList = []
    voDict = {}

    for lfnList in lfnChunks:
      for lfn in lfnList:
        if lfn:
          did = self.__getDid(lfn)
          fullDidList.append(did)
          voDict[did['scope'] + ':' + did['name']] = self.__getLfnElements(lfn)[0]

      replicasListing = self.replicaClient.listReplicas(fullDidList)
      if replicasListing['OK']:
        rep = replicasListing['Value']
        if rep:
          key = rep['scope'] + ':' + rep['name']
          lfn = os.path.join('/', voDict[key], rep['scope'], rep['name'])
          for rse in rep['rses']:
            se = self.__rucio2DiracSE(rse)
            successful.setdefault(lfn, {})[se] = rep['rses'][rse][0]
        else:
          pass
 # unclear         failed.update({did['name']:'Error getting replicas from Did' for did in fullDidList})
    return S_OK({'Successful': successful, 'Failed': failed})

  @checkCatalogArguments
  def getFileMetadata(self, lfns):
    """
    Get file metadata from the catalog.

    :param lfns: Logical file names (Dirac style)
    :type lfns: list
    :return: Dirac object with successful and failed metadata keyed by Dirac lfn
    :rtype: dict
    """
    successful = {}
    failed = {}

    for lfn in lfns:
      lfn = lfn.encode("ascii")
      # need a Rucio lfn here:
      rlfn = self.__diracLFN2RucioLFN(lfn)
      scope, name = rlfn.split(':')
      metaresult = self.didClient.getMetadata(scope, name)
      if metaresult['OK']:
        meta = metaresult['Value']
        if meta['did_type'] in ['DATASET', 'CONTAINER']:
          pass
        else:
          successful[lfn] = {'Checksum': meta['adler32'].encode("ascii"), 'ChecksumType': 'Adler32',
                             'CreationDate': meta['created_at'], 'GUID': meta['guid'], 'Mode': 436,
                             'ModificationDate': meta['updated_at'], 'NumberOfLinks': 1,
                             'Size': meta['bytes'], 'Status': '-'}
      else:
        failed[rlfn] = meta['Message']
    return S_OK({'Successful': successful, 'Failed': failed})

  def getPfnFromLfn(self, lfn, se):
    """
    Get a physical file name on a Rucio storage element from a supplied
    logical file name.

    :param lfn: Dirac LFN to be translated to Rucio pfn
    :type lfn: str
    :param se: Dirac SE name
    :type se: str
    :return: a Dirac return object with pfn as a Value in case of success or an error object.
    :rtype: dict
    """
    lfn = self.__diracLFN2RucioLFN(lfn)
    try:
      pfn = self.client.lfns2pfns(self.__diracRucioSE(se), [lfn])
      return S_OK(pfn[lfn])
    except Exception as exc:
      return S_ERROR(str(exc))

  def __getLfnElements(self, lfn):
    """
    Get the VO, scope and the did name from the Dirac lfn.

    :param lfn: Dirac LFN
    :type lfn: str
    """
    parts = lfn.split('/')
    VO = parts[1]
    scope = parts[2]
    name = os.path.join('', *parts[3:])
    return VO, scope, name

  def __diracLFN2RucioLFN(self, lfn):
    """
    Convert a Dirac LFN to a Rucio LFN.

    :param lfns:
    :type lfns:
    :return:
    :rtype:
    """
    VO, scope, name = self.__getLfnElements(lfn)
    return scope + ':' + name

  def __dirac2RucioSE(self, se):
    """
    Get Rucio RSE name from Dirac SE name.

    :param se:
    :type se:
    :return:
    :rtype:
    """
    se2rse = self.__diracRucioSEMap()
    return se2rse.get(se, se)

  def __rucio2DiracSE(self, rse):
    """
    Reverse RSE - SE mapping.

    :param rse:
    :type rse:
    :return:
    :rtype:
    """
    se2rse = self.__diracRucioSEMap()
    rse2se = {se2rse[k]: k for k in se2rse}
    return rse2se.get(rse, rse)

  def __diracRucioSEMap(self):
    """
    Dirac SE to Rucio RSE map, has to be 1 to 1.
    :return:
    :rtype:
    """
    se2rse = {'UKI-LT2-IC-HEP-disk': 'UKI-LT2-IC-HEP-DISK'}
    return se2rse

  def __getDid(self, lfn):
    """
    Convert a Dirac-style lfn to Rucio Did dictionary.
    :param lfn: Dirac lfn
    :type lfn: str
    :return: Rucio Did
    :rtype: dict
    """

    vo, scope, name = self.__getLfnElements(lfn)
    return {'scope': scope, 'name': name}
