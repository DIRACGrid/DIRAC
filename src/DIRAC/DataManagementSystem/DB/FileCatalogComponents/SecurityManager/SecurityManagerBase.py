""" DIRAC FileCatalog Security Manager base class
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security.Properties import FC_MANAGEMENT

_readMethods = ['exists', 'isFile', 'getFileSize', 'getFileMetadata',
                'getReplicas', 'getReplicaStatus', 'getFileAncestors',
                'getFileDescendents', 'listDirectory', 'isDirectory',
                'getDirectoryReplicas', 'getDirectorySize', 'getDirectoryMetadata']

_writeMethods = ['changePathOwner', 'changePathGroup', 'changePathMode',
                 'addFile', 'setFileStatus', 'removeFile', 'addReplica',
                 'removeReplica', 'setReplicaStatus', 'setReplicaHost',
                 'addFileAncestors', 'createDirectory', 'removeDirectory',
                 'setMetadata', '__removeMetadata']


class SecurityManagerBase(object):

  def __init__(self, database=None):
    self.db = database

  def setDatabase(self, database):
    self.db = database

  def getPathPermissions(self, paths, credDict):
    """ Get path permissions according to the policy
    """
    return S_ERROR('The getPathPermissions method must be implemented in the inheriting class')

  def hasAccess(self, opType, paths, credDict):
    # Map the method name to Read/Write
    if opType in _readMethods:
      opType = 'Read'
    elif opType in _writeMethods:
      opType = 'Write'

    # Check if admin access is granted first
    result = self.hasAdminAccess(credDict)
    if not result['OK']:
      return result
    if result['Value']:
      # We are admins, allow everything
      permissions = {}
      for path in paths:
        permissions[path] = True
      return S_OK({'Successful': permissions, 'Failed': {}})

    successful = {}
    failed = {}
    if not opType.lower() in ['read', 'write', 'execute']:
      return S_ERROR("Operation type not known")
    if self.db.globalReadAccess and (opType.lower() == 'read'):
      for path in paths:
        successful[path] = True
      resDict = {'Successful': successful, 'Failed': {}}
      return S_OK(resDict)

    result = self.getPathPermissions(paths, credDict)
    if not result['OK']:
      return result

    permissions = result['Value']['Successful']
    for path, permDict in permissions.items():
      if permDict[opType]:
        successful[path] = True
      else:
        successful[path] = False

    failed.update(result['Value']['Failed'])

    resDict = {'Successful': successful, 'Failed': failed}
    return S_OK(resDict)

  def hasAdminAccess(self, credDict):
    if FC_MANAGEMENT in credDict['properties']:
      return S_OK(True)
    return S_OK(False)
