""" DIRAC FileCatalog Security Manager mix-in class for access check only on the directory level
    with a special treatment of the Delete operation
"""

__RCSID__ = "$Id$"

from DIRAC.DataManagementSystem.DB.FileCatalogComponents.SecurityManager.DirectorySecurityManager import \
    DirectorySecurityManager
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.SecurityManager.SecurityManagerBase import \
    _readMethods, _writeMethods


class DirectorySecurityManagerWithDelete(DirectorySecurityManager):
  """ This security manager implements a Delete operation.
       For Read, Write, Execute, it's behavior is the one of DirectorySecurityManager.
       For Delete, if the directory does not exist, we return True.
       If the directory exists, then we test the Write permission

  """

  def hasAccess(self, opType, paths, credDict):
    # The other SecurityManager do not support the Delete operation,
    # and it is transformed in Write
    # so we keep the original one

    if opType in ['removeFile', 'removeReplica', 'removeDirectory']:
      self.opType = 'Delete'
    elif opType in _readMethods:
      self.opType = 'Read'
    elif opType in _writeMethods:
      self.opType = 'Write'

    res = super(DirectorySecurityManagerWithDelete, self).hasAccess(opType, paths, credDict)

    # We reinitialize self.opType in case someone would call getPathPermissions directly
    self.opType = ''

    return res
