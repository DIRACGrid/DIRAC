""" DIRAC FileCatalog Security Manager mix-in class for access check only on the directory level
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os

from DIRAC import S_OK
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.SecurityManager.SecurityManagerBase import SecurityManagerBase


class DirectorySecurityManager(SecurityManagerBase):

  def getPathPermissions(self, paths, credDict):
    """ Get path permissions according to the policy
    """

    toGet = dict(zip(paths, [[path] for path in paths]))
    permissions = {}
    failed = {}
    while toGet:
      res = self.db.dtree.getPathPermissions(list(toGet), credDict)
      if not res['OK']:
        return res
      for path, mode in list(res['Value']['Successful'].items()):
        for resolvedPath in toGet[path]:
          permissions[resolvedPath] = mode
        toGet.pop(path)
      for path, error in list(res['Value']['Failed'].items()):
        if error != 'No such file or directory':
          for resolvedPath in toGet[path]:
            failed[resolvedPath] = error
          toGet.pop(path)
      for path, resolvedPaths in list(toGet.items()):
        if path == '/':
          for resolvedPath in resolvedPaths:
            permissions[path] = {'Read': True, 'Write': True, 'Execute': True}
        if os.path.dirname(path) not in toGet:
          toGet[os.path.dirname(path)] = []
        toGet[os.path.dirname(path)] += resolvedPaths
        toGet.pop(path)

    if self.db.globalReadAccess:
      for path in permissions:
        permissions[path]['Read'] = True

    return S_OK({'Successful': permissions, 'Failed': failed})
