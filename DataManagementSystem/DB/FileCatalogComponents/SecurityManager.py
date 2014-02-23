########################################################################
# $HeadURL$
########################################################################
""" DIRAC FileCatalog Security Manager mix-in class
"""

__RCSID__ = "$Id$"

import os
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security.Properties import FC_MANAGEMENT

class SecurityManagerBase:

  def __init__( self, database=None ):
    self.db = database

  def setDatabase( self, database ):
    self.db = database

  def getPathPermissions( self, paths, credDict ):
    """ Get path permissions according to the policy
    """
    return S_ERROR('The getPathPermissions method must be implemented in the inheriting class')

  def hasAccess(self,opType,paths,credDict):
    
    # Check if admin access is granted first
    result = self.hasAdminAccess( credDict )
    if not result['OK']:
      return result
    if result['Value']:
      # We are admins, allow everything
      permissions = {}
      for path in paths:
        permissions[path] = True
      return S_OK( {'Successful':permissions,'Failed':{}} )
    
    successful = {}
    failed = {}
    if not opType.lower() in ['read','write','execute']:
      return S_ERROR("Operation type not known")
    if self.db.globalReadAccess and (opType.lower() == 'read'):
      for path in paths:
        successful[path] = True
      resDict = {'Successful':successful,'Failed':{}}
      return S_OK(resDict)

    result = self.getPathPermissions(paths,credDict)
    if not result['OK']:
      return result

    permissions = result['Value']['Successful']
    for path,permDict in permissions.items():
      if permDict[opType]:
        successful[path] = True
      else:
        successful[path] = False
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def hasAdminAccess(self,credDict):
    if FC_MANAGEMENT in credDict['properties']:
      return S_OK(True)
    return S_OK(False)

class NoSecurityManager(SecurityManagerBase):

  def getPathPermissions(self,paths,credDict):
    """ Get path permissions according to the policy
    """

    permissions = {}
    for path in paths:
      permissions[path] = {'Read':True,'Write':True,'Execute':True}

    return S_OK( {'Successful':permissions,'Failed':{}} )

  def hasAccess(self,opType,paths,credDict):
    successful = {}
    for path in paths:
      successful[path] = True
    resDict = {'Successful':successful,'Failed':{}}
    return S_OK(resDict)

  def hasAdminAccess(self,credDict):
    return S_OK(True)

class DirectorySecurityManager(SecurityManagerBase):

  def getPathPermissions(self,paths,credDict):
    """ Get path permissions according to the policy
    """
    
    toGet = dict(zip(paths,[ [path] for path in paths ]))
    permissions = {}
    failed = {}
    while toGet:
      res = self.db.dtree.getPathPermissions(toGet.keys(),credDict)
      if not res['OK']:
        return res
      for path,mode in res['Value']['Successful'].items():
        for resolvedPath in toGet[path]:
          permissions[resolvedPath] = mode
        toGet.pop(path)
      for path,error in res['Value']['Failed'].items():
        if error != 'No such file or directory':
          for resolvedPath in toGet[path]:
            failed[resolvedPath] = error
          toGet.pop(path)
      for path,resolvedPaths in toGet.items():
        if path == '/':
          for resolvedPath in resolvedPaths:
            permissions[path] = {'Read':True,'Write':True,'Execute':True}
        if not toGet.has_key(os.path.dirname(path)):
          toGet[os.path.dirname(path)] = []
        toGet[os.path.dirname(path)] += resolvedPaths
        toGet.pop(path)

    if self.db.globalReadAccess:
      for path in permissions:
        permissions[path]['Read'] = True

    return S_OK( {'Successful':permissions,'Failed':failed} )

class FullSecurityManager(SecurityManagerBase):

  def getPathPermissions(self,paths,credDict):
    """ Get path permissions according to the policy
    """
    
    toGet = dict(zip(paths,[ [path] for path in paths ]))
    permissions = {}
    failed = {}
    res = self.db.fileManager.getPathPermissions(paths,credDict)
    if not res['OK']:
      return res
    for path,mode in res['Value']['Successful'].items():
      for resolvedPath in toGet[path]:
        permissions[resolvedPath] = mode
      toGet.pop(path)
    for path,resolvedPaths in toGet.items():
      if path == '/':
        for resolvedPath in resolvedPaths:
          permissions[path] = {'Read':True,'Write':True,'Execute':True}
      if not toGet.has_key(os.path.dirname(path)):
        toGet[os.path.dirname(path)] = []
      toGet[os.path.dirname(path)] += resolvedPaths
      toGet.pop(path)
    while toGet:
      paths = toGet.keys()
      res = self.db.dtree.getPathPermissions(paths,credDict)
      if not res['OK']:
        return res
      for path,mode in res['Value']['Successful'].items():
        for resolvedPath in toGet[path]:
          permissions[resolvedPath] = mode
        toGet.pop(path)
      for path,error in res['Value']['Failed'].items():
        if error != 'No such file or directory':
          for resolvedPath in toGet[path]:
            failed[resolvedPath] = error
          toGet.pop(path)
      for path,resolvedPaths in toGet.items():
        if path == '/':
          for resolvedPath in resolvedPaths:
            permissions[path] = {'Read':True,'Write':True,'Execute':True}
        if not toGet.has_key(os.path.dirname(path)):
          toGet[os.path.dirname(path)] = []
        toGet[os.path.dirname(path)] += resolvedPaths
        toGet.pop(path)

    if self.db.globalReadAccess:
      for path in permissions:
        permissions[path]['Read'] = True

    return S_OK( {'Successful':permissions,'Failed':failed} )
