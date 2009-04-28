########################################################################
# $Id: PoolXMLFile.py,v 1.3 2009/04/28 08:50:33 acsmith Exp $
########################################################################
""" The POOL XML File module provides a means to extract the GUID of a file or list
    of files by searching for an appropriate POOL XML Catalog in the specified directory.
"""

__RCSID__ = "$Id: PoolXMLFile.py,v 1.3 2009/04/28 08:50:33 acsmith Exp $"

import os,glob,re,tarfile,string

from DIRAC.DataManagementSystem.Client.PoolXMLCatalog import PoolXMLCatalog
from DIRAC.Core.Utilities.List                        import uniqueElements
from DIRAC.Core.Utilities.File                        import makeGuid
from DIRAC                                            import S_OK, S_ERROR, gLogger, gConfig

#############################################################################
def getGUID(fileNames,directory=''):
  """ This function searches the directory for POOL XML catalog files and extracts the GUID.

      fileNames can be a string or a list, directory defaults to PWD.
  """
  if not directory:
    directory = os.getcwd()

  if not os.path.isdir(directory):
    return S_ERROR('%s is not a directory' %directory)

  if not type(fileNames)==type([]):
    fileNames = [fileNames]

  gLogger.verbose('Will look for POOL XML Catalog GUIDs in %s for %s' %(directory,string.join(fileNames,', ')))
  patterns = ['*.xml','*.xml*gz']
  omissions = ['\.bak$'] # to be ignored for production files

  #First obtain valid list of unpacked catalog files in directory
  poolCatalogList = []

  for pattern in patterns:
    fileList = glob.glob(os.path.join(directory,pattern))
    for fname in fileList:
      if fname.endswith('.bak'):
        gLogger.verbose('Ignoring BAK file: %s' %fname)
      elif tarfile.is_tarfile(fname):
        try:
          gLogger.debug('Unpacking catalog XML file %s' %(os.path.join(directory,fname)))
          tarFile = tarfile.open(os.path.join(directory,fname),'r')
          for member in tarFile.getmembers():
            tarFile.extract(member,directory)
            poolCatalogList.append(os.path.join(directory,member.name))
        except Exception,x :
          gLogger.error('Could not untar %s with exception %s' %(fname,str(x)) )
      else:
        poolCatalogList.append(fname)

  poolCatalogList = uniqueElements(poolCatalogList)

  #Now have list of all XML files but some may not be Pool XML catalogs...
  finalCatList = []
  for possibleCat in poolCatalogList:
    try:
      cat = PoolXMLCatalog(possibleCat)
      finalCatList.append(possibleCat)
    except Exception,x:
      gLogger.debug('Ignoring non-POOL catalogue file %s' %possibleCat)

  #Create POOL catalog with final list of catalog files and extract GUIDs
  generated = []
  pfnGUIDs = {}
  gLogger.debug('Final list of catalog files are: %s' %string.join(finalCatList,', '))
  catalog = PoolXMLCatalog(finalCatList)
  for fname in fileNames:
    guid = str(catalog.getGuidByPfn(fname))
    if not guid:
      guid = makeGuid()
      generated.append(fname)

    pfnGUIDs[fname]=guid

  if not generated:
    gLogger.info('Found GUIDs from POOL XML Catalogue for all files: %s' %string.join(fileNames,', '))
  else:
    gLogger.info('GUIDs not found from POOL XML Catalogue (and were generated) for: %s' %string.join(generated,', '))

  result = S_OK(pfnGUIDs)
  result['directory']=directory
  result['generated']=generated
  return result
