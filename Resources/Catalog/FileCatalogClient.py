########################################################################
# $HeadURL$
########################################################################
""" The FileCatalogClient is a class representing the client of the DIRAC File Catalog  """ 

__RCSID__ = "$Id$"

from types import ListType, DictType
import os
from DIRAC                              import S_OK, S_ERROR
from DIRAC.Core.Base.Client             import Client

class FileCatalogClient(Client):
  """ Client code to the DIRAC File Catalogue
  """
  def __init__( self, url=None, **kwargs ):
    """ Constructor function.
    """
    Client.__init__( self, **kwargs )
    self.setServer('DataManagement/FileCatalog')
    if url:
      self.setServer(url)
    self.available = False
#    res = self.isOK()
#    if res['OK']:
#      self.available = res['Value']

  def isOK(self, rpc=None, url='', timeout=120):
    """ Check that the service is OK
    """
    if not self.available:
      rpcClient = self._getRPC(rpc=rpc, url=url, timeout=timeout)
      res = rpcClient.isOK()
      if not res['OK']:
        self.available = False
      else:
        self.available = True
    return S_OK(self.available)
    
  def getReplicas(self, lfns, allStatus=False, rpc='', url='', timeout=120):
    """ Get the replicas of the given files
    """
    rpcClient = self._getRPC(rpc=rpc, url=url, timeout=timeout)
    result = rpcClient.getReplicas(lfns, allStatus)
    if not result['OK']:
      return result
    
    lfnDict = result['Value']
    seDict = result['Value'].get( 'SEPrefixes', {} )
    for lfn in lfnDict['Successful']:
      for se in lfnDict['Successful'][lfn]:
        if not lfnDict['Successful'][lfn][se] and se in seDict:
          lfnDict['Successful'][lfn][se] = seDict[se] + lfn
      
    return S_OK( lfnDict )  

  def listDirectory(self, lfn, verbose=False, rpc='', url='', timeout=120):
    """ List the given directory's contents
    """
    rpcClient = self._getRPC(rpc=rpc, url=url, timeout=timeout)
    result = rpcClient.listDirectory(lfn, verbose)
    if not result['OK']:
      return result
    # Force returned directory entries to be LFNs
    for entryType in ['Files', 'SubDirs', 'Links']:
      for path in result['Value']['Successful']:
        entryDict = result['Value']['Successful'][path][entryType]
        for fname in entryDict.keys():
          detailsDict = entryDict.pop( fname )
          lfn = '%s/%s' % ( path, os.path.basename( fname ) )
          entryDict[lfn] = detailsDict
    return result      

  def removeDirectory(self, lfn, recursive=False, rpc='', url='', timeout=120):
    """ Remove the directory from the File Catalog. The recursive keyword is for the ineterface.
    """
    rpcClient = self._getRPC(rpc=rpc, url=url, timeout=timeout)
    return rpcClient.removeDirectory(lfn)

  def getDirectoryReplicas(self, lfns, allStatus=False, rpc='', url='', timeout=120):
    """ Find all the given directories' replicas
    """
    rpcClient = self._getRPC(rpc=rpc, url=url, timeout=timeout)
    result = rpcClient.getDirectoryReplicas(lfns, allStatus)
    if not result['OK']:
      return result
    
    seDict = result['Value'].get( 'SEPrefixes', {} )
    for path in result['Value']['Successful']:
      pathDict = result['Value']['Successful'][path]
      for fname in pathDict.keys():
        detailsDict = pathDict.pop( fname )
        lfn = '%s/%s' % ( path, os.path.basename( fname ) )
        for se in detailsDict:
          if not detailsDict[se] and se in seDict:
            detailsDict[se] = seDict[se] + lfn
        pathDict[lfn] = detailsDict
    return result      

  def findFilesByMetadata(self, metaDict, path='/', rpc='', url='', timeout=120):
    """ Find files given the meta data query and the path
    """
    rpcClient = self._getRPC(rpc=rpc, url=url, timeout=timeout)
    result = rpcClient.findFilesByMetadata(metaDict, path)
    if not result['OK']:
      return result
    if type(result['Value']) == ListType:
      return result
    elif type(result['Value']) == DictType:
      # Process into the lfn list
      fileList = []
      for dir_, fList in result['Value'].items():
        for f in fList:
          fileList.append( dir_+'/'+f )
      result['Value'] = fileList    
      return result
    else:
      return S_ERROR( 'Illegal return value type %s' % type( result['Value'] ) ) 
       
  def getFileUserMetadata(self, path, rpc='', url='', timeout=120):
    """Get the meta data attached to a file, but also to 
    the its corresponding directory
    """
    directory = "/".join(path.split("/")[:-1])
    rpcClient = self._getRPC(rpc=rpc, url=url, timeout=timeout)
    result = rpcClient.getFileUserMetadata(path)
    if not result['OK']:
      return result
    fmeta = result['Value']
    result = rpcClient.getDirectoryMetadata(directory)
    if not result['OK']:
      return result
    fmeta.update(result['Value'])
    
    return S_OK(fmeta)
        
    
  
  
  
