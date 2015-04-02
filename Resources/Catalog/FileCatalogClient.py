########################################################################
# $HeadURL$
########################################################################
""" The FileCatalogClient is a class representing the client of the DIRAC File Catalog  """ 

__RCSID__ = "$Id$"

from types import ListType, DictType
import os
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.Client import Client
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOMSAttributeForGroup, getDNForUsername

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


  def setReplicaProblematic( self, lfns, revert = False ):
    """
      Set replicas to problematic.
      :param lfn lfns has to be formated this way :
                  { lfn : { se1 : pfn1, se2 : pfn2, ...}, ...}
      :param revert If True, remove the problematic flag

      :return { successful : { lfn : [ ses ] } : failed : { lfn : { se : msg } } }
    """

    # This method does a batch treatment because the setReplicaStatus can only take one replica per lfn at once
    #
    # Illustration :
    #
    # lfns {'L2': {'S1': 'P3'}, 'L3': {'S3': 'P5', 'S2': 'P4', 'S4': 'P6'}, 'L1': {'S2': 'P2', 'S1': 'P1'}}
    #
    # loop1: lfnSEs {'L2': ['S1'], 'L3': ['S3', 'S2', 'S4'], 'L1': ['S2', 'S1']}
    # loop1 : batch {'L2': {'Status': 'P', 'SE': 'S1', 'PFN': 'P3'}, 'L3': {'Status': 'P', 'SE': 'S4', 'PFN': 'P6'}, 'L1': {'Status': 'P', 'SE': 'S1', 'PFN': 'P1'}}
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

    status = 'AprioriGood' if revert else 'Trash'

    # { lfn : [ se1, se2, ...], ...}
    lfnsSEs = dict( ( lfn, [se for se in lfns[lfn]] ) for lfn in lfns )

    while lfnsSEs:

      # { lfn : { 'SE' : se1, 'PFN' : pfn1, 'Status' : status }, ... }
      batch = {}

      for lfn in lfnsSEs.keys():
        # If there are still some Replicas (SE) for the given LFN, we put it in the next batch
        # else we remove the entry from the lfnsSEs dict
        if lfnsSEs[lfn]:
          se = lfnsSEs[lfn].pop()
          batch[lfn] = { 'SE' : se, 'PFN' : lfns[lfn][se], 'Status' : status }
        else:
          del lfnsSEs[lfn]

      # Happens when there is nothing to treat anymore
      if not batch:
        break

      res = self.setReplicaStatus( batch )
      if not res['OK']:
        for lfn in batch:
          failed.setdefault( lfn, {} )[batch[lfn]['SE']] = res['Message']
        continue

      for lfn in res['Value']['Failed']:
        failed.setdefault( lfn, {} )[batch[lfn]['SE']] = res['Value']['Failed'][lfn]

      for lfn in res['Value']['Successful']:
        successful.setdefault( lfn, [] ).append( batch[lfn]['SE'] )

    return S_OK( {'Successful' : successful, 'Failed': failed} )


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
          lfn = os.path.join( path, os.path.basename( fname ) )
          entryDict[lfn] = detailsDict
    return result      

  def getDirectoryMetadata( self, lfns, rpc='', url='', timeout=120):
    ''' Get standard directory metadata
    '''
    rpcClient = self._getRPC(rpc=rpc, url=url, timeout=timeout)
    result = rpcClient.getDirectoryMetadata( lfns )
    if not result['OK']:
      return result
    # Add some useful fields
    for path in result['Value']['Successful']:
      owner = result['Value']['Successful'][path]['Owner']
      group = result['Value']['Successful'][path]['OwnerGroup']
      res = getDNForUsername( owner )
      if result['OK']:
        result['Value']['Successful'][path]['OwnerDN'] = res['Value'][0]
      else:
        result['Value']['Successful'][path]['OwnerDN'] = ''
      result['Value']['Successful'][path]['OwnerDRole'] = getVOMSAttributeForGroup( group )
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
    result = rpcClient.getDirectoryUserMetadata(directory)
    if not result['OK']:
      return result
    fmeta.update(result['Value'])
    
    return S_OK(fmeta)
        
    
  
  
  
