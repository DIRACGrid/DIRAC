########################################################################
# $HeadURL:  $
########################################################################

""" DIRAC FileCatalog plugin class to manage file metadata. This contains only
    non-indexed metadata for the moment.
"""

__RCSID__ = "$Id:  $"

import time, os, types
from DIRAC import S_OK, S_ERROR

class FileMetadata:

  def __init__(self,database = None):
          
    self.db = database
    
  def setDatabase( self, database ):
    self.db = database
        
##############################################################################
#
#  Manage Metadata fields
#
##############################################################################  

  def setMetadata(self, path,metadataDict, credDict):
    """ General metadata setting method
    """      
    
    for metaName, metaValue in metadataDict.items():
      result = self.setMetaParameter( path, metaName, metaValue, credDict )   
      if not result['OK']:
        return result

    return S_OK()         

  def setMetaParameter( self, path, metaName, metaValue, credDict ):
    """ Set an meta parameter - metadata which is not used in the the data
        search operations
    """
    result = self.db.fileManager._findFiles( [path] )
    if not result['OK']:
      return result
    if result['Value']['Successful']:
      fileID = result['Value']['Successful'][path]['FileID']
    else:
      return S_ERROR('File not found')           

    result = self.db._insert( 'FC_FileMeta',
                          ['FileID', 'MetaKey', 'MetaValue'],
                          [fileID, metaName, str( metaValue )] )
    return result

  def getFileUserMetadata(self, path, credDict ):
    """ Get metadata for the given file
    """
    # Only non-indexed for the moment
    return self.getFileMetaParameters(path, credDict)
  
  def getFileMetaParameters( self, path, credDict ):
    """ Get meta parameters for the given file
    """
    
    result = self.db.fileManager._findFiles( [path] )
    if not result['OK']:
      return result          
    if result['Value']['Successful']:
       fileID = result['Value']['Successful'][path]['FileID']   
    else:
      return S_ERROR('File not found')     
    
    req = "SELECT FileID,MetaKey,MetaValue from FC_FileMeta where FileID=%d " % fileID
    result = self.db._query( req )
    if not result['OK']:
      return result
    if not result['Value']:
      return S_OK( {} )
    metaDict = {}
    for fileID, key, value in result['Value']:
      if metaDict.has_key( key ):
        if type( metaDict[key] ) == ListType:
          metaDict[key].append( value )
        else:
          metaDict[key] = [metaDict[key]].append( value )
      else:
        metaDict[key] = value

    return S_OK( metaDict )
  
  def getFileMetadataFields( self ):
    """  Get file metadata fields
    """
    
    # Use unindexed field for the moment, to be changed later A.T.
    req = "SELECT DISTINCT(MetaKey) FROM FC_FileMeta"
    result = self.db._query( req )
    if not result['OK']:
      return result 
    
    metaDict = {}
    for row in result['Value']:
      metaDict[row[0]] = "String"
      
    return S_OK(metaDict)  
  
  def filterByFileMetadata( self,fileList,metaDict ):
    """ Filter the given file list by file metadata
    """
    
    result = self.getFileMetadataFields()
    if not result['OK']:
      return result 
    
    fileMetaInfo = result['Value']
    
    metaParameters = {}
    for meta in metaDict.keys():
      if meta in fileMetaInfo.keys():
        metaParameters.append[meta] = metaDict[meta]
      
    if not metaParameters:
      return S_OK(fileList)
    
    reqList = []
    for key,value in metaParameters.items():
      reqList.append( "%s='%s'" % (key,value) )
    condString = ' AND '.join(reqList)
    fileIDString = ','.join( [ str(x[0]) for x in fileList ] )
    
    req = "SELECT FileID FROM FC_FileMeta WHERE %s AND FileID in (%s)" % (condString,fileIDString)  
    result = self.db._query( req )
    if not result['OK']:
      return result 
    
    if not result['Value']:
      return S_OK([])
    
    selectedFiles = [ int(x[0]) for x in result['Value'] ]
    resultList = []
    for f in fileList:
      if f[0] in selectedFiles:
        resultList.append(f)
        
    return S_OK(resultList)      
  
  def findFilesByMetadata( self, metaDict, credDict ):
    """ Find Files satisfying the given metadata
    """

    result = self.db.dmeta.findDirectoriesByMetadata( metaDict, credDict )
    if not result['OK']:
      return result

    # Find 
    dirMetaInfo = result['Value']['DirectoryMetadataInfo']

    dirDict = result['Value']
    dirList = dirDict.keys()
    fileList = []
    result = self.db.dtree.getFilesInDirectory( dirList, credDict )
    if not result['OK']:
      return result
    
    result = self.filterByFileMetadata( self,result['Value'],metaDict )
    if not result['OK']:
      return result
    
    for fileID,dirID,fname in result['Value']:
      fileList.append( dirDict[dirID] + '/' + os.path.basename( fname ) )

    return S_OK( fileList )