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