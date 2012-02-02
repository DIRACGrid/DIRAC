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
  def addMetadataField( self, pname, ptype, credDict ):
    """ Add a new metadata parameter to the Metadata Database.
        pname - parameter name, ptype - parameter type in the MySQL notation
    """

    result = self.getMetadataFields( credDict )
    if not result['OK']:
      return result
    if pname in result['Value'].keys():
      if ptype.lower() == result['Value'][pname].lower():
        return S_OK( 'Already exists' )
      else:
        return S_ERROR( 'Attempt to add an existing metadata with different type: %s/%s' %
                        ( ptype, result['Value'][pname] ) )

    valueType = ptype
    if ptype == "MetaSet":
      valueType = "VARCHAR(64)"
    req = "CREATE TABLE FC_FileMeta_%s ( FileID INTEGER NOT NULL, Value %s, PRIMARY KEY (FileID), INDEX (Value) )" \
                              % ( pname, valueType )
    result = self.db._query( req )
    if not result['OK']:
      return result

    result = self.db._insert( 'FC_FileMetaFields', ['MetaName', 'MetaType'], [pname, ptype] )
    if not result['OK']:
      return result

    metadataID = result['lastRowId']
    result = self.__transformMetaParameterToData( pname )
    if not result['OK']:
      return result

    return S_OK( "Added new metadata: %d" % metadataID )

  def deleteMetadataField( self, pname, credDict ):
    """ Remove metadata field
    """

    req = "DROP TABLE FC_FileMeta_%s" % pname
    result = self.db._update( req )
    return result

  def getFileMetadataFields( self, credDict ):
    """ Get all the defined metadata fields
    """

    req = "SELECT MetaName,MetaType FROM FC_FileMetaFields"
    result = self.db._query( req )
    if not result['OK']:
      return result

    metaDict = {}
    for row in result['Value']:
      metaDict[row[0]] = row[1]

    return S_OK( metaDict )                 
          
###########################################################
#
# Set and get metadata for files
#
###########################################################
          
  def setMetadata( self, path, metadict, credDict ):
    """ Set the value of a given metadata field for the the given directory path
    """
    result = self.getMetadataFields( credDict )
    if not result['OK']:
      return result
    metaFields = result['Value']
    
    result = self.db.fileManager._findFiles( [path] )
    if not result['OK']:
      return result
    if result['Value']['Successful']:
      fileID = result['Value']['Successful'][path]['FileID']
    else:
      return S_ERROR('File %s not found' % path)  

    for metaName, metaValue in metadict.items():
      if not metaName in metaFields:
        result = self.__setFileMetaParameter( fileID, metaName, metaValue, credDict )
      else:
        result = self.db._insert( 'FC_FileMeta_%s' % metaName, ['FileID', 'Value'], [fileID, metaValue] )
        if not result['OK']:
          if result['Message'].find( 'Duplicate' ) != -1:
            req = "UPDATE FC_FileMeta_%s SET Value='%s' WHERE FileID=%d" % ( metaName, metaValue, fileID )
            result = self.db._update( req )
            if not result['OK']:
              return result
          else:
            return result

    return S_OK()     
  
  def __getFileID( self, path ):
    
    result = self.db.fileManager._findFiles( [path] )
    if not result['OK']:
      return result
    if result['Value']['Successful']:
      fileID = result['Value']['Successful'][path]['FileID']
    else:
      return S_ERROR('File not found') 
    return S_OK(fileID)

  def __setFileMetaParameter( self, fileID, metaName, metaValue, credDict ):
    """ Set an meta parameter - metadata which is not used in the the data
        search operations
    """
    result = self.db._insert( 'FC_FileMeta',
                          ['FileID', 'MetaKey', 'MetaValue'],
                          [fileID, metaName, str( metaValue )] )
    return result
  
  def setFileMetaParameter( self, path, metaName, metaValue, credDict ):

    result = self.__getFileID(path)
    if not result['OK']:
      return result
    fileID = result['Value']
    return self.__setFileMetaParameter( fileID, metaName, metaValue, credDict )

  def getFileUserMetadata(self, path, credDict ):
    """ Get metadata for the given file
    """
    # First file metadata
    result = self.getFileMetadataFields( credDict )
    if not result['OK']:
      return result
    metaFields = result['Value']

    result = self.__getFileID(path)
    if not result['OK']:
      return result
    fileID = result['Value']

    metaDict = {}
    metaTypeDict = {}
    for meta in metaFields:
      req = "SELECT Value,FileID FROM FC_FileMeta_%s WHERE FileID=%d" % ( meta, fileID )
      result = self.db._query( req )
      if not result['OK']:
        return result
      if result['Value']:
        metaDict[meta] = result['Value'][0][0]
      metaTypeDict[meta] = metaFields[meta]      
      
    result = self.getFileMetaParameters(path, credDict)
    if result['OK']:
      metaDict.update( result['Value'] )
      for meta in result['Value']:
        metaTypeDict[meta] = 'NonSearchable'
        
    result = S_OK( metaDict )
    result['MetadataType'] = metaTypeDict
    return result
  
  def __getFileMetaParameters( self, fileID, credDict ):
  
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
  
  def getFileMetaParameters( self, path, credDict ):
    """ Get meta parameters for the given file
    """
    
    result = self.__getFileID(path)
    if not result['OK']:
      return result
    fileID = result['Value']   
    
    return self.__getFileMetaParameters( fileID,credDict )
  
  def __transformMetaParameterToData( self, metaname ):
    """ Relocate the meta parameters of all the directories to the corresponding
        indexed metadata table
    """

    req = "SELECT FileID,MetaValue from FC_FileMeta WHERE MetaKey='%s'" % metaname
    result = self.db._query( req )
    if not result['OK']:
      return result
    if not result['Value']:
      return S_OK()

    fileDict = {}
    for fileID, meta in result['Value']:
      fileDict[fileID] = meta
    fileList = fileDict.keys()

    insertValueList = []
    for fileID in fileList:
      insertValueList.append( "( %d,'%s' )" % ( fileID, meta ) )

    req = "INSERT INTO FC_FileMeta_%s (FileID,Value) VALUES %s" % ( metaname, ', '.join( insertValueList ) )
    result = self.db._update( req )
    if not result['OK']:
      return result

    req = "DELETE FROM FC_FileMeta WHERE MetaKey='%s'" % metaname
    result = self.db._update( req )
    return result

  def __findFilesByMetadata( self,metaDict,dirList,credDict ):

    result = self.getFileMetadataFields( credDict )
    if not result['OK']:
      return result

    fileMetaInfo = result['Value']

    metaParameters = {}
    for meta in metaDict.keys():
      if meta in fileMetaInfo.keys():
        metaParameters[meta] = metaDict[meta]

    if not metaParameters:
      return S_OK(fileList)

    reqList = []
    for key,value in metaParameters.items():
      reqList.append( "MetaKey='%s' AND MetaValue='%s'" % (key,value) )
    condString = ' AND '.join(reqList)
    #fileIDString = ','.join( [ str(x[0]) for x in fileList ] )

    req = "SELECT FileID FROM FC_FileMeta WHERE %s" % condString
    #if fileList:
    #  req += " AND FileID in (%s)" % fileIDString

    print "AT >>> __findFilesByMetadata req", req

    result = self.db._query( req )
    if not result['OK']:
      return result

    if not result['Value']:
      return S_OK([])

    fileIDList = [ int(x[0]) for x in result['Value'] ] 
    return S_OK(fileIDList)

  
  def filterByFileMetadata( self,fileList,metaDict,path,credDict ):
    """ Filter the given file list by file metadata
    """
    
    result = self.__findFilesByMetadata( metaDict,path,credDict )
    if not result['OK']:
      return result

    selectedFiles = result['Value']

    print "AT >>> filterByFileMetadata/selectedFiles", selectedFiles, fileList


    resultList = []
    for f in fileList:
      if f[0] in selectedFiles:
        resultList.append(f)
        
    return S_OK(resultList)      
  
  def findFilesByMetadata( self, metaDict, path, credDict ):
    """ Find Files satisfying the given metadata
    """

    print "AT >>> findFilesByMetadata/path", metaDict, path

    result = self.db.dmeta.findDirectoriesByMetadata( metaDict, path, credDict )
    if not result['OK']:
      return result

    # Find 
    extraMetadata = result.get('ExtraMetadata',{})

    dirDict = result['Value']
    dirFlag = dirDict.get(0) 
    if dirFlag == "None":
      return S_OK([])
    elif dirFlag == "All":
      result = self.__findFilesByMetadata( extraMetadata, path, credDict )
      if not result['OK']:
        return result
      fileList = result['Value']
      result = self.db.fileManager._getFileLFNs(fileList)

      print "AT >>> findFilesByMetadata _getFileLFNs", result
 
      lfnList = [ x[1] for x in result['Value']['Successful'].items() ]     
      return S_OK(lfnList)    
     
    dirList = dirDict.keys()
    fileList = []
    result = self.db.dtree.getFilesInDirectory( dirList, credDict )
    if not result['OK']:
      return result
    
    if extraMetadata:
      result = self.filterByFileMetadata( result['Value'],extraMetadata,path,credDict )
      if not result['OK']:
        return result
    
    for fileID,dirID,fname in result['Value']:
      fileList.append( dirDict[dirID] + '/' + os.path.basename( fname ) )

    return S_OK( fileList )
