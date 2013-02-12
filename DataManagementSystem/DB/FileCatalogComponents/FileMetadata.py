########################################################################
# $HeadURL$
########################################################################

""" DIRAC FileCatalog plugin class to manage file metadata. This contains only
    non-indexed metadata for the moment.
"""

__RCSID__ = "$Id$"

import time, os, types
from DIRAC import S_OK, S_ERROR
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.Utilities import queryTime

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

    result = self.db.dmeta.getMetadataFields( credDict )
    if not result['OK']:
      return result
    if pname in result['Value'].keys():
      return S_ERROR( 'The metadata %s is already defined for Directories' % pname )

    result = self.getFileMetadataFields( credDict )
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
    error = ''
    if not result['OK']:
      error = result["Message"]
    req = "DELETE FROM FC_FileMetaFields WHERE MetaName='%s'" % pname
    result = self.db._update( req )
    if not result['OK']:
      if error:
        result["Message"] = error + "; " + result["Message"] 
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
    result = self.getFileMetadataFields( credDict )
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
  
  def removeMetadata( self, path, metadata, credDict ):
    """ Remove the specified metadata for the given file
    """
    result = self.getFileMetadataFields( credDict )
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
    
    failedMeta = {}
    for meta in metadata:
      if meta in metaFields:
        # Indexed meta case
        req = "DELETE FROM FC_FileMeta_%s WHERE FileID=%d" % (meta,fileID)
        result = self.db._update(req)
        if not result['OK']:
          failedMeta[meta] = result['Value']
      else:
        # Meta parameter case
        req = "DELETE FROM FC_FileMeta WHERE MetaKey='%s' AND FileID=%d" % (meta,fileID)
        result = self.db._update(req)
        if not result['OK']:
          failedMeta[meta] = result['Value']    
          
    if failedMeta:
      metaExample = failedMeta.keys()[0]
      result = S_ERROR('Failed to remove %d metadata, e.g. %s' % (len(failedMeta),failedMeta[metaExample]) )
      result['FailedMetadata'] = failedMeta
    else:
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

  def _getFileUserMetadataByID( self, fileIDList, credDict, connection=False ):
    """ Get file user metadata for the list of file IDs
    """
    # First file metadata
    result = self.getFileMetadataFields( credDict )
    if not result['OK']:
      return result
    metaFields = result['Value']

    stringIDs = ','.join( [ '%s' % id for id in fileIDList ] )
    metaDict = {}
    for meta in metaFields:
      req = "SELECT Value,FileID FROM FC_FileMeta_%s WHERE FileID in (%s)" % ( meta, stringIDs )
      result = self.db._query( req, conn=connection )
      if not result['OK']:
        return result
      for value, fileID in result['Value']:
        metaDict.setdefault( fileID, {} )
        metaDict[fileID][meta] = value      
      
    req = "SELECT FileID,MetaKey,MetaValue from FC_FileMeta where FileID in (%s)" % stringIDs
    result = self.db._query( req, conn=connection )
    if not result['OK']:
      return result  
    for fileID,key,value in result['Value']:
      metaDict.setdefault( fileID, {} )
      metaDict[fileID][key] = value      
        
    return S_OK( metaDict )

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

  def __createMetaSelection( self,meta,value,table='' ):

    if type( value ) == types.DictType:
      selectList = []
      for operation, operand in value.items():
        if operation in ['>', '<', '>=', '<=']:
          if type( operand ) == types.ListType:
            return S_ERROR( 'Illegal query: list of values for comparison operation' )
          if type( operand ) in [types.IntType, types.LongType]:
            selectList.append( "%sValue%s%d" % ( table, operation, operand ) )
          elif type( operand ) == types.FloatType:
            selectList.append( "%sValue%s%f" % ( table, operation, operand ) )
          else:
            selectList.append( "%sValue%s'%s'" % ( table, operation, operand ) )
        elif operation == 'in' or operation == "=":
          if type( operand ) == types.ListType:
            vString = ','.join( [ "'" + str( x ) + "'" for x in operand] )
            selectList.append( "%sValue IN (%s)" % ( table, vString) )
          else:
            selectList.append( "%sValue='%s'" % ( table, operand ) )
        elif operation == 'nin' or operation == "!=":
          if type( operand ) == types.ListType:
            vString = ','.join( [ "'" + str( x ) + "'" for x in operand] )
            selectList.append( "%sValue NOT IN (%s)" % ( table, vString ) )
          else:
            selectList.append( "%sValue!='%s'" % ( table, operand ) )
        selectString = ' AND '.join( selectList )
    elif type( value ) == types.ListType:
      vString = ','.join( [ "'" + str( x ) + "'" for x in value] )
      selectString = "%sValue in %s" % ( table, vString )
    else:
      if value == "Any":
        selectString = ''
      else:
        selectString = "%sValue='%s' " % ( table, value )

    return S_OK(selectString)

  def __findFilesForMetaValue( self, meta, value, dirList ):
    """ Find files in the given list of directories corresponding to the given
        selection criteria
    """

    result = self.__createMetaSelection( meta, value, "M." )
    if not result['OK']:
      return result
    selectString = result['Value']

    dirString = ','.join([ str(x) for x in dirList])

    req = " SELECT F.FileID, F.DirID FROM FC_FileMeta_%s AS M, FC_Files AS F" % meta
    if dirString:
      req += " WHERE F.DirID in (%s)" % dirString
    if selectString:
      if dirString:
        req += " AND %s AND F.FileID=M.FileID" % selectString
      else:
        req += " WHERE %s AND F.FileID=M.FileID" % selectString


    result = self.db._query( req )
    if not result['OK']:
      return result
    if not result['Value']:
      return S_OK( [] )

    fileList = []
    for row in result['Value']:
      fileID = row[0]
      fileList.append( fileID )

    return S_OK( fileList )


  def __findFilesByMetadata( self,metaDict,dirList,credDict ):
    """ Find a list of file IDs meeting the metaDict requirements and belonging
        to directories in dirList 
    """

    fileList = []
    first = True    
    for meta,value in metaDict.items():
      result = self.__findFilesForMetaValue( meta,value,dirList )
      if not result['OK']:
        return result
      mList = result['Value']
      if first:
        fileList = mList
        first = False
      else:
        newList = []
        for f in fileList:
          if f in mList:
            newList.append( f )
        fileList = newList

    return S_OK(fileList)

  @queryTime
  def findFilesByMetadata( self, metaDict, path, credDict ):
    """ Find Files satisfying the given metadata
    """

    start = time.time()

    if not path:
      path = '/'

    result = self.db.dmeta.findDirIDsByMetadata( metaDict, path, credDict )
    if not result['OK']:
      return result
    dirList = result['Value']
    dirFlag = result['Selection']

    # Find files by metadata
    result = self.getFileMetadataFields( credDict )
    if not result['OK']:
      return result

    fileMetaDict = {}
    for key,value in metaDict.items():
      if key in result['Value']:
        fileMetaDict[key] = value

    fileList = []
    lfnList = []

    if dirFlag == "None":
      return S_OK([])
    elif dirFlag == "All":
      result = self.__findFilesByMetadata( fileMetaDict, [], credDict )
      if not result['OK']:
        return result
      fileList = result['Value']
      if fileList:
        result = self.db.fileManager._getFileLFNs(fileList)
        lfnList = [ x[1] for x in result['Value']['Successful'].items() ]   
        return S_OK(lfnList)  
      else:
        return S_OK([])

    if fileMetaDict:
      result = self.__findFilesByMetadata( fileMetaDict,dirList,credDict )
      if not result['OK']:
        return result
      fileList = result['Value']
    else:
      result = self.db.dtree.getFileLFNsInDirectoryByDirectory( dirList, credDict )
      return result

    if fileList:
      result = self.db.fileManager._getFileLFNs(fileList)
      lfnList = [ x[1] for x in result['Value']['Successful'].items() ]

    return S_OK( lfnList ) 
