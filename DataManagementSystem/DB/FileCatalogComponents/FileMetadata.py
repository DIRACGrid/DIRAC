########################################################################
# $HeadURL$
########################################################################

""" DIRAC FileCatalog plugin class to manage file metadata. This contains only
    non-indexed metadata for the moment.
"""

__RCSID__ = "$Id$"

# import time 
import types
from DIRAC import S_OK, S_ERROR, gLogger

from DIRAC.DataManagementSystem.DB.FileCatalogComponents.Utilities import queryTime
from DIRAC.Core.Utilities.List import intListToString

FILE_STANDARD_METAKEYS = [ 'SE', 'CreationDate', 'ModificationDate', 'LastAccessDate', 'User'
                           'Group', 'Path', 'Name' ]

class FileMetadata:

<<<<<<< HEAD
  _tables = {}
  _tables["FC_FileMeta"] = { "Fields": {
                                       "FileID": "INTEGER NOT NULL",
                                       "MetaKey": "VARCHAR(31) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT 'Noname'",
                                       "MetaValue": "VARCHAR(31) NOT NULL DEFAULT 'Noname'"
                                      },
                            "UniqueIndexes": { "FileID": ["MetaKey"] }
                          }
  
  _tables["FC_FileMetaFields"] = { "Fields": {
                                              "MetaID": "INT AUTO_INCREMENT",
                                              "MetaName": "VARCHAR(64) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL",
                                              "MetaType": "VARCHAR(128) NOT NULL"
                                             },
                                   "PrimaryKey": "MetaID"
                                 }

  def __init__(self,database = None):
    self.db = None
    if database is not None:
      self.setDatabase( database )

  def setDatabase( self, database ):
    self.db = database
    result = self.db._createTables( self._tables )
    if not result['OK']:
      gLogger.error( "Failed to create tables", str( self._tables.keys() ) )
    elif result['Value']:
      gLogger.info( "Tables created: %s" % ','.join( result['Value'] ) )  
    return result
        
=======
  def __init__( self, database = None ):

    self.db = database

  def setDatabase( self, database ):
    self.db = database

>>>>>>> rel-v6r12
##############################################################################
#
#  Manage Metadata fields
#
##############################################################################  
  def addMetadataField( self, pname, ptype, credDict ):
    """ Add a new metadata parameter to the Metadata Database.
        pname - parameter name, ptype - parameter type in the MySQL notation
    """

    if pname in FILE_STANDARD_METAKEYS:
      return S_ERROR( 'Illegal use of reserved metafield name' )

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
      return S_ERROR( 'File %s not found' % path )

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
      return S_ERROR( 'File %s not found' % path )

    failedMeta = {}
    for meta in metadata:
      if meta in metaFields:
        # Indexed meta case
        req = "DELETE FROM FC_FileMeta_%s WHERE FileID=%d" % ( meta, fileID )
        result = self.db._update( req )
        if not result['OK']:
          failedMeta[meta] = result['Value']
      else:
        # Meta parameter case
        req = "DELETE FROM FC_FileMeta WHERE MetaKey='%s' AND FileID=%d" % ( meta, fileID )
        result = self.db._update( req )
        if not result['OK']:
          failedMeta[meta] = result['Value']

    if failedMeta:
      metaExample = failedMeta.keys()[0]
      result = S_ERROR( 'Failed to remove %d metadata, e.g. %s' % ( len( failedMeta ), failedMeta[metaExample] ) )
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
      return S_ERROR( 'File not found' )
    return S_OK( fileID )

  def __setFileMetaParameter( self, fileID, metaName, metaValue, credDict ):
    """ Set an meta parameter - metadata which is not used in the the data
        search operations
    """
    result = self.db._insert( 'FC_FileMeta',
                          ['FileID', 'MetaKey', 'MetaValue'],
                          [fileID, metaName, str( metaValue )] )
    return result

  def setFileMetaParameter( self, path, metaName, metaValue, credDict ):

    result = self.__getFileID( path )
    if not result['OK']:
      return result
    fileID = result['Value']
    return self.__setFileMetaParameter( fileID, metaName, metaValue, credDict )

  def _getFileUserMetadataByID( self, fileIDList, credDict, connection = False ):
    """ Get file user metadata for the list of file IDs
    """
    # First file metadata
    result = self.getFileMetadataFields( credDict )
    if not result['OK']:
      return result
    metaFields = result['Value']

    stringIDs = ','.join( [ '%s' % id_ for id_ in fileIDList ] )
    metaDict = {}
    for meta in metaFields:
      req = "SELECT Value,FileID FROM FC_FileMeta_%s WHERE FileID in (%s)" % ( meta, stringIDs )
      result = self.db._query( req, conn = connection )
      if not result['OK']:
        return result
      for value, fileID in result['Value']:
        metaDict.setdefault( fileID, {} )
        metaDict[fileID][meta] = value

    req = "SELECT FileID,MetaKey,MetaValue from FC_FileMeta where FileID in (%s)" % stringIDs
    result = self.db._query( req, conn = connection )
    if not result['OK']:
      return result
    for fileID, key, value in result['Value']:
      metaDict.setdefault( fileID, {} )
      metaDict[fileID][key] = value

    return S_OK( metaDict )

  def getFileUserMetadata( self, path, credDict ):
    """ Get metadata for the given file
    """
    # First file metadata
    result = self.getFileMetadataFields( credDict )
    if not result['OK']:
      return result
    metaFields = result['Value']

    result = self.__getFileID( path )
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

    result = self.getFileMetaParameters( path, credDict )
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
        if type( metaDict[key] ) == types.ListType:
          metaDict[key].append( value )
        else:
          metaDict[key] = [metaDict[key]].append( value )
      else:
        metaDict[key] = value

    return S_OK( metaDict )

  def getFileMetaParameters( self, path, credDict ):
    """ Get meta parameters for the given file
    """

    result = self.__getFileID( path )
    if not result['OK']:
      return result
    fileID = result['Value']

    return self.__getFileMetaParameters( fileID, credDict )

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

  def __createMetaSelection( self, meta, value, table = '' ):

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
            selectList.append( "%sValue IN (%s)" % ( table, vString ) )
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

    return S_OK( selectString )

  def __findFilesForMetaValue( self, meta, value, dirList ):
    """ Find files in the given list of directories corresponding to the given
        selection criteria
    """

    result = self.__createMetaSelection( meta, value, "M." )
    if not result['OK']:
      return result
    selectString = result['Value']

    dirString = ','.join( [ str( x ) for x in dirList] )

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

  def __findFilesForSE( self, se, dirList ):
    """ Find files in the given list of directories having replicas in the given se(s)
    """
    seList = se
    if type( se ) in types.StringTypes:
      seList = [se]
    seIDs = []
    for se in seList:
      result = self.db.seManager.getSEID( se )
      if not result['OK']:
        return result
      seIDs.append( result['Value'] )
    seString = intListToString( seIDs )
    dirString = intListToString( dirList )

    req = "SELECT F.FileID FROM FC_Files as F, FC_Replicas as R WHERE F.DirID IN (%s)" % dirString
    req += " AND R.SEID IN (%s) AND F.FileID=R.FileID" % seString
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


  def __findFilesForStandardMetaValue( self, meta, value, dirList ):
    """ Find files in the given list of directories corresponding to the given
        selection criteria using standard file metadata
    """
    return S_OK( [] )

  def __buildSEQuery( self, storageElement ):
    """  Return a tuple with table and condition to locate files in a given SE
    """
    if not storageElement:
      return S_OK( [] )
    result = self.db.seManager.getSEID( storageElement )
    if not result['OK']:
      return result
    seID = result['Value']
    table = 'FC_Replicas'
    query = '%%s.SEID = %s' % seID
    return S_OK( [ ( table, query ) ] )

  def __buildUserMetaQuery( self, userMetaDict ):
    """  Return a list of tuples with tables and conditions to locate files for a given user Metadata
    """
    if not userMetaDict:
      return S_OK( [] )
    result = []
    for meta, value in userMetaDict.items():
      table = 'FC_FileMeta_%s' % meta

      if type( value ) in types.StringTypes and value.tolower() == 'any':
        # 'ANY' 
        query = ''
        result.append( ( table, query ) )

      elif type( value ) == types.ListType:
        if not value:
          query = ''
          result.append( ( table, query ) )
        else:
          escapeValues = self.db._escapeValues( value )
          if not escapeValues['OK']:
            return escapeValues
          query = '%%s.Value IN ( %s )' % ', '.join( escapeValues['Value'] )
          result.append( ( table, query ) )

      elif type( value ) == types.DictType:
        for operation, operand in value.items():
          if type( operand ) == types.ListType:
            escapeValues = self.db._escapeValues( value )
            if not escapeValues['OK']:
              return escapeValues
            escapedOperand = ', '.join( escapeValues['Value'] )
          if type( operand ) in [types.IntType, types.LongType]:
            escapedOperand = '%d' % operand
          elif type( operand ) == types.FloatType:
            escapedOperand = '%f' % operand
          else:
            escapedOperand = self.db._escapeString( operand )
            if not escapedOperand['OK']:
              return escapedOperand
            escapedOperand = escapeValue['Value']

          if operation in ['>', '<', '>=', '<=']:
            if type( operand ) == types.ListType:
              return S_ERROR( 'Illegal query: list of values for comparison operation' )
            else:
              query = '%%s.Value %s %s' % ( operation, escapedOperand )
              result.append( ( table, query ) )
          elif operation == 'in' or operation == "=":
            if type( operand ) == types.ListType:
              query = '%%s.Value IN ( %s )' % escapedOperand
              result.append( ( table, query ) )
            else:
              query = '%%s.Value = %s' % escapedOperand
              result.append( ( table, query ) )
          elif operation == 'nin' or operation == "!=":
            if type( operand ) == types.ListType:
              query = '%%s.Value NOT IN ( %s )' % escapedOperand
              result.append( ( table, query ) )
            else:
              query = '%%s.Value != %s' % escapedOperand
              result.append( ( table, query ) )

      else:
        escapedValue = self.db._escapeString( value )
        if not escapedValue['OK']:
          return escapedValue
        escapedValue = escapedValue['Value']
        query = '%%s.Value = %s' % escapedValue['Value']
        result.append( ( table, query ) )

    return S_OK( result )

  def __buildStandardMetaQuery( self, standardMetaDict ):

    result = []
    return S_OK( result )

  def __findFilesByMetadata( self, metaDict, dirList, credDict ):
    """ Find a list of file IDs meeting the metaDict requirements and belonging
        to directories in dirList 
    """

    # 1.- classify Metadata keys
    storageElement = None
    standardMetaDict = {}
    userMetaDict = {}
    for meta, value in metaDict.items():
      if meta == "SE":
        storageElement = value
      elif meta in FILE_STANDARD_METAKEYS:
        standardMetaDict[meta] = value
      else:
        userMetaDict[meta] = value

    tablesAndConditions = []
    # 2.- standard search
    result = self.__buildStandardMetaQuery( standardMetaDict )
    if not result['OK']:
      return result
    tablesAndConditions.extend( result['Value'] )
    # 3.- user search
    result = self.__buildUserMetaQuery( userMetaDict )
    if not result['OK']:
      return result
    tablesAndConditions.extend( result['Value'] )
    # 4.- SE constrain
    result = self.__buildSEQuery( storageElement )
    if not result['OK']:
      return result
    tablesAndConditions.extend( result['Value'] )

    query = 'SELECT F.FileID FROM '

    conditions = []
    tables = [ 'FC_Files as F' ]

    if dirList:
      dirString = intListToString( dirList )
      conditions.append( "F.DirID in (%s)" % dirString )

    counter = 0
    for table, condition in tablesAndConditions:
      counter += 1
      tables.append( '%s as M%d' % ( table, counter ) )
      table = 'M%d' % counter
      condition = condition % table + ' AND F.FileID = %s.FileID' % table
      conditions.append( '( %s )' % condition )

    query += ', '.join( tables )
    if conditions:
      query += ' WHERE %s' % ' AND '.join( conditions )

    result = self.db._query( query )
    if not result['OK']:
      return result
    if not result['Value']:
      return S_OK( [] )

    fileList = [ row[0] for row in result['Value' ] ]
    fileList = []
    for row in result['Value']:
      fileID = row[0]
      fileList.append( fileID )

    return S_OK( fileList )

  @queryTime
  def findFilesByMetadata( self, metaDict, path, credDict, extra = False ):
    """ Find Files satisfying the given metadata
    """
    if not path:
      path = '/'

    # 1.- Get Directories matching the metadata query
    result = self.db.dmeta.findDirIDsByMetadata( metaDict, path, credDict )
    if not result['OK']:
      return result
    dirList = result['Value']
    dirFlag = result['Selection']

    # 2.- Get known file metadata fields
    fileMetaDict = {}
    result = self.getFileMetadataFields( credDict )
    if not result['OK']:
      return result
    fileMetaKeys = result['Value'].keys() + FILE_STANDARD_METAKEYS
    fileMetaDict = dict( item for item in metaDict.items() if item[0] in fileMetaKeys )

    fileList = []
    lfnIdDict = {}
    lfnList = []

    if dirFlag != 'None':
      # None means that no Directory satisfies the given query, thus the search is empty
      if dirFlag == 'All':
        # All means that there is no Directory level metadata in query, full name space is considered
        dirList = []

      if fileMetaDict:
        # 3.- Do search in File Metadata
        result = self.__findFilesByMetadata( fileMetaDict, dirList, credDict )
        if not result['OK']:
          return result
        fileList = result['Value']
      elif dirList:
        # 4.- if not File Metadata, return the list of files in given directories
        return self.db.dtree.getFileLFNsInDirectoryByDirectory( dirList, credDict )
      else:
        # if there is no File Metadata and no Dir Metadata, return an empty list
        lfnList = []

    if fileList:
      # 5.- get the LFN
      result = self.db.fileManager._getFileLFNs( fileList )
      if not result['OK']:
        return result
      lfnList = result['Value']['Successful'].values()
      if extra:
        lfnIdDict = result['Value']['Successful']

    result = S_OK( lfnList )
    if extra:
      result['LFNIDDict'] = lfnIdDict

    return result
