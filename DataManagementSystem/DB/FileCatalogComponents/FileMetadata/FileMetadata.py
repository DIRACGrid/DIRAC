""" DIRAC FileCatalog plugin class to manage file metadata. This contains only
    non-indexed metadata for the moment.
"""

__RCSID__ = "$Id$"

import six
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.Time import queryTime
from DIRAC.Core.Utilities.List import intListToString
from DIRAC.DataManagementSystem.Client.MetaQuery import FILE_STANDARD_METAKEYS, \
    FILES_TABLE_METAKEYS, \
    FILEINFO_TABLE_METAKEYS
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.MetaNameMixIn import MetaNameMixIn


class FileMetadata(MetaNameMixIn):
  def __init__(self, database=None):

    super(FileMetadata, self).__init__()
    self.db = database

  def setDatabase(self, database):
    self.db = database

  ##############################################################################
  #
  #  Manage Metadata fields
  #
  ##############################################################################
  def addMetadataField(self, pname, ptype, credDict):
    """
    Add a new metadata parameter to the Metadata Database.

    :param str pname: parameter name
    :param str ptype: parameter type in the MySQL notation
    :param dict credDict: client credential dictionary
    :return: standard Dirac result object
    """

    if pname in FILE_STANDARD_METAKEYS:
      return S_ERROR('Illegal use of reserved metafield name')

    result = self.db.dmeta.getMetadataFields(credDict)
    if not result['OK']:
      return result
    # existing pnames are fully qualified, so
    fqPname = self.getMetaName(pname, credDict)
    if fqPname in result['Value']:
      return S_ERROR('The metadata %s is already defined for Directories' % fqPname)

    result = self.getFileMetadataFields(credDict)
    if not result['OK']:
      return result
    if fqPname in result['Value']:
      if ptype.lower() == result['Value'][fqPname].lower():
        return S_OK('Already exists')
      else:
        return S_ERROR('Attempt to add an existing metadata with different type: %s/%s' %
                       (ptype, result['Value'][fqPname]))
    valueType = ptype
    if ptype == "MetaSet":
      valueType = "VARCHAR(64)"
    req = "CREATE TABLE FC_FileMeta_%s ( FileID INTEGER NOT NULL, Value %s, PRIMARY KEY (FileID), INDEX (Value) )" \
          % (fqPname, valueType)
    result = self.db._query(req)
    if not result['OK']:
      return result

    result = self.db.insertFields('FC_FileMetaFields', ['MetaName', 'MetaType'], [fqPname, ptype])
    if not result['OK']:
      return result

    metadataID = result['lastRowId']
    result = self.__transformMetaParameterToData(fqPname)
    if not result['OK']:
      return result

    return S_OK("Added new metadata: %d" % metadataID)

  def deleteMetadataField(self, pname, credDict):
    """
    Remove metadata field (only from user's own VO)

    :param str pname: parameter name
    :param dict credDict: client credential dictionary
    :return: standard Dirac result object
    """

    fqPname = self.getMetaName(pname, credDict)
    req = "DROP TABLE FC_FileMeta_%s" % fqPname
    result = self.db._update(req)
    error = ''
    if not result['OK']:
      error = result["Message"]
    req = "DELETE FROM FC_FileMetaFields WHERE MetaName='%s'" % fqPname
    result = self.db._update(req)
    if not result['OK']:
      if error:
        result["Message"] = error + "; " + result["Message"]
    return result

  def getFileMetadataFields(self, credDict, stripVO=False):
    """
    Get all the defined metadata fields.

    :param dict credDict: client credential dictionary
    :param bool stripVO: flag whether to return metadata name with
     or without a VO suffix
    :return: standard Dirac result object
    """

    req = "SELECT MetaName,MetaType FROM FC_FileMetaFields"
    result = self.db._query(req)
    if not result['OK']:
      return result

    metaDict = {}
    for row in result['Value']:
      metaDict[row[0]] = row[1]

    # strip the suffix, if required (for clients)
    if stripVO:
      metaDict = self.stripSuffix(metaDict, credDict)
    return S_OK(metaDict)

  ###########################################################
  #
  # Set and get metadata for files
  #
  ###########################################################

  def setMetadata(self, path, metadict, credDict):
    """
    Set the value of a given metadata field for the the given directory path.
    Modified to use fully qualified metadata name.

    :param str path: directory path
    :param dict metadict: metadata dictionary {name:value}
    :param dict credDict: client credential dictionary
    :return: standard Dirac result object
    """

    result = self.getFileMetadataFields(credDict)
    if not result['OK']:
      return result
    metaFields = result['Value']

    result = self.db.fileManager._findFiles([path])
    if not result['OK']:
      return result
    if result['Value']['Successful']:
      fileID = result['Value']['Successful'][path]['FileID']
    else:
      return S_ERROR('File %s not found' % path)

    for metaName, metaValue in metadict.items():
      fqMetaName = self.getMetaName(metaName, credDict)
      if fqMetaName not in metaFields:
        result = self.__setFileMetaParameter(fileID, metaName, metaValue, credDict)
      else:
        result = self.db.insertFields('FC_FileMeta_%s' % fqMetaName, ['FileID', 'Value'], [fileID, metaValue])
        if not result['OK']:
          if result['Message'].find('Duplicate') != -1:
            req = "UPDATE FC_FileMeta_%s SET Value='%s' WHERE FileID=%d" % (fqMetaName, metaValue, fileID)
            result = self.db._update(req)
            if not result['OK']:
              return result
          else:
            return result

    return S_OK()

  def removeMetadata(self, path, metadata, credDict):
    """
    Remove the specified metadata for the given file (for user's own VO only)

    :param str path: file path
    :param dict metadata: metadata dictionary
    :param dict credDict: client credential dictionary
    :return: standard Dirac result object
    """

    # this would be fully qualified already
    result = self.getFileMetadataFields(credDict)
    if not result['OK']:
      return result
    metaFields = result['Value']

    result = self.db.fileManager._findFiles([path])
    if not result['OK']:
      return result
    if result['Value']['Successful']:
      fileID = result['Value']['Successful'][path]['FileID']
    else:
      return S_ERROR('File %s not found' % path)

    failedMeta = {}
    for meta in metadata:
      # get fully qualified metadata name
      meta = self.getMetaName(meta, credDict)
      if meta in metaFields:
        # Indexed meta case
        req = "DELETE FROM FC_FileMeta_%s WHERE FileID=%d" % (meta, fileID)
        result = self.db._update(req)
        if not result['OK']:
          failedMeta[meta] = result['Value']
      else:
        # Meta parameter case
        req = "DELETE FROM FC_FileMeta WHERE MetaKey='%s' AND FileID=%d" % (meta, fileID)
        result = self.db._update(req)
        if not result['OK']:
          failedMeta[meta] = result['Value']

    if failedMeta:
      metaExample = failedMeta.keys()[0]
      result = S_ERROR('Failed to remove %d metadata, e.g. %s' % (len(failedMeta), failedMeta[metaExample]))
      result['FailedMetadata'] = failedMeta
    else:
      return S_OK()

  def __getFileID(self, path):

    result = self.db.fileManager._findFiles([path])
    if not result['OK']:
      return result
    if result['Value']['Successful']:
      fileID = result['Value']['Successful'][path]['FileID']
    else:
      return S_ERROR('File not found')
    return S_OK(fileID)

  def __setFileMetaParameter(self, fileID, metaName, metaValue, credDict):
    """
    Set an meta parameter - metadata which is not used in the the data
    search operations. metaName is VO aware.

    :param int fileID: file ID
    :param str metaName: metadata name
    :param metaValue: metadata value
    :param dict credDict: client credential dictionary
    :return: standard Dirac result object
    """

    result = self.db.insertFields('FC_FileMeta',
                                  ['FileID', 'MetaKey', 'MetaValue'],
                                  [fileID, self.getMetaName(metaName, credDict), str(metaValue)])
    return result

  def setFileMetaParameter(self, path, metaName, metaValue, credDict):

    result = self.__getFileID(path)
    if not result['OK']:
      return result
    fileID = result['Value']
    return self.__setFileMetaParameter(fileID, metaName, metaValue, credDict)

  def _getFileUserMetadataByID(self, fileIDList, credDict, connection=False):
    """ Get file user metadata for the list of file IDs
    """
    # First file metadata
    result = self.getFileMetadataFields(credDict)
    if not result['OK']:
      return result
    metaFields = result['Value']

    stringIDs = ','.join(['%s' % fId for fId in fileIDList])
    metaDict = {}
    for meta in metaFields:
      req = "SELECT Value,FileID FROM FC_FileMeta_%s WHERE FileID in (%s)" % (meta, stringIDs)
      result = self.db._query(req, conn=connection)
      if not result['OK']:
        return result
      for value, fileID in result['Value']:
        metaDict.setdefault(fileID, {})
        metaDict[fileID][meta] = value

    req = "SELECT FileID,MetaKey,MetaValue from FC_FileMeta where FileID in (%s)" % stringIDs
    result = self.db._query(req, conn=connection)
    if not result['OK']:
      return result
    for fileID, key, value in result['Value']:
      metaDict.setdefault(fileID, {})
      metaDict[fileID][key] = value

    return S_OK(metaDict)

  def getFileUserMetadata(self, path, credDict, stripVO=False):
    """
    Get metadata for the given file.

    :param str path:  file path
    :param dict credDict: client credential dictionary
    :param bool stripVO: If True, the VO suffix is stripped from metadata name
    :return: standard Dirac result object
    """

    # First file metadata
    result = self.getFileMetadataFields(credDict)
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
      req = "SELECT Value,FileID FROM FC_FileMeta_%s WHERE FileID=%d" % (meta, fileID)
      result = self.db._query(req)
      if not result['OK']:
        return result
      if result['Value']:
        metaDict[meta] = result['Value'][0][0]
      metaTypeDict[meta] = metaFields[meta]

    result = self.getFileMetaParameters(path, credDict)
    if result['OK']:
      metaDict.update(result['Value'])
      for meta in result['Value']:
        metaTypeDict[meta] = 'NonSearchable'

    if stripVO:
      metaDict = self.stripSuffix(metaDict, credDict)

    result = S_OK(metaDict)
    result['MetadataType'] = metaTypeDict
    return result

  def __getFileMetaParameters(self, fileID, credDict):

    req = "SELECT FileID,MetaKey,MetaValue from FC_FileMeta where FileID=%d " % fileID
    result = self.db._query(req)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_OK({})
    metaDict = {}
    for fileID, key, value in result['Value']:
      if key in metaDict:
        if isinstance(metaDict[key], list):
          metaDict[key].append(value)
        else:
          metaDict[key] = [metaDict[key]].append(value)
      else:
        metaDict[key] = value

    return S_OK(metaDict)

  def getFileMetaParameters(self, path, credDict):
    """ Get meta parameters for the given file
    """

    result = self.__getFileID(path)
    if not result['OK']:
      return result
    fileID = result['Value']

    return self.__getFileMetaParameters(fileID, credDict)

  def __transformMetaParameterToData(self, metaname):
    """ Relocate the meta parameters of all the directories to the corresponding
        indexed metadata table
    """

    req = "SELECT FileID,MetaValue from FC_FileMeta WHERE MetaKey='%s'" % metaname
    result = self.db._query(req)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_OK()

    insertValueList = []
    for fileID, meta in result['Value']:
      insertValueList.append("( %d,'%s' )" % (fileID, meta))

    req = "INSERT INTO FC_FileMeta_%s (FileID,Value) VALUES %s" % (metaname, ', '.join(insertValueList))
    result = self.db._update(req)
    if not result['OK']:
      return result

    req = "DELETE FROM FC_FileMeta WHERE MetaKey='%s'" % metaname
    result = self.db._update(req)
    return result

  #########################################################################
  #
  #  Finding files by metadata
  #
  #########################################################################

  def __createMetaSelection(self, value):
    ''' Create selection string to be used in the SQL query
    '''
    queryList = []
    if isinstance(value, float):
      queryList.append(('=', '%f' % value))
    elif isinstance(value, six.integer_types):
      queryList.append(('=', '%d' % value))
    elif isinstance(value, str):
      if value.lower() == 'any':
        queryList.append(('IS', 'NOT NULL'))
      elif value.lower() == 'missing':
        queryList.append(('IS', 'NULL'))
      elif value:
        result = self.db._escapeString(value)
        if not result['OK']:
          return result
        eValue = result['Value']
        if '*' in eValue or '?' in eValue:
          eValue = eValue.replace('*', '%%')
          eValue = eValue.replace('?', '_')
          queryList.append(('LIKE', eValue))
        else:
          queryList.append(('=', eValue))
      else:
        queryList.append(('', ''))
    elif isinstance(value, list):
      if not value:
        queryList.append(('', ''))
      else:
        result = self.db._escapeValues(value)
        if not result['OK']:
          return result
        query = '( $s )' % ', '.join(result['Value'])
        queryList.append(('IN', query))
    elif isinstance(value, dict):
      for operation, operand in value.iteritems():

        # Prepare the escaped operand first
        if isinstance(operand, list):
          result = self.db._escapeValues(operand)
          if not result['OK']:
            return result
          escapedOperand = ', '.join(result['Value'])
        elif isinstance(operand, six.integer_types):
          escapedOperand = '%d' % operand
        elif isinstance(operand, float):
          escapedOperand = '%f' % operand
        else:
          result = self.db._escapeString(operand)
          if not result['OK']:
            return result
          escapedOperand = result['Value']

        # Treat the operations
        if operation in ['>', '<', '>=', '<=']:
          if isinstance(operand, list):
            return S_ERROR('Illegal query: list of values for comparison operation')
          else:
            queryList.append((operation, escapedOperand))
        elif operation == 'in' or operation == "=":
          if isinstance(operand, list):
            queryList.append(('IN', '( %s )' % escapedOperand))
          else:
            queryList.append(('=', escapedOperand))
        elif operation == 'nin' or operation == "!=":
          if isinstance(operand, list):
            queryList.append(('NOT IN', '( %s )' % escapedOperand))
          else:
            queryList.append(('!=', escapedOperand))

    return S_OK(queryList)

  def __buildSEQuery(self, storageElements):
    """  Return a tuple with table and condition to locate files in a given SE
    """
    if not storageElements:
      return S_OK([])

    seIDList = []
    for se in storageElements:
      seID = self.db.seNames.get(se, -1)
      if seID == -1:
        return S_ERROR('Unknown SE %s' % se)
      seIDList.append(seID)
    table = 'FC_Replicas'
    seString = intListToString(seIDList)
    query = '%%s.SEID IN ( %s )' % seString
    return S_OK([(table, query)])

  def __buildUserMetaQuery(self, userMetaDict):
    """  Return a list of tuples with tables and conditions to locate files for a given user Metadata
    """
    if not userMetaDict:
      return S_OK([])
    resultList = []
    leftJoinTables = []
    for meta, value in userMetaDict.iteritems():
      table = 'FC_FileMeta_%s' % meta

      result = self.__createMetaSelection(value)
      if not result['OK']:
        return result
      for operation, operand in result['Value']:
        resultList.append((table, '%%s.Value %s %s' % (operation, operand)))
        if operand == 'NULL':
          leftJoinTables.append(table)

    result = S_OK(resultList)
    result['LeftJoinTables'] = leftJoinTables
    return result

  def __buildStandardMetaQuery(self, standardMetaDict):

    table = 'FC_Files'
    queriesFiles = []
    queriesFileInfo = []
    for infield, invalue in standardMetaDict.iteritems():
      value = invalue
      if infield in FILES_TABLE_METAKEYS:
        if infield == 'User':
          value = self.db.users.get(invalue, -1)
          if value == '-1':
            return S_ERROR('Unknown user %s' % invalue)
        elif infield == 'Group':
          value = self.db.groups.get(invalue, -1)
          if value == '-1':
            return S_ERROR('Unknown group %s' % invalue)

        table = 'FC_Files'
        tableIndex = 'F'
        field = FILES_TABLE_METAKEYS[infield]
        result = self.__createMetaSelection(value)
        if not result['OK']:
          return result
        for operation, operand in result['Value']:
          queriesFiles.append('%s.%s %s %s' % (tableIndex, field, operation, operand))
      elif infield in FILEINFO_TABLE_METAKEYS:
        table = 'FC_FileInfo'
        tableIndex = 'FI'
        field = FILEINFO_TABLE_METAKEYS[infield]
        result = self.__createMetaSelection(value)
        if not result['OK']:
          return result
        for operation, operand in result['Value']:
          queriesFileInfo.append('%s.%s %s %s' % (tableIndex, field, operation, operand))
      else:
        return S_ERROR('Illegal standard meta key %s' % infield)

    resultList = []
    if queriesFiles:
      query = ' AND '.join(queriesFiles)
      resultList.append(('FC_Files', query))
    if queriesFileInfo:
      query = ' AND '.join(queriesFileInfo)
      resultList.append(('FC_FileInfo', query))

    return S_OK(resultList)

  def __findFilesByMetadata(self, metaDict, dirList, credDict):
    """ Find a list of file IDs meeting the metaDict requirements and belonging
        to directories in dirList
    """
    # 1.- classify Metadata keys
    storageElements = None
    standardMetaDict = {}
    userMetaDict = {}
    leftJoinTables = []
    for meta, value in metaDict.iteritems():
      if meta == "SE":
        if isinstance(value, dict):
          storageElements = value.get('in', [])
        else:
          storageElements = [value]
      elif meta in FILE_STANDARD_METAKEYS:
        standardMetaDict[meta] = value
      else:
        userMetaDict[self.getMetaName(meta, credDict)] = value

    tablesAndConditions = []
    leftJoinTables = []
    # 2.- standard search
    if standardMetaDict:
      result = self.__buildStandardMetaQuery(standardMetaDict)
      if not result['OK']:
        return result
      tablesAndConditions.extend(result['Value'])
    # 3.- user search
    if userMetaDict:
      result = self.__buildUserMetaQuery(userMetaDict)
      if not result['OK']:
        return result
      tablesAndConditions.extend(result['Value'])
      leftJoinTables = result['LeftJoinTables']
    # 4.- SE constraint
    if storageElements:
      result = self.__buildSEQuery(storageElements)
      if not result['OK']:
        return result
      tablesAndConditions.extend(result['Value'])

    query = 'SELECT F.FileID FROM FC_Files F '
    conditions = []
    tables = []

    if dirList:
      dirString = intListToString(dirList)
      conditions.append("F.DirID in (%s)" % dirString)

    counter = 0
    for table, condition in tablesAndConditions:
      if table == 'FC_FileInfo':
        query += 'INNER JOIN FC_FileInfo FI USING( FileID ) '
        condition = condition.replace('%%', '%')
      elif table == 'FC_Files':
        condition = condition.replace('%%', '%')
      else:
        counter += 1
        if table in leftJoinTables:
          tables.append('LEFT JOIN %s M%d USING( FileID )' % (table, counter))
        else:
          tables.append('INNER JOIN %s M%d USING( FileID )' % (table, counter))
        table = 'M%d' % counter
        condition = condition % table
      conditions.append(condition)

    query += ' '.join(tables)
    if conditions:
      query += ' WHERE %s' % ' AND '.join(conditions)

    result = self.db._query(query)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_OK([])

    #     fileList = [ row[0] for row in result['Value' ] ]
    fileList = []
    for row in result['Value']:
      fileID = row[0]
      fileList.append(fileID)

    return S_OK(fileList)

  @queryTime
  def findFilesByMetadata(self, metaDict, path, credDict):
    """ Find Files satisfying the given metadata

        :param dict metaDict: dictionary with the metaquery parameters
        :param str path: Path to search into
        :param dict credDict: Dictionary with the user credentials

        :return: S_OK/S_ERROR, Value ID:LFN dictionary of selected files
    """
    if not path:
      path = '/'

    # 1.- Get Directories matching the metadata query
    result = self.db.dmeta.findDirIDsByMetadata(metaDict, path, credDict)
    if not result['OK']:
      return result
    dirList = result['Value']
    dirFlag = result['Selection']

    # 2.- Get known file metadata fields
    #     fileMetaDict = {}
    result = self.getFileMetadataFields(credDict)  # this returns fully qualified meta name
    if not result['OK']:
      return result
    fileMetaKeys = result['Value'].keys() + FILE_STANDARD_METAKEYS.keys()
    fileMetaDict = dict(item for item in metaDict.items()
                        if self.getMetaName(item[0], credDict) in fileMetaKeys)
    fileList = []
    idLfnDict = {}

    if dirFlag != 'None':
      # None means that no Directory satisfies the given query, thus the search is empty
      if dirFlag == 'All':
        # All means that there is no Directory level metadata in query, full name space is considered
        dirList = []

      if fileMetaDict:
        # 3.- Do search in File Metadata
        result = self.__findFilesByMetadata(fileMetaDict, dirList, credDict)
        if not result['OK']:
          return result
        fileList = result['Value']
      elif dirList:
        # 4.- if not File Metadata, return the list of files in given directories
        result = self.db.dtree.getFileLFNsInDirectoryByDirectory(dirList, credDict)
        if not result['OK']:
          return result
        return S_OK(result['Value']['IDLFNDict'])
      else:
        # if there is no File Metadata and no Dir Metadata, return an empty list
        idLfnDict = {}

    if fileList:
      # 5.- get the LFN
      result = self.db.fileManager._getFileLFNs(fileList)
      if not result['OK']:
        return result
      idLfnDict = result['Value']['Successful']

    return S_OK(idLfnDict)
