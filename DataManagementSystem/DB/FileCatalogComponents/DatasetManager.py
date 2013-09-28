########################################################################
# $HeadURL$
########################################################################

""" DIRAC FileCatalog plug-in class to manage dynamic datasets defined by a metadata query
"""

__RCSID__ = "$Id$"

try:
  import hashlib 
  md5 = hashlib
except ImportError:
  import md5
from DIRAC import S_OK, S_ERROR

class DatasetManager:

  def __init__( self, database = None ):

    self.db = database

  def setDatabase( self, database ):
    self.db = database
    
  def addDataset( self, datasetName, metaQuery, credDict ):  
    
    result = self.db.ugManager.getUserAndGroupID( credDict )
    if not result['OK']:
      return result
    uid, gid = result['Value']
    
    result = self.__getMetaQueryParameters( metaQuery, credDict )
    if not result['OK']:
      return result
    totalSize = result['Value']['TotalSize']
    datasetHash = result['Value']['DatasetHash']
    numberOfFiles = result['Value']['NumberOfFiles']
      
    result = self.db.fileManager._getStatusInt( 'Dynamic' )
    if not result['OK']:
      return result
    intStatus = result['Value']  
      
    # Add the new dataset entry now
    inDict = {
               'DatasetName': datasetName,
               'MetaQuery': str(metaQuery), 
               'TotalSize': totalSize, 
               'NumberOfFiles': numberOfFiles, 
               'UID': uid, 
               'GID': gid,
               'CreationDate': 'UTC_TIMESTAMP()',
               'ModificationDate': 'UTC_TIMESTAMP()',
               'DatasetHash': datasetHash,
               'Status': intStatus
             }
    result = self.db.insertFields( 'FC_MetaDatasets', inDict = inDict )
    if not result['OK']:
      if "Duplicate" in result['Message']:
        return S_ERROR( 'Dataset %s already exists' % datasetName )
      else:
        return result
    datasetID = result['lastRowId']
    return S_OK( datasetID )
       
  def __getMetaQueryParameters( self, metaQuery, credDict ):  
    """ Get parameters ( hash, total size, number of files ) for the given metaquery
    """
    findMetaQuery = dict( metaQuery )
    
    path = '/' 
    if "Path" in findMetaQuery:
      path = findMetaQuery['Path']
      findMetaQuery.pop( 'Path' )
    
    result = self.db.fmeta.findFilesByMetadata( findMetaQuery, path, credDict, extra=True )
    if not result['OK']:
      return S_ERROR( 'Failed to apply the metaQuery' )
        
    lfnList = result['Value']
    lfnIDDict = result['LFNIDDict']
    lfnList.sort()
    myMd5 = md5.md5()
    myMd5.update( str( lfnList ) )
    datasetHash = myMd5.hexdigest().upper()
    numberOfFiles = len( lfnList )
    result = self.db.fileManager.getFileSize( lfnList )
    totalSize = 0
    if result['OK']:
      totalSize = result['TotalSize']
      
    result = S_OK( { 'DatasetHash': datasetHash,
                     'NumberOfFiles': numberOfFiles,
                     'TotalSize': totalSize,
                     'LFNList': lfnList,
                     'LFNIDDict': lfnIDDict } )
    return result  
    
  def checkDataset( self, datasetName, credDict ):
    """ Check that the dataset parameters correspond to the actual state
    """  
    req = "SELECT MetaQuery,DatasetHash,TotalSize,NumberOfFiles FROM FC_MetaDatasets"
    req += " WHERE DatasetName='%s'" % datasetName
    result = self.db._query( req )
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR( 'Unknown MetaDataset %s' % datasetName )
    
    row = result['Value'][0]
    metaQuery = eval( row[0] )
    datasetHashOld = row[1]
    totalSizeOld = int( row[2] )
    numberOfFilesOld = int( row[3] )
    
    result = self.__getMetaQueryParameters( metaQuery, credDict )
    if not result['OK']:
      return result
    totalSize = result['Value']['TotalSize']
    datasetHash = result['Value']['DatasetHash']
    numberOfFiles = result['Value']['NumberOfFiles']
    
    changeDict = {}
    if totalSize != totalSizeOld:
      changeDict['TotalSize'] = ( totalSizeOld, totalSize )
    if datasetHash != datasetHashOld:
      changeDict['DatasetHash'] = ( datasetHashOld, datasetHash )
    if numberOfFiles != numberOfFilesOld:
      changeDict['NumberOfFiles'] = ( numberOfFilesOld, numberOfFiles )
      
    result = S_OK( changeDict )
    return result
  
  def updateDataset( self, datasetName, credDict, changeDict=None ):
    """ Update the dataset parameters
    """
    
    if changeDict is None:
      result = self.checkDataset( datasetName, credDict )
      if not result['OK']:
        return result
      if not result['Value']:
        # The dataset is not changed
        return S_OK()
      else:
        changeDict = result['Value']
        
    req = "UPDATE FC_MetaDatasets SET "
    for field in changeDict:
      req += "%s=%s, " % ( field, str( changeDict[field] ) )
    req += "ModificationDate=UTC_TIMESTAMP() "  
    req += "WHERE DatasetName=%s" % datasetName
    result = self.db._update( req )
    return result
          
  def getDatasetParameters( self, datasetName, credDict ):        
    """ Get the currently stored dataset parameters
    """
    parameterList = ['DatasetID','MetaQuery','DirID','TotalSize','NumberOfFiles',
                     'UID','GID','Status','CreationDate','ModificationDate','DatasetHash','Mode']
    parameterString = ','.join( parameterList )
    
    req = "SELECT %s FROM FC_MetaDatasets WHERE DatasetName='%s'" % ( parameterString, datasetName )
    result = self.db._query( req )
    if not result['OK']:
      return result
    
    resultDict = {}
    row = result['Value'][0]
    resultDict['DatasetID'] = int( row[0] )
    resultDict['MetaQuery'] = eval( row[1] )
    resultDict['DirID'] = int( row[2] )
    resultDict['TotalSize'] = int( row[3] )
    resultDict['NumberOfFiles'] = int( row[4] )
    uid = int( row[5] )
    gid = int( row[6] )
    result = self.db.ugManager.getUserName( uid )
    if result['OK']:
      resultDict['User'] = result['Value']
    else:
      resultDict['User'] = 'Unknown'
    result = self.db.ugManager.getGroupName( gid )
    if result['OK']:
      resultDict['Group'] = result['Value']
    else:
      resultDict['Group'] = 'Unknown'
    intStatus = int( row[7] )
    result = self.db.fileManager._getIntStatus( intStatus )
    if result['OK']:
      resultDict['Status'] = result['Value']
    else:
      resultDict['Status'] = 'Unknown'   
    resultDict['CreationDate'] = row[8]
    resultDict['ModificationDate'] = row[9]   
    resultDict['DatasetHash'] = row[10]
    resultDict['Mode'] = row[11]
    
    return S_OK( resultDict )    
  
  def setDatasetStatus( self, datasetName, status ):
    """ Set the given dataset status
    """         
    result = self.db.fileManager._getStatusInt( status )
    if not result['OK']:
      return result
    intStatus = result['Value']
    req = "UPDATE FC_MetaDatasets SET Status=%d, ModificationDate=UTC_TIMESTAMP() " % intStatus
    req += "WHERE DatasetName='%s'" % datasetName
    result = self.db._update( req )
    return result
  
  def getDatasetStatus( self, datasetName, credDict ):
    """ Get status of the given dataset
    """
    
    result = self.getDatasetParameters( datasetName, credDict )
    if not result['OK']:
      return result
    status = result['Status']
    return S_OK( status )
          
  def __getDynamicDatasetFiles( self, datasetName, credDict ):
    """ Get dataset lfns from a dynamic meta query
    """    
    req = "SELECT MetaQuery FROM FC_MetaDatasets WHERE DatasetName='%s'" % datasetName
    result = self.db._query( req )
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR( 'Unknown MetaDataset %s' % datasetName )
    
    metaQuery = eval( result['Value'][0][0] )
    result = self.__getMetaQueryParameters( metaQuery, credDict )
    if not result['OK']:
      return result  
    
    lfnList = result['Value']['LFNList']
    result = S_OK(lfnList)
    result['FileIDList'] = result['Value']['LFNIDDict'].keys() 
    return result    
  
  def __getFrozenDatasetFiles( self, datasetName, credDict ):
    """ Get dataset lfns from a frozen snapshot 
    """     
    result = self.getDatasetParameters( datasetName, credDict )
    if not result['OK']:
      return result
    status = result['Value']['Status']
    if status != "Frozen":
      return S_ERROR( 'The dataset is in a dynamic state' )
    datasetID = result['Value']['DatasetID']
    
    req = "SELECT FileID FROM FC_MetaDatasetFiles WHERE DatasetID=%d" % datasetID
    result = self.db._query( req )
    if not result['OK']:
      return result
    
    fileIDList = [ row[0] for row in result['Value'] ]
    result = self.db.fileManager._getFileLFNs( fileIDList )
    if not result['OK']:
      return result
    
    lfnDict = result['Value']['Successful']
    lfnList = [ lfnDict[i] for i in lfnDict.keys() ]
    result = S_OK( lfnList )
    result['FileIDList'] = lfnDict.keys()
    return result
  
  def getDatasetFiles( self, datasetName, credDict ):
    """ Get dataset files
    """ 
    result = self.getDatasetParameters( datasetName, credDict )
    if not result['OK']:
      return result
    status = result['Value']['Status']
    if status in ["Frozen","Static"]:
      return self.__getFrozenDatasetFiles( datasetName, credDict )
    else:
      return self.__getDynamicDatasetFiles( datasetName, credDict )
         
  def freezeDataset( self, datasetName, credDict ):
    """ Freeze the contents of the dataset
    """    
    result = self.getDatasetParameters( datasetName, credDict )
    if not result['OK']:
      return result
    status = result['Value']['Status']
    if status == "Frozen":
      return S_OK()
    
    datasetID = result['Value']['DatasetID']
    req = "DELETE FROM FC_MetaDatasetFiles WHERE DatasetID=%d" % datasetID
    result = self.db._update( req )
    
    result = self.__getDynamicDatasetFiles( datasetName, credDict )
    if not result['OK']:
      return result
    fileIDList = result['FileIDList']
    valueList = []
    for fileID in fileIDList:
      valueList.append( '(%d,%d)' % (datasetID,fileID) )
    valueString = ','.join( valueList )
    req = "INSERT INTO FC_MetaDatasetFiles (DatasetID,FileID) VALUES %s" % valueString 
    result = self.db._update( req )
    if not result['OK']:
      return result
    
    result = self.setDatasetStatus( datasetName, 'Frozen' )
    return result  
    
  def releaseDataset( self, datasetName, credDict ):
    """ return the dataset to a dynamic state
    """  
    result = self.getDatasetParameters( datasetName, credDict )
    if not result['OK']:
      return result
    status = result['Value']['Status']
    if status == "Dynamic":
      return S_OK()
    
    datasetID = result['Value']['DatasetID']
    req = "DELETE FROM FC_MetaDatasetFiles WHERE DatasetID=%d" % datasetID
    result = self.db._update( req )
    
    result = self.setDatasetStatus( datasetName, 'Dynamic' )
    return result  
    
      
      
    
    
    
    
    