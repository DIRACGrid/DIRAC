########################################################################
# $Id$
########################################################################

__RCSID__ = "$Id$"

from DIRAC                                  import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.List              import intListToString
from DIRAC.Core.Utilities.Pfn               import pfnparse, pfnunparse

import os, stat
from types import ListType, StringTypes

class FileManagerBase:

  _base_tables = {}
  _base_tables['FC_FileAncestors'] = { "Fields":
                                     { 
                                       "FileID": "INT NOT NULL DEFAULT 0",
                                       "AncestorID": "INT NOT NULL DEFAULT 0",
                                       "AncestorDepth": "INT NOT NULL DEFAULT 0"
                                     }, 
                                       "Indexes": {"FileID": ["FileID"], 
                                                 "AncestorID": ["AncestorID"],
                                                 "AncestorDepth": ["AncestorDepth"]},
                                       "UniqueIndexes": { "File_Ancestor": ["FileID","AncestorID"]}  
                                     } 


  def __init__( self, database = None ):
    self.db = None
    if database is not None:
      self.setDatabase( database )

  def _getConnection( self, connection ):
    if connection:
      return connection
    res = self.db._getConnection()
    if res['OK']:
      return res['Value']
    gLogger.warn( "Failed to get MySQL connection", res['Message'] )
    return connection

  def setDatabase( self, database ):
    self.db = database
    result = self.db._createTables( self._base_tables )
    if not result['OK']:
      gLogger.error( "Failed to create tables", str( self._base_tables.keys() ) )
      return result
    if result['Value']:
      gLogger.info( "Tables created: %s" % ','.join( result['Value'] ) )
    result = self.db._createTables( self._tables )
    if not result['OK']:
      gLogger.error( "Failed to create tables", str( self._tables.keys() ) )
    elif result['Value']:
      gLogger.info( "Tables created: %s" % ','.join( result['Value'] ) )  
    return result
  
  def getFileCounters( self, connection = False ):
    connection = self._getConnection( connection )
  
    resultDict = {}
    req = "SELECT COUNT(*) FROM FC_Files;"
    res = self.db._query( req, connection )
    if not res['OK']:
      return res
    resultDict['Files'] = res['Value'][0][0]

    req = "SELECT COUNT(FileID) FROM FC_Files WHERE FileID NOT IN ( SELECT FileID FROM FC_Replicas )"
    res = self.db._query( req, connection )
    if not res['OK']:
      return res
    resultDict['Files w/o Replicas'] = res['Value'][0][0]
    
    req = "SELECT COUNT(RepID) FROM FC_Replicas WHERE FileID NOT IN ( SELECT FileID FROM FC_Files )"
    res = self.db._query( req, connection )
    if not res['OK']:
      return res
    resultDict['Replicas w/o Files'] = res['Value'][0][0]

    treeTable = self.db.dtree.getTreeTable()
    req = "SELECT COUNT(FileID) FROM FC_Files WHERE DirID NOT IN ( SELECT DirID FROM %s)" % treeTable
    res = self.db._query( req, connection )
    if not res['OK']:
      return res
    resultDict['Orphan Files'] = res['Value'][0][0]

    req = "SELECT COUNT(FileID) FROM FC_Files WHERE FileID NOT IN ( SELECT FileID FROM FC_FileInfo)"
    res = self.db._query( req, connection )
    if not res['OK']:
      return res
    resultDict['Files w/o FileInfo'] = res['Value'][0][0]

    req = "SELECT COUNT(FileID) FROM FC_FileInfo WHERE FileID NOT IN ( SELECT FileID FROM FC_Files)"
    res = self.db._query( req, connection )
    if not res['OK']:
      return res
    resultDict['FileInfo w/o Files'] = res['Value'][0][0]

    return S_OK( resultDict )

  def getReplicaCounters( self, connection = False ):
    connection = self._getConnection( connection )
    req = "SELECT COUNT(*) FROM FC_Replicas;"
    res = self.db._query( req, connection )
    if not res['OK']:
      return res
    return S_OK( {'Replicas':res['Value'][0][0]} )

  ######################################################
  #
  # File write methods
  #

  def _insertFiles( self, lfns, uid, gid, connection = False ):
    """To be implemented on derived class
    """
    return S_ERROR( "To be implemented on derived class" )

  def _deleteFiles( self, toPurge, connection = False ):
    """To be implemented on derived class
    """
    return S_ERROR( "To be implemented on derived class" )

  def _insertReplicas( self, lfns, master = False, connection = False ):
    """To be implemented on derived class
    """
    return S_ERROR( "To be implemented on derived class" )

  def _findFiles( self, lfns, metadata = ["FileID"], connection = False ):
    """To be implemented on derived class
    """
    return S_ERROR( "To be implemented on derived class" )

  def _getFileReplicas( self, fileIDs, fields_input = ['PFN'], connection = False ):
    """To be implemented on derived class
    """
    return S_ERROR( "To be implemented on derived class" )

  def _getFileIDFromGUID( self, guid, connection = False ):
    """To be implemented on derived class
    """
    return S_ERROR( "To be implemented on derived class" )

  def _setFileParameter( self, fileID, paramName, paramValue, connection = False ):
    """To be implemented on derived class
    """
    return S_ERROR( "To be implemented on derived class" )

  def _deleteReplicas( self, lfns, connection = False ):
    """To be implemented on derived class
    """
    return S_ERROR( "To be implemented on derived class" )

  def _setReplicaStatus( self, fileID, se, status, connection = False ):
    """To be implemented on derived class
    """
    return S_ERROR( "To be implemented on derived class" )

  def _setReplicaHost( self, fileID, se, newSE, connection = False ):
    """To be implemented on derived class
    """
    return S_ERROR( "To be implemented on derived class" )

  def _getDirectoryFiles( self, dirID, fileNames, metadata, allStatus = False, connection = False ):
    """To be implemented on derived class
    """
    return S_ERROR( "To be implemented on derived class" )

  def _getFileLFNs(self,fileIDs):
    """ Get the file LFNs for a given list of file IDs
    """
    stringIDs = intListToString(fileIDs)
    treeTable = self.db.dtree.getTreeTable()

    req = "SELECT F.FileID, CONCAT(D.DirName,'/',F.FileName) from FC_Files as F, %s as D WHERE F.FileID IN ( %s ) AND F.DirID=D.DirID" % (treeTable,stringIDs)
    result = self.db._query(req)
    if not result['OK']:
      return result

    fileNameDict = {}
    for row in result['Value']:
      fileNameDict[row[0]] = row[1]
    
    failed = {}
    successful = fileNameDict
    if len(fileNameDict) != len(fileIDs):
      for id_ in fileIDs:
        if not id_ in fileNameDict:
          failed[id_] = "File ID not found"

    return S_OK({'Successful':successful,'Failed':failed})
    
  def _getFileLFNs_old(self,fileIDs):
    """ Get the file LFNs for a given list of file IDs
    """            
    stringIDs = intListToString(fileIDs)
    req = "SELECT DirID, FileID, FileName from FC_Files WHERE FileID IN ( %s )" % stringIDs
    result = self.db._query(req)
    if not result['OK']:
      return result

    dirPathDict = {}  
    fileNameDict = {}
    for row in result['Value']:
      if not row[0] in dirPathDict:
        dirPathDict[row[0]] = self.db.dtree.getDirectoryPath(row[0])['Value']      
      fileNameDict[row[1]] = '%s/%s' % (dirPathDict[row[0]],row[2])

    failed = {}
    successful = fileNameDict
    for id_ in fileIDs:
      if not id_ in fileNameDict:
        failed[id_] = "File ID not found"

    return S_OK({'Successful':successful,'Failed':failed})          
                                    

  def addFile( self, lfns, credDict, connection = False ):
    """ Add files to the catalog """
    connection = self._getConnection( connection )
    successful = {}
    failed = {}
    for lfn, info in lfns.items():
      res = self._checkInfo( info, ['PFN', 'SE', 'Size', 'Checksum'] )
      if not res['OK']:
        failed[lfn] = res['Message']
        lfns.pop( lfn )
    res = self._addFiles( lfns, credDict, connection = connection )
    if not res['OK']:
      for lfn in lfns.keys():
        failed[lfn] = res['Message']
    else:
      failed.update( res['Value']['Failed'] )
      successful.update( res['Value']['Successful'] )
    return S_OK( {'Successful':successful, 'Failed':failed} )

  def _addFiles( self, lfns, credDict, connection = False ):
    """ Main file adding method
    """
    connection = self._getConnection( connection )
    successful = {}
    result = self.db.ugManager.getUserAndGroupID( credDict )
    if not result['OK']:
      return result
    uid, gid = result['Value']

    # prepare lfns with master replicas - the first in the list or a unique replica
    masterLfns = {}
    extraLfns = {}
    for lfn in lfns:
      masterLfns[lfn] = dict( lfns[lfn] )
      if 'SE' in lfns[lfn] and type( lfns[lfn]['SE'] ) == ListType:
        masterLfns[lfn]['SE'] = lfns[lfn]['SE'][0]  
        if len( lfns[lfn]['SE'] ) > 1:
          extraLfns[lfn] = dict( lfns[lfn] )
          extraLfns[lfn]['SE'] = lfns[lfn]['SE'][1:]

    # Check whether the supplied files have been registered already
    existingMetadata, failed = self._getExistingMetadata( masterLfns.keys(), connection = connection )
    if existingMetadata:
      success, fail = self._checkExistingMetadata( existingMetadata, masterLfns )
      successful.update( success )
      failed.update( fail )
      for lfn in ( success.keys() + fail.keys() ):
        masterLfns.pop( lfn )

    # If GUIDs are supposed to be unique check their pre-existance 
    if self.db.uniqueGUID:
      fail = self._checkUniqueGUID( masterLfns, connection = connection )
      failed.update( fail )
      for lfn in fail:
        masterLfns.pop( lfn )

    # If we have files left to register
    if masterLfns:
      # Create the directories for the supplied files and store their IDs
      directories = self._getFileDirectories( masterLfns.keys() )
      for directory, fileNames in directories.items():
        res = self.db.dtree.makeDirectories( directory, credDict )
        for fileName in fileNames:
          lfn = "%s/%s" % ( directory, fileName )
          lfn = lfn.replace( '//', '/' )
          if not res['OK']:
            failed[lfn] = "Failed to create directory for file"
            masterLfns.pop( lfn )
          else:
            masterLfns[lfn]['DirID'] = res['Value']

    # If we still have files left to register
    if masterLfns:
      res = self._insertFiles( masterLfns, uid, gid, connection = connection )
      if not res['OK']:
        for lfn in masterLfns.keys():
          failed[lfn] = res['Message']
          masterLfns.pop( lfn )
      else:
        for lfn, error in res['Value']['Failed'].items():
          failed[lfn] = error
          masterLfns.pop( lfn )
        masterLfns = res['Value']['Successful']

    # Add the ancestors
    if masterLfns:
      res = self._populateFileAncestors( masterLfns, connection = connection )
      toPurge = []
      if not res['OK']:
        for lfn in masterLfns.keys():
          failed[lfn] = "Failed while registering ancestors"
          toPurge.append( masterLfns[lfn]['FileID'] )
      else:
        failed.update( res['Value']['Failed'] )
        for lfn, error in res['Value']['Failed'].items():
          toPurge.append( masterLfns[lfn]['FileID'] )
      if toPurge:
        self._removeFileAncestors( toPurge, connection = connection )
        self._deleteFiles( toPurge, connection = connection )

    # Register the replicas
    newlyRegistered = {}
    if masterLfns:
      res = self._insertReplicas( masterLfns, master = True, connection = connection )
      toPurge = []
      if not res['OK']:
        for lfn in masterLfns.keys():
          failed[lfn] = "Failed while registering replica"
          toPurge.append( masterLfns[lfn]['FileID'] )
      else:
        newlyRegistered = res['Value']['Successful']
        successful.update( newlyRegistered )
        failed.update( res['Value']['Failed'] )
        for lfn, error in res['Value']['Failed'].items():
          toPurge.append( masterLfns[lfn]['FileID'] )
      if toPurge:
        self._removeFileAncestors( toPurge, connection = connection )
        self._deleteFiles( toPurge, connection = connection )
   
    # Add extra replicas for successfully registered LFNs
    for lfn in extraLfns.keys():
      if not lfn in successful:
        extraLfns.pop( lfn )

    if extraLfns:
      res = self._findFiles( extraLfns.keys(), ['FileID','DirID'], connection=connection )
      if not res['OK']:
        for lfn in lfns.keys():
          failed[lfn] = 'Failed while registering extra replicas'
          successful.pop( lfn )
          extraLfns.pop( lfn )
      else:
        failed.update(res['Value']['Failed'])
        for lfn in res['Value']['Failed'].keys():
          successful.pop(lfn)
          extraLfns.pop( lfn )
        for lfn,fileDict in res['Value']['Successful'].items():
          extraLfns[lfn]['FileID'] = fileDict['FileID']
          extraLfns[lfn]['DirID'] = fileDict['DirID']
 
      if extraLfns:
        res = self._insertReplicas( extraLfns, master = False, connection = connection )
        if not res['OK']:
          for lfn in extraLfns.keys():
            failed[lfn] = "Failed while registering extra replicas"
            successful.pop( lfn )
        else:
          newlyRegistered = res['Value']['Successful']
          successful.update( newlyRegistered )
          failed.update( res['Value']['Failed'] )

    return S_OK( {'Successful':successful, 'Failed':failed} )

  def _updateDirectoryUsage( self, directorySEDict, change, connection = False ):
    connection = self._getConnection( connection )
    for directoryID in directorySEDict.keys():
      result = self.db.dtree.getPathIDsByID( directoryID )
      if not result['OK']:
        return result
      parentIDs = result['Value']
      dirDict = directorySEDict[directoryID]
      for seID in dirDict.keys() :
        seDict = dirDict[seID]
        files = seDict['Files']
        size = seDict['Size']
        insertTuples = []
        for dirID in parentIDs:
          insertTuples.append( '(%d,%d,%d,%d,UTC_TIMESTAMP())' % ( dirID, seID, size, files ) )
    
        req = "INSERT INTO FC_DirectoryUsage (DirID,SEID,SESize,SEFiles,LastUpdate) "
        req += "VALUES %s" % ','.join( insertTuples )
        req += " ON DUPLICATE KEY UPDATE SESize=SESize%s%d, SEFiles=SEFiles%s%d, LastUpdate=UTC_TIMESTAMP() " \
                                                           % ( change, size, change, files )
        res = self.db._update( req )
        if not res['OK']:
          gLogger.warn( "Failed to update FC_DirectoryUsage", res['Message'] )
    return S_OK()
    
  def _populateFileAncestors( self, lfns, connection = False ):
    connection = self._getConnection( connection )
    successful = {}
    failed = {}
    for lfn, lfnDict in lfns.items():
      originalFileID = lfnDict['FileID']
      originalDepth = lfnDict.get( 'AncestorDepth', 1 )
      ancestors = lfnDict.get( 'Ancestors', [] )
      if type( ancestors ) == type( ' ' ):
        ancestors = [ancestors]
      if lfn in ancestors:
        ancestors.remove( lfn )
      if not ancestors:
        successful[lfn] = True
        continue
      res = self._findFiles( ancestors, connection = connection )
      if res['Value']['Failed']:
        failed[lfn] = "Failed to resolve ancestor files"
        continue
      ancestorIDs = res['Value']['Successful']
      fileIDLFNs = {}
      toInsert = {}
      for ancestor in ancestorIDs.keys():
        fileIDLFNs[ancestorIDs[ancestor]['FileID']] = ancestor
        toInsert[ancestorIDs[ancestor]['FileID']] = originalDepth
      res = self._getFileAncestors( fileIDLFNs.keys() )
      if not res['OK']:
        failed[lfn] = "Failed to obtain all ancestors"
        continue
      fileIDAncestorDict = res['Value']
      for fileIDDict in fileIDAncestorDict.values():
        for ancestorID, relativeDepth in fileIDDict.items():
          toInsert[ancestorID] = relativeDepth + originalDepth
      res = self._insertFileAncestors( originalFileID, toInsert, connection = connection )
      if not res['OK']:
        if "Duplicate" in res['Message']:
          failed[lfn] = "Failed to insert ancestor files: duplicate entry"
        else:              
          failed[lfn] = "Failed to insert ancestor files"
      else:
        successful[lfn] = True
    return S_OK( {'Successful':successful, 'Failed':failed} )

  def _insertFileAncestors( self, fileID, ancestorDict, connection = False ):
    connection = self._getConnection( connection )
    ancestorTuples = []
    for ancestorID, depth in ancestorDict.items():
      ancestorTuples.append( "(%d,%d,%d)" % ( fileID, ancestorID, depth ) )
    if not ancestorTuples:
      return S_OK()
    req = "INSERT INTO FC_FileAncestors (FileID, AncestorID, AncestorDepth) VALUES %s" \
                              % intListToString( ancestorTuples )
    return self.db._update( req, connection )

  def _getFileAncestors( self, fileIDs, depths = [], connection = False ):
    connection = self._getConnection( connection )
    req = "SELECT FileID, AncestorID, AncestorDepth FROM FC_FileAncestors WHERE FileID IN (%s)" \
                              % intListToString( fileIDs )
    if depths:
      req = "%s AND AncestorDepth IN (%s);" % ( req, intListToString( depths ) )
    res = self.db._query( req, connection )
    if not res['OK']:
      return res
    fileIDAncestors = {}
    for fileID, ancestorID, depth in res['Value']:
      if not fileIDAncestors.has_key( fileID ):
        fileIDAncestors[fileID] = {}
      fileIDAncestors[fileID][ancestorID] = depth
    return S_OK( fileIDAncestors )

  def _getFileDescendents( self, fileIDs, depths, connection = False ):
    connection = self._getConnection( connection )
    req = "SELECT AncestorID, FileID, AncestorDepth FROM FC_FileAncestors WHERE AncestorID IN (%s)" \
                                % intListToString( fileIDs )
    if depths:
      req = "%s AND AncestorDepth IN (%s);" % ( req, intListToString( depths ) )
    res = self.db._query( req, connection )
    if not res['OK']:
      return res
    fileIDAncestors = {}
    for ancestorID, fileID, depth in res['Value']:
      if not fileIDAncestors.has_key( ancestorID ):
        fileIDAncestors[ancestorID] = {}
      fileIDAncestors[ancestorID][fileID] = depth
    return S_OK( fileIDAncestors )

  def addFileAncestors(self,lfns, connection = False ):
    """ Add file ancestors to the catalog """
    connection = self._getConnection( connection )
    failed = {}
    successful = {}
    result = self._findFiles( lfns.keys(), connection = connection )
    if not result['OK']:
      return result
    if result['Value']['Failed']:
      failed.update(result['Value']['Failed'])
      for lfn in result['Value']['Failed']:
        lfns.pop(lfn)
    if not lfns:
      return S_OK({'Successful':successful,'Failed':failed})
    
    for lfn in  result['Value']['Successful']:
      lfns[lfn]['FileID'] = result['Value']['Successful'][lfn]['FileID']
    
    result = self._populateFileAncestors(lfns, connection)
    if not result['OK']:
      return result
    failed.update(result['Value']['Failed'])
    successful = result['Value']['Successful']
    return S_OK({'Successful':successful,'Failed':failed})                                           

  def _removeFileAncestors(self, fileIDs, connection = False ):
    """ Remove from the FC_FileAncestors the entries corresponding to the input files"""
    connection = self._getConnection( connection )
    successful = {}
    failed = {}
    for FileID in fileIDs:
      res = self.db.deleteEntries( "FC_FileAncestors" , { 'AncestorID' : FileID } )
      if not res[ 'OK' ]:
        failed[FileID] = res['Message']
        continue
      res = self.db.deleteEntries( "FC_FileAncestors" , { 'FileID' : FileID } )
      if not res[ 'OK' ]:
        failed[FileID] = res['Message']
        continue
      successful[FileID] = 'OK'
    #Once could/should? fix the depth of related files.  
    return S_OK( {'Successful' : successful, 'Failed' : failed} )
    
  def _getFileRelatives( self, lfns, depths, relation, connection = False ):
    connection = self._getConnection( connection )
    failed = {}
    successful = {}
    result = self._findFiles( lfns.keys(), connection = connection )
    if not result['OK']:
      return result
    if result['Value']['Failed']:
      failed.update(result['Value']['Failed'])
      for lfn in result['Value']['Failed']:
        lfns.pop(lfn)
    if not lfns:
      return S_OK({'Successful':successful,'Failed':failed})
    
    inputIDDict = {}
    for lfn in result['Value']['Successful']:
      inputIDDict[ result['Value']['Successful'][lfn]['FileID'] ] = lfn
  
    inputIDs = inputIDDict.keys()
    if relation == 'ancestor':
      result = self._getFileAncestors(inputIDs,depths, connection)
    else:
      result = self._getFileDescendents(inputIDs,depths, connection)      
            
    if not result['OK']:
      return result
   
    failed = {}
    successful = {}
    relDict = result['Value']
    for id_ in inputIDs:
      if id_ in relDict:
        aList = relDict[id_].keys()
        result = self._getFileLFNs(aList)       
        if not result['OK']:
          failed[inputIDDict[id]] = "Failed to find %s" % relation    
        else:
          if result['Value']['Successful']:
            resDict = {}
            for aID in result['Value']['Successful']:        
              resDict[ result['Value']['Successful'][aID] ] = relDict[id_][aID]         
            successful[inputIDDict[id_]] = resDict
          for aID in result['Value']['Failed']:
            failed[inputIDDict[id_]] = "Failed to get the ancestor LFN"             
      else:
        successful[inputIDDict[id_]] = {}                                     
      
    return S_OK({'Successful':successful,'Failed':failed})
    
  def getFileAncestors( self, lfns, depths, connection = False ):
    return self._getFileRelatives(lfns, depths, 'ancestor', connection)

  def getFileDescendents( self, lfns, depths, connection = False ):
    return self._getFileRelatives(lfns, depths, 'descendent', connection)


  def _getExistingMetadata( self, lfns, connection = False ):
    connection = self._getConnection( connection )
    # Check whether the files already exist before adding
    res = self._findFiles( lfns, ['FileID', 'Size', 'Checksum', 'GUID'], connection = connection )
    successful = res['Value']['Successful']
    failed = res['Value']['Failed']
    for lfn, error in res['Value']['Failed'].items():
      if error == 'No such file or directory':
        failed.pop( lfn )
    return successful, failed

  def _checkExistingMetadata( self, existingLfns, lfns ):
    failed = {}
    successful = {}
    fileIDLFNs = {}
    for lfn, fileDict in existingLfns.items():
      fileIDLFNs[fileDict['FileID']] = lfn
    # For those that exist get the replicas to determine whether they are already registered
    res = self._getFileReplicas( fileIDLFNs.keys() )
    if not res['OK']:
      for lfn in fileIDLFNs.values():
        failed[lfn] = 'Failed checking pre-existing replicas'
    else:
      replicaDict = res['Value']
      for fileID, lfn in fileIDLFNs.items():
        fileMetadata = existingLfns[lfn]
        existingGuid = fileMetadata['GUID']
        existingSize = fileMetadata['Size']
        existingChecksum = fileMetadata['Checksum']
        newGuid = lfns[lfn]['GUID']
        newSize = lfns[lfn]['Size']
        newChecksum = lfns[lfn]['Checksum']
        # Ensure that the key file metadata is the same
        if ( existingGuid != newGuid ) or \
           ( existingSize != newSize ) or \
           ( existingChecksum != newChecksum ):
          failed[lfn] = "File already registered with alternative metadata"
        # If the DB does not have replicas for this file return an error
        elif not fileID in replicaDict or not replicaDict[fileID]:
          failed[lfn] = "File already registered with no replicas"
        # If the supplied SE is not in the existing replicas return an error
        elif not lfns[lfn]['SE'] in replicaDict[fileID].keys():
          failed[lfn] = "File already registered with alternative replicas"
        # If we get here the file being registered already exists exactly in the DB
        else:
          successful[lfn] = True
    return successful, failed

  def _checkUniqueGUID( self, lfns, connection = False ):
    connection = self._getConnection( connection )
    guidLFNs = {}
    failed = {}
    for lfn, fileDict in lfns.items():
      guidLFNs[fileDict['GUID']] = lfn
    res = self._getFileIDFromGUID( guidLFNs.keys(), connection = connection )
    if not res['OK']:
      return dict.fromkeys( lfns, res['Message'] )
    for guid, fileID in res['Value'].items():
      failed[guidLFNs[guid]] = "GUID already registered for another file %s" % fileID # resolve this to LFN
    return failed

  def removeFile( self, lfns, connection = False ):
    connection = self._getConnection( connection )
    """ Remove file from the catalog """
    successful = {}
    failed = {}
    res = self._findFiles( lfns, ['DirID', 'FileID', 'Size'], connection = connection )
    for lfn, error in res['Value']['Failed'].items():
      if error == 'No such file or directory':
        successful[lfn] = True
      else:
        failed[lfn] = error
    fileIDLfns = {}
    lfns = res['Value']['Successful']
    for lfn, lfnDict in lfns.items():
      fileIDLfns[lfnDict['FileID']] = lfn

    # Resolve the replicas to calculate reduction in storage usage
    res = self._getFileReplicas( fileIDLfns.keys(), connection = connection )
    if not res['OK']:
      return res
    directorySESizeDict = {}
    for fileID, seDict in res['Value'].items():
      dirID = lfns[fileIDLfns[fileID]]['DirID']
      size = lfns[lfn]['Size']
      directorySESizeDict.setdefault( dirID, {} )
      directorySESizeDict[dirID].setdefault( 0, {'Files':0,'Size':0} )
      directorySESizeDict[dirID][0]['Size'] += size
      directorySESizeDict[dirID][0]['Files'] += 1
      for seName in seDict.keys():
        res = self.db.seManager.findSE( seName )
        if not res['OK']:
          return res
        seID = res['Value']
        size = lfns[fileIDLfns[fileID]]['Size']
        directorySESizeDict[dirID].setdefault( seID, {'Files':0,'Size':0} )
        directorySESizeDict[dirID][seID]['Size'] += size
        directorySESizeDict[dirID][seID]['Files'] += 1

    #Remove files from Ancestor tables
    res = self._removeFileAncestors(fileIDLfns.keys(), connection = connection )
    if res['OK'] and res['Value']:
      for fid in res['Value']['Successful'].keys():
        successful[fileIDLfns[fid]] = True
      for fid, reason in res['Value']['Failed'].items():
        failed[fileIDLfns[fid]] = reason
        
    # Now do removal  
    res = self._deleteFiles( fileIDLfns.keys(), connection = connection )
    if not res['OK']:
      for lfn in fileIDLfns.values():
        failed[lfn] = res['Message']
    else:
      # Update the directory usage
      self._updateDirectoryUsage( directorySESizeDict, '-', connection = connection )
      for lfn in fileIDLfns.values():
        successful[lfn] = True
    return S_OK( {"Successful":successful, "Failed":failed} )

  def _setFileOwner( self, fileID, owner, connection = False ):
    """ Set the file owner """
    connection = self._getConnection( connection )
    if type( owner ) in StringTypes:
      result = self.db.ugManager.findUser( owner )
      if not result['OK']:
        return result
      owner = result['Value']
    return self._setFileParameter( fileID, 'UID', owner, connection = connection )

  def _setFileGroup( self, fileID, group, connection = False ):
    """ Set the file group """
    connection = self._getConnection( connection )
    if type( group ) in StringTypes:
      result = self.db.ugManager.findGroup( group )
      if not result['OK']:
        return result
      group = result['Value']
    return self._setFileParameter( fileID, 'GID', group, connection = connection )

  def _setFileMode( self, fileID, mode, connection = False ):
    """ Set the file mode """
    connection = self._getConnection( connection )
    return self._setFileParameter( fileID, 'Mode', mode, connection = connection )
  
  def _setFileStatus( self, fileID, status, connection = False ):
    """ Set file status
    """
    connection = self._getConnection( connection )
    return self._setFileParameter( fileID, 'Status', status, connection = connection )
  
  def setFileStatus( self, lfns, connection = False ):
    """ Set the status of the given files
    """
    successful = {}
    failed = {}
    for lfn,status in lfns.items():
      result = self._findFiles( [lfn], ['FileID'], connection = connection )
      if not result['Value']['Successful'].has_key( lfn ):
        failed[lfn] = result['Value']['Failed'][lfn]
        continue
      fileID = result['Value']['Successful'][lfn]['FileID']
      result = self._setFileStatus( fileID, status, connection )
      if not result['OK']:
        failed[lfn] = result['Message']
      else:
        successful['lfn'] = True  
      
    return S_OK( {'Successful':successful, 'Failed':failed} )  

  ######################################################
  #
  # Replica write methods
  #

  def addReplica( self, lfns, connection = False ):
    """ Add replica to the catalog """
    connection = self._getConnection( connection )
    successful = {}
    failed = {}
    for lfn, info in lfns.items():
      res = self._checkInfo( info, ['PFN', 'SE'] )
      if not res['OK']:
        failed[lfn] = res['Message']
        lfns.pop( lfn )
    res = self._addReplicas( lfns, connection = connection )
    if not res['OK']:
      for lfn in lfns.keys():
        failed[lfn] = res['Message']
    else:
      failed.update( res['Value']['Failed'] )
      successful.update( res['Value']['Successful'] )
    return S_OK( {'Successful':successful, 'Failed':failed} )

  def _addReplicas( self, lfns, connection = False ):

    connection = self._getConnection( connection )
    successful = {}
    res = self._findFiles( lfns.keys(), ['DirID', 'FileID', 'Size'], connection = connection )
    failed = res['Value']['Failed']
    for lfn in failed.keys():
      lfns.pop( lfn )
    lfnFileIDDict = res['Value']['Successful']
    for lfn, fileDict in lfnFileIDDict.items():
      lfns[lfn].update( fileDict )
    res = self._insertReplicas( lfns, connection = connection )
    if not res['OK']:
      for lfn in lfns.keys():
        failed[lfn] = res['Message']
    else:
      successful = res['Value']['Successful']
      failed.update( res['Value']['Failed'] )
    return S_OK( {'Successful':successful, 'Failed':failed} )

  def removeReplica( self, lfns, connection = False ):
    """ Remove replica from catalog """
    connection = self._getConnection( connection )
    successful = {}
    failed = {}
    for lfn, info in lfns.items():
      res = self._checkInfo( info, ['SE'] )
      if not res['OK']:
        failed[lfn] = res['Message']
        lfns.pop( lfn )
    res = self._deleteReplicas( lfns, connection = connection )
    if not res['OK']:
      for lfn in lfns.keys():
        failed[lfn] = res['Message']
    else:
      failed.update( res['Value']['Failed'] )
      successful.update( res['Value']['Successful'] )
    return S_OK( {'Successful':successful, 'Failed':failed} )

  def setReplicaStatus( self, lfns, connection = False ):
    """ Set replica status in the catalog """
    connection = self._getConnection( connection )
    successful = {}
    failed = {}
    for lfn, info in lfns.items():
      res = self._checkInfo( info, ['SE', 'Status'] )
      if not res['OK']:
        failed[lfn] = res['Message']
        continue
      status = info['Status']
      se = info['SE']
      res = self._findFiles( [lfn], ['FileID'], connection = connection )
      if not res['Value']['Successful'].has_key( lfn ):
        failed[lfn] = res['Value']['Failed'][lfn]
        continue
      fileID = res['Value']['Successful'][lfn]['FileID']
      res = self._setReplicaStatus( fileID, se, status, connection = connection )
      if res['OK']:
        successful[lfn] = res['Value']
      else:
        failed[lfn] = res['Message']
    return S_OK( {'Successful':successful, 'Failed':failed} )

  def setReplicaHost( self, lfns, connection = False ):
    """ Set replica host in the catalog """
    connection = self._getConnection( connection )
    successful = {}
    failed = {}
    for lfn, info in lfns.items():
      res = self._checkInfo( info, ['SE', 'NewSE'] )
      if not res['OK']:
        failed[lfn] = res['Message']
        continue
      newSE = info['NewSE']
      se = info['SE']
      res = self._findFiles( [lfn], ['FileID'], connection = connection )
      if not res['Value']['Successful'].has_key( lfn ):
        failed[lfn] = res['Value']['Failed'][lfn]
        continue
      fileID = res['Value']['Successful'][lfn]['FileID']
      res = self._setReplicaHost( fileID, se, newSE, connection = connection )
      if res['OK']:
        successful[lfn] = res['Value']
      else:
        failed[lfn] = res['Message']
    return S_OK( {'Successful':successful, 'Failed':failed} )

  ######################################################
  #
  # File read methods
  #

  def exists( self, lfns, connection = False ):
    """ Determine whether a file exists in the catalog """
    connection = self._getConnection( connection )
    res = self._findFiles( lfns, connection = connection )
    successful = dict.fromkeys( res['Value']['Successful'], True )
    failed = {}
    for lfn, error in res['Value']['Failed'].items():
      if error == 'No such file or directory':
        successful[lfn] = False
      else:
        failed[lfn] = error
    return S_OK( {"Successful":successful, "Failed":failed} )

  def isFile( self, lfns, connection = False ):
    """ Determine whether a path is a file in the catalog """
    connection = self._getConnection( connection )
    #TO DO, should check whether it is a directory if it fails
    return self.exists( lfns, connection = connection )

  def getFileSize( self, lfns, connection = False ):
    """ Get file size from the catalog """
    connection = self._getConnection( connection )
    #TO DO, should check whether it is a directory if it fails
    res = self._findFiles( lfns, ['Size'], connection = connection )
    if not res['OK']:
      return res
    
    totalSize = 0
    for lfn in res['Value']['Successful'].keys():
      size = res['Value']['Successful'][lfn]['Size']
      res['Value']['Successful'][lfn] = size
      totalSize += size
      
    res['TotalSize'] = totalSize  
    return res

  def getFileMetadata( self, lfns, connection = False ):
    """ Get file metadata from the catalog """
    connection = self._getConnection( connection )
    #TO DO, should check whether it is a directory if it fails
    return self._findFiles( lfns, ['Size', 'Checksum',
                                   'ChecksumType', 'UID',
                                   'GID', 'GUID',
                                   'CreationDate', 'ModificationDate',
                                   'Mode', 'Status'], connection = connection )

  def getPathPermissions( self, paths, credDict, connection = False ):
    """ Get the permissions for the supplied paths """
    connection = self._getConnection( connection )
    res = self.db.ugManager.getUserAndGroupID( credDict )
    if not res['OK']:
      return res
    uid, gid = res['Value']
    res = self._findFiles( paths, metadata = ['Mode', 'UID', 'GID'], connection = connection )
    if not res['OK']:
      return res
    successful = {}
    for dirName, dirDict in res['Value']['Successful'].items():
      mode = dirDict['Mode']
      p_uid = dirDict['UID']
      p_gid = dirDict['GID']
      successful[dirName] = {}
      if p_uid == uid:
        successful[dirName]['Read'] = mode & stat.S_IRUSR
        successful[dirName]['Write'] = mode & stat.S_IWUSR
        successful[dirName]['Execute'] = mode & stat.S_IXUSR
      elif p_gid == gid:
        successful[dirName]['Read'] = mode & stat.S_IRGRP
        successful[dirName]['Write'] = mode & stat.S_IWGRP
        successful[dirName]['Execute'] = mode & stat.S_IXGRP
      else:
        successful[dirName]['Read'] = mode & stat.S_IROTH
        successful[dirName]['Write'] = mode & stat.S_IWOTH
        successful[dirName]['Execute'] = mode & stat.S_IXOTH
    return S_OK( {'Successful':successful, 'Failed':res['Value']['Failed']} )


  ######################################################
  #
  # Replica read methods
  #
  
  def __getReplicasForIDs( self, fileIDLfnDict, allStatus, connection = False ):
    """ Get replicas for files with already resolved IDs
    """
    replicas = {}
    if fileIDLfnDict:
      fields = []
      if not self.db.lfnPfnConvention or self.db.lfnPfnConvention == "Weak":
        fields = ['PFN']
      res = self._getFileReplicas( fileIDLfnDict.keys(), fields_input=fields,
                                   allStatus = allStatus, connection = connection )
      if not res['OK']:
        return res
      for fileID, seDict in res['Value'].items():
        lfn = fileIDLfnDict[fileID]
        replicas[lfn] = {}
        for se, repDict in seDict.items():
          pfn = repDict.get('PFN','')
          #if not pfn or self.db.lfnPfnConvention:
          #  res = self._resolvePFN( lfn, se )
          #  if res['OK']:
          #    pfn = res['Value']
          replicas[lfn][se] = pfn
                
    result = S_OK( replicas )
    return result

  def getReplicas( self, lfns, allStatus, connection = False ):
    """ Get file replicas from the catalog """
    connection = self._getConnection( connection )

    # Get FileID <-> LFN correspondence first
    res = self._findFileIDs( lfns, connection = connection )
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    fileIDLFNs = {}
    for lfn, fileID in res['Value']['Successful'].items():
      fileIDLFNs[fileID] = lfn

    result = self.__getReplicasForIDs( fileIDLFNs, allStatus, connection)
    if not result['OK']:
      return result
    replicas = result['Value']
    
    result = S_OK( { "Successful": replicas, 'Failed': failed } )
    
    if self.db.lfnPfnConvention:
      sePrefixDict = {}
      resSE = self.db.seManager.getSEPrefixes()
      if resSE['OK']:
        sePrefixDict = resSE['Value']
      result['Value']['SEPrefixes'] = sePrefixDict
      
    return result
  
  def getReplicasByMetadata( self, metaDict, path, allStatus, credDict, connection = False ):
    """ Get file replicas for files corresponding to the given metadata """
    connection = self._getConnection( connection )

    # Get FileID <-> LFN correspondence first
    failed = {}
    result = self.db.fmeta.findFilesByMetadata( metaDict, path, credDict, extra = True)
    if not result['OK']:
      return result
    fileIDLFNs = result['Value']

    result = self.__getReplicasForIDs( fileIDLFNs, allStatus, connection)
    if not result['OK']:
      return result
    replicas = result['Value']
    
    result = S_OK( { "Successful": replicas, 'Failed': failed } )
    
    if self.db.lfnPfnConvention:
      sePrefixDict = {}
      resSE = self.db.seManager.getSEPrefixes()
      if resSE['OK']:
        sePrefixDict = resSE['Value']
      result['Value']['SEPrefixes'] = sePrefixDict
      
    return result
  
  def _resolvePFN(self,lfn,se):
    resSE = self.db.seManager.getSEDefinition(se)
    if not resSE['OK']:
      return resSE
    pfnDict = dict(resSE['Value']['SEDict'])
    if "PFNPrefix" in pfnDict:
      return S_OK(pfnDict['PFNPrefix']+lfn)
    else:
      pfnDict['FileName'] = lfn
      return pfnunparse(pfnDict)

  def getReplicaStatus( self, lfns, connection = False ):
    """ Get replica status from the catalog """
    connection = self._getConnection( connection )
    res = self._findFiles( lfns, connection = connection )
    failed = res['Value']['Failed']
    fileIDLFNs = {}
    for lfn, fileDict in res['Value']['Successful'].items():
      fileID = fileDict['FileID']
      fileIDLFNs[fileID] = lfn
    successful = {}
    if fileIDLFNs:
      res = self._getFileReplicas( fileIDLFNs.keys(), connection = connection )
      if not res['OK']:
        return res
      for fileID, seDict in res['Value'].items():
        lfn = fileIDLFNs[fileID]
        requestedSE = lfns[lfn]
        if not requestedSE:
          failed[lfn] = "Replica info not supplied"
        elif requestedSE not in seDict.keys():
          failed[lfn] = "No replica at supplied site"
        else:
          successful[lfn] = seDict[requestedSE]['Status']
    return S_OK( {'Successful':successful, 'Failed':failed} )

  ######################################################
  #
  # General usage methods
  #

  def _getStatusInt( self, status, connection = False ):
    connection = self._getConnection( connection )
    req = "SELECT StatusID FROM FC_Statuses WHERE Status = '%s';" % status
    res = self.db._query( req, connection )
    if not res['OK']:
      return res
    if res['Value']:
      return S_OK( res['Value'][0][0] )
    req = "INSERT INTO FC_Statuses (Status) VALUES ('%s');" % status
    res = self.db._update( req, connection )
    if not res['OK']:
      return res
    return S_OK( res['lastRowId'] )

  def _getIntStatus(self,statusID,connection=False):
    if statusID in self.statusDict:
      return S_OK(self.statusDict[statusID])
    connection = self._getConnection(connection)
    req = "SELECT StatusID,Status FROM FC_Statuses" 
    res = self.db._query(req,connection)
    if not res['OK']:
      return res
    if res['Value']:
      for row in res['Value']:
        self.statusDict[int(row[0])] = row[1]
    if statusID in self.statusDict:
      return S_OK(self.statusDict[statusID])
    return S_OK('Unknown')

  def getFilesInDirectory( self, dirID, verbose = False, connection = False ):
    connection = self._getConnection( connection )
    files = {}
    res = self._getDirectoryFiles( dirID, [], ['FileID', 'Size',
                                               'Checksum', 'ChecksumType',
                                               'Type', 'UID',
                                               'GID', 'CreationDate',
                                               'ModificationDate', 'Mode',
                                               'Status'], connection = connection )
    if not res['OK']:
      return res
    if not res['Value']:
      return S_OK( files )
    fileIDNames = {}
    for fileName, fileDict in res['Value'].items():
      files[fileName] = {}
      files[fileName]['MetaData'] = fileDict
      fileIDNames[fileDict['FileID']] = fileName
      
    if verbose:
      result = self._getFileReplicas( fileIDNames.keys(), connection = connection )
      if not result['OK']:
        return result
      for fileID, seDict in result['Value'].items():
        fileName = fileIDNames[fileID]
        files[fileName]['Replicas'] = seDict
        
    return S_OK( files )

  def getDirectoryReplicas( self, dirID, path, allStatus = False, connection = False ):
    
    connection = self._getConnection( connection )
    result = self._getDirectoryReplicas( dirID, allStatus, connection)
    if not result['OK']:
      return result
    
    resultDict = {}
    seDict = {}
    for fileName, fileID, seID, pfn in result['Value']:
      resultDict.setdefault( fileName, {} )
      if not seID in seDict:
        res = self.db.seManager.getSEName(seID)
        if not res['OK']:
          seDict[seID] = 'Unknown'
        else:  
          seDict[seID] = res['Value']
      se = seDict[seID]    
      resultDict[fileName][se] = pfn

    return S_OK( resultDict )

  def _getFileDirectories( self, lfns ):
    dirDict = {}
    for lfn in lfns:
      lfnDir = os.path.dirname( lfn )
      lfnFile = os.path.basename( lfn )
      dirDict.setdefault( lfnDir, [] )
      dirDict[lfnDir].append( lfnFile )
    return dirDict

  def _checkInfo( self, info, requiredKeys ):
    if not info:
      return S_ERROR( "Missing parameters" )
    for key in requiredKeys:
      if not key in info:
        return S_ERROR( "Missing '%s' parameter" % key )
    return S_OK()

  # def _checkLFNPFNConvention( self, lfn, pfn, se ):
  #   """ Check that the PFN corresponds to the LFN-PFN convention """
  #   if pfn == lfn:
  #     return S_OK()
  #   if ( len( pfn ) < len( lfn ) ) or ( pfn[-len( lfn ):] != lfn ) :
  #     return S_ERROR( 'PFN does not correspond to the LFN convention' )
  #  return S_OK()

  def _checkLFNPFNConvention( self, lfn, pfn, se ):
    """ Check that the PFN corresponds to the LFN-PFN convention
    """
    # Check if the PFN corresponds to the LFN convention
    if pfn == lfn:
      return S_OK()
    lfn_pfn = True   # flag that the lfn is contained in the pfn
    if ( len( pfn ) < len( lfn ) ) or ( pfn[-len( lfn ):] != lfn ) :
      return S_ERROR( 'PFN does not correspond to the LFN convention' )
    if not pfn.endswith( lfn ):
      return S_ERROR()
    # Check if the pfn corresponds to the SE definition
    result = self._getStorageElement( se )
    if not result['OK']:
      return result
    selement = result['Value']
    res = pfnparse( pfn )
    if not res['OK']:
      return res
    pfnDict = res['Value']
    protocol = pfnDict['Protocol']
    pfnpath = pfnDict['Path']
    result = selement.getStorageParameters( protocol )
    if not result['OK']:
      return result
    seDict = result['Value']
    sePath = seDict['Path']
    ind = pfnpath.find( sePath )
    if ind == -1:
      return S_ERROR( 'The given PFN %s does not correspond to the %s SE definition' % ( pfn, se ) )
    # Check the full LFN-PFN-SE convention
    if lfn_pfn:
      seAccessDict = dict( seDict )
      seAccessDict['Path'] = sePath + '/' + lfn
      check_pfn = pfnunparse( seAccessDict )
      if check_pfn != pfn:
        return S_ERROR( 'PFN does not correspond to the LFN convention' )
    return S_OK()

  def _getStorageElement( self, seName ):
    from DIRAC.Resources.Storage.StorageElement              import StorageElement
    storageElement = StorageElement( seName )
    if not storageElement.valid:
      return S_ERROR( storageElement.errorReason )
    return S_OK( storageElement )

  def setFileGroup( self, lfns, uid=0, gid=0, connection = False ):
    """ Get set the group for the supplied files """
    connection = self._getConnection( connection )
    res = self._findFiles( lfns, ['FileID', 'GID'], connection = connection )
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    successful = {}
    for lfn in res['Value']['Successful'].keys():
      group = lfns[lfn]['Group']
      if type( group ) in StringTypes:
        groupRes = self.db.ugManager.findGroup( group )
        if not groupRes['OK']:
          return groupRes
        group = groupRes['Value']
      currentGroup = res['Value']['Successful'][lfn]['GID']
      if int( group ) == int( currentGroup ):
        successful[lfn] = True
      else:
        fileID = res['Value']['Successful'][lfn]['FileID']
        res = self._setFileGroup( fileID, group, connection = connection )
        if not res['OK']:
          failed[lfn] = res['Message']
        else:
          successful[lfn] = True
    return S_OK( {'Successful':successful, 'Failed':failed} )

  def setFileOwner( self, lfns, uid=0, gid=0, connection = False ):
    """ Get set the group for the supplied files """
    connection = self._getConnection( connection )
    res = self._findFiles( lfns, ['FileID', 'UID'], connection = connection )
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    successful = {}
    for lfn in res['Value']['Successful'].keys():
      owner = lfns[lfn]['Owner']
      if type( owner ) in StringTypes:
        userRes = self.db.ugManager.findUser( owner )
        if not userRes['OK']:
          return userRes
        owner = userRes['Value']
      currentOwner = res['Value']['Successful'][lfn]['UID']
      if int( owner ) == int( currentOwner ):
        successful[lfn] = True
      else:
        fileID = res['Value']['Successful'][lfn]['FileID']
        res = self._setFileOwner( fileID, owner, connection = connection )
        if not res['OK']:
          failed[lfn] = res['Message']
        else:
          successful[lfn] = True
    return S_OK( {'Successful':successful, 'Failed':failed} )

  def setFileMode( self, lfns, uid=0, gid=0, connection = False ):
    """ Get set the mode for the supplied files """
    connection = self._getConnection( connection )
    res = self._findFiles( lfns, ['FileID', 'Mode'], connection = connection )
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    successful = {}
    for lfn in res['Value']['Successful'].keys():
      mode = lfns[lfn]['Mode']
      currentMode = res['Value']['Successful'][lfn]['Mode']
      if int( currentMode ) == int( mode ):
        successful[lfn] = True
      else:
        fileID = res['Value']['Successful'][lfn]['FileID']
        res = self._setFileMode( fileID, mode, connection = connection )
        if not res['OK']:
          failed[lfn] = res['Message']
        else:
          successful[lfn] = True
    return S_OK( {'Successful':successful, 'Failed':failed} )

  def changePathOwner( self, paths, credDict, recursive = False ):
    """ Bulk method to change Owner for the given paths """
    return self._changePathFunction( paths, credDict, self.db.dtree.changeDirectoryOwner,
                                    self.setFileOwner, recursive )

  def changePathGroup( self, paths, credDict, recursive = False ):
    """ Bulk method to change Owner for the given paths """
    return self._changePathFunction( paths, credDict, self.db.dtree.changeDirectoryGroup,
                                    self.setFileGroup, recursive )

  def changePathMode( self, paths, credDict, recursive = False ):
    """ Bulk method to change Owner for the given paths """
    return self._changePathFunction( paths, credDict, self.db.dtree.changeDirectoryMode,
                                    self.setFileMode, recursive )

  def _changePathFunction( self, paths, credDict, change_function_directory, change_function_file, recursive = False ):
    """ A generic function to change Owner, Group or Mode for the given paths """
    result = self.db.ugManager.getUserAndGroupID( credDict )
    if not result['OK']:
      return result
    uid, gid = result['Value']

    dirList = []
    result = self.db.isDirectory( paths, credDict )
    if not result['OK']:
      return result
    for p in result['Value']['Successful']:
      if result['Value']['Successful'][p]:
        dirList.append( p )
    fileList = []
    if len( dirList ) < len( paths ):
      result = self.isFile( paths )
      if not result['OK']:
        return result
      fileList = result['Value']['Successful'].keys()

    successful = {}
    failed = {}

    dirArgs = {}
    fileArgs = {}

    for path in paths:
      if ( not path in dirList ) and ( not path in fileList ):
        failed[path] = 'Path not found'
      if path in dirList:
        dirArgs[path] = paths[path]
      elif path in fileList:
        fileArgs[path] = paths[path]
    if dirArgs:
      result = change_function_directory( dirArgs, uid, gid )
      if not result['OK']:
        return result
      successful.update( result['Value']['Successful'] )
      failed.update( result['Value']['Failed'] )
    if fileArgs:
      result = change_function_file( fileArgs, uid, gid )
      if not result['OK']:
        return result
      successful.update( result['Value']['Successful'] )
      failed.update( result['Value']['Failed'] )
    return S_OK( {'Successful':successful, 'Failed':failed} )
  
