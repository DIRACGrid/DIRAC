########################################################################
# $HeadURL$
########################################################################
""" DataIntegrityDB class is a front-end to the Data Integrity Database. """
__RCSID__ = "$Id$"

from DIRAC import gConfig, gLogger, S_OK
from DIRAC.Core.Base.DB import DB

#############################################################################
class DataIntegrityDB( DB ):
  """
CREATE TABLE Problematics(
  FileID INTEGER NOT NULL AUTO_INCREMENT,
  Prognosis VARCHAR(32) NOT NULL,
  LFN VARCHAR(255) NOT NULL,
  PFN VARCHAR(255),
  Size BIGINT(20),
  SE VARCHAR(32),
  GUID VARCHAR(255),
  Status VARCHAR(32) DEFAULT 'New',
  Retries INTEGER DEFAULT 0,
  InsertDate DATETIME NOT NULL,
  LastUpdate DATETIME NOT NULL,
  Source VARCHAR(127) NOT NULL DEFAULT 'Unknown',
  PRIMARY KEY(FileID),
  INDEX (Prognosis,Status)
);
"""

  tableName = 'Problematics'
  tableDict = { tableName: { 'Fields' : { 'FileID': 'INTEGER NOT NULL AUTO_INCREMENT',
                                          'Prognosis': 'VARCHAR(32) NOT NULL',
                                          'LFN': 'VARCHAR(255) NOT NULL',
                                          'PFN': 'VARCHAR(255)',
                                          'Size': 'BIGINT(20)',
                                          'SE': 'VARCHAR(32)',
                                          'GUID': 'VARCHAR(255)',
                                          'Status': 'VARCHAR(32) DEFAULT "New"',
                                          'Retries': 'INTEGER DEFAULT 0',
                                          'InsertDate': 'DATETIME NOT NULL',
                                          'LastUpdate': 'DATETIME NOT NULL',
                                          'Source': 'VARCHAR(127) NOT NULL DEFAULT "Unknown"',
                                         },
                             'PrimaryKey': 'FileID',
                             'Indexes': { 'PS': ['Prognosis', 'Status']
                                         }
                            }
               }

  fieldList = ['FileID', 'LFN', 'PFN', 'Size', 'SE', 'GUID', 'Prognosis']

  def __init__( self, maxQueueSize = 10 ):
    """ Standard Constructor
    """
    DB.__init__( self, 'DataIntegrityDB', 'DataManagement/DataIntegrityDB', maxQueueSize )

  def _checkTable( self ):
    """ Make sure the table is created
    """
    return self._createTables( self.tableDict, force = False )

#############################################################################
  def insertProblematic( self, source, fileMetadata ):
    """ Insert the supplied file metadata into the problematics table
    """
    failed = {}
    successful = {}
    for lfn, metadata in fileMetadata.items():
      condDict = dict( ( key, metadata[key] ) for key in ['Prognosis', 'PFN', 'SE'] )
      condDict['LFN'] = lfn
      res = self.getFields( self.tableName, ['FileID'], condDict = condDict )
      if not res['OK']:
        failed[lfn] = res['Message']
      elif res['Value']:
        successful[lfn] = 'Already exists'
      else:
        metadata['LFN'] = lfn
        metadata['Source'] = source
        metadata['InsertDate'] = 'UTC_TIMESTAMP()'
        metadata['LastUpdate'] = 'UTC_TIMESTAMP()'
        res = self.insertFields( self.tableName, inDict = metadata )
        if res['OK']:
          successful[lfn] = True
        else:
          failed[lfn] = res['Message']
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

#############################################################################
  def getProblematicsSummary( self ):
    """ Get a summary of the current problematics table
    """
    res = self.getCounters( self.tableName, ['Prognosis', 'Status'], {} )
    if not res['OK']:
      return res
    resDict = {}
    for counterDict, count in res['Value']:
      resDict.setdefault( counterDict['Prognosis'], {} )
      resDict[counterDict['Prognosis']][counterDict['Status']] = int( count )
    return S_OK( resDict )

#############################################################################
  def getDistinctPrognosis( self ):
    """ Get a list of all the current problematic types
    """
    return self.getDistinctAttributeValues( self.tableName, 'Prognosis' )

#############################################################################
  def getProblematic( self ):
    """ Get the next file to resolve
    """
    res = self.getFields( self.tableName, self.fieldList,
                          condDict = {'Status': 'New' }, limit = 1, orderAttribute = 'LastUpdate:ASC' )
    if not res['OK']:
      return res
    if not res['Value'][0]:
      return S_OK()
    valueList = list( res['Value'][0] )
    return S_OK( dict( ( key, valueList.pop( 0 ) ) for key in self.fieldList ) )

  def getPrognosisProblematics( self, prognosis ):
    """ Get all the active files with the given problematic
    """
    res = self.getFields( self.tableName, self.fieldList,
                          condDict = {'Prognosis': prognosis, 'Status': 'New' },
                          orderAttribute = ['Retries', 'LastUpdate'] )
    if not res['OK']:
      return res
    problematics = []
    for valueTuple in res['Value']:
      valueList = list( valueTuple )
      problematics.append( dict( ( key, valueList.pop( 0 ) ) for key in self.fieldList ) )
    return S_OK( problematics )

  def getTransformationProblematics( self, transID ):
    """ Get problematic files matching a given production
    """
    req = "SELECT LFN,FileID FROM Problematics WHERE Status = 'New' AND LFN LIKE '%%/%08d/%%';" % transID
    res = self._query( req )
    if not res['OK']:
      return res
    problematics = {}
    for lfn, fileID in res['Value']:
      problematics[lfn] = fileID
    return S_OK( problematics )

  def incrementProblematicRetry( self, fileID ):
    """ Increment retry count
    """
    req = "UPDATE Problematics SET Retries=Retries+1, LastUpdate=UTC_TIMESTAMP() WHERE FileID = %s;" % ( fileID )
    res = self._update( req )
    return res

  def removeProblematic( self, fileID ):
    """ Remove Problematic file by FileID
    """
    return self.deleteEntries( self.tableName, condDict = { 'FileID': fileID } )

  def setProblematicStatus( self, fileID, status ):
    """ Set Status for problematic file by FileID
    """
    return self.updateFields( self.tableName, condDict = { 'FileID': fileID },
                              updateDict = { 'Status': status, 'LastUpdate':'UTC_TIMESTAMP()' } )

  def changeProblematicPrognosis( self, fileID, newPrognosis ):
    """ Change prognisis for file by FileID
    """
    return self.updateFields( self.tableName, condDict = { 'FileID': fileID },
                              updateDict = { 'Prognosis': newPrognosis, 'LastUpdate':'UTC_TIMESTAMP()' } )

def test():
  """ Some test cases
  """

  # building up some fake CS values
  gConfig.setOptionValue( 'DIRAC/Setup', 'Test' )
  gConfig.setOptionValue( '/DIRAC/Setups/Test/DataManagement', 'Test' )

  host = '127.0.0.1'
  user = 'Dirac'
  pwd = 'Dirac'
  db = 'AccountingDB'

  gConfig.setOptionValue( '/Systems/DataManagement/Test/Databases/DataIntegrityDB/Host', host )
  gConfig.setOptionValue( '/Systems/DataManagement/Test/Databases/DataIntegrityDB/DBName', db )
  gConfig.setOptionValue( '/Systems/DataManagement/Test/Databases/DataIntegrityDB/User', user )
  gConfig.setOptionValue( '/Systems/DataManagement/Test/Databases/DataIntegrityDB/Password', pwd )

  diDB = DataIntegrityDB()
  assert diDB._connect()['OK']

  source = 'Test'
  prognosis = 'TestError'
  prodID = 1234
  lfn = '/Test/%08d/File1' % prodID
  fileMetadata1 = {lfn: {'Prognosis': prognosis, 'PFN': 'File1', 'SE': 'Test-SE'}}
  fileOut1 = {'FileID': 1L, 'LFN': lfn, 'PFN': 'File1', 'Prognosis': prognosis,
              'GUID': None, 'SE': 'Test-SE', 'Size': None}
  newStatus = 'Solved'
  newPrognosis = 'AnotherError'

  try:
    gLogger.info( '\n Creating Table\n' )
    # Make sure it is there and it has been created for this test
    result = diDB._checkTable()
    assert result['OK']

    result = diDB._checkTable()
    assert not result['OK']
    assert result['Message'] == 'The requested table already exist'

    result = diDB.insertProblematic( source, fileMetadata1 )
    assert result['OK']
    assert result['Value'] == {'Successful': {lfn: True}, 'Failed': {}}

    result = diDB.insertProblematic( source, fileMetadata1 )
    assert result['OK']
    assert result['Value'] == {'Successful': {lfn: 'Already exists'}, 'Failed': {}}

    result = diDB.getProblematicsSummary()
    assert result['OK']
    assert result['Value'] == {'TestError': {'New': 1}}

    result = diDB.getDistinctPrognosis()
    assert result['OK']
    assert result['Value'] == ['TestError']

    result = diDB.getProblematic()
    assert result['OK']
    assert result['Value'] == fileOut1

    result = diDB.incrementProblematicRetry( result['Value']['FileID'] )
    assert result['OK']
    assert result['Value'] == 1

    result = diDB.getProblematic()
    assert result['OK']
    assert result['Value'] == fileOut1

    result = diDB.getPrognosisProblematics( prognosis )
    assert result['OK']
    assert result['Value'] == [fileOut1]

    result = diDB.getTransformationProblematics( prodID )
    assert result['OK']
    assert result['Value'][lfn] == 1

    result = diDB.setProblematicStatus( 1, newStatus )
    assert result['OK']
    assert result['Value'] == 1

    result = diDB.changeProblematicPrognosis( 1, newPrognosis )
    assert result['OK']
    assert result['Value'] == 1

    result = diDB.getPrognosisProblematics( prognosis )
    assert result['OK']
    assert result['Value'] == []

    result = diDB.removeProblematic( 1 )
    assert result['OK']
    assert result['Value'] == 1

    result = diDB.getProblematicsSummary()
    assert result['OK']
    assert result['Value'] == {}

    gLogger.info( '\n Removing Table\n' )
    result = diDB._update( 'DROP TABLE `%s`' % diDB.tableName )
    assert result['OK']


    gLogger.info( '\n OK\n' )


  except AssertionError:
    print 'ERROR ',
    if not result['OK']:
      print result['Message']
    else:
      print result

    sys.exit( 1 )

if __name__ == '__main__':
  import sys
  import os
  from DIRAC.Core.Base import Script
  Script.parseCommandLine()
  gLogger.setLevel( 'VERBOSE' )

  if 'PYTHONOPTIMIZE' in os.environ and os.environ['PYTHONOPTIMIZE']:
    gLogger.info( 'Unset pyhthon optimization "PYTHONOPTIMIZE"' )
    sys.exit( 0 )

  test()
