from DIRAC import gConfig, gLogger
from DIRAC.DataManagementSystem.DB.DataIntegrityDB import DataIntegrityDB

def test():
  """ Some test cases
  """

  host = '127.0.0.1'
  user = 'Dirac'
  pwd = 'Dirac'
  db = 'DataIntegrityDB'

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
