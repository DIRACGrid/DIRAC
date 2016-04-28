# FIXME: to bring back to life

"""  This program tests that the Logging DB can be actually queried from DIRAC
"""
import DIRAC
from DIRAC.FrameworkSystem.DB.SystemLoggingDB import SystemLoggingDB

DBpoint=SystemLoggingDB()


DIRAC.exit()





def testSystemLoggingDB():
  """ Some test cases
  """

  # building up some fake CS values
  gConfig.setOptionValue( 'DIRAC/Setup', 'Test' )
  gConfig.setOptionValue( '/DIRAC/Setups/Test/Framework', 'Test' )

  host = '127.0.0.1'
  user = 'Dirac'
  pwd = 'Dirac'
  db = 'AccountingDB'

  gConfig.setOptionValue( '/Systems/Framework/Test/Databases/SystemLoggingDB/Host', host )
  gConfig.setOptionValue( '/Systems/Framework/Test/Databases/SystemLoggingDB/DBName', db )
  gConfig.setOptionValue( '/Systems/Framework/Test/Databases/SystemLoggingDB/User', user )
  gConfig.setOptionValue( '/Systems/Framework/Test/Databases/SystemLoggingDB/Password', pwd )

  from DIRAC.FrameworkSystem.private.logging.Message import tupleToMessage

  systemName = 'TestSystem'
  subSystemName = 'TestSubSystem'
  level = 10
  time = Time.toString()
  msgTest = 'Hello'
  variableText = time
  frameInfo = ""
  message = tupleToMessage( ( systemName, level, time, msgTest, variableText, frameInfo, subSystemName ) )
  site = 'somewehere'
  longSite = 'somewehere1234567890123456789012345678901234567890123456789012345678901234567890'
  nodeFQDN = '127.0.0.1'
  userDN = 'Yo'
  userGroup = 'Us'
  remoteAddress = 'elsewhere'

  records = 10

  db = SystemLoggingDB()
  assert db._connect()['OK']

  try:
    if False:
      for tableName in db.tableDict.keys():
        result = db._update( 'DROP TABLE  IF EXISTS `%s`' % tableName )
        assert result['OK']

      gLogger.info( '\n Creating Table\n' )
      # Make sure it is there and it has been created for this test
      result = db._checkTable()
      assert result['OK']

    result = db._checkTable()
    assert not result['OK']
    assert result['Message'] == 'The requested table already exist'

    gLogger.info( '\n Inserting some records\n' )
    for k in range( records ):
      result = db.insertMessage( message, site, nodeFQDN,
                                  userDN, userGroup, remoteAddress )
      assert result['OK']
      assert result['lastRowId'] == k + 1
      assert result['Value'] == 1

    result = db.insertMessage( message, longSite, nodeFQDN,
                                  userDN, userGroup, remoteAddress )
    assert not result['OK']

    result = db._queryDB( showFieldList = [ 'SiteName' ] )
    assert result['OK']
    assert result['Value'][0][0] == site

    result = db._queryDB( showFieldList = [ 'SystemName' ] )
    assert result['OK']
    assert result['Value'][0][0] == systemName

    result = db._queryDB( showFieldList = [ 'SubSystemName' ] )
    assert result['OK']
    assert result['Value'][0][0] == subSystemName

    result = db._queryDB( showFieldList = [ 'OwnerGroup' ] )
    assert result['OK']
    assert result['Value'][0][0] == userGroup

    result = db._queryDB( showFieldList = [ 'FixedTextString' ] )
    assert result['OK']
    assert result['Value'][0][0] == msgTest

    result = db._queryDB( showFieldList = [ 'VariableText', 'SiteName' ], count = True, groupColumn = 'VariableText' )
    assert result['OK']
    assert result['Value'][0][1] == site
    assert result['Value'][0][2] == records


    gLogger.info( '\n Removing Table\n' )
    for tableName in [ 'MessageRepository', 'FixedTextMessages', 'SubSystems', 'Systems',
                       'AgentPersistentData', 'ClientIPs', 'Sites', 'UserDNs' ]:
      result = db._update( 'DROP TABLE `%s`' % tableName )
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
  from DIRAC.Core.Base import Script
  Script.parseCommandLine()
  gLogger.setLevel( 'VERBOSE' )

  if 'PYTHONOPTIMIZE' in os.environ and os.environ['PYTHONOPTIMIZE']:
    gLogger.info( 'Unset pyhthon optimization "PYTHONOPTIMIZE"' )
    sys.exit( 0 )

  testSystemLoggingDB()
