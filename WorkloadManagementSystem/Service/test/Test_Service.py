import unittest, sys
# from DIRAC.WorkloadManagementSystem.Service.JobPolicy import JobPolicy, RIGHT_GET_INFO
import DIRAC.WorkloadManagementSystem.Service.JobPolicy as JP

def myGetPropertiesForGroup( a, b ):
  if a == 'user':
    return ['NormalUser']
  elif a == 'monitor':
    return ['JobMonitor', 'NormalUser']
  elif a == 'mgr':
    return ['ProductionManagement', 'NormalUser', 'JobSharing', 'JobAdministrator', 'SiteManager', 'Operator']
  else:
    return []

def myGetUsernameForDN( a ):
  return {'OK':True, 'Value':'aUser'}

def myGetGroupsForUser( a ):
  if a == 'aUser':
    return {'OK': True, 'Value': ['user']}
  else:
    return {'OK': True, 'Value': ['user', 'monitor']}

class ServiceTestCase( unittest.TestCase ):
  """ Base class for the Service test cases
  """
  def setUp( self ):
    pass

  def tearDown( self ):
    pass

class JobPolicySuccess( ServiceTestCase ):
  def test_aTest( self ):

    sys.modules['DIRAC.WorkloadManagementSystem.Service.JobPolicy'].getPropertiesForGroup = myGetPropertiesForGroup
    sys.modules['DIRAC.WorkloadManagementSystem.Service.JobPolicy'].getUsernameForDN = myGetUsernameForDN
    sys.modules['DIRAC.WorkloadManagementSystem.Service.JobPolicy'].getGroupsForUser = myGetGroupsForUser

    jp = JP.JobPolicy( '/just/a/DN', 'user', True )
    res = jp.getControlledUsers( JP.RIGHT_GET_INFO )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 'ALL' )

    jp = JP.JobPolicy( '/just/a/DN', 'user', False )
    res = jp.getControlledUsers( JP.RIGHT_GET_INFO )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], [( 'aUser', 'user' )] )

    jp = JP.JobPolicy( '/just/a/DN', 'monitor', True )
    res = jp.getControlledUsers( JP.RIGHT_GET_INFO )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 'ALL' )

    jp = JP.JobPolicy( '/just/a/DN', 'monitor', False )
    res = jp.getControlledUsers( JP.RIGHT_GET_INFO )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 'ALL' )

    jp = JP.JobPolicy( '/just/a/DN', 'mgr', True )
    res = jp.getControlledUsers( JP.RIGHT_GET_INFO )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 'ALL' )

    jp = JP.JobPolicy( '/just/a/DN', 'mgr', False )
    res = jp.getControlledUsers( JP.RIGHT_GET_INFO )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 'ALL' )


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ServiceTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( JobPolicySuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

