
import mock
import unittest
import stat
import VOMSPolicy

# from DIRAC import S_OK, S_ERROR
def S_OK( value ):
  return { 'OK' : True, 'Value': value}

def S_ERROR( msg ):
  return { 'OK' : False, 'Message' : msg}


diracGrps = [{'grp_admin' : None,
              'grp_data' : 'vomsProd',
              'grp_mc' : 'vomsProd',
              'grp_user' : 'vomsUser',
              'grp_nothing' : None}]


def makeNode(owner, group, mode ):
  return {'owner' : owner, 'OwnerGroup' : group, 'mode' : mode}

directoryTree = {}

def setupTree():
  global directoryTree
  directoryTree = {}

  # Only root and members from grp_admin should be able to create something in the root directory
  directoryTree['/'] = makeNode( 'root', 'grp_admin', 0o775 )

  # groups with vomsProd should be able to create and remove directories
  # in /realData
  directoryTree['/realData'] = makeNode( 'dm', 'grp_data', 0o775 )
  directoryTree['/realData/run1'] = makeNode( 'dm', 'grp_data', 0o775 )
  directoryTree['/realData/run2'] = makeNode( 'dm', 'grp_data', 0o775 )
  # No one should be able to write in this one
  directoryTree['/realData/run3'] = makeNode( 'otherdm', 'grp_data', 0o555 )

  # Only root can create new user directories or remove them
  # Only the user can create or remove its subdirs
  directoryTree['/users'] = makeNode( 'root', 'grp_admin', 0o755 )
  directoryTree['/users/usr1'] = makeNode( 'usr1', 'grp_user', 0o755 )
  directoryTree['/users/usr2'] = makeNode( 'usr2', 'grp_user', 0o755 )

  # grp_data and grp_mc atre both prodVoms role so should be able to write
  directoryTree['/mc'] = makeNode( 'mc1', 'grp_mc', 0o775 )
  directoryTree['/mc/prod1'] = makeNode( 'mc1', 'grp_data', 0o775 )
  directoryTree['/mc/prod2'] = makeNode( 'mc2', 'grp_mc', 0o775 )


nonExistingDirectories = ['/realData/futurRun', '/fakeBaseDir', '/users/usr1/subUsr1', 'users/usr2/subUsr2']


class mock_DirectoryManager(object):
  def __init__(self):
    pass
  
  def exists( self, lfns ):
    return S_OK( {'Successful' : dict( ( lfn, lfn in directoryTree ) for lfn in lfns ), 'Failed' : {}} )
  
  def getDirectoryParameters(self, path):
    return S_OK( directoryTree[path] ) if path in directoryTree else S_ERROR( 'Directory not found' )

  def getDirectoryPermissions(self, path, credDict):
    if path not in directoryTree:
      return S_ERROR( 'Directory not found' )

    owner = ( credDict['username'] == directoryTree[path]['owner'] )
    group = ( credDict['group'] == directoryTree[path]['OwnerGroup'] )
    mode = directoryTree[path]['mode']
    resultDict = {}

    resultDict['Read'] = ( owner and mode & stat.S_IRUSR > 0 )\
                         or ( group and mode & stat.S_IRGRP > 0 )\
                         or mode & stat.S_IROTH > 0
                           
    resultDict['Write'] = ( owner and mode & stat.S_IWUSR > 0 )\
                          or ( group and mode & stat.S_IWGRP > 0 )\
                          or mode & stat.S_IWOTH > 0

    resultDict['Execute'] = ( owner and mode & stat.S_IXUSR > 0 )\
                            or ( group and mode & stat.S_IXGRP > 0 )\
                            or mode & stat.S_IXOTH > 0

    return S_OK( resultDict )


class mock_db(object):
  def __init__(self):
    self.globalReadAccess = False
    self.dtree = mock_DirectoryManager()

class mock_SecurityManagerBase( object ):
  
  def __init__( self, database = False ):
    self.db = mock_db()

  def hasAdminAccess( self, credDict ):
    return S_OK( False )

VOMSPolicy.VOMSPolicy.__bases__ = ( mock_SecurityManagerBase, )


def mock_getAllGroups():
  print "IC OME HERE !!!"
  return diracGrps.keys()

def mock_getGroupOption( grpName, grpOption ):
  print "IC OME THERE !!!"

  return diracGrps[grpName]



class TestBase(unittest.TestCase):

  def setUp( self ):
    self.policy = VOMSPolicy.VOMSPolicy()
    
  @mock.patch( 'VOMSPolicy.getGroupOption', side_effect = mock_getGroupOption )
  @mock.patch( 'VOMSPolicy.getAllGroups', side_effect = mock_getAllGroups )
  def call( self, methodName, mk_getAllGroups, mk_getGroupOption ):
    self.existingRet = self.policy.hasAccess( methodName, directoryTree.keys(), self.credDict )
    self.nonExistingRet = self.policy.hasAccess( methodName, nonExistingDirectories, self.credDict )

  def compareResult(self):
    
    # For existing dirs first
    self.assert_( self.existingRet['OK'] == self.expectedExistingRet['OK'] )
    

  def test_removeDirectory( self ):
    self.call( 'removeDirectory' )
    self.compareResult()
    



class TestNonExistingUser( TestBase ):

  def setUp( self ):
    super( TestNonExistingUser, self ).setUp()
    self.credDict = {'username':'anon', 'group':'grp_nothing'}


  def test_removeDirectory( self ):
    self.expectedExistingRet = {'OK': True, 'Value': {'Successful': {'/mc': False, '/realData/run1': False, '/realData/run3': False, '/realData/run2': False, '/mc/prod2': False, '/': False, '/mc/prod1': False, '/users/usr1': False, '/users/usr2': False, '/realData': False, '/users': False}, 'Failed': {}}}
    super( TestNonExistingUser, self ).test_removeDirectory()

    





if __name__ == '__main__':
  setupTree()
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestNonExistingUser )
  unittest.TextTestRunner( verbosity = 2 ).run( suite )
