""" test File Plugin
"""

import mock
import unittest
import itertools

from DIRAC import S_OK
from DIRAC.Resources.Storage.StorageElement import StorageElementItem
from DIRAC.Resources.Storage.StorageBase import StorageBase


class fake_SRM2Plugin( StorageBase ):
  """ Fake SRM2 plugin.
      Only implements the two methods needed
      for transfer, so we can test that it is really this plugin
      that returned
  """
  def putFile( self, lfns, sourceSize = 0 ):
    return S_OK( {'Successful' : dict.fromkeys( lfns, "srm:putFile" ), 'Failed' : {}} )

  def getTransportURL( self, path, protocols = False ):
    return S_OK( {'Successful' : dict.fromkeys( path, "srm:getTransportURL" ), 'Failed' : {}} )

class fake_XROOTPlugin( StorageBase ):
  """ Fake XROOT plugin.
      Only implements the two methods needed
      for transfer, so we can test that it is really this plugin
      that returned
  """

  def putFile( self, lfns, sourceSize = 0 ):
    return S_OK( {'Successful' : dict.fromkeys( lfns, "root:putFile" ), 'Failed' : {}} )

  def getTransportURL( self, path, protocols = False ):
    return S_OK( {'Successful' : dict.fromkeys( path, "root:getTransportURL" ), 'Failed' : {}} )


def mock_StorageFactory_generateStorageObject( storageName, pluginName, parameters, hideExceptions = False ):
  """ Generate fake storage object"""
  storageObj = StorageBase( storageName, parameters )

  if pluginName == "SRM2":
    storageObj = fake_SRM2Plugin( storageName, parameters )
    storageObj.protocolParameters['InputProtocols'] = ['file', 'root', 'srm']
    storageObj.protocolParameters['OutputProtocols'] = ['file', 'root', 'dcap', 'gsidcap', 'rfio', 'srm']
  elif pluginName == "File":
    # Not needed to do anything, StorageBase should do it :)
    pass
  elif pluginName == 'XROOT':
    storageObj = fake_XROOTPlugin( storageName, parameters )
    storageObj.protocolParameters['InputProtocols'] = ['file', 'root']
    storageObj.protocolParameters['OutputProtocols'] = ['root']

  storageObj.pluginName = pluginName

  return S_OK( storageObj )

def mock_StorageFactory_getConfigStorageName( storageName, referenceType ):
  return S_OK( storageName )

def mock_StorageFactory_getConfigStorageOptions( storageName, derivedStorageName = None ):
  """ Get the options associated to the StorageElement as defined in the CS
  """

  options = {'BackendType': 'local',
                            'ReadAccess': 'Active',
                            'WriteAccess': 'Active',
            }

  if storageName in ( 'StorageE', ):
    options['WriteProtocols'] = ['root', 'srm']




  return S_OK( options )

def mock_StorageFactory_getConfigStorageProtocols( storageName, derivedStorageName = None ):
  """ Protocol specific information is present as sections in the Storage configuration
  """
  protocolDetails = { 'StorageA' : [{'PluginName': 'File',
                                     'Protocol': 'file',
                                     'Path' : '',
                                     },
                                   ],
                     'StorageB' : [{'PluginName': 'SRM2',
                                     'Protocol': 'srm',
                                     'Path' : '',
                                     },
                                   ],
                     'StorageC' : [{'PluginName': 'XROOT',
                                     'Protocol': 'root',
                                     'Path' : '',
                                     },
                                   ],
                     'StorageD' : [
                                   {'PluginName': 'SRM2',
                                     'Protocol': 'srm',
                                     'Path' : '',
                                   },
                                   {'PluginName': 'XROOT',
                                     'Protocol': 'root',
                                     'Path' : '',
                                   },
                                   ],
                     'StorageE' : [
                                   {'PluginName': 'SRM2',
                                     'Protocol': 'srm',
                                     'Path' : '',
                                   },
                                   {'PluginName': 'XROOT',
                                     'Protocol': 'root',
                                     'Path' : '',
                                   },
                                   ],
                    }

  return S_OK( protocolDetails[storageName] )


class fake_DMSHelpers( object ):
  """ Fake DMS helpers. Used to get the protocol lists
      inside the StorageElement
  """
  def __init__( self, vo = None ):
    pass

  def getAccessProtocols( self ):
    return ['fakeProto', 'root']

  def getWriteProtocols( self ):
    return ['srm']




class TestBase( unittest.TestCase ):
  """ Base test class. Defines all the method to test
  """


  @mock.patch( 'DIRAC.Resources.Storage.StorageFactory.StorageFactory._getConfigStorageName',
                side_effect = mock_StorageFactory_getConfigStorageName )
  @mock.patch( 'DIRAC.Resources.Storage.StorageFactory.StorageFactory._getConfigStorageOptions',
                side_effect = mock_StorageFactory_getConfigStorageOptions )
  @mock.patch( 'DIRAC.Resources.Storage.StorageFactory.StorageFactory._getConfigStorageProtocols',
                side_effect = mock_StorageFactory_getConfigStorageProtocols )
  @mock.patch( 'DIRAC.Resources.Storage.StorageFactory.StorageFactory._StorageFactory__generateStorageObject',
                side_effect = mock_StorageFactory_generateStorageObject )
  @mock.patch( 'DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE',
                return_value = S_OK( True ) )  # Pretend it's local
  @mock.patch( 'DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation',
                return_value = None )  # Don't send accounting
  @mock.patch( 'DIRAC.Resources.Storage.StorageElement.DMSHelpers', side_effect = fake_DMSHelpers )
  def setUp( self, _mk_getConfigStorageName, _mk_getConfigStorageOptions, _mk_getConfigStorageProtocols,
             _mk_generateStorage, _mk_isLocalSE, _mk_addAccountingOperation, _mk_dmsHelpers ):
    self.seA = StorageElementItem( 'StorageA' )
    self.seA.vo = 'lhcb'
    self.seB = StorageElementItem( 'StorageB' )
    self.seB.vo = 'lhcb'
    self.seC = StorageElementItem( 'StorageC' )
    self.seC.vo = 'lhcb'
    self.seD = StorageElementItem( 'StorageD' )
    self.seD.vo = 'lhcb'
    self.seE = StorageElementItem( 'StorageE' )
    self.seE.vo = 'lhcb'



  def tearDown( self ):
    pass



  @mock.patch( 'DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE',
                return_value = S_OK( True ) )  # Pretend it's local
  @mock.patch( 'DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation',
                return_value = None )  # Don't send accounting
  def test_01_negociateProtocolWithOtherSE( self, mk_isLocalSE, mk_addAccounting ):
    """Testing negotiation algorithm"""

    # Find common protocol between SRM2 and File
    res = self.seA.negociateProtocolWithOtherSE( self.seB )
    self.assertTrue( res['OK'], res )
    self.assertEqual( res['Value'], ['file'] )

    # Find common protocol between File and SRM@
    res = self.seB.negociateProtocolWithOtherSE( self.seA )
    self.assertTrue( res['OK'], res )
    self.assertEqual( res['Value'], ['file'] )


    # Find common protocol between XROOT and File
    # Nothing goes from xroot to file
    res = self.seA.negociateProtocolWithOtherSE( self.seC )
    self.assertTrue( res['OK'], res )
    self.assertEqual( res['Value'], [] )

    # Find common protocol between File and XROOT
    res = self.seC.negociateProtocolWithOtherSE( self.seA )
    self.assertTrue( res['OK'], res )
    self.assertEqual( res['Value'], ['file'] )

    # Find common protocol between File and File
    res = self.seA.negociateProtocolWithOtherSE( self.seA )
    self.assertTrue( res['OK'], res )
    self.assertEqual( res['Value'], ['file'] )

    # Find common protocol between SRM and SRM
    res = self.seB.negociateProtocolWithOtherSE( self.seB )
    self.assertTrue( res['OK'], res )
    self.assertEqual( sorted( res['Value'] ), sorted( ['file', 'root', 'srm'] ) )


    # Find common protocol between SRM and XROOT
    res = self.seC.negociateProtocolWithOtherSE( self.seB )
    self.assertTrue( res['OK'], res )
    self.assertEqual( sorted( res['Value'] ), sorted( ['root', 'file'] ) )

    # Find common protocol between XROOT and SRM
    res = self.seC.negociateProtocolWithOtherSE( self.seB )
    self.assertTrue( res['OK'], res )
    self.assertEqual( sorted( res['Value'] ), sorted( ['root', 'file'] ) )

    # Testing restrictions
    res = self.seC.negociateProtocolWithOtherSE( self.seB, protocols = ['file'] )
    self.assertTrue( res['OK'], res )
    self.assertEqual( sorted( res['Value'] ), ['file'] )

    res = self.seC.negociateProtocolWithOtherSE( self.seB, protocols = ['nonexisting'] )
    self.assertTrue( res['OK'], res )
    self.assertEqual( res['Value'], [] )


  @mock.patch( 'DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE',
                return_value = S_OK( True ) )  # Pretend it's local
  @mock.patch( 'DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation',
                return_value = None )  # Don't send accounting
  def test_02_followOrder( self, _mk_isLocalSE, _mk_addAccounting ):
    """Testing If the order of preferred protocols is respected"""

    for permutation in itertools.permutations( ['srm', 'file', 'root', 'nonexisting'] ):
      permuList = list( permutation )
      # Don't get tricked ! remove cannot be put
      # after the conversion, because it is inplace modification
      permuList.remove( 'nonexisting' )
      res = self.seD.negociateProtocolWithOtherSE( self.seD, protocols = permutation )
      self.assertTrue( res['OK'], res )
      self.assertEqual( res['Value'], permuList )



  @mock.patch( 'DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE',
                return_value = S_OK( True ) )  # Pretend it's local
  @mock.patch( 'DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation',
                return_value = None )  # Don't send accounting
  def test_03_multiProtocolThirdParty( self, _mk_isLocalSE, _mk_addAccounting ):
    """
      Test case for storages with several protocols

      Here comes the fun :-)
      Suppose we have endpoints that we can read in root, but cannot write
      If we have root in the accessProtocols and thirdPartyProtocols lists
      but not in the writeProtocols, we should get a root url to read,
      and write with SRM

      We reproduce here the behavior of DataManager.replicate

    """

    thirdPartyProtocols = ['root', 'srm']

    lfn = '/lhcb/fake/lfn'
    res = self.seD.negociateProtocolWithOtherSE( self.seD, protocols = thirdPartyProtocols )
    self.assertTrue( res['OK'], res )
    self.assertEqual( res['Value'] , thirdPartyProtocols )


    # Only the XROOT plugin here implements the geTransportURL
    # that returns what we want, so we know that
    # if the return is successful, it is because of the XROOT
    res = self.seD.getURL( lfn, protocol = res['Value'] )
    self.assertTrue( res['OK'], res )
    self.assertTrue( lfn in res['Value']['Successful'], res )

    srcUrl = res['Value']['Successful'][lfn]
    self.assertEqual( srcUrl, "root:getTransportURL" )

    # Only the SRM2 plugin here implements the putFile method
    # so if we get a success here, it means that we used the SRM plugin
    res = self.seD.replicateFile( {lfn:srcUrl},
                                   sourceSize = 123,
                                   inputProtocol = 'root' )

    self.assertTrue( res['OK'], res )
    self.assertTrue( lfn in res['Value']['Successful'], res )
    self.assertEqual( res['Value']['Successful'][lfn], "srm:putFile" )


  @mock.patch( 'DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE',
                return_value = S_OK( True ) )  # Pretend it's local
  @mock.patch( 'DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation',
                return_value = None )  # Don't send accounting
  def test_04_thirdPartyLocalWrite( self, _mk_isLocalSE, _mk_addAccounting ):
    """
      Test case for storages with several protocols

      Here, we locally define the write protocol to be root and srm
      So we should be able to do everything with XROOT plugin

    """

    thirdPartyProtocols = ['root', 'srm']

    lfn = '/lhcb/fake/lfn'
    res = self.seE.negociateProtocolWithOtherSE( self.seE, protocols = thirdPartyProtocols )
    self.assertTrue( res['OK'], res )
    self.assertEqual( res['Value'] , thirdPartyProtocols )

    res = self.seE.getURL( lfn, protocol = res['Value'] )
    self.assertTrue( res['OK'], res )
    self.assertTrue( lfn in res['Value']['Successful'], res )

    srcUrl = res['Value']['Successful'][lfn]
    self.assertEqual( srcUrl, "root:getTransportURL" )

    res = self.seE.replicateFile( {lfn:srcUrl},
                                   sourceSize = 123,
                                   inputProtocol = 'root' )

    self.assertTrue( res['OK'], res )
    self.assertTrue( lfn in res['Value']['Successful'], res )
    self.assertEqual( res['Value']['Successful'][lfn], "root:putFile" )


  @mock.patch( 'DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE',
                return_value = S_OK( True ) )  # Pretend it's local
  @mock.patch( 'DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation',
                return_value = None )  # Don't send accounting
  def test_05_thirdPartyMix( self, _mk_isLocalSE, _mk_addAccounting ):
    """
      Test case for storages with several protocols

      Here, we locally define the write protocol for the destination, so it should
      all go directly through the XROOT plugin

    """

    thirdPartyProtocols = ['root', 'srm']

    lfn = '/lhcb/fake/lfn'
    res = self.seE.negociateProtocolWithOtherSE( self.seD, protocols = thirdPartyProtocols )
    self.assertTrue( res['OK'], res )
    self.assertEqual( res['Value'] , thirdPartyProtocols )

    res = self.seD.getURL( lfn, protocol = res['Value'] )
    self.assertTrue( res['OK'], res )
    self.assertTrue( lfn in res['Value']['Successful'], res )

    srcUrl = res['Value']['Successful'][lfn]
    self.assertEqual( srcUrl, "root:getTransportURL" )

    res = self.seE.replicateFile( {lfn:srcUrl},
                                   sourceSize = 123,
                                   inputProtocol = 'root' )

    self.assertTrue( res['OK'], res )
    self.assertTrue( lfn in res['Value']['Successful'], res )
    self.assertEqual( res['Value']['Successful'][lfn], "root:putFile" )


  @mock.patch( 'DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE',
                return_value = S_OK( True ) )  # Pretend it's local
  @mock.patch( 'DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation',
                return_value = None )  # Don't send accounting
  def test_06_thirdPartyMixOpposite( self, _mk_isLocalSE, _mk_addAccounting ):
    """
      Test case for storages with several protocols

      Here, we locally define the write protocol for the source, so it should
      get the source directly using XROOT, and perform the put using SRM

    """

    thirdPartyProtocols = ['root', 'srm']

    lfn = '/lhcb/fake/lfn'
    res = self.seD.negociateProtocolWithOtherSE( self.seE, protocols = thirdPartyProtocols )
    self.assertTrue( res['OK'], res )
    self.assertEqual( res['Value'] , thirdPartyProtocols )

    res = self.seE.getURL( lfn, protocol = res['Value'] )
    self.assertTrue( res['OK'], res )
    self.assertTrue( lfn in res['Value']['Successful'], res )

    srcUrl = res['Value']['Successful'][lfn]
    self.assertEqual( srcUrl, "root:getTransportURL" )

    res = self.seD.replicateFile( {lfn:srcUrl},
                                   sourceSize = 123,
                                   inputProtocol = 'root' )

    self.assertTrue( res['OK'], res )
    self.assertTrue( lfn in res['Value']['Successful'], res )
    self.assertEqual( res['Value']['Successful'][lfn], "srm:putFile" )


  @mock.patch( 'DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__isLocalSE',
                return_value = S_OK( True ) )  # Pretend it's local
  @mock.patch( 'DIRAC.Resources.Storage.StorageElement.StorageElementItem.addAccountingOperation',
                return_value = None )  # Don't send accounting
  def test_07_multiProtocolSrmOnly( self, _mk_isLocalSE, _mk_addAccounting ):
    """
      Test case for storages with several protocols

      Here comes the fun :-)
      Suppose we have endpoints that we can read in root, but cannot write
      If we have root in the accessProtocols and thirdPartyProtocols lists
      but not in the writeProtocols, we should get a root url to read,
      and write with SRM

      We reproduce here the behavior of DataManager.replicate

    """

    thirdPartyProtocols = [ 'srm']

    print "negociate"
    lfn = '/lhcb/fake/lfn'
    res = self.seD.negociateProtocolWithOtherSE( self.seD, protocols = thirdPartyProtocols )
    self.assertTrue( res['OK'], res )
    self.assertEqual( res['Value'] , thirdPartyProtocols )

    print "get source url"

    res = self.seD.getURL( lfn, protocol = res['Value'] )
    self.assertTrue( res['OK'], res )
    self.assertTrue( lfn in res['Value']['Successful'], res )

    srcUrl = res['Value']['Successful'][lfn]
    self.assertEqual( srcUrl, "srm:getTransportURL" )

    print "replicate"
    res = self.seD.replicateFile( {lfn:srcUrl},
                                   sourceSize = 123,
                                   inputProtocol = 'srm' )

    self.assertTrue( res['OK'], res )
    self.assertTrue( lfn in res['Value']['Successful'], res )
    self.assertEqual( res['Value']['Successful'][lfn], "srm:putFile" )


if __name__ == '__main__':
  from DIRAC import gLogger
  gLogger.setLevel( 'DEBUG' )
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestBase )

  unittest.TextTestRunner( verbosity = 2 ).run( suite )
