import unittest

from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Core.DISET.RequestHandler import RequestHandler

class ObjectLoaderMainSuccessScenario( unittest.TestCase ):

  def setUp( self ):
    self.ol = ObjectLoader()

  def __check( self, result ):
    if not result[ 'OK' ]:
      self.fail( result[ 'Message' ] )
    return result[ 'Value' ]
    

  def test_load( self ):
    self.__check( self.ol.loadObject( "Core.Utilities.List", 'fromChar' ) )
    self.__check( self.ol.loadObject( "Core.Utilities.ObjectLoader", "ObjectLoader" ) )
    dataFilter = self.__check( self.ol.getObjects( "WorkloadManagementSystem.Service", ".*Handler" ) )
    dataClass = self.__check( self.ol.getObjects( "WorkloadManagementSystem.Service", parentClass = RequestHandler )  )
    self.assertEqual( sorted( dataFilter.keys() ), sorted( dataClass.keys() ) )

if __name__ == "__main__":
  unittest.main()