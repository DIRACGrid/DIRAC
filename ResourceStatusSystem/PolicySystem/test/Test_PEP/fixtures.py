import unittest, sys

from DIRAC.ResourceStatusSystem.Utilities.mock                 import CS
from DIRAC.ResourceStatusSystem.mock                           import ValidRes, \
  ValidStatus,ValidStatusTypes, ValidSiteType, ValidServiceType, ValidResourceType
from DIRAC.ResourceStatusSystem.PolicySystem.mock.PDP          import PDP
from DIRAC.ResourceStatusSystem.API.mock.ResourceStatusAPI     import ResourceStatusAPI
from DIRAC.ResourceStatusSystem.API.mock.ResourceManagementAPI import ResourceManagementAPI
from DIRAC.ResourceStatusSystem.Client.mock.NotificationClient import NotificationClient
from DIRAC.ResourceStatusSystem.Client.mock.CSAPI              import CSAPI

class UnitFixture( unittest.TestCase ):

  def setUp( self ):   
    
    import DIRAC.ResourceStatusSystem.PolicySystem.PEP as mockedModule
    
    mockedModule.CS                     = CS
    mockedModule.ValidRes               = ValidRes
    mockedModule.ValidStatus            = ValidStatus
    mockedModule.ValidStatusTypes       = ValidStatusTypes
    mockedModule.ValidSiteType          = ValidSiteType
    mockedModule.ValidServiceType       = ValidServiceType
    mockedModule.ValidResourceType      = ValidResourceType
    mockedModule.PDP                    = PDP
    mockedModule.ResourceStausAPI       = ResourceStatusAPI
    mockedModule.ResourceManagementAPI  = ResourceManagementAPI
    mockedModule.NotificationClient     = NotificationClient
    mockedModule.CSAPI                  = CSAPI
    
    self.pep = mockedModule.PEP( "LHCb" )
    
  def tearDown( self ):  
    
    del sys.modules[ 'DIRAC.ResourceStatusSystem.PolicySystem.PEP' ]
#    del sys.modules[ 'DIRAC.ResourceStatusSystem.Utilities.mock' ]
#    del sys.modules[ 'DIRAC.ResourceStatusSystem.mock' ]
#    del sys.modules[ 'DIRAC.ResourceStatusSystem.PolicySystem.PDP' ]
#    del sys.modules[ 'DIRAC.ResourceStatusSystem.API.mock.ResourceStatusAPI' ]
#    del sys.modules[ 'DIRAC.ResourceStatusSystem.API.mock.ResourceManagementAPI' ]
#    del sys.modules[ 'DIRAC.ResourceStatusSystem.Client.mock.NotificationClient' ]
#    del sys.modules[ 'DIRAC.ResourceStatusSystem.Client.mock.CSAPI' ]
    
    