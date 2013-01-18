#import unittest
#
#from DIRAC.Core.Base import Script
#Script.parseCommandLine()
#
#from DIRAC.ResourceStatusSystem.Utilities.mock                  import Mock
#from DIRAC.ResourceStatusSystem.Client.JobsClient               import JobsClient
#from DIRAC.ResourceStatusSystem.Client.PilotsClient             import PilotsClient
#from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
#from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
#
#from DIRAC.ResourceStatusSystem.Utilities                       import CS
#
#ValidRes = CS.getTypedDictRootedAt("GeneralConfig")['Resource']
#ValidStatus = CS.getTypedDictRootedAt("GeneralConfig")['Status']
#
##############################################################################
#
#class ClientsTestCase( unittest.TestCase ):
#  """ Base class for the clients test cases
#  """
#  def setUp( self ):
#
#    self.mockRSS = Mock()
#
#    self.RSCli = ResourceStatusClient( serviceIn = self.mockRSS )
#    self.RMCli = ResourceManagementClient( serviceIn = self.mockRSS )
#    self.PilotsCli = PilotsClient()
#    self.JobsCli = JobsClient()
#
##############################################################################
#
#class ResourceStatusClientSuccess( ClientsTestCase ):
#
#  def test_getPeriods( self ):
#    self.mockRSS.getPeriods.return_value = {'OK':True, 'Value':[]}
#    for granularity in ValidRes:
#      for status in ValidStatus:
#        res = self.RSCli.getPeriods( granularity, 'XX', status, 20 )
#        self.assertEqual(res['OK'], True)
#        self.assertEqual( res['Value'], [] )
#
#  def test_getServiceStats( self ):
#    self.mockRSS.getServiceStats.return_value = {'OK':True, 'Value':[]}
#    res = self.RSCli.getServiceStats( 'Site', '' )
#    self.assertEqual( res['Value'], [] )
#
#  def test_getResourceStats( self ):
#    self.mockRSS.getResourceStats.return_value = {'OK':True, 'Value':[]}
#    res = self.RSCli.getResourceStats( 'Site', '' )
#    self.assertEqual( res['Value'], [] )
#    res = self.RSCli.getResourceStats( 'Service', '' )
#    self.assertEqual( res['Value'], [] )
#
#  def test_getStorageElementsStats( self ):
#    self.mockRSS.getStorageElementsStats.return_value = {'OK':True, 'Value':[]}
#    res = self.RSCli.getStorageElementsStats( 'Site', '', "Read" )
#    self.assertEqual( res['Value'], [] )
#    res = self.RSCli.getStorageElementsStats( 'Resource', '', "Read")
#    self.assertEqual( res['Value'], [] )
#
#  def test_getMonitoredStatus( self ):
#    self.mockRSS.getSitesStatusWeb.return_value = {'OK':True, 'Value': {'Records': [['', '', '', '', 'Active', '']]}}
#    self.mockRSS.getServicesStatusWeb.return_value = {'OK':True, 'Value':{'Records': [['', '', '', '', 'Active', '']]}}
#    self.mockRSS.getResourcesStatusWeb.return_value = {'OK':True, 'Value':{'Records': [['', '', '', '', '', 'Active', '']]}}
#    self.mockRSS.getStorageElementsStatusWeb.return_value = {'OK':True, 'Value':{'Records': [['', '', '', '', 'Active', '']]}}
#    for g in ValidRes:
#      res = self.RSCli.getMonitoredStatus( g, 'a' )
#      self.assertEqual( res['Value'], ['Active'] )
#      res = self.RSCli.getMonitoredStatus( g, ['a'] )
#      self.assertEqual( res['Value'], ['Active'] )
#      res = self.RSCli.getMonitoredStatus( g, ['a', 'b'] )
#      self.assertEqual( res['Value'], ['Active', 'Active'] )
#
#  def test_getCachedAccountingResult( self ):
#    self.mockRSS.getCachedAccountingResult.return_value = {'OK':True, 'Value':[]}
#    res = self.RMCli.getCachedAccountingResult( 'XX', 'pippo', 'ZZ' )
#    self.assertEqual( res['Value'], [] )
#
#  def test_getCachedResult( self ):
#    self.mockRSS.getCachedResult.return_value = {'OK':True, 'Value':[]}
#    res = self.RMCli.getCachedResult( 'XX', 'pippo', 'ZZ', 1 )
#    self.assertEqual( res['Value'], [] )
#
#  def test_getCachedIDs( self ):
#    self.mockRSS.getCachedIDs.return_value = {'OK':True,
#                                              'Value':[78805473L, 78805473L, 78805473L, 78805473L]}
#    res = self.RMCli.getCachedIDs( 'XX', 'pippo' )
#    self.assertEqual( res['Value'], [78805473L, 78805473L, 78805473L, 78805473L] )
#
#
#
##############################################################################
#
#class JobsClientSuccess( ClientsTestCase ):
#
#  def test_getJobsSimpleEff( self ):
#    WMS_Mock = Mock()
#    WMS_Mock.getSiteSummaryWeb.return_value = {'OK': True,
#                                               'rpcStub': ( ( 'WorkloadManagement/WMSAdministrator',
#                                                            {'skipCACheck': True,
#                                                             'delegatedGroup': 'diracAdmin',
#                                                             'delegatedDN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=fstagni/CN=693025/CN=Federico Stagni', 'timeout': 600} ),
#                                                             'getSiteSummaryWeb', ( {'Site': 'LCG.CERN.ch'}, [], 0, 500 ) ),
#                                                             'Value': {'TotalRecords': 1,
#                                                                       'ParameterNames': ['Site', 'GridType', 'Country', 'Tier', 'MaskStatus', 'Received', 'Checking', 'Staging', 'Waiting', 'Matched', 'Running', 'Stalled', 'Done', 'Completed', 'Failed', 'Efficiency', 'Status'],
#                                                                       'Extras': {'ru': {'Received': 0, 'Staging': 0, 'Checking': 0, 'Completed': 0, 'Waiting': 0, 'Failed': 0, 'Running': 0, 'Done': 0, 'Stalled': 0, 'Matched': 0}, 'fr': {'Received': 0, 'Staging': 0, 'Checking': 0, 'Completed': 0, 'Waiting': 12L, 'Failed': 0, 'Running': 0, 'Done': 0, 'Stalled': 0, 'Matched': 0}, 'ch': {'Received': 0, 'Staging': 0, 'Checking': 0, 'Completed': 0, 'Waiting': 4L, 'Failed': 0, 'Running': 0, 'Done': 0, 'Stalled': 0, 'Matched': 1L}, 'nl': {'Received': 0, 'Staging': 0, 'Checking': 0, 'Completed': 0, 'Waiting': 0, 'Failed': 0, 'Running': 0, 'Done': 0, 'Stalled': 0, 'Matched': 0}, 'uk': {'Received': 0, 'Staging': 0, 'Checking': 0, 'Completed': 0, 'Waiting': 0, 'Failed': 0, 'Running': 0, 'Done': 0, 'Stalled': 0, 'Matched': 0}, 'Unknown': {'Received': 0, 'Staging': 0, 'Checking': 0, 'Completed': 0, 'Waiting': 0, 'Failed': 0, 'Running': 0, 'Done': 0, 'Stalled': 0, 'Matched': 0}, 'de': {'Received': 0, 'Staging': 0, 'Checking': 0, 'Completed': 0, 'Waiting': 1L, 'Failed': 0, 'Running': 0, 'Done': 0, 'Stalled': 0, 'Matched': 0}, 'it': {'Received': 0, 'Staging': 0, 'Checking': 1L, 'Completed': 0, 'Waiting': 2L, 'Failed': 0, 'Running': 0, 'Done': 0, 'Stalled': 0, 'Matched': 0}, 'hu': {'Received': 0, 'Staging': 0, 'Checking': 0, 'Completed': 0, 'Waiting': 0, 'Failed': 0, 'Running': 0, 'Done': 0, 'Stalled': 0, 'Matched': 0}, 'cy': {'Received': 0, 'Staging': 0, 'Checking': 0, 'Completed': 0, 'Waiting': 0, 'Failed': 0, 'Running': 0, 'Done': 0, 'Stalled': 0, 'Matched': 0}, 'bg': {'Received': 0, 'Staging': 0, 'Checking': 0, 'Completed': 0, 'Waiting': 0, 'Failed': 0, 'Running': 0, 'Done': 0, 'Stalled': 0, 'Matched': 0}, 'au': {'Received': 0, 'Staging': 0, 'Checking': 0, 'Completed': 0, 'Waiting': 10L, 'Failed': 0, 'Running': 0, 'Done': 0, 'Stalled': 0, 'Matched': 0}, 'il': {'Received': 0, 'Staging': 0, 'Checking': 0, 'Completed': 0, 'Waiting': 0, 'Failed': 0, 'Running': 0, 'Done': 0, 'Stalled': 0, 'Matched': 0}, 'br': {'Received': 0, 'Staging': 0, 'Checking': 0, 'Completed': 0, 'Waiting': 0, 'Failed': 0, 'Running': 0, 'Done': 0, 'Stalled': 0, 'Matched': 0}, 'ie': {'Received': 0, 'Staging': 0, 'Checking': 0, 'Completed': 0, 'Waiting': 0, 'Failed': 0, 'Running': 0, 'Done': 0, 'Stalled': 0, 'Matched': 0}, 'pl': {'Received': 0, 'Staging': 0, 'Checking': 0, 'Completed': 0, 'Waiting': 0, 'Failed': 0, 'Running': 0, 'Done': 0, 'Stalled': 0, 'Matched': 0}, 'es': {'Received': 0, 'Staging': 0, 'Checking': 0, 'Completed': 0, 'Waiting': 0, 'Failed': 0, 'Running': 0, 'Done': 2L, 'Stalled': 0, 'Matched': 0}},
#                                                                       'Records': [['LCG.CERN.ch', 'LCG', 'ch', 'Tier-1', 'Active', 0, 0, 0, 4L, 1L, 0, 0, 0, 0, 0, '0.0', 'Idle']]}}
#    res = self.JobsCli.getJobsSimpleEff( 'XX', RPCWMSAdmin = WMS_Mock )
#    self.assertEqual( res, {'LCG.CERN.ch': 'Idle'} )
#
##############################################################################
#
#class PilotsClientSuccess( ClientsTestCase ):
#
##  def test_getPilotsStats(self):
##    self.mockRSS.getPeriods.return_value = {'OK':True, 'Value':[]}
##    for granularity in ValidRes:
##      for status in ValidStatus:
##        res = self.RSCli.getPeriods(granularity, 'XX', status, 20)
##        self.assertEqual(res['Periods'], [])
#
#  def test_getPilotsSimpleEff( self ):
#    #self.mockRSS.getPilotsSimpleEff.return_value = {'OK':True, 'Value':{'Records': [['', '', 0, 3L, 0, 0, 0, 283L, 66L, 0, 0, 352L, '1.00', '81.25', 'Fair', 'Yes']]}}
#
#    WMS_Mock = Mock()
#    WMS_Mock.getPilotSummaryWeb.return_value = {'OK': True,
#                                                'rpcStub': ( ( 'WorkloadManagement/WMSAdministrator',
#                                                             {'skipCACheck': True,
#                                                              'delegatedGroup': 'diracAdmin',
#                                                              'delegatedDN': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=fstagni/CN=693025/CN=Federico Stagni', 'timeout': 600} ),
#                                                              'getPilotSummaryWeb', ( {'GridSite': 'LCG.Ferrara.it'}, [], 0, 500 ) ),
#                                                              'Value': {
#                                                                        'TotalRecords': 0,
#                                                                        'ParameterNames': ['Site', 'CE', 'Submitted', 'Ready', 'Scheduled', 'Waiting', 'Running', 'Done', 'Aborted', 'Done_Empty', 'Aborted_Hour', 'Total', 'PilotsPerJob', 'PilotJobEff', 'Status', 'InMask'],
#                                                                        'Extras': {'Scheduled': 0, 'Status': 'Poor', 'Aborted_Hour': 20L, 'Waiting': 59L, 'Submitted': 6L, 'PilotsPerJob': '1.03', 'Ready': 0, 'Running': 0, 'PilotJobEff': '39.34', 'Done': 328L, 'Aborted': 606L, 'Done_Empty': 9L, 'Total': 999L},
#                                                                        'Records': []}}
#
#    res = self.PilotsCli.getPilotsSimpleEff( 'Site', 'LCG.Ferrara.it', RPCWMSAdmin = WMS_Mock )
#    self.assertEqual( res, None )
#    res = self.PilotsCli.getPilotsSimpleEff( 'Resource', 'grid0.fe.infn.it', 'LCG.Ferrara.it', RPCWMSAdmin = WMS_Mock )
#    self.assertEqual( res, None )
#
##############################################################################
#
#if __name__ == '__main__':
#  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientsTestCase )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ResourceStatusClientSuccess ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( JobsClientSuccess ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( PilotsClientSuccess ) )
#  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
