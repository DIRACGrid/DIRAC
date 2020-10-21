''' Test_RSS_Command_CEAvailabilityCommand

'''
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import importlib
import mock

from DIRAC import gLogger

import DIRAC.ResourceStatusSystem.Command.CEAvailabilityCommand as moduleTested

__RCSID__ = '$Id$'

################################################################################


class CEAvailabilityCommand_TestCase(unittest.TestCase):

  def setUp(self):
    '''
    Setup
    '''

    gLogger.setLevel('DEBUG')

    # Mock external libraries / modules not interesting for the unit test
    getVOs = mock.MagicMock()
    getVOs.return_value = {"OK": True, "Value": ["lhcb", 'lhcb']}

    self.DAObjectMock = mock.MagicMock()
    self.DAMock = mock.MagicMock()
    self.DAMock.return_value = self.DAObjectMock
    self.CEAvailabilityCommand_m = importlib.import_module("DIRAC.ResourceStatusSystem.Command.CEAvailabilityCommand")
    self.CEAvailabilityCommand_m.DiracAdmin = self.DAMock
    self.CEAvailabilityCommand_m.getVOs = getVOs

    # Add mocks to moduleTested
    self.moduleTested = moduleTested
    self.testClass = self.moduleTested.CEAvailabilityCommand

  def tearDown(self):
    '''
    TearDown
    '''
    del self.testClass
    del self.moduleTested

################################################################################
# Tests


class CEAvailabilityCommand_Success(CEAvailabilityCommand_TestCase):

  def test_instantiate(self):
    ''' tests that we can instantiate one object of the tested class
    '''

    command = self.testClass()
    self.assertEqual('CEAvailabilityCommand', command.__class__.__name__)

  def test_init(self):
    ''' tests that the init method does what it should do
    '''

    command = self.testClass({"ce": "ce_foo"})
    self.assertEqual(command.args["ce"], "ce_foo")

  def test_doCommand(self):
    ''' tests the doCommand method
    '''

    # verify when it's "Production"
    self.DAObjectMock.getBDIICEState.return_value = {
	'OK': True,
	'Value': [
	    {
		'GlueCEInfoLRMSType': 'torque',
		'GlueCEInfoDataDir': 'unset',
		'GlueCEInfoTotalCPUs': '928',
		'GlueCEImplementationName': 'CREAM',
		'GlueCEInfoJobManager': 'pbs',
		'GlueCEUniqueID': 'cygnus.grid.rug.nl:8443/short',
		'GlueCEPolicyMaxCPUTime': '240',
		'GlueCEStateTotalJobs': '20',
		'GlueCEInfoDefaultSE': 'srm.target.rug.nl',
		'GlueCEInfoContactString': 'https://cygnus.grid.rug.nl:8443/ce-cream/services',
		'GlueCEStateStatus': 'Production',
		'GlueCEStateFreeCPUs': '784',
		'GlueCEPolicyMaxWallClockTime': '240',
		'GlueCEStateWaitingJobs': '0',
		'GlueCEStateRunningJobs': '20',
		'GlueCEPolicyMaxObtainableCPUTime': '240',
		'GlueForeignKey': 'GlueClusterUniqueID=cygnus.grid.rug.nl',
		'GlueCEStateWorstResponseTime': '14440',
		'GlueCEInfoApplicationDir': '/vo-software',
		'dn': 'GlueCEUniqueID=cygnus.grid.rug.nl:8443/short,Mds-Vo-name=RUG-CIT,Mds-Vo-name=local,o=grid',
		'GlueCEInfoLRMSVersion': '2.5.13',
		'GlueCEPolicyMaxWaitingJobs': '2147483647',
		'GlueCEStateEstimatedResponseTime': '9534',
		'GlueCEPolicyMaxObtainableWallClockTime': '240',
		'GlueCEStateFreeJobSlots': '784',
		'GlueCEInfoHostName': 'cygnus.grid.rug.nl',
		'GlueCEImplementationVersion': '1.16.4',
		'GlueCEPolicyMaxRunningJobs': '2147483647',
		'GlueSchemaVersionMinor': '3',
		'GlueCEInfoGatekeeperPort': '8443',
		'GlueInformationServiceURL': 'ldap://cygnus.grid.rug.nl:2170/mds-vo-name=resource,o=grid',
		'GlueCEName': 'short',
		'GlueCEPolicyPriority': '2147483647',
		'GlueCEPolicyMaxTotalJobs': '2147483647',
		'GlueCEAccessControlBaseRule': [
		    'VO:ops',
		    'VO:dteam',
		    'VO:pvier',
		    'VO:ops.biggrid.nl',
		    'VO:lhcb',
		    'VO:biomed',
		    'VO:astron',
		    'VO:bbmri.nl',
		    'VO:chem.biggrid.nl',
		    'VO:dans',
		    'VO:drihm.eu',
		    'VO:esr',
		    'VO:euclid-ec.org',
		    'VO:ildg',
		    'VO:lofar',
		    'VO:lsgrid',
		    'VO:omegac',
		    'VO:projects.nl',
		    'VO:tutor',
		    'VO:vlemed',
		    'VO:vo.panda.gsi.de'],
		'GlueCEPolicyMaxSlotsPerJob': '2147483647',
		'GlueCEPolicyPreemption': '0',
		'GlueCECapability': [
		    'CPUScalingReferenceSI00=2421',
		    'glexec'],
		'GlueSchemaVersionMajor': '1',
		'GlueCEPolicyAssignedJobSlots': '928',
		'GlueCEHostingCluster': 'cygnus.grid.rug.nl'},
	    {
		'GlueCEInfoLRMSType': 'torque',
		'GlueCEInfoDataDir': 'unset',
		'GlueCEInfoTotalCPUs': '928',
		'GlueCEImplementationName': 'CREAM',
		'GlueCEInfoJobManager': 'pbs',
		'GlueCEUniqueID': 'cygnus.grid.rug.nl:8443/pbs',
		'GlueCEPolicyMaxCPUTime': '2160',
		'GlueCEStateTotalJobs': '94',
		'GlueCEInfoDefaultSE': 'srm.target.rug.nl',
		'GlueCEInfoContactString': 'https://cygnus.grid.rug.nl:8443/ce-cream/services',
		'GlueCEStateStatus': 'Production',
		'GlueCEStateFreeCPUs': '784',
		'GlueCEPolicyMaxWallClockTime': '2160',
		'GlueCEStateWaitingJobs': '0',
		'GlueCEStateRunningJobs': '94',
		'GlueCEPolicyMaxObtainableCPUTime': '2160',
		'GlueForeignKey': 'GlueClusterUniqueID=cygnus.grid.rug.nl',
		'GlueCEStateWorstResponseTime': '26',
		'GlueCEInfoApplicationDir': '/vo-software',
		'dn': 'GlueCEUniqueID=cygnus.grid.rug.nl:8443/pbs,Mds-Vo-name=RUG-CIT,Mds-Vo-name=local,o=grid',
		'GlueCEInfoLRMSVersion': '2.5.13',
		'GlueCEPolicyMaxWaitingJobs': '2147483647',
		'GlueCEStateEstimatedResponseTime': '13',
		'GlueCEPolicyMaxObtainableWallClockTime': '2160',
		'GlueCEStateFreeJobSlots': '784',
		'GlueCEInfoHostName': 'cygnus.grid.rug.nl',
		'GlueCEImplementationVersion': '1.16.4',
		'GlueCEPolicyMaxRunningJobs': '2147483647',
		'GlueSchemaVersionMinor': '3',
		'GlueCEInfoGatekeeperPort': '8443',
		'GlueInformationServiceURL': 'ldap://cygnus.grid.rug.nl:2170/mds-vo-name=resource,o=grid',
		'GlueCEName': 'medium',
		'GlueCEPolicyPriority': '2147483647',
		'GlueCEPolicyMaxTotalJobs': '2147483647',
		'GlueCEAccessControlBaseRule': [
		    'VO:ops',
		    'VO:dteam',
		    'VO:pvier',
		    'VO:ops.biggrid.nl',
		    'VO:lhcb',
		    'VO:biomed',
		    'VO:astron',
		    'VO:bbmri.nl',
		    'VO:chem.biggrid.nl',
		    'VO:dans',
		    'VO:drihm.eu',
		    'VO:esr',
		    'VO:euclid-ec.org',
		    'VO:ildg',
		    'VO:lofar',
		    'VO:lsgrid',
		    'VO:omegac',
		    'VO:projects.nl',
		    'VO:tutor',
		    'VO:vlemed',
		    'VO:vo.panda.gsi.de'],
		'GlueCEPolicyMaxSlotsPerJob': '2147483647',
		'GlueCEPolicyPreemption': '0',
		'GlueCECapability': [
		    'CPUScalingReferenceSI00=2421',
		    'glexec'],
		'GlueSchemaVersionMajor': '1',
		'GlueCEPolicyAssignedJobSlots': '928',
		'GlueCEHostingCluster': 'cygnus.grid.rug.nl'}]}
    command = self.testClass({"ce": "cygnus.grid.rug.nl"})
    res = command.doCommand()
    self.assertEqual(res['Value']['Status'], 'Production')

    # verify when it's not "Production"
    self.DAObjectMock.getBDIICEState.return_value = {
	'OK': True,
	'Value': [
	    {
		'GlueCEInfoLRMSType': 'torque',
		'GlueCEInfoDataDir': 'unset',
		'GlueCEInfoTotalCPUs': '928',
		'GlueCEImplementationName': 'CREAM',
		'GlueCEInfoJobManager': 'pbs',
		'GlueCEUniqueID': 'cygnus.grid.rug.nl:8443/short',
		'GlueCEPolicyMaxCPUTime': '240',
		'GlueCEStateTotalJobs': '20',
		'GlueCEInfoDefaultSE': 'srm.target.rug.nl',
		'GlueCEInfoContactString': 'https://cygnus.grid.rug.nl:8443/ce-cream/services',
		'GlueCEStateStatus': 'Downtime',
		'GlueCEStateFreeCPUs': '784',
		'GlueCEPolicyMaxWallClockTime': '240',
		'GlueCEStateWaitingJobs': '0',
		'GlueCEStateRunningJobs': '20',
		'GlueCEPolicyMaxObtainableCPUTime': '240',
		'GlueForeignKey': 'GlueClusterUniqueID=cygnus.grid.rug.nl',
		'GlueCEStateWorstResponseTime': '14440',
		'GlueCEInfoApplicationDir': '/vo-software',
		'dn': 'GlueCEUniqueID=cygnus.grid.rug.nl:8443/short,Mds-Vo-name=RUG-CIT,Mds-Vo-name=local,o=grid',
		'GlueCEInfoLRMSVersion': '2.5.13',
		'GlueCEPolicyMaxWaitingJobs': '2147483647',
		'GlueCEStateEstimatedResponseTime': '9534',
		'GlueCEPolicyMaxObtainableWallClockTime': '240',
		'GlueCEStateFreeJobSlots': '784',
		'GlueCEInfoHostName': 'cygnus.grid.rug.nl',
		'GlueCEImplementationVersion': '1.16.4',
		'GlueCEPolicyMaxRunningJobs': '2147483647',
		'GlueSchemaVersionMinor': '3',
		'GlueCEInfoGatekeeperPort': '8443',
		'GlueInformationServiceURL': 'ldap://cygnus.grid.rug.nl:2170/mds-vo-name=resource,o=grid',
		'GlueCEName': 'short',
		'GlueCEPolicyPriority': '2147483647',
		'GlueCEPolicyMaxTotalJobs': '2147483647',
		'GlueCEAccessControlBaseRule': [
		    'VO:ops',
		    'VO:dteam',
		    'VO:pvier',
		    'VO:ops.biggrid.nl',
		    'VO:lhcb',
		    'VO:biomed',
		    'VO:astron',
		    'VO:bbmri.nl',
		    'VO:chem.biggrid.nl',
		    'VO:dans',
		    'VO:drihm.eu',
		    'VO:esr',
		    'VO:euclid-ec.org',
		    'VO:ildg',
		    'VO:lofar',
		    'VO:lsgrid',
		    'VO:omegac',
		    'VO:projects.nl',
		    'VO:tutor',
		    'VO:vlemed',
		    'VO:vo.panda.gsi.de'],
		'GlueCEPolicyMaxSlotsPerJob': '2147483647',
		'GlueCEPolicyPreemption': '0',
		'GlueCECapability': [
		    'CPUScalingReferenceSI00=2421',
		    'glexec'],
		'GlueSchemaVersionMajor': '1',
		'GlueCEPolicyAssignedJobSlots': '928',
		'GlueCEHostingCluster': 'cygnus.grid.rug.nl'},
	    {
		'GlueCEInfoLRMSType': 'torque',
		'GlueCEInfoDataDir': 'unset',
		'GlueCEInfoTotalCPUs': '928',
		'GlueCEImplementationName': 'CREAM',
		'GlueCEInfoJobManager': 'pbs',
		'GlueCEUniqueID': 'cygnus.grid.rug.nl:8443/pbs',
		'GlueCEPolicyMaxCPUTime': '2160',
		'GlueCEStateTotalJobs': '94',
		'GlueCEInfoDefaultSE': 'srm.target.rug.nl',
		'GlueCEInfoContactString': 'https://cygnus.grid.rug.nl:8443/ce-cream/services',
		'GlueCEStateStatus': 'Production',
		'GlueCEStateFreeCPUs': '784',
		'GlueCEPolicyMaxWallClockTime': '2160',
		'GlueCEStateWaitingJobs': '0',
		'GlueCEStateRunningJobs': '94',
		'GlueCEPolicyMaxObtainableCPUTime': '2160',
		'GlueForeignKey': 'GlueClusterUniqueID=cygnus.grid.rug.nl',
		'GlueCEStateWorstResponseTime': '26',
		'GlueCEInfoApplicationDir': '/vo-software',
		'dn': 'GlueCEUniqueID=cygnus.grid.rug.nl:8443/pbs,Mds-Vo-name=RUG-CIT,Mds-Vo-name=local,o=grid',
		'GlueCEInfoLRMSVersion': '2.5.13',
		'GlueCEPolicyMaxWaitingJobs': '2147483647',
		'GlueCEStateEstimatedResponseTime': '13',
		'GlueCEPolicyMaxObtainableWallClockTime': '2160',
		'GlueCEStateFreeJobSlots': '784',
		'GlueCEInfoHostName': 'cygnus.grid.rug.nl',
		'GlueCEImplementationVersion': '1.16.4',
		'GlueCEPolicyMaxRunningJobs': '2147483647',
		'GlueSchemaVersionMinor': '3',
		'GlueCEInfoGatekeeperPort': '8443',
		'GlueInformationServiceURL': 'ldap://cygnus.grid.rug.nl:2170/mds-vo-name=resource,o=grid',
		'GlueCEName': 'medium',
		'GlueCEPolicyPriority': '2147483647',
		'GlueCEPolicyMaxTotalJobs': '2147483647',
		'GlueCEAccessControlBaseRule': [
		    'VO:ops',
		    'VO:dteam',
		    'VO:pvier',
		    'VO:ops.biggrid.nl',
		    'VO:lhcb',
		    'VO:biomed',
		    'VO:astron',
		    'VO:bbmri.nl',
		    'VO:chem.biggrid.nl',
		    'VO:dans',
		    'VO:drihm.eu',
		    'VO:esr',
		    'VO:euclid-ec.org',
		    'VO:ildg',
		    'VO:lofar',
		    'VO:lsgrid',
		    'VO:omegac',
		    'VO:projects.nl',
		    'VO:tutor',
		    'VO:vlemed',
		    'VO:vo.panda.gsi.de'],
		'GlueCEPolicyMaxSlotsPerJob': '2147483647',
		'GlueCEPolicyPreemption': '0',
		'GlueCECapability': [
		    'CPUScalingReferenceSI00=2421',
		    'glexec'],
		'GlueSchemaVersionMajor': '1',
		'GlueCEPolicyAssignedJobSlots': '928',
		'GlueCEHostingCluster': 'cygnus.grid.rug.nl'}]}
    command = self.testClass({"ce": "cygnus.grid.rug.nl"})
    res = command.doCommand()
    self.assertNotEqual(res['Value']['Status'], 'Production', "Test: it's not Production ")

#############################################################################
# Test Suite run
#############################################################################


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(CEAvailabilityCommand_TestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(CEAvailabilityCommand_Success))

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
