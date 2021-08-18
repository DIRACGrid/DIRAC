""" VirtualMachineManager Service Integration Tests
"""

import uuid
import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.Core.DISET.RPCClient import RPCClient


class VirtualMachineManagerTests(unittest.TestCase):
  """ These tests check the VirtualMachineManager functions.
   """

  def _assertState(self, uniqueID, state):
    """ Asserts the given instance (UUID) is in the the given state.
        Returns the full instance info dict for further inspection if needed.
    """
    res = self.__client.getAllInfoForUniqueID(uniqueID)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['Instance']['Status'], state)
    return res['Value']

  def setUp(self):
    self.__client = RPCClient("WorkloadManagement/VirtualMachineManager")
    res = self.__client.checkVmWebOperation('Any')
    self.assertTrue(res['OK'])
    if res['Value'] != 'Auth':
      self.fail("Client has insufficient privs to test VM calls")
    # Create a test instance
    self.__inst_uuid = str(uuid.uuid4())
    self.__inst_image = 'ClientImage'
    self.__inst_name = 'ClientInst-%s' % self.__inst_uuid
    self.__inst_ep = 'UKI-CLOUD::testcloud.cloud'
    self.__inst_pod = 'clientvo'
    res = self.__client.insertInstance(self.__inst_uuid,
                                       self.__inst_image,
                                       self.__inst_name,
                                       self.__inst_ep,
                                       self.__inst_pod)
    self.assertTrue(res['OK'])
    self.__id = res['Value']

  def test_instanceBasics(self):
    """ Check that we can add an instance and then get the details back.
    """
    # Create instance
    # Check get functions
    res = self.__client.getUniqueID(str(self.__id))
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], self.__inst_uuid)
    res = self.__client.getUniqueIDByName(self.__inst_name)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], self.__inst_uuid)
    # Check set functions
    INST_ALT_UUID = str(uuid.uuid4())
    self.assertTrue(self.__client.setInstanceUniqueID(self.__id, INST_ALT_UUID)['OK'])
    res = self.__client.getAllInfoForUniqueID(INST_ALT_UUID)  # Use new UUID to fetch VM
    self.assertTrue(res['OK'])
    # Check get by state
    res = self.__client.getInstancesByStatus('Submitted')
    self.assertTrue(res['OK'])
    self.assertIn(INST_ALT_UUID, res['Value'][self.__inst_image])

  def test_instanceStates(self):
    """ Create an instance entry and check we can set its state. """
    self._assertState(self.__inst_uuid, 'Submitted')
    self.assertTrue(self.__client.declareInstanceRunning(self.__inst_uuid, "127.127.127.1")['OK'])
    self._assertState(self.__inst_uuid, 'Running')
    self.assertTrue(self.__client.declareInstancesStopping([self.__id])['OK'])
    self._assertState(self.__inst_uuid, 'Stopping')
    self.assertTrue(self.__client.declareInstanceHalting(self.__inst_uuid, 0.0)['OK'])
    self._assertState(self.__inst_uuid, 'Halted')

  def test_instanceProps(self):
    """ Check that instance properties get set correctly.
    """
    INST_LOAD = 1.2
    INST_JOBS = 3
    INST_FILES = 4
    INST_BYTES = 5
    INST_UPTIME = 67
    INST_IP = "127.127.127.1"
    self.assertTrue(self.__client.declareInstanceRunning(self.__inst_uuid, INST_IP)['OK'])
    res = self.__client.instanceIDHeartBeat(self.__inst_uuid, INST_LOAD, INST_JOBS,
                                            INST_FILES, INST_BYTES, INST_UPTIME)
    self.assertTrue(res['OK'])
    res = self._assertState(self.__inst_uuid, 'Running')
    inst = res['Instance']
    self.assertEqual(inst['Name'], self.__inst_name)
    self.assertEqual(inst['Endpoint'], self.__inst_ep)
    self.assertEqual(inst['RunningPod'], self.__inst_pod)
    self.assertEqual(inst['PrivateIP'], INST_IP)
    self.assertEqual(inst['Load'], INST_LOAD)
    self.assertEqual(inst['Jobs'], INST_JOBS)
    self.assertEqual(inst['Uptime'], INST_UPTIME)
    img = res['Image']
    self.assertEqual(img['Name'], self.__inst_image)
    self.assertEqual(img['Status'], 'Validated')
