""" VirtualMachineDB Integration Tests
"""

import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.WorkloadManagementSystem.DB.VirtualMachineDB import VirtualMachineDB


class VirtualMachineDBInstanceTests(unittest.TestCase):
  """ This tests the instance related database functions.
      It creates two entries, one is used for the tests and the second
      is checked after each test to check it hasn't changed.
  """

  INST_UUID = "1111-2222-3333"
  INST_NAME = "TestInst"
  INST_IMAGE = "TestImage"
  INST_EP = "MyCloud::mycloud.domain"
  INST_POD = "testvo"  # pod is generally just set to VO name and not really used

  def setUp(self):
    """ Adds an instance which we can then check the properties of """
    self.__db = VirtualMachineDB()
    # Start by clearing the database so we don't get any surprises
    for vmTable in VirtualMachineDB.tablesDesc:
      res = self.__db._query("DELETE FROM `%s`" % vmTable)
      self.assertTrue(res['OK'])
    # Now create our test instances
    res = self.__db.insertInstance(self.INST_UUID, self.INST_IMAGE, self.INST_NAME, self.INST_EP, self.INST_POD)
    self.assertTrue(res['OK'])
    # Most functions will need the internal DB instance ID
    res = self.__db.getInstanceID(self.INST_UUID)
    self.assertTrue(res['OK'])
    self.__id = res['Value']
    # Create the second "canary" instance entry that should always be unchanged
    res = self.__db.insertInstance(self.INST_UUID + '2', self.INST_IMAGE + '2', self.INST_NAME + '2',
                                   self.INST_EP + '2', self.INST_POD + '2')
    self.assertTrue(res['OK'])

  def tearDown(self):
    """ Checks that the second instance is unchanged and then clears the DB tables
        so that any changes the test made aren't retained for future tests """
    test_id = self.__db.getInstanceID(self.INST_UUID + '2')['Value']
    for paramName, paramValue in [('UniqueID', self.INST_UUID + '2'),
                                  ('InstanceID', test_id),
                                  ('Name', self.INST_NAME + '2'),
                                  ('Endpoint', self.INST_EP + '2'),
                                  ('RunningPod', self.INST_POD + '2'),
                                  ('Status', 'Submitted')
                                  ]:
      res = self.__db.getInstanceParameter(paramName, test_id)
      self.assertTrue(res['OK'])
      self.assertEqual(res['Value'], paramValue)
    res = self.__db.checkImageStatus(self.INST_IMAGE + '2')
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], 'New')

  def test_imageStatus(self):
    """ Adding an instance should automatically add its image in the "New" state """
    res = self.__db.checkImageStatus(self.INST_IMAGE)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], 'New')
    # An image is considered validated when an instance using it starts running
    # returning at least one heartbeat
    self.assertTrue(self.__db.declareInstanceRunning(self.INST_UUID, "127.0.0.1")['OK'])
    self.assertTrue(self.__db.instanceIDHeartBeat(self.INST_UUID, 0.0, 0, 0, 0, 60)['OK'])
    res = self.__db.checkImageStatus(self.INST_IMAGE)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], 'Validated')

  def test_instanceParam(self):
    """ Check we can get all of the instance parameters and that unknown properties fail  """
    for paramName, paramValue in [('UniqueID', self.INST_UUID),
                                  ('InstanceID', self.__id),
                                  ('Name', self.INST_NAME),
                                  ('Endpoint', self.INST_EP),
                                  ('RunningPod', self.INST_POD),
                                  ('Status', 'Submitted')
                                  ]:
      res = self.__db.getInstanceParameter(paramName, self.__id)
      self.assertTrue(res['OK'])
      self.assertEqual(res['Value'], paramValue)
    res = self.__db.getInstanceParameter("BadParam", self.__id)
    self.assertFalse(res['OK'])
    # Some parameters have dedicated access functions too
    res = self.__db.getEndpointFromInstance(self.INST_UUID)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], self.INST_EP)
    res = self.__db.getInstanceStatus(self.__id)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], "Submitted")
    # A running instance with a heartbeat has a few of extra parameters
    # Bytes and files are stored in the history table and not tested here
    INST_LOAD = 4.0
    INST_JOBS = 5
    INST_FILES = 6
    INST_BYTES = 7
    INST_UPTIME = 8
    self.assertTrue(self.__db.declareInstanceRunning(self.INST_UUID, "127.0.0.1")['OK'])
    res = self.__db.instanceIDHeartBeat(self.INST_UUID, INST_LOAD, INST_JOBS,
                                        INST_FILES, INST_BYTES, INST_UPTIME)
    self.assertTrue(res['OK'])
    for paramName, paramValue in [('Load', INST_LOAD),
                                  ('Jobs', INST_JOBS),
                                  ('Uptime', INST_UPTIME)
                                  ]:
      res = self.__db.getInstanceParameter(paramName, self.__id)
      self.assertTrue(res['OK'])
      self.assertEqual(res['Value'], paramValue)
    # There is also a function to get all fields in one go
    # Check a small selection of the fields in that
    res = self.__db.getAllInfoForUniqueID(self.INST_UUID)
    self.assertTrue(res['OK'])
    inst = res['Value']['Instance']
    self.assertEqual(inst['Name'], self.INST_NAME)
    self.assertEqual(inst['Endpoint'], self.INST_EP)
    self.assertEqual(inst['RunningPod'], self.INST_POD)
    self.assertEqual(inst['Status'], "Running")

  def test_changeInstanceID(self):
    """ Check we can update an instance's ID. """
    NEW_ID = "4444-5555-6666"
    res = self.__db.setInstanceUniqueID(self.__id, NEW_ID)
    self.assertTrue(res['OK'])
    # Check it changed and that we can get it by the new ID
    res = self.__db.getUniqueID(self.__id)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], NEW_ID)
    res = self.__db.getInstanceID(NEW_ID)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], self.__id)

  def test_getByName(self):
    """ Check we can get an instance by name. """
    res = self.__db.getUniqueIDByName(self.INST_NAME)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], self.INST_UUID)

  def test_instanceStates(self):
    """ Swap the instance through all of the states and check it updates. """
    self.assertTrue(self.__db.declareInstanceRunning(self.INST_UUID, "127.0.0.1")['OK'])
    self.assertEqual(self.__db.getInstanceStatus(self.__id)['Value'], "Running")
    self.assertTrue(self.__db.declareInstanceStopping(self.__id)['OK'])
    self.assertEqual(self.__db.getInstanceStatus(self.__id)['Value'], "Stopping")
    self.assertTrue(self.__db.declareInstanceHalting(self.INST_UUID, 0.0)['OK'])
    self.assertEqual(self.__db.getInstanceStatus(self.__id)['Value'], "Halted")
    # We don't test all invalid transitions, but an instance cannot go from halted
    # back to running, so this should not update the state
    self.__db.declareInstanceRunning(self.INST_UUID, "127.0.0.1")
    self.assertEqual(self.__db.getInstanceStatus(self.__id)['Value'], "Halted")

  def test_recordDBHalt(self):
    """ This function lets an admin declare an instance in the halting state,
        test that it does update the DB. This only works on a running instance.
    """
    # We should get an error as we can't halt a submitted instnace
    self.assertFalse(self.__db.recordDBHalt(self.__id, 0.0)['OK'])
    # Put it into the running state and try again
    self.assertTrue(self.__db.declareInstanceRunning(self.INST_UUID, "127.0.0.1")['OK'])
    self.assertTrue(self.__db.recordDBHalt(self.__id, 0.0)['OK'])
    self.assertEqual(self.__db.getInstanceStatus(self.__id)['Value'], "Halted")

  def test_instanceIPs(self):
    """ Check instance IP assignment functions. """
    PUBLIC_IP = "123.123.123.123"
    PRIVATE_IP = "127.123.123.123"
    # This function should strip an IPv6 mapping from the public IP
    # Test this at the same time
    res = self.__db.declareInstanceRunning(self.INST_UUID, "::ffff:" + PUBLIC_IP, PRIVATE_IP)
    self.assertTrue(res['OK'])
    res = self.__db.getInstanceParameter('PrivateIP', self.__id)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], PRIVATE_IP)
    res = self.__db.getInstanceParameter('PublicIP', self.__id)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], PUBLIC_IP)

  def test_heartbeats(self):
    """ Test that the heartbeat mechanism works as expected. """
    # Put the instance into the running state and check that it doesn't get pruned
    self.assertTrue(self.__db.declareInstanceRunning(self.INST_UUID, "127.0.0.1")['OK'])
    self.assertTrue(self.__db.instanceIDHeartBeat(self.INST_UUID, 0.0, 0, 0, 0, 60)['OK'])
    self.assertTrue(self.__db.declareStalledInstances()['OK'])
    self.assertEqual(self.__db.getInstanceStatus(self.__id)['Value'], "Running")
    # We now manually patch the heartbeat we added so that it's further in the past
    # Far enough back that the instance appears stuck to check it gets marked as stalled
    exp_time = self.__db.stallingInterval * 2  # Use twice the interval for safety
    sql = "UPDATE vm_Instances SET LastUpdate = LastUpdate - INTERVAL %u SECOND WHERE InstanceID = %s" \
          % (exp_time, self.__id)
    self.assertTrue(self.__db._query(sql)['OK'])
    res = self.__db.getAllInfoForUniqueID(self.INST_UUID)
    self.assertTrue(self.__db.declareStalledInstances()['OK'])
    self.assertEqual(self.__db.getInstanceStatus(self.__id)['Value'], "Stalled")

  def test_getByState(self):
    """ Tests the getInstancesByStatus function. """
    # First check that a bad status isn't accepted by the function
    self.assertFalse(self.__db.getInstancesByStatus("BadState")['OK'])
    # Now do the tests with valid inputs
    res = self.__db.getInstancesByStatus("Submitted")
    self.assertTrue(res['OK'])
    images = res['Value']
    # We should have two instances
    # Check that the structure looks correct {image: [instnaces]}
    self.assertEqual(len(images), 2)
    self.assertIn(self.INST_IMAGE, images)
    self.assertIn(self.INST_UUID, images[self.INST_IMAGE])
    # Mark the test instance as running and check it doesn't appear in the output
    self.assertTrue(self.__db.declareInstanceRunning(self.INST_UUID, "127.0.0.1")['OK'])
    res = self.__db.getInstancesByStatus("Submitted")
    self.assertTrue(res['OK'])
    images = res['Value']
    self.assertEqual(len(images), 1)
    self.assertNotIn(self.INST_IMAGE, images)
