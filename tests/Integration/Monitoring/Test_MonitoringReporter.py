"""
It is used to test the MonitoringReporter.
It requires MonitoringDB which is based on elasticsearch and MQ which is optional...

CS (not strictly necessary):

Systems
{
  Monitoring
  {
     Certification
     {
      Databases
      {
        MonitoringDB
        {
          Host = localhost
          Port = 9200
        }
      }
    }
  }
}

If you want to test with MQ:

Resources
{
  MQServices
  {
    Monitoring
    {
      MQType = Stomp
      VHost = /
      Host = xxxx.cern.ch
      Port = 61613
      User = username
      Password = xxxx
      Queues
      {
        QueueName
        {
          Acknowledgement = True
        }
      }
    }
  }
}

"""

# pylint: disable=invalid-name,wrong-import-position

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import unittest
import sys
from datetime import datetime

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC import gLogger

from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter
from DIRAC.MonitoringSystem.DB.MonitoringDB import MonitoringDB


class MonitoringTestCase(unittest.TestCase):

  def setUp(self):
    gLogger.setLevel('INFO')

    self.monitoringDB = MonitoringDB()
    self.wmsMonitoringReporter = MonitoringReporter(monitoringType="WMSHistory")
    self.componentMonitoringReporter = MonitoringReporter(monitoringType="ComponentMonitoring")

    self.data = [
	{"Status": "Waiting", "Jobs": 2, "timestamp": 1458130176,
	 "JobSplitType": "MCStripping", "MinorStatus": "unset",
	 "Site": "LCG.GRIDKA.de", "Reschedules": 0, "ApplicationStatus": "unset",
	 "User": "phicharp", "JobGroup": "00049848", "UserGroup": "lhcb_mc", "metric": "WMSHistory"},
	{u'Status': u'Waiting', 'Jobs': 1, u'timestamp': 1458130176,
	 u'JobSplitType': u'User', u'MinorStatus': u'unset', u'Site': u'LCG.PIC.es',
	 u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'olupton',
	 u'JobGroup': u'lhcb', u'UserGroup': u'lhcb_user', u'metric': u'WMSHistory'},
	{u'Status': u'Waiting', 'Jobs': 1, u'timestamp': 1458130176,
	 u'JobSplitType': u'User', u'MinorStatus': u'unset', u'Site': u'LCG.RAL.uk',
	 u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'olupton',
	 u'JobGroup': u'lhcb', u'UserGroup': u'lhcb_user', u'metric': u'WMSHistory'},
	{u'Status': u'Waiting', 'Jobs': 1, u'timestamp': 1458130176,
	 u'JobSplitType': u'MCStripping', u'MinorStatus': u'unset', u'Site': u'LCG.RAL.uk',
	 u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00049845',
	 u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Waiting', 'Jobs': 34, u'timestamp': 1458141578,
	 u'JobSplitType': u'DataStripping', u'MinorStatus': u'unset', u'Site': u'Group.RAL.uk',
	 u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050299',
	 u'UserGroup': u'lhcb_data', u'metric': u'WMSHistory'},
	{u'Status': u'Waiting', 'Jobs': 120, u'timestamp': 1458141578,
	 u'JobSplitType': u'User', u'MinorStatus': u'unset', u'Site': u'LCG.CERN.ch', u'Reschedules': 0,
	 u'ApplicationStatus': u'unset', u'User': u'mvesteri', u'JobGroup': u'lhcb',
	 u'UserGroup': u'lhcb_user', u'metric': u'WMSHistory'},
	{u'Status': u'Waiting', 'Jobs': 1, u'timestamp': 1458141578,
	 u'JobSplitType': u'MCStripping', u'MinorStatus': u'unset', u'Site': u'LCG.CNAF.it',
	 u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00049845',
	 u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Waiting', 'Jobs': 2, u'timestamp': 1458141578,
	 u'JobSplitType': u'MCStripping', u'MinorStatus': u'unset', u'Site': u'LCG.CNAF.it',
	 u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00049848',
	 u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Waiting', 'Jobs': 1, u'timestamp': 1458141578,
	 u'JobSplitType': u'MCReconstruction', u'MinorStatus': u'unset', u'Site': u'LCG.CNAF.it',
	 u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050286',
	 u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Waiting', 'Jobs': 95, u'timestamp': 1458199202,
	 u'JobSplitType': u'User', u'MinorStatus': u'unset', u'Site': u'Multiple', u'Reschedules': 0,
	 u'ApplicationStatus': u'unset', u'User': u'mamartin', u'JobGroup': u'lhcb',
	 u'UserGroup': u'lhcb_user',
	 u'metric': u'WMSHistory'},
	{u'Status': u'Waiting', 'Jobs': 3, u'timestamp': 1458199202,
	 u'JobSplitType': u'User', u'MinorStatus': u'unset', u'Site': u'Multiple', u'Reschedules': 0,
	 u'ApplicationStatus': u'unset', u'User': u'olupton', u'JobGroup': u'lhcb', u'UserGroup': u'lhcb_user',
	 u'metric': u'WMSHistory'},
	{u'Status': u'Waiting', 'Jobs': 129, u'timestamp': 1458199202,
	 u'JobSplitType': u'MCSimulation', u'MinorStatus': u'unset', u'Site': u'Multiple', u'Reschedules': 0,
	 u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00049844',
	 u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Running', 'Jobs': 5, u'timestamp': 1458217812,
	 u'JobSplitType': u'MCSimulation', u'MinorStatus': u'unset', u'Site': u'LCG.IHEP.su',
	 u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050232',
	 u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Running', 'Jobs': 7, u'timestamp': 1458217812,
	 u'JobSplitType': u'MCSimulation', u'MinorStatus': u'unset', u'Site': u'LCG.IHEP.su',
	 u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050234',
	 u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Running', 'Jobs': 1, u'timestamp': 1458217812,
	 u'JobSplitType': u'MCSimulation', u'MinorStatus': u'unset', u'Site': u'LCG.IHEP.su',
	 u'Reschedules': 1, u'ApplicationStatus': u'unset', u'User': u'phicharp',
	 u'JobGroup': u'00050236', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Running', 'Jobs': 3, u'timestamp': 1458217812, u'JobSplitType': u'MCSimulation',
	 u'MinorStatus': u'unset', u'Site': u'LCG.IHEP.su', u'Reschedules': 0,
	 u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050238',
	 u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Running', 'Jobs': 2, u'timestamp': 1458217812, u'JobSplitType': u'MCSimulation',
	 u'MinorStatus': u'unset', u'Site': u'LCG.IHEP.su', u'Reschedules': 0, u'ApplicationStatus': u'unset',
	 u'User': u'phicharp', u'JobGroup': u'00050248', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Running', 'Jobs': 12, u'timestamp': 1458218413, u'JobSplitType': u'MCSimulation',
	 u'MinorStatus': u'unset', u'Site': u'LCG.CNAF.it', u'Reschedules': 0, u'ApplicationStatus': u'unset',
	 u'User': u'phicharp', u'JobGroup': u'00050248', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Running', 'Jobs': 5, u'timestamp': 1458218413, u'JobSplitType': u'MCSimulation',
	 u'MinorStatus': u'unset', u'Site': u'LCG.CNAF.it', u'Reschedules': 0, u'ApplicationStatus': u'unset',
	 u'User': u'phicharp', u'JobGroup': u'00050250', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Running', 'Jobs': 4, u'timestamp': 1458218413, u'JobSplitType': u'MCReconstruction',
	 u'MinorStatus': u'unset', u'Site': u'LCG.CNAF.it', u'Reschedules': 0, u'ApplicationStatus': u'unset',
	 u'User': u'phicharp', u'JobGroup': u'00050251', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Running', 'Jobs': 1, u'timestamp': 1458218413, u'JobSplitType': u'MCReconstruction',
	 u'MinorStatus': u'unset', u'Site': u'LCG.CNAF.it', u'Reschedules': 0, u'ApplicationStatus': u'unset',
	 u'User': u'phicharp', u'JobGroup': u'00050280', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Running', 'Jobs': 24, u'timestamp': 1458219012, u'JobSplitType': u'MCSimulation',
	 u'MinorStatus': u'unset', u'Site': u'LCG.NIKHEF.nl', u'Reschedules': 0,
	 u'ApplicationStatus': u'unset',
	 u'User': u'phicharp', u'JobGroup': u'00050248', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Running', 'Jobs': 3, u'timestamp': 1458219012, u'JobSplitType': u'MCReconstruction',
	 u'MinorStatus': u'unset', u'Site': u'LCG.NIKHEF.nl', u'Reschedules': 0,
	 u'ApplicationStatus': u'unset',
	 u'User': u'phicharp', u'JobGroup': u'00050251', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Running', 'Jobs': 1, u'timestamp': 1458222013, u'JobSplitType': u'MCSimulation',
	 u'MinorStatus': u'unset', u'Site': u'LCG.Bologna.it', u'Reschedules': 0,
	 u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050303',
	 u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Running', 'Jobs': 7, u'timestamp': 1458222013, u'JobSplitType': u'User',
	 u'MinorStatus': u'unset', u'Site': u'LCG.Bristol.uk', u'Reschedules': 0,
	 u'ApplicationStatus': u'unset', u'User': u'clangenb', u'JobGroup': u'lhcb',
	 u'UserGroup': u'lhcb_user', u'metric': u'WMSHistory'},
	{u'Status': u'Running', 'Jobs': 2, u'timestamp': 1458222013, u'JobSplitType': u'User',
	 u'MinorStatus': u'unset', u'Site': u'LCG.Bristol.uk', u'Reschedules': 0,
	 u'ApplicationStatus': u'unset', u'User': u'mrwillia', u'JobGroup': u'lhcb',
	 u'UserGroup': u'lhcb_user', u'metric': u'WMSHistory'},
	{u'Status': u'Running', 'Jobs': 1, u'timestamp': 1458222013, u'JobSplitType': u'MCSimulation',
	 u'MinorStatus': u'unset', u'Site': u'LCG.Bari.it', u'Reschedules': 0,
	 u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050244',
	 u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Running', 'Jobs': 11, u'timestamp': 1458222013, u'JobSplitType': u'MCSimulation',
	 u'MinorStatus': u'unset', u'Site': u'LCG.Bari.it', u'Reschedules': 0,
	 u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050246',
	 u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Running', 'Jobs': 22, u'timestamp': 1458222013, u'JobSplitType': u'MCSimulation',
	 u'MinorStatus': u'unset', u'Site': u'LCG.Bari.it', u'Reschedules': 0,
	 u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050248',
	 u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Running', 'Jobs': 23, u'timestamp': 1458225013, u'JobSplitType': u'MCSimulation',
	 u'MinorStatus': u'unset', u'Site': u'LCG.DESYZN.de', u'Reschedules': 0,
	 u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00049844',
	 u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Running', 'Jobs': 18, u'timestamp': 1458225013, u'JobSplitType': u'MCSimulation',
	 u'MinorStatus': u'unset', u'Site': u'LCG.DESYZN.de', u'Reschedules': 0,
	 u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00049847',
	 u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Running', 'Jobs': 1, u'timestamp': 1458225013, u'JobSplitType': u'MCSimulation',
	 u'MinorStatus': u'unset', u'Site': u'LCG.DESYZN.de', u'Reschedules': 0,
	 u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050238',
	 u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Running', 'Jobs': 1, u'timestamp': 1458225013, u'JobSplitType': u'MCSimulation',
	 u'MinorStatus': u'unset', u'Site': u'LCG.DESYZN.de', u'Reschedules': 0,
	 u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050246',
	 u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Waiting', 'Jobs': 1, u'timestamp': 1458226213, u'JobSplitType': u'MCReconstruction',
	 u'MinorStatus': u'unset', u'Site': u'LCG.RRCKI.ru', u'Reschedules': 0,
	 u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050243',
	 u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Waiting', 'Jobs': 1, u'timestamp': 1458226213, u'JobSplitType': u'MCReconstruction',
	 u'MinorStatus': u'unset', u'Site': u'LCG.RRCKI.ru', u'Reschedules': 0,
	 u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050251',
	 u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Waiting', 'Jobs': 1, u'timestamp': 1458226213, u'JobSplitType': u'MCStripping',
	 u'MinorStatus': u'unset', u'Site': u'LCG.RRCKI.ru', u'Reschedules': 0,
	 u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050256',
	 u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Waiting', 'Jobs': 1, u'timestamp': 1458226213, u'JobSplitType': u'MCReconstruction',
	 u'MinorStatus': u'unset', u'Site': u'LCG.RAL.uk', u'Reschedules': 0,
	 u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050229',
	 u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Waiting', 'Jobs': 1, u'timestamp': 1458226213, u'JobSplitType': u'MCReconstruction',
	 u'MinorStatus': u'unset', u'Site': u'LCG.RAL.uk', u'Reschedules': 0,
	 u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050241',
	 u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Waiting', 'Jobs': 1, u'timestamp': 1458226213, u'JobSplitType': u'MCReconstruction',
	 u'MinorStatus': u'unset', u'Site': u'LCG.RAL.uk', u'Reschedules': 0,
	 u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050243',
	 u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
	{u'Status': u'Waiting', 'Jobs': 2, u'timestamp': 1458226213, u'JobSplitType': u'MCReconstruction',
	 u'MinorStatus': u'unset', u'Site': u'LCG.RAL.uk', u'Reschedules': 0,
	 u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050247',
	 u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'}]

    # This dataset is used for the ComponentMonitoringType as the data which gets stored in this type
    # is usually with these type of fields.
    self.activityMonitoringData = [
        {u'site': u'dirac-dev', 'componentType': u'service',
         u'component': u'Framework_SystemAdministrator',
         u'componentLocation': u'dips://dirac-dev:9162/Framework/SystemAdministrator', u'Connections': 92946,
         u'Queries': 1880, u'PendingQueries': 200, u'ActiveQueries': 200,
         u'RunningThreads': 200, u'MaxFD': 200, u'timestamp': 1458226213},
        {u'site': u'dirac-dev', 'componentType': u'service',
         u'component': u'Framework_SystemAdministrator',
         u'componentLocation': u'dips://dirac-dev:9162/Framework/SystemAdministrator', u'Connections': 92946,
         u'Queries': 1880, u'PendingQueries': 200, u'ActiveQueries': 200,
         u'RunningThreads': 200, u'MaxFD': 200, u'timestamp': 1458226213},
        {u'site': u'dirac-dev', 'componentType': u'service',
         u'component': u'Framework_SystemAdministrator',
         u'componentLocation': u'dips://dirac-dev:9162/Framework/SystemAdministrator', u'Connections': 92946,
         u'Queries': 1880, u'PendingQueries': 200, u'ActiveQueries': 200,
         u'RunningThreads': 200, u'MaxFD': 200, u'timestamp': 1458226213},
        {u'site': u'dirac-dev', 'componentType': u'service',
         u'component': u'Framework_SystemAdministrator',
         u'componentLocation': u'dips://dirac-dev:9162/Framework/SystemAdministrator', u'Connections': 92946,
         u'Queries': 1880, u'PendingQueries': 200, u'ActiveQueries': 200,
         u'RunningThreads': 200, u'MaxFD': 200, u'timestamp': 1458226213},
        {u'site': u'dirac-dev', 'componentType': u'service',
         u'component': u'Framework_SystemAdministrator',
         u'componentLocation': u'dips://dirac-dev:9162/Framework/SystemAdministrator', u'Connections': 92946,
         u'Queries': 1880, u'PendingQueries': 200, u'ActiveQueries': 200,
         u'RunningThreads': 200, u'MaxFD': 200, u'timestamp': 1458226213},
        {u'site': u'dirac-dev', 'componentType': u'service',
         u'component': u'Framework_SystemAdministrator',
         u'componentLocation': u'dips://dirac-dev:9162/Framework/SystemAdministrator', u'Connections': 92946,
         u'Queries': 1880, u'PendingQueries': 200, u'ActiveQueries': 200,
         u'RunningThreads': 200, u'MaxFD': 200, u'timestamp': 1458226213},
        {u'site': u'dirac-dev', 'componentType': u'service',
         u'component': u'Framework_SystemAdministrator',
         u'componentLocation': u'dips://dirac-dev:9162/Framework/SystemAdministrator', u'Connections': 92946,
         u'Queries': 1880, u'PendingQueries': 200, u'ActiveQueries': 200,
         u'RunningThreads': 200, u'MaxFD': 200, u'timestamp': 1458226213},
        {u'site': u'dirac-dev', 'componentType': u'service',
         u'component': u'Framework_SystemAdministrator',
         u'componentLocation': u'dips://dirac-dev:9162/Framework/SystemAdministrator', u'Connections': 92946,
         u'Queries': 1880, u'PendingQueries': 200, u'ActiveQueries': 200,
         u'RunningThreads': 200, u'MaxFD': 200, u'timestamp': 1458226213},
        {u'site': u'dirac-dev', 'componentType': u'service',
         u'component': u'Framework_SystemAdministrator',
         u'componentLocation': u'dips://dirac-dev:9162/Framework/SystemAdministrator', u'Connections': 92946,
         u'Queries': 1880, u'PendingQueries': 200, u'ActiveQueries': 200,
         u'RunningThreads': 200, u'MaxFD': 200, u'timestamp': 1458226213},
        {u'site': u'dirac-dev', 'componentType': u'service',
         u'component': u'Framework_SystemAdministrator',
         u'componentLocation': u'dips://dirac-dev:9162/Framework/SystemAdministrator', u'Connections': 92946,
         u'Queries': 1880, u'PendingQueries': 200, u'ActiveQueries': 200,
         u'RunningThreads': 200, u'MaxFD': 200, u'timestamp': 1458226213},
        {u'site': u'dirac-dev', 'componentType': u'service',
         u'component': u'Framework_SystemAdministrator',
         u'componentLocation': u'dips://dirac-dev:9162/Framework/SystemAdministrator', u'Connections': 92946,
         u'Queries': 1880, u'PendingQueries': 200, u'ActiveQueries': 200,
         u'RunningThreads': 200, u'MaxFD': 200, u'timestamp': 1458226213},
        {u'site': u'dirac-dev', 'componentType': u'service',
         u'component': u'Framework_SystemAdministrator',
         u'componentLocation': u'dips://dirac-dev:9162/Framework/SystemAdministrator', u'Connections': 92946,
         u'Queries': 1880, u'PendingQueries': 200, u'ActiveQueries': 200,
         u'RunningThreads': 200, u'MaxFD': 200, u'timestamp': 1458226213},
        {u'site': u'dirac-dev', 'componentType': u'service',
         u'component': u'Framework_SystemAdministrator',
         u'componentLocation': u'dips://dirac-dev:9162/Framework/SystemAdministrator', u'Connections': 92946,
         u'Queries': 1880, u'PendingQueries': 200, u'ActiveQueries': 200,
         u'RunningThreads': 200, u'MaxFD': 200, u'timestamp': 1458226213},
        {u'site': u'dirac-dev', 'componentType': u'service',
         u'component': u'Framework_SystemAdministrator',
         u'componentLocation': u'dips://dirac-dev:9162/Framework/SystemAdministrator', u'Connections': 92946,
         u'Queries': 1880, u'PendingQueries': 200, u'ActiveQueries': 200,
         u'RunningThreads': 200, u'MaxFD': 200, u'timestamp': 1458226213},
        {u'site': u'dirac-dev', 'componentType': u'service',
         u'component': u'Framework_SystemAdministrator',
         u'componentLocation': u'dips://dirac-dev:9162/Framework/SystemAdministrator', u'Connections': 92946,
         u'Queries': 1880, u'PendingQueries': 200, u'ActiveQueries': 200,
         u'RunningThreads': 200, u'MaxFD': 200, u'timestamp': 1458226213}]

  def tearDown(self):
    pass


class MonitoringReporterAdd(MonitoringTestCase):

  def test_addWMSRecords(self):
    for record in self.data:
      self.wmsMonitoringReporter.addRecord(record)
    result = self.wmsMonitoringReporter.commit()
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], len(self.data))

  def test_addComponentRecords(self):
    for record in self.activityMonitoringData:
      self.componentMonitoringReporter.addRecord(record)
    result = self.componentMonitoringReporter.commit()
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], len(self.activityMonitoringData))


class MonitoringDeleteChain(MonitoringTestCase):

  def test_deleteWMSIndex(self):
    result = self.monitoringDB.getIndexName('WMSHistory')
    self.assertTrue(result['OK'])

    today = datetime.today().strftime("%Y-%m-%d")
    indexName = "%s-%s" % (result['Value'], today)
    res = self.monitoringDB.deleteIndex(indexName)
    self.assertTrue(res['OK'])

  def test_deleteComponentIndex(self):
    result = self.monitoringDB.getIndexName('ComponentMonitoring')
    self.assertTrue(result['OK'])

    today = datetime.today().strftime("%Y-%m")
    indexName = "%s-%s" % (result['Value'], today)
    res = self.monitoringDB.deleteIndex(indexName)
    self.assertTrue(res['OK'])


if __name__ == '__main__':
  testSuite = unittest.defaultTestLoader.loadTestsFromTestCase(MonitoringTestCase)
  testSuite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MonitoringReporterAdd))
  testSuite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MonitoringDeleteChain))
  testResult = unittest.TextTestRunner(verbosity=2).run(testSuite)
  sys.exit(not testResult.wasSuccessful())
