"""
It is used to test the MonitoringReporter. It requires MonitoringDB which is based on elasticsearch and MQ which is optional...

CS:

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
    xxxx.cern.ch (hostname where we run MQ)
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

import unittest
from datetime import datetime

from DIRAC import gLogger

from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter
from DIRAC.MonitoringSystem.DB.MonitoringDB import MonitoringDB

#pylint: disable=line-too-long
#pylint: disable=missing-docstring

class MonitoringTestCase( unittest.TestCase ):

  def setUp( self ):
    gLogger.setLevel( 'INFO' )

    self.monitoringDB = MonitoringDB()
    self.monitoringReporter = MonitoringReporter( monitoringType = "WMSHistory" )

    self.data = [{u'Status': u'Waiting', 'Jobs': 2, u'time': 1458130176, u'JobSplitType': u'MCStripping', u'MinorStatus': u'unset', u'Site': u'LCG.GRIDKA.de', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00049848', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 1, u'time': 1458130176, u'JobSplitType': u'User', u'MinorStatus': u'unset', u'Site': u'LCG.PIC.es', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'olupton', u'JobGroup': u'lhcb', u'UserGroup': u'lhcb_user', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 1, u'time': 1458130176, u'JobSplitType': u'User', u'MinorStatus': u'unset', u'Site': u'LCG.RAL.uk', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'olupton', u'JobGroup': u'lhcb', u'UserGroup': u'lhcb_user', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 1, u'time': 1458130176, u'JobSplitType': u'MCStripping', u'MinorStatus': u'unset', u'Site': u'LCG.RAL.uk', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00049845', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 34, u'time': 1458141578, u'JobSplitType': u'DataStripping', u'MinorStatus': u'unset', u'Site': u'Group.RAL.uk', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050299', u'UserGroup': u'lhcb_data', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 120, u'time': 1458141578, u'JobSplitType': u'User', u'MinorStatus': u'unset', u'Site': u'LCG.CERN.ch', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'mvesteri', u'JobGroup': u'lhcb', u'UserGroup': u'lhcb_user', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 1, u'time': 1458141578, u'JobSplitType': u'MCStripping', u'MinorStatus': u'unset', u'Site': u'LCG.CNAF.it', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00049845', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 2, u'time': 1458141578, u'JobSplitType': u'MCStripping', u'MinorStatus': u'unset', u'Site': u'LCG.CNAF.it', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00049848', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 1, u'time': 1458141578, u'JobSplitType': u'MCReconstruction', u'MinorStatus': u'unset', u'Site': u'LCG.CNAF.it', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050286', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 95, u'time': 1458199202, u'JobSplitType': u'User', u'MinorStatus': u'unset', u'Site': u'Multiple', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'mamartin', u'JobGroup': u'lhcb', u'UserGroup': u'lhcb_user', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 3, u'time': 1458199202, u'JobSplitType': u'User', u'MinorStatus': u'unset', u'Site': u'Multiple', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'olupton', u'JobGroup': u'lhcb', u'UserGroup': u'lhcb_user', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 129, u'time': 1458199202, u'JobSplitType': u'MCSimulation', u'MinorStatus': u'unset', u'Site': u'Multiple', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00049844', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 5, u'time': 1458217812, u'JobSplitType': u'MCSimulation', u'MinorStatus': u'unset', u'Site': u'LCG.IHEP.su', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050232', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 7, u'time': 1458217812, u'JobSplitType': u'MCSimulation', u'MinorStatus': u'unset', u'Site': u'LCG.IHEP.su', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050234', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 1, u'time': 1458217812, u'JobSplitType': u'MCSimulation', u'MinorStatus': u'unset', u'Site': u'LCG.IHEP.su', u'Reschedules': 1, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050236', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 3, u'time': 1458217812, u'JobSplitType': u'MCSimulation', u'MinorStatus': u'unset', u'Site': u'LCG.IHEP.su', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050238', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 2, u'time': 1458217812, u'JobSplitType': u'MCSimulation', u'MinorStatus': u'unset', u'Site': u'LCG.IHEP.su', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050248', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 12, u'time': 1458218413, u'JobSplitType': u'MCSimulation', u'MinorStatus': u'unset', u'Site': u'LCG.CNAF.it', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050248', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 5, u'time': 1458218413, u'JobSplitType': u'MCSimulation', u'MinorStatus': u'unset', u'Site': u'LCG.CNAF.it', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050250', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 4, u'time': 1458218413, u'JobSplitType': u'MCReconstruction', u'MinorStatus': u'unset', u'Site': u'LCG.CNAF.it', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050251', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 1, u'time': 1458218413, u'JobSplitType': u'MCReconstruction', u'MinorStatus': u'unset', u'Site': u'LCG.CNAF.it', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050280', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 24, u'time': 1458219012, u'JobSplitType': u'MCSimulation', u'MinorStatus': u'unset', u'Site': u'LCG.NIKHEF.nl', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050248', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 3, u'time': 1458219012, u'JobSplitType': u'MCReconstruction', u'MinorStatus': u'unset', u'Site': u'LCG.NIKHEF.nl', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050251', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 1, u'time': 1458222013, u'JobSplitType': u'MCSimulation', u'MinorStatus': u'unset', u'Site': u'LCG.Bologna.it', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050303', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 7, u'time': 1458222013, u'JobSplitType': u'User', u'MinorStatus': u'unset', u'Site': u'LCG.Bristol.uk', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'clangenb', u'JobGroup': u'lhcb', u'UserGroup': u'lhcb_user', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 2, u'time': 1458222013, u'JobSplitType': u'User', u'MinorStatus': u'unset', u'Site': u'LCG.Bristol.uk', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'mrwillia', u'JobGroup': u'lhcb', u'UserGroup': u'lhcb_user', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 1, u'time': 1458222013, u'JobSplitType': u'MCSimulation', u'MinorStatus': u'unset', u'Site': u'LCG.Bari.it', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050244', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 11, u'time': 1458222013, u'JobSplitType': u'MCSimulation', u'MinorStatus': u'unset', u'Site': u'LCG.Bari.it', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050246', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 22, u'time': 1458222013, u'JobSplitType': u'MCSimulation', u'MinorStatus': u'unset', u'Site': u'LCG.Bari.it', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050248', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 23, u'time': 1458225013, u'JobSplitType': u'MCSimulation', u'MinorStatus': u'unset', u'Site': u'LCG.DESYZN.de', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00049844', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 18, u'time': 1458225013, u'JobSplitType': u'MCSimulation', u'MinorStatus': u'unset', u'Site': u'LCG.DESYZN.de', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00049847', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 1, u'time': 1458225013, u'JobSplitType': u'MCSimulation', u'MinorStatus': u'unset', u'Site': u'LCG.DESYZN.de', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050238', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Running', 'Jobs': 1, u'time': 1458225013, u'JobSplitType': u'MCSimulation', u'MinorStatus': u'unset', u'Site': u'LCG.DESYZN.de', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050246', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 1, u'time': 1458226213, u'JobSplitType': u'MCReconstruction', u'MinorStatus': u'unset', u'Site': u'LCG.RRCKI.ru', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050243', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 1, u'time': 1458226213, u'JobSplitType': u'MCReconstruction', u'MinorStatus': u'unset', u'Site': u'LCG.RRCKI.ru', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050251', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 1, u'time': 1458226213, u'JobSplitType': u'MCStripping', u'MinorStatus': u'unset', u'Site': u'LCG.RRCKI.ru', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050256', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 1, u'time': 1458226213, u'JobSplitType': u'MCReconstruction', u'MinorStatus': u'unset', u'Site': u'LCG.RAL.uk', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050229', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 1, u'time': 1458226213, u'JobSplitType': u'MCReconstruction', u'MinorStatus': u'unset', u'Site': u'LCG.RAL.uk', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050241', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 1, u'time': 1458226213, u'JobSplitType': u'MCReconstruction', u'MinorStatus': u'unset', u'Site': u'LCG.RAL.uk', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050243', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'},
                 {u'Status': u'Waiting', 'Jobs': 2, u'time': 1458226213, u'JobSplitType': u'MCReconstruction', u'MinorStatus': u'unset', u'Site': u'LCG.RAL.uk', u'Reschedules': 0, u'ApplicationStatus': u'unset', u'User': u'phicharp', u'JobGroup': u'00050247', u'UserGroup': u'lhcb_mc', u'metric': u'WMSHistory'}]

  def tearDown( self ):
    pass

class MonitoringReporterAdd( MonitoringTestCase ):

  def test_addRecords( self ):
    for record in self.data:
      self.monitoringReporter.addRecord( record )
    result = self.monitoringReporter.commit()
    self.assert_( result['OK'] )
    self.assertEqual( result['Value'], len( self.data ) )


class MonitoringDeleteChain( MonitoringTestCase ):


  def test_deleteIndex( self ):
    result = self.monitoringDB.getIndexName('WMSHistory')
    self.assert_( result['OK'] )

    today = datetime.today().strftime( "%Y-%m-%d" )
    indexName = "%s-%s" % ( result['Value'], today )
    res = self.monitoringDB.deleteIndex( indexName )
    self.assert_( res['OK'] )



if __name__ == '__main__':
  testSuite = unittest.defaultTestLoader.loadTestsFromTestCase( MonitoringTestCase )
  testSuite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MonitoringReporterAdd ) )
  testSuite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MonitoringDeleteChain ) )
  unittest.TextTestRunner( verbosity = 2 ).run( testSuite )
