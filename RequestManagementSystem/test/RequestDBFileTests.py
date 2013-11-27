########################################################################
# $HeadURL $
# File: RequestDBFileTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/06/13 07:51:20
########################################################################

""" :mod: RequestDBFileTests 
    =======================
 
    .. module: RequestDBFileTests
    :synopsis: Test suites for RequestDBFile module. 
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    Test suites for RequestDBFile module. 

    :deprecated:
"""

__RCSID__ = "$Id $"

##
# @file RequestDBFileTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/06/13 07:51:37
# @brief Definition of RequestDBFileTests class.

## imports 
import unittest
import mock
## from DIRAC
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.RequestManagementSystem.DB.RequestDBFile import RequestDBFile
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer 

reqStr = """<?xml version="1.0" encoding="UTF-8" ?>

<DIRAC_REQUEST>
<Header 
             Status="Waiting"
             LastUpdate="2012-06-01 04:57:02"
             DIRACSetup="LHCb-Certification"
             CreationTime="0000-00-00 00:00:00"
             OwnerGroup="None"
             RequestName="testRequest"
             SourceComponent="None"
             JobID="Unknown"
/>

<TRANSFER_SUBREQUEST element_type="dictionary">

        <Files element_type="list">
                <EncodedString element_type="leaf"><![CDATA[lds6:Addlers8:cf4f13f9s7:Attempti1es6:FileIDi18es4:GUIDs36:5D5F45D5-9565-7AFB-3896-DB8695F2B35As3:LFNs33:/lhcb/user/c/cibak/cert-test1k-18s3:Md5ns3:PFNs75:srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/user/c/cibak/cert-test1k-18s4:Sizei1024es6:Statuss7:Waitingee]]></EncodedString>
        </Files>
        <Attributes element_type="dictionary">
                <Status element_type="leaf"><![CDATA[Waiting]]></Status>
                <LastUpdate element_type="leaf"><![CDATA[2012-06-01 04:57:02]]></LastUpdate>
                <TargetSE element_type="leaf"><![CDATA[PIC-USER]]></TargetSE>
                <ExecutionOrder element_type="leaf"><![CDATA[0]]></ExecutionOrder>
                <SubRequestID element_type="leaf"><![CDATA[1]]></SubRequestID>
                <CreationTime element_type="leaf"><![CDATA[2012-06-01 04:57:02]]></CreationTime>
                <SourceSE element_type="leaf"><![CDATA[CERN-USER]]></SourceSE>
                <Catalogue element_type="leaf"><![CDATA[LcgFileCatalogCombined]]></Catalogue>
                <Arguments element_type="leaf"><![CDATA[None]]></Arguments>
                <Error element_type="leaf"><![CDATA[]]></Error>
                <SubmissionTime element_type="leaf"><![CDATA[2012-06-01 04:57:02]]></SubmissionTime>
                <Operation element_type="leaf"><![CDATA[replicateAndRegister]]></Operation>
        </Attributes>
</TRANSFER_SUBREQUEST>
<REMOVAL_SUBREQUEST element_type="dictionary">

        <Files element_type="list">
                <EncodedString element_type="leaf"><![CDATA[lds6:Addlers8:cf4f13f9s7:Attempti1es6:FileIDi18es4:GUIDs36:5D5F45D5-9565-7AFB-3896-DB8695F2B35As3:LFNs33:/lhcb/user/c/cibak/cert-test1k-18s3:Md5ns3:PFNs75:srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/user/c/cibak/cert-test1k-18s4:Sizei1024es6:Statuss7:Waitingee]]></EncodedString>
        </Files>
        <Attributes element_type="dictionary">
                <Status element_type="leaf"><![CDATA[Waiting]]></Status>
                <LastUpdate element_type="leaf"><![CDATA[2012-06-01 04:57:02]]></LastUpdate>
                <TargetSE element_type="leaf"><![CDATA[PIC-USER]]></TargetSE>
                <ExecutionOrder element_type="leaf"><![CDATA[0]]></ExecutionOrder>
                <SubRequestID element_type="leaf"><![CDATA[1]]></SubRequestID>
                <CreationTime element_type="leaf"><![CDATA[2012-06-01 04:57:02]]></CreationTime>
                <SourceSE element_type="leaf"><![CDATA[CERN-USER]]></SourceSE>
                <Catalogue element_type="leaf"><![CDATA[LcgFileCatalogCombined]]></Catalogue>
                <Arguments element_type="leaf"><![CDATA[None]]></Arguments>
                <Error element_type="leaf"><![CDATA[]]></Error>
                <SubmissionTime element_type="leaf"><![CDATA[2012-06-01 04:57:02]]></SubmissionTime>
                <Operation element_type="leaf"><![CDATA[replicaRemoval]]></Operation>
        </Attributes>
</REMOVAL_SUBREQUEST>
</DIRAC_REQUEST>
"""

########################################################################
class RequestDBFileTests(unittest.TestCase):
  """
  .. class:: RequestDBFileTests
  
  """

  def setUp( self ):
    """ set up

    :param self: self reference
    """
    self.log = gLogger.getSubLogger( self.__class__.__name__ )
    self.requestDB = RequestDBFile()
    setRequest = self.requestDB.setRequest( "testRequest", reqStr )

  def tearDown( self ):
    #deleteRequest = self.requestDB.deleteRequest( "testRequest" )
    pass
    
  def test_01_getRequestStatus( self ):

    self.assertEqual( self.requestDB.getRequestStatus( "testRequest" ), 
                      {'OK': True, 'Value': {'SubRequestStatus': 'Waiting', 'RequestStatus': 'Waiting'}}) 

    ## get request
    getRemoval =  self.requestDB.getRequest( "removal" )
    oRequest = RequestContainer( getRemoval["Value"]["RequestString"] )

    
    self.assertEqual( self.requestDB.getRequestStatus( "testRequest" ), 
                      {'OK': True, 'Value': {'SubRequestStatus': 'Assigned', 'RequestStatus': 'Waiting'}} )

    ## make removal Done
    oRequest.subRequests["removal"][0]["Attributes"]["Status"] = "Done"
    oRequest.subRequests["removal"][0]["Files"][0]["Status"] = "Done"
   
    update = self.requestDB.updateRequest( getRemoval["Value"]["RequestName"], 
                                           oRequest.toXML()["Value"] )

    ## get status
    self.assertEqual( self.requestDB.getRequestStatus( "testRequest" ), 
                      {'OK': True, 'Value': {'SubRequestStatus': 'Waiting', 'RequestStatus': u'Waiting'}})

    ## make transfer Done
    oRequest.subRequests["transfer"][0]["Attributes"]["Status"] = "Done"
    oRequest.subRequests["transfer"][0]["Files"][0]["Status"] = "Done"
    update = self.requestDB.updateRequest( getRemoval["Value"]["RequestName"], 
                                           oRequest.toXML()["Value"] )
    ## get status
    self.assertEqual( self.requestDB.getRequestStatus( "testRequest" ),
                      {'OK': True, 'Value': {'SubRequestStatus': 'Done', 'RequestStatus': 'Done'}} )

  def test_02_getRequest( self ):
    """ getRequest and JobID """
    getRequest = self.requestDB.getRequest("transfer")
    self.assertEqual( getRequest["OK"], True )
    self.assertEqual( getRequest["Value"]["JobID"], 0 )
    

## test exeution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase( RequestDBFileTests )
  suite = unittest.TestSuite( [ suite ] )
  unittest.TextTestRunner(verbosity=3).run(suite)

