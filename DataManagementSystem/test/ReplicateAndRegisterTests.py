########################################################################
# $HeadURL $
# File: ReplicateAndRegisterTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/05/13 18:38:55
########################################################################
""" :mod: ReplicateAndRegisterTests
    ===============================

    .. module: ReplicateAndRegisterTests
    :synopsis: unittest for replicateAndRegister operation handler
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittest for replicateAndRegister operation handler
"""
__RCSID__ = "$Id: $"
# #
# @file ReplicateAndRegisterTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/05/13 18:39:13
# @brief Definition of ReplicateAndRegisterTests class.

# # imports
import unittest
import random
import os
# # from DIRAC
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
# # from Core
from DIRAC.Core.Utilities.Adler import fileAdler
from DIRAC.Core.Utilities.File import makeGuid
# # from RMS and DMS
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient

########################################################################
class ReplicateAndRegisterTests( unittest.TestCase ):
  """
  .. class:: ReplicateAndRegisterTests

  """
  def setUp( self ):
    """ test setup """

    self.reqName = "fullChain"

    self.fname = "/tmp/testPutAndRegister"
    self.file = open( self.fname, "w+" )
    for i in range( 100 ):
      self.file.write( str( random.randint( 0, i ) ) )
    self.file.close()

    self.size = os.stat( self.fname ).st_size
    self.checksum = fileAdler( self.fname )
    self.guid = makeGuid( self.fname )

    self.putFile = File()
    self.putFile.PFN = self.fname
    self.putFile.LFN = "/lhcb/user/c/cibak/" + self.fname.split( "/" )[-1]
    self.putFile.Checksum = self.checksum
    self.putFile.ChecksumType = "adler32"
    self.putFile.Size = self.size
    self.putFile.GUID = self.guid

    self.putAndRegister = Operation()
    self.putAndRegister.Type = "PutAndRegister"
    self.putAndRegister.TargetSE = "RAL-USER"
    # self.putAndRegister.Catalog = "LcgFileCatalogCombined"

    self.putAndRegister.addFile( self.putFile )

    self.repFile = File()
    self.repFile.LFN = self.putFile.LFN
    self.repFile.Size = self.size
    self.repFile.Checksum = self.checksum
    self.repFile.ChecksumType = "adler32"

    self.replicateAndRegister = Operation()
    self.replicateAndRegister.Type = "ReplicateAndRegister"
    self.replicateAndRegister.TargetSE = "RAL-USER,PIC-USER"
    self.replicateAndRegister.addFile( self.repFile )


    self.removeReplica = Operation()
    self.removeReplica.Type = "RemoveReplica"
    self.removeReplica.TargetSE = "RAl-USER"
    self.removeReplica.addFile( File( {"LFN": self.putFile.LFN } ) )

    self.removeFile = Operation()
    self.removeFile.Type = "RemoveFile"
    self.removeFile.addFile( File( { "LFN": self.putFile.LFN } ) )

    self.removeFileInit = Operation()
    self.removeFileInit.Type = "RemoveFile"
    self.removeFileInit.addFile( File( {"LFN": self.putFile.LFN } ) )

    self.req = Request()
    self.req.RequestName = self.reqName
    self.req.OwnerDN = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=cibak/CN=605919/CN=Krzysztof Ciba"
    self.req.OwnerGroup = "dirac_user"
    self.req.addOperation( self.removeFileInit )
    self.req.addOperation( self.putAndRegister )
    self.req.addOperation( self.replicateAndRegister )
    self.req.addOperation( self.removeReplica )
    self.req.addOperation( self.removeFile )

    self.reqClient = ReqClient()


  def tearDown( self ):
    """ tear down """
    del self.req
    del self.putAndRegister
    del self.replicateAndRegister
    del self.removeFile
    del self.putFile
    del self.repFile
    del self.size
    del self.guid
    del self.checksum
    del self.reqName

  def test( self ):
    """ test case """
    delete = self.reqClient.deleteRequest( self.reqName )
    print delete
    put = self.reqClient.putRequest( self.req )
    self.assertEqual( put["OK"], True, "putRequest failed" )

# # test execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase( ReplicateAndRegisterTests )
  suite = unittest.TestSuite( [ suite ] )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )

