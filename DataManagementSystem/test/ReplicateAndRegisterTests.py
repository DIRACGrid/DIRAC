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

    files = []
    for i in range( 5 ):
      fname = "/tmp/testPutAndRegister-%s" % i
      lfn = "/lhcb/user/c/cibak/" + fname.split( "/" )[-1]
      fh = open( fname, "w+" )
      for i in range( 100 ):
        fh.write( str( random.randint( 0, i ) ) )
      fh.close()

      size = os.stat( fname ).st_size
      checksum = fileAdler( fname )
      guid = makeGuid( fname )

      files.append( ( fname, lfn, size, checksum, guid ) )


    self.putAndRegister = Operation()
    self.putAndRegister.Type = "PutAndRegister"
    self.putAndRegister.TargetSE = "RAL-USER"
    for fname, lfn, size, checksum, guid in files:
      putFile = File()
      putFile.LFN = lfn
      putFile.PFN = fname
      putFile.Checksum = checksum
      putFile.ChecksumType = "adler32"
      putFile.Size = size
      putFile.GUID = guid
      self.putAndRegister.addFile( putFile )

    self.replicateAndRegister = Operation()
    self.replicateAndRegister.Type = "ReplicateAndRegister"
    self.replicateAndRegister.TargetSE = "RAL-USER,CNAF-USER"
    for fname, lfn, size, checksum, guid in files:
      repFile = File()
      repFile.LFN = lfn
      repFile.Size = size
      repFile.Checksum = checksum
      repFile.ChecksumType = "adler32"
      self.replicateAndRegister.addFile( repFile )

    self.removeReplica = Operation()
    self.removeReplica.Type = "RemoveReplica"
    self.removeReplica.TargetSE = "RAL-USER"
    for fname, lfn, size, checksum, guid in files:
      self.removeReplica.addFile( File( {"LFN": lfn } ) )

    self.removeFile = Operation()
    self.removeFile.Type = "RemoveFile"
    for fname, lfn, size, checksum, guid in files:
      self.removeFile.addFile( File( {"LFN": lfn } ) )

    self.removeFileInit = Operation()
    self.removeFileInit.Type = "RemoveFile"
    for fname, lfn, size, checksum, guid in files:
      self.removeFileInit.addFile( File( {"LFN": lfn } ) )

    self.req = Request()
    self.req.RequestName = self.reqName
    # self.req.OwnerDN = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=cibak/CN=605919/CN=Krzysztof Ciba"
    # self.req.OwnerGroup = "dirac_user"
    self.req.addOperation( self.removeFileInit )
    self.req.addOperation( self.putAndRegister )
    self.req.addOperation( self.replicateAndRegister )
    self.req.addOperation( self.removeReplica )
    self.req.addOperation( self.removeFile )

    self.reqClient = ReqClient()

  def userFiles( self ):

    """ get list of files in user domain """
    files = {}
    for i in range( 5 ):
      fname = "/tmp/testUserFile-%s" % i
      lfn = "/lhcb/user/c/cibak/" + fname.split( "/" )[-1]
      fh = open( fname, "w+" )
      for i in range( 100 ):
        fh.write( str( random.randint( 0, i ) ) )
      fh.close()

      size = os.stat( fname ).st_size
      checksum = fileAdler( fname )
      guid = makeGuid( fname )

      files[lfn] = ( fname, size, checksum, guid )

    return files

  def certFiles( self ):
    """ get list of files in cert domain """
    files = {}
    for i in range( 5 ):
      fname = "/tmp/testCertFile-%s" % i
      lfn = "/lhcb/certification/rmsdms/" + fname.split( "/" )[-1]
      fh = open( fname, "w+" )
      for i in range( 100 ):
        fh.write( str( random.randint( 0, i ) ) )
      fh.close()
      size = os.stat( fname ).st_size
      checksum = fileAdler( fname )
      guid = makeGuid( fname )
      files[lfn] = ( fname, size, checksum, guid )
    return files



  def testUser( self ):
    """ test case """
    delete = self.reqClient.deleteRequest( self.reqName )
    print delete
    put = self.reqClient.putRequest( self.req )
    self.assertEqual( put["OK"], True, "putRequest failed: %s" % put.get( "Message", "" ) )


# # test execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase( ReplicateAndRegisterTests )
  suite = unittest.TestSuite( [ suite ] )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )

