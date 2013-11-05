# TODO: to be moved to TestDIRAC and transformed into a real test

########################################################################
# File: ReplicateAndRegisterTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/05/13 18:38:55
########################################################################
""" :mod: FullChainTest
    ===================

    .. module: FullChainTests
    :synopsis: full chain integration test for DMS operation handlers
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittest for replicateAndRegister operation handler
"""
# #
# @file ReplicateAndRegisterTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/05/13 18:39:13
# @brief Definition of ReplicateAndRegisterTests class.

# # imports
import random
import os
import sys
# # from DIRAC
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
from DIRAC import gLogger
# # from Core
from DIRAC.Core.Utilities.Adler import fileAdler
from DIRAC.Core.Utilities.File import makeGuid
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getGroupsForUser, getDNForUsername
# # from RMS and DMS
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient

########################################################################
class FullChainTest( object ):
  """
  .. class:: FullChainTest

  creates and puts to the ReqDB full chain tests for RMS and DMS operations
  * RemoveFile
  * PutAndRegister
  * ReplicateAndRegister
  * RemoveReplica
  * RemoveFile

  """

  def buildRequest( self, owner, group, sourceSE, targetSE1, targetSE2 ):

    files = self.files( owner, group )

    putAndRegister = Operation()
    putAndRegister.Type = "PutAndRegister"
    putAndRegister.TargetSE = sourceSE
    for fname, lfn, size, checksum, guid in files:
      putFile = File()
      putFile.LFN = lfn
      putFile.PFN = fname
      putFile.Checksum = checksum
      putFile.ChecksumType = "adler32"
      putFile.Size = size
      putFile.GUID = guid
      putAndRegister.addFile( putFile )

    replicateAndRegister = Operation()
    replicateAndRegister.Type = "ReplicateAndRegister"
    replicateAndRegister.TargetSE = "%s,%s" % ( targetSE1, targetSE2 )
    for fname, lfn, size, checksum, guid in files:
      repFile = File()
      repFile.LFN = lfn
      repFile.Size = size
      repFile.Checksum = checksum
      repFile.ChecksumType = "adler32"
      replicateAndRegister.addFile( repFile )

    removeReplica = Operation()
    removeReplica.Type = "RemoveReplica"
    removeReplica.TargetSE = sourceSE
    for fname, lfn, size, checksum, guid in files:
      removeReplica.addFile( File( {"LFN": lfn } ) )

    removeFile = Operation()
    removeFile.Type = "RemoveFile"
    for fname, lfn, size, checksum, guid in files:
      removeFile.addFile( File( {"LFN": lfn } ) )

    removeFileInit = Operation()
    removeFileInit.Type = "RemoveFile"
    for fname, lfn, size, checksum, guid in files:
      removeFileInit.addFile( File( {"LFN": lfn } ) )

    req = Request()
    req.addOperation( removeFileInit )
    req.addOperation( putAndRegister )
    req.addOperation( replicateAndRegister )
    req.addOperation( removeReplica )
    req.addOperation( removeFile )
    return req

  def files( self, userName, userGroup ):
    """ get list of files in user domain """
    files = []
    for i in range( 10 ):
      fname = "/tmp/testUserFile-%s" % i
      if userGroup == "lhcb_user":
        lfn = "/lhcb/user/%s/%s/%s" % ( userName[0], userName, fname.split( "/" )[-1] )
      else:
        lfn = "/lhcb/certification/test/rmsdms/%s" % fname.split( "/" )[-1]
      fh = open( fname, "w+" )
      for i in range( 100 ):
        fh.write( str( random.randint( 0, i ) ) )
      fh.close()
      size = os.stat( fname ).st_size
      checksum = fileAdler( fname )
      guid = makeGuid( fname )
      files.append( ( fname, lfn, size, checksum, guid ) )
    return files

  def putRequest( self, userName, userDN, userGroup, sourceSE, targetSE1, targetSE2 ):
    """ test case for user """

    req = self.buildRequest( userName, userGroup, sourceSE, targetSE1, targetSE2 )

    req.RequestName = "test%s-%s" % ( userName, userGroup )
    req.OwnerDN = userDN
    req.OwnerGroup = userGroup

    gLogger.always( "putRequest: request '%s'" % req.RequestName )
    for op in req:
      gLogger.always( "putRequest: => %s %s %s" % ( op.Order, op.Type, op.TargetSE ) )
      for f in op:
        gLogger.always( "putRequest: ===> file %s" % f.LFN )

    reqClient = ReqClient()

    delete = reqClient.deleteRequest( req.RequestName )
    if not delete["OK"]:
      gLogger.error( "putRequest: %s" % delete["Message"] )
      return delete
    put = reqClient.putRequest( req )
    if not put["OK"]:
      gLogger.error( "putRequest: %s" % put["Message"] )
    return put

# # test execution
if __name__ == "__main__":

  if len( sys.argv ) != 5:
    gLogger.error( "Usage:\n python %s userGroup SourceSE TargetSE1 TargetSE2\n" )
    sys.exit( -1 )
  userGroup = sys.argv[1]
  sourceSE = sys.argv[2]
  targetSE1 = sys.argv[3]
  targetSE2 = sys.argv[4]

  gLogger.always( "will use '%s' group" % userGroup )

  admin = DiracAdmin()

  userName = admin._getCurrentUser()
  if not userName["OK"]:
    gLogger.error( userName["Message"] )
    sys.exit( -1 )
  userName = userName["Value"]
  gLogger.always( "current user is '%s'" % userName )

  userGroups = getGroupsForUser( userName )
  if not userGroups["OK"]:
    gLogger.error( userGroups["Message"] )
    sys.exit( -1 )
  userGroups = userGroups["Value"]

  if userGroup not in userGroups:
    gLogger.error( "'%s' is not a member of the '%s' group" % ( userName, userGroup ) )
    sys.exit( -1 )

  userDN = getDNForUsername( userName )
  if not userDN["OK"]:
    gLogger.error( userDN["Message"] )
    sys.exit( -1 )
  userDN = userDN["Value"][0]
  gLogger.always( "userDN is %s" % userDN )

  fct = FullChainTest()
  put = fct.putRequest( userName, userDN, userGroup, sourceSE, targetSE1, targetSE2 )


