'''
Created on May 5, 2015

@author: Corentin Berger
'''
import json
import os
import socket
import zlib

from DIRAC import S_OK, gLogger
from DIRAC.Core.Base.Client               import Client

from DIRAC.DataManagementSystem.private.DLDecoder import DLDecoder
from DIRAC.DataManagementSystem.Client.DataLogging.DLUserName import DLUserName
from DIRAC.DataManagementSystem.Client.DataLogging.DLGroup import DLGroup
from DIRAC.DataManagementSystem.Client.DataLogging.DLHostName import DLHostName
from DIRAC.Core.Security.ProxyInfo import getProxyInfo

"""
  Client for Data Logging System
  forward calls to DLS service
"""
class DataLoggingClient( Client ):

  def __init__( self, url = None, **kwargs ):
    Client.__init__( self, **kwargs )
    if url :
      self.setServer( url )
    else :
      self.setServer( "DataManagement/DataLogging" )
    self.dataLoggingManager = self._getRPC()


  def insertSequence( self, sequence, directInsert = False ):
    """
      This insert a sequence into DataLoggingDB database
      :param sequence, the sequence to insert
      :param directInsert, a boolean, if we want to insert directly as a DLSequence and not a DLCompressedSequence
    """
    # we get some informations from os environement
    extraArgsToGetFromEnviron = ['JOBID', 'AGENTNAME']
    for arg in extraArgsToGetFromEnviron :
      if os.environ.has_key( arg ):
        sequence.addExtraArg( arg, os.environ[ arg ] )

    # we get some infos from proxy
    res = getProxyInfo()
    if res['OK']:
      proxyInfo = res['Value']
      sequence.userName = DLUserName( proxyInfo.get( 'username' ) )
      sequence.group = DLGroup( proxyInfo.get( 'group' ) )
    # we get the host name
    sequence.hostName = DLHostName( socket.gethostname() )

    sequenceJSON = sequence.toJSON()
    if not sequenceJSON["OK"]:
      gLogger.error( sequenceJSON['Message'] )
      return sequenceJSON
    sequenceJSON = sequenceJSON['Value']
    seq = zlib.compress( sequenceJSON )
    res = self.dataLoggingManager.insertSequence( seq, directInsert )
    return res

  def getSequence( self, fileName = None, callerName = None, before = None, after = None, status = None, extra = None,
                   userName = None, hostName = None, group = None ):
    """
      This select all Sequence with  different criterias

      :param fileName, a LFN
      :param callerName, a caller name
      :param before, a date
      :param after, a date
      :param status, a str in [ Failed, Successful, Unknown ], can be None
      :param extra, a list of tuple [ ( extraArgsName1, value1 ), ( extraArgsName2, value2 ) ]
      :param userName, a DIRAC user name
      :param hostName, an host name
      :param group, a DIRAC group

      :return sequences, a list of sequence
    """
    res = self.dataLoggingManager.getSequence( fileName, callerName, before, after, status, extra, userName, hostName, group )
    if not res["OK"]:
      return res

    sequences = [json.loads( seq, cls = DLDecoder ) for seq in res['Value']]

    return S_OK( sequences )

  def getSequenceByID( self, IDSeq ):
    """
      Get the sequence with the id passed in parameter

      :param IDSeq, ID of the sequence

      :return sequence, a list with one sequence
    """
    res = self.dataLoggingManager.getSequenceByID( IDSeq )
    if not res["OK"]:
      return res
    sequences = [json.loads( seq, cls = DLDecoder ) for seq in res['Value']]
    return S_OK( sequences )

  def getMethodCallOnFile( self, fileName, before = None, after = None, status = None ):
    """
      This select all method call about a file, you can specify a date before, a date after and both to make a between

      :param fileName, a LFN
      :param before, a date
      :param after, a date
      :param status, a str in [ Failed, Successful, Unknown ], can be None

      :return methodCalls, a list of method call
    """
    res = self.dataLoggingManager.getMethodCallOnFile( fileName, before, after, status )
    if not res["OK"]:
      return res
    methodCalls = [json.loads( call, cls = DLDecoder ) for call in res['Value']]
    return S_OK( methodCalls )

  def getMethodCallByName( self, name, before = None, after = None, status = None ):
    """
      This select all method call about a specific method name, you can specify a date before, a date after and both to make a between

      :param name, name of the method
      :param before, a date
      :param after, a date
      :param status, a str in [ Failed, Successful, Unknown ], can be None

      :return methodCalls, a list of method call
    """
    res = self.dataLoggingManager.getMethodCallByName( name, before, after, status )
    if not res["OK"]:
      return res
    methodCalls = [json.loads( call, cls = DLDecoder ) for call in res['Value']]
    return S_OK( methodCalls )
