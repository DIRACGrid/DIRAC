########################################################################
# File: FTSJob.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/02 13:41:20
########################################################################
""" :mod: FTSJob
    ============

    .. module: FTSJob
    :synopsis: class representing FTS job
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    class representing single FTS request
"""

__RCSID__ = "$Id $"
# #
# @file FTSJob.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/02 13:41:37
# @brief Definition of FTSJob class.

# # imports
import os
import datetime, time
import re
import tempfile
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.Grid import executeGridCommand
from DIRAC.Core.Utilities.File import checkGuid
# from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.Core.Utilities.TypedList import TypedList
from DIRAC.DataManagementSystem.Client.FTSFile import FTSFile
# # from RMS
from DIRAC.RequestManagementSystem.private.Record import Record
# # from Resources
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Resources.Catalog.FileCatalog     import FileCatalog
from DIRAC.Resources.Utilities import Utils

########################################################################
class FTSJob( Record ):
  """
  .. class:: FTSJob

  class describing one FTS job
  """

  # # initial states
  INITSTATES = ( "Submitted", "Ready", "Staging" )
  # # ongoing transfer states
  TRANSSTATES = ( "Active", "Hold" )
  # # failed states
  FAILEDSTATES = ( "Canceled", "Failed" )
  # # finished
  FINALSTATES = ( "Finished", "FinishedDirty", "Failed", "Canceled" )


  # # missing source regexp patterns
  missingSourceErrors = [
    re.compile( r"SOURCE error during TRANSFER_PREPARATION phase: \[INVALID_PATH\] Failed" ),
    re.compile( r"SOURCE error during TRANSFER_PREPARATION phase: \[INVALID_PATH\] No such file or directory" ),
    re.compile( r"SOURCE error during PREPARATION phase: \[INVALID_PATH\] Failed" ),
    re.compile( r"SOURCE error during PREPARATION phase: \[INVALID_PATH\] The requested file either does not exist" ),
    re.compile( r"TRANSFER error during TRANSFER phase: \[INVALID_PATH\] the server sent an error response: 500 500"\
               " Command failed. : open error: No such file or directory" ),
    re.compile( r"SOURCE error during TRANSFER_PREPARATION phase: \[USER_ERROR\] source file doesnt exist" ) ]

  def __init__( self, fromDict = None ):
    """c'tor

    :param self: self reference
    :param dict fromDict: data dict
    """
    Record.__init__( self )
    now = datetime.datetime.utcnow().replace( microsecond = 0 )
    self.__data__["CreationTime"] = now
    self.__data__["SubmitTime"] = now
    self.__data__["LastUpdate"] = now
    self.__data__["Status"] = "Submitted"
    self.__data__["Completeness"] = 0
    self.__data__["FTSJobID"] = 0
    self._regTime = 0.
    self._regSuccess = 0
    self._regTotal = 0
    self.__files__ = TypedList( allowedTypes = FTSFile )

    self._fc = FileCatalog()

    self._log = gLogger.getSubLogger( "FTSJob-%s" % self.FTSJobID , True )

    fromDict = fromDict if fromDict else {}
    for ftsFileDict in fromDict.get( "FTSFiles", [] ):
      self +=FTSFile( ftsFileDict )
    if "FTSFiles" in fromDict: del fromDict["FTSFiles"]
    for key, value in fromDict.items():
      if key not in self.__data__:
        raise AttributeError( "Unknown FTSJob attribute '%s'" % key )
      if value:
        setattr( self, key, value )



  @staticmethod
  def tableDesc():
    """ get table desc """
    return { "Fields" :
             { "FTSJobID" : "INTEGER NOT NULL AUTO_INCREMENT",
               "FTSGUID" :  "VARCHAR(64) NOT NULL",
               "OperationID": "INTEGER NOT NULL",
               "RequestID": "INTEGER NOT NULL",
               "SourceSE" : "VARCHAR(128) NOT NULL",
               "TargetSE" : "VARCHAR(128) NOT NULL",
               "FTSServer" : "VARCHAR(255) NOT NULL",
               "TargetToken": "VARCHAR(255)",
               "SourceToken": "VARCHAR(255)",
               "Size": "INTEGER NOT NULL",
               "Files": "INTEGER NOT NULL",
               "Completeness": "INTEGER NOT NULL DEFAULT 0",
               "FailedFiles": "INTEGER DEFAULT 0",
               "FailedSize": "INTEGER DEFAULT 0",
               "Status" : "ENUM( 'Submitted', 'Ready', 'Staging', 'Canceled', 'Active', 'Hold', "\
                "'Failed', 'Finished', 'FinishedDirty', 'Assigned' ) DEFAULT 'Submitted'",
               "Error" : "VARCHAR(255)",
               "CreationTime" : "DATETIME",
               "SubmitTime" : "DATETIME",
               "LastUpdate" : "DATETIME"  },
             "PrimaryKey" : [ "FTSJobID" ],
             "Indexes" : { "FTSJobID" : [ "FTSJobID" ], "FTSGUID": [ "FTSGUID" ] } }

  @property
  def FTSJobID( self ):
    """ FTSJobID getter """
    return self.__data__["FTSJobID"]

  @FTSJobID.setter
  def FTSJobID( self, value ):
    """ FTSJobID setter """
    self.__data__["FTSJobID"] = long( value ) if value else 0

  @property
  def RequestID( self ):
    """ RequestID getter """
    return self.__data__["RequestID"]

  @RequestID.setter
  def RequestID( self, value ):
    """ RequestID setter """
    self.__data__["RequestID"] = long( value ) if value else 0

  @property
  def OperationID( self ):
    """ OperationID getter """
    return self.__data__["OperationID"]

  @OperationID.setter
  def OperationID( self, value ):
    """ OperationID setter """
    self.__data__["OperationID"] = long( value ) if value else 0

  @property
  def FTSGUID( self ):
    """ FTSGUID prop """
    return self.__data__["FTSGUID"]

  @FTSGUID.setter
  def FTSGUID( self, value ):
    """ FTSGUID setter """
    if value:
      if type( value ) not in ( str, unicode ):
        raise TypeError( "FTSGUID should be a string!" )
      if not checkGuid( value ):
        raise ValueError( "'%s' is not a valid GUID!" % str( value ) )
    self.__data__["FTSGUID"] = value

  @property
  def FTSServer( self ):
    """ FTSServer getter """
    return self.__data__["FTSServer"]

  @FTSServer.setter
  def FTSServer( self, url ):
    """ FTSServer getter """
    self.__data__["FTSServer"] = url

  @property
  def Completeness( self ):
    """ completeness getter """
    return self.__data__["Completeness"]

  @Completeness.setter
  def Completeness( self, value ):
    """ completeness setter """
    self.__data__["Completeness"] = int( value ) if value else 0

  @property
  def Error( self ):
    """ error getter """
    return self.__data__["Error"]

  @Error.setter
  def Error( self, error ):
    """ error setter """
    self.__data__["Error"] = str( error )[255:]

  @property
  def Files( self ):
    """ nb files getter """
    self.__data__["Files"] = len( self )
    return self.__data__["Files"]

  @Files.setter
  def Files( self, value ):
    """ nb files setter """
    self.__data__["Files"] = len( self )

  @property
  def Status( self ):
    """ status prop """
    if not self.__data__["Status"]:
      self.__data__["Status"] = "Waiting"
    return self.__data__["Status"]

  @Status.setter
  def Status( self, value ):
    """ status setter """
    reStatus = re.compile( "Submitted|Ready|Staging|Hold|Canceled|Active|Failed|Finished|FinishedDirty|Assigned" )
    if not reStatus.match( value ):
      raise ValueError( "Unknown FTSJob Status: %s" % str( value ) )
    self.__data__["Status"] = value

  @property
  def FailedFiles( self ):
    """ nb failed files getter """
    self.__data__["FailedFiles"] = len( [ ftsFile for ftsFile in self
                                         if ftsFile.Status in FTSFile.FAILED_STATES ] )
    return self.__data__["FailedFiles"]

  @FailedFiles.setter
  def FailedFiles( self, value ):
    """ nb failed files setter """
    if value:
      self.__data__["FailedFiles"] = value
    else:
      self.__data__["FailedFiles"] = sum( [ ftsFile for ftsFile in self
                                           if ftsFile.Status in FTSFile.FAILED_STATES ] )

  @property
  def Size( self ):
    """ size getter """
    # if not self.__data__["Size"]:
    self.__data__["Size"] = sum( [ ftsFile.Size for ftsFile in self ] )
    return self.__data__["Size"]

  @Size.setter
  def Size( self, value ):
    """ size setter """
    if value:
      self.__data__["Size"] = value
    else:
      self.__data__["Size"] = sum( [ ftsFile.Size for ftsFile in self ] )

  @property
  def FailedSize( self ):
    """ size getter """
    if not self.__data__["FailedSize"]:
      self.__data__["FailedSize"] = sum( [ ftsFile.Size for ftsFile in self
                                          if ftsFile.Status in FTSFile.FAILED_STATES ] )
    return self.__data__["FailedSize"]

  @FailedSize.setter
  def FailedSize( self, value ):
    """ size setter """
    if value:
      self.__data__["FailedSize"] = value
    else:
      self.__data__["FailedSize"] = sum( [ ftsFile.Size for ftsFile in self
                                          if ftsFile.Status in FTSFile.FAILED_STATES ] )

  @property
  def CreationTime( self ):
    """ creation time getter """
    return self.__data__["CreationTime"]

  @CreationTime.setter
  def CreationTime( self, value = None ):
    """ creation time setter """
    if type( value ) not in ( datetime.datetime, str ) :
      raise TypeError( "CreationTime should be a datetime.datetime!" )
    if type( value ) == str:
      value = datetime.datetime.strptime( value.split( "." )[0], '%Y-%m-%d %H:%M:%S' )
    self.__data__["CreationTime"] = value

  @property
  def SubmitTime( self ):
    """ request's submission time getter """
    return self.__data__["SubmitTime"]

  @SubmitTime.setter
  def SubmitTime( self, value = None ):
    """ submission time setter """
    if type( value ) not in ( datetime.datetime, str ):
      raise TypeError( "SubmitTime should be a datetime.datetime!" )
    if type( value ) == str:
      value = datetime.datetime.strptime( value.split( "." )[0], '%Y-%m-%d %H:%M:%S' )
    self.__data__["SubmitTime"] = value

  @property
  def LastUpdate( self ):
    """ last update getter """
    return self.__data__["LastUpdate"]

  @LastUpdate.setter
  def LastUpdate( self, value = None ):
    """ last update setter """
    if type( value ) not in  ( datetime.datetime, str ):
      raise TypeError( "LastUpdate should be a datetime.datetime!" )
    if type( value ) == str:
      value = datetime.datetime.strptime( value.split( "." )[0], '%Y-%m-%d %H:%M:%S' )
    self.__data__["LastUpdate"] = value

  @property
  def TargetSE( self ):
    """ target SE getter """
    return self.__data__["TargetSE"]

  @TargetSE.setter
  def TargetSE( self, targetSE ):
    """ target SE setter """
    self.__data__["TargetSE"] = targetSE

  @property
  def SourceSE( self ):
    """ source SE getter """
    return self.__data__["SourceSE"]

  @SourceSE.setter
  def SourceSE( self, sourceSE ):
    """ source SE setter """
    self.__data__["SourceSE"] = sourceSE

  @property
  def SourceToken( self ):
    """ source token getter """
    return self.__data__["SourceToken"]

  @SourceToken.setter
  def SourceToken( self, sourceToken ):
    """ source SE setter """
    self.__data__["SourceToken"] = sourceToken

  @property
  def TargetToken( self ):
    """ target token getter """
    return self.__data__["TargetToken"]

  @TargetToken.setter
  def TargetToken( self, targetToken ):
    """ target SE setter """
    self.__data__["TargetToken"] = targetToken

  # # FTSJobFiles arithmetics
  def __contains__( self, subFile ):
    """ in operator """
    return subFile in self.__files__

  def __iadd__( self, ftsFile ):
    """ += operator """
    if ftsFile not in self:
      self.__files__.append( ftsFile )
      ftsFile._parent = self
      self.Files
      self.Size
    return self

  def __add__( self, ftsFile ):
    """ + operator """
    self +=ftsFile

  def addFile( self, ftsFile ):
    """ add :ftsFile: to FTS job """
    self +=ftsFile

  def subFile( self, ftsFile ):
    """ remove ftsFile from this job """
    if ftsFile in self:
      ftsFile._parent = None
      self.__files__.remove( ftsFile )

  # # helpers for looping
  def __iter__( self ):
    """ files iterator """
    return self.__files__.__iter__()

  def __getitem__( self, i ):
    """ [] op for files """
    return self.__files__.__getitem__( i )

  def __delitem__( self, i ):
    """ del ftsJob[i] """
    self.__files__.__delitem__( i )

  def __setitem__( self, i, ftsFile ):
    """ ftsJob[i] = ftsFile """
    self.__files__.__setitem__( i, ftsFile )

  def fileStatusList( self ):
    """ get list of files statuses """
    return [ ftsFile.Status for ftsFile in self ]

  def __len__( self ):
    """ nb of subFiles """
    return len( self.__files__ )

  def _surlPairs( self ):
    """ create and return SURL pair file """
    surls = []
    for ftsFile in self:
      checksum = "%s:%s" % ( ftsFile.ChecksumType, ftsFile.Checksum ) if ftsFile.ChecksumType and ftsFile.Checksum else ""
      surls.append( "%s %s %s" % ( ftsFile.SourceSURL, ftsFile.TargetSURL, checksum ) )
    return "\n".join( surls )

  def submitFTS2( self, stageFiles = False ):
    """ submit fts job using FTS2 client """
    if self.FTSGUID:
      return S_ERROR( "FTSJob already has been submitted" )
    surls = self._surlPairs()
    if not surls:
      return S_ERROR( "No files to submit" )
    fd, fileName = tempfile.mkstemp()
    surlFile = os.fdopen( fd, 'w' )
    surlFile.write( surls )
    surlFile.close()
    submitCommand = [ "glite-transfer-submit",
                     "-s",
                     self.FTSServer,
                     "-f",
                     fileName,
                     "-o",
                     "--compare-checksums" ]
    if self.TargetToken:
      submitCommand.append( "-t %s" % self.TargetToken )
    if self.SourceToken:
      submitCommand.append( "-S %s" % self.SourceToken )
    if stageFiles:
      submitCommand.append( "--copy-pin-lifetime 86400" )

    submit = executeGridCommand( "", submitCommand )
    os.remove( fileName )
    if not submit["OK"]:
      return submit
    returnCode, output, errStr = submit["Value"]
    if not returnCode == 0:
      return S_ERROR( errStr )
    self.FTSGUID = output.replace( "\n", "" )
    self.Status = "Submitted"
    for ftsFile in self:
      ftsFile.FTSGUID = self.FTSGUID
      ftsFile.Status = "Submitted"
    return S_OK()

  def monitorFTS2( self, full = False ):
    """ monitor fts job """
    if not self.FTSGUID:
      return S_ERROR( "FTSGUID not set, FTS job not submitted?" )

    monitorCommand = [ "glite-transfer-status",
                       "--verbose",
                       "-s",
                       self.FTSServer,
                       self.FTSGUID ]

    if full:
      monitorCommand.append( "-l" )

    monitor = executeGridCommand( "", monitorCommand )
    if not monitor["OK"]:
      return monitor
    returnCode, outputStr, errStr = monitor["Value"]

    # Returns a non zero status if error
    if returnCode != 0:
      return S_ERROR( errStr )

    outputStr = outputStr.replace( "'" , "" ).replace( "<", "" ).replace( ">", "" )

    # # set FTS job status
    regExp = re.compile( "Status:\s+(\S+)" )

    self.Status = re.search( regExp, outputStr ).group( 1 )

    statusSummary = {}
    for state in FTSFile.ALL_STATES:
      regExp = re.compile( "\s+%s:\s+(\d+)" % state )
      if regExp.search( outputStr ):
        statusSummary[state] = int( re.search( regExp, outputStr ).group( 1 ) )

    total = sum( statusSummary.values() )
    completed = sum( [ statusSummary.get( state, 0 ) for state in FTSFile.FINAL_STATES ] )
    self.Completeness = 100 * completed / total

    if not full:
      return S_OK( statusSummary )

    regExp = re.compile( "[ ]+Source:[ ]+(\S+)\n[ ]+Destination:[ ]+(\S+)\n[ ]+State:[ ]+(\S+)\n[ ]+Retries:[ ]+(\d+)\n[ ]+Reason:[ ]+([\S ]+).+?[ ]+Duration:[ ]+(\d+)", re.S )
    fileInfo = re.findall( regExp, outputStr )
    for sourceURL, _targetURL, fileStatus, _retries, reason, duration in fileInfo:
      candidateFile = None
      for ftsFile in self:
        if ftsFile.SourceSURL == sourceURL:
          candidateFile = ftsFile
          break
      if not candidateFile:
        continue
      candidateFile.Status = fileStatus
      candidateFile.Error = reason
      candidateFile._duration = duration

      if candidateFile.Status == "Failed":
        for missingSource in self.missingSourceErrors:
          if missingSource.match( reason ):
            candidateFile.Error = "MissingSource"

    # # register successful files
    if self.Status in FTSJob.FINALSTATES:
      return self.finalize()

    return S_OK()

  def finalize( self ):
    """ register successfully transferred  files """

    if self.Status not in FTSJob.FINALSTATES:
      return S_OK()

    startTime = time.time()
    targetSE = StorageElement( self.TargetSE )
    toRegister = [ ftsFile for ftsFile in self if ftsFile.Status == "Finished" ]
    toRegisterDict = {}
    for ftsFile in toRegister:
      pfn = Utils.executeSingleFileOrDirWrapper( targetSE.getPfnForProtocol( ftsFile.TargetSURL, protocol = "SRM2", withPort = False ) )
      if not pfn["OK"]:
        continue
      pfn = pfn["Value"]
      toRegisterDict[ ftsFile.LFN ] = { "PFN": pfn, "SE": self.TargetSE }

    if toRegisterDict:
      self._regTotal += len( toRegisterDict )
      register = self._fc.addReplica( toRegisterDict )
      self._regTime += time.time() - startTime
      if not register["OK"]:
        # FIXME: shouldn't be a print!
        for ftsFile in toRegister:
          ftsFile.Error = "AddCatalogReplicaFailed"
          print ftsFile.Error
        return register
      register = register["Value"]
      self._regSuccess += len( register.get( 'Successful', {} ) )
      failedFiles = register.get( "Failed", {} )
      # FIXME
      for ftsFile in toRegister:
        if ftsFile.LFN in failedFiles:
          ftsFile.Error = "AddCatalogReplicaFailed"
          print ftsFile.Error

    return S_OK()

  def toSQL( self ):
    """ prepare SQL INSERT or UPDATE statement

    :return: str with SQL fragment
    """
    colVals = [ ( "`%s`" % column, "'%s'" % value if type( value ) in ( str, datetime.datetime ) else str( value ) )
                for column, value in self.__data__.items()
                if value and column not in  ( "FTSJobID", "LastUpdate" ) ]
    colVals.append( ( "`LastUpdate`", "UTC_TIMESTAMP()" ) )
    query = []
    if self.FTSJobID:
      query.append( "UPDATE `FTSJob` SET " )
      query.append( ",".join( [ "%s=%s" % item for item in colVals  ] ) )
      query.append( " WHERE `FTSJobID`=%d;\n" % self.FTSJobID )
    else:
      query.append( "INSERT INTO `FTSJob` " )
      columns = "(%s)" % ",".join( [ column for column, value in colVals ] )
      values = "(%s)" % ",".join( [ value for column, value in colVals ] )
      query.append( columns )
      query.append( " VALUES %s;" % values )

    return S_OK( "".join( query ) )

  def toJSON( self ):
    """ dump to JSON format """
    digest = dict( zip( self.__data__.keys(),
                        [ str( val ) if val else "" for val in self.__data__.values() ] ) )
    digest["FTSFiles"] = []
    for ftsFile in self:
      fileJSON = ftsFile.toJSON()
      if not fileJSON["OK"]:
        return fileJSON
      digest["FTSFiles"].append( fileJSON["Value"] )
    return S_OK( digest )

