########################################################################
# File: FTSJob.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/02 13:41:20
########################################################################
"""
.. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

FTSJob class representing single FTS request
"""

__RCSID__ = "$Id $"
# #
# @file FTSJob.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/02 13:41:37
# @brief Definition of FTSJob class.

# # imports
import os
import datetime
import time
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
# # from Resources
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Resources.Catalog.FileCatalog     import FileCatalog
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult

import fts3.rest.client.easy as fts3

# We can't use the default pycurl because of known bugs
# memory leak: https://bugzilla.mozilla.org/show_bug.cgi?id=1202413
# SIGALRM handling: https://curl.haxx.se/mail/lib-2008-09/0197.html
# So we need to use the Request module. The correct version is available
# from fts-rest v3.5.2 or in the lcg-bundle 2017-01-27
from fts3.rest.client.request import Request as ftsSSLRequest

########################################################################
class FTSJob( object ):
  """ Class describing one FTS job
  """

  # # initial states
  INITSTATES = ( "Submitted", "Ready", "Staging" )
  # # ongoing transfer states
  TRANSSTATES = ( "Active", "Hold" )
  # # failed states
  FAILEDSTATES = ( "Canceled", "Failed" )
  # # finished (careful, must be capitalized)
  FINALSTATES = ( "Finished", "Finisheddirty", "FinishedDirty", "Failed", "Canceled" )


  # # missing source regexp patterns
  missingSourceErrors = [
      re.compile( r".*INVALID_PATH\] Failed" ),
      re.compile( r".*INVALID_PATH\] No such file or directory" ),
      re.compile( r".*INVALID_PATH\] The requested file either does not exist" ),
      re.compile( r".*INVALID_PATH\] the server sent an error response: 500 500"\
                 " Command failed. : open error: No such file or directory" ),
      re.compile( r"SOURCE error during TRANSFER_PREPARATION phase: \[USER_ERROR\] source file doesnt exist" ) ]

  def __init__( self, fromDict = None ):
    """c'tor

    :param self: self reference
    :param dict fromDict: data dict
    """
    self.__data__ = dict.fromkeys( self.tableDesc()["Fields"].keys(), None )

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
    self._fts3context = None

    self._states = tuple( set( self.INITSTATES + self.TRANSSTATES + self.FAILEDSTATES + self.FINALSTATES ) )

    fromDict = fromDict if fromDict else {}
    for ftsFileDict in fromDict.get( "FTSFiles", [] ):
      self +=FTSFile( ftsFileDict )
    if "FTSFiles" in fromDict:
      del fromDict["FTSFiles"]
    for key, value in fromDict.items():
      if key not in self.__data__:
        raise AttributeError( "Unknown FTSJob attribute '%s'" % key )
      if value:
        setattr( self, key, value )
    self._log = gLogger.getSubLogger( "req_%s/FTSJob-%s" % ( self.RequestID, self.FTSGUID ) , True )

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
               "Size": "BIGINT NOT NULL",
               "Files": "INTEGER NOT NULL",
               "Completeness": "INTEGER NOT NULL DEFAULT 0",
               "FailedFiles": "INTEGER DEFAULT 0",
               "FailedSize": "BIGINT DEFAULT 0",
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

    # I REALLY don't see that happening
    # but in case we change the server after the
    # context was created, I reset it
    # (I don't initialize because maybe we are in FTS2 mode...)
    self._fts3context = None

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
    value = self._normalizedStatus( value.strip() )
    if value not in self._states:
      raise ValueError( "Unknown FTSJob Status: '%s'" % str( value ) )
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
      self.__data__["FailedFiles"] = len( [ftsFile for ftsFile in self if ftsFile.Status in FTSFile.FAILED_STATES] )

  @property
  def Size( self ):
    """ size getter """
    # if not self.__data__["Size"]:
    self.__data__["Size"] = sum( ftsFile.Size for ftsFile in self )
    return self.__data__["Size"]

  @Size.setter
  def Size( self, value ):
    """ size setter """
    if value:
      self.__data__["Size"] = value
    else:
      self.__data__["Size"] = sum( ftsFile.Size for ftsFile in self )

  @property
  def FailedSize( self ):
    """ size getter """
    if not self.__data__["FailedSize"]:
      self.__data__["FailedSize"] = sum( ftsFile.Size for ftsFile in self if ftsFile.Status in FTSFile.FAILED_STATES )
    return self.__data__["FailedSize"]

  @FailedSize.setter
  def FailedSize( self, value ):
    """ size setter """
    if value:
      self.__data__["FailedSize"] = value
    else:
      self.__data__["FailedSize"] = sum( ftsFile.Size for ftsFile in self if ftsFile.Status in FTSFile.FAILED_STATES )

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

  def __nonzero__( self ):
    """ for comparisons
    """
    return True

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

  def submitFTS2( self, command = 'glite-transfer-submit', pinTime = False ):
    """ submit fts job using FTS2 client """
    if self.FTSGUID:
      return S_ERROR( "FTSJob has already been submitted" )
    surls = self._surlPairs()
    if not surls:
      return S_ERROR( "No files to submit" )
    fd, fileName = tempfile.mkstemp()
    surlFile = os.fdopen( fd, 'w' )
    surlFile.write( surls )
    surlFile.close()
    submitCommand = command.split() + \
                     [ "-s",
                       self.FTSServer,
                       "-f",
                       fileName,
                       "-o",
                       "-K" ]
    if self.TargetToken:
      submitCommand += [ "-t", self.TargetToken]
    if self.SourceToken:
      submitCommand += [ "-S", self.SourceToken ]
    if pinTime:
      submitCommand += [ "--copy-pin-lifetime", "%d" % pinTime, "--bring-online", '86400' ]

    submit = executeGridCommand( "", submitCommand )
    os.remove( fileName )
    if not submit["OK"]:
      return submit
    returnCode, output, errStr = submit["Value"]
    if returnCode != 0:
      return S_ERROR( errStr if errStr else output )
    self.FTSGUID = output.replace( "\n", "" )
    self.Status = "Submitted"
    for ftsFile in self:
      ftsFile.FTSGUID = self.FTSGUID
      ftsFile.Status = "Submitted"
    return S_OK()

  def _normalizedStatus( self, status ):
    for st in self._states:
      if status.lower() == st.lower():
        return st
    return status

  def monitorFTS2( self, command = "glite-transfer-status", full = False ):
    """ monitor fts job """
    if not self.FTSGUID:
      return S_ERROR( "FTSGUID not set, FTS job not submitted?" )

    monitorCommand = command.split() + \
                       ["--verbose",
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
      if 'was not found' in outputStr and not errStr:
        errStr = 'Job was not found'
      return S_ERROR( errStr )

    outputStr = outputStr.replace( "'" , "" ).replace( "<", "" ).replace( ">", "" )

    # # set FTS job status
    regExp = re.compile( "Status:\\s+(\\S+)" )

    # with FTS3 this can be uppercase
    self.Status = re.search( regExp, outputStr ).group( 1 )

    statusSummary = {}
    # This is capitalized, even in FTS3!
    for state in FTSFile.ALL_STATES:
      regExp = re.compile( "\\s+%s:\\s+(\\d+)" % state )
      if regExp.search( outputStr ):
        statusSummary[state] = int( re.search( regExp, outputStr ).group( 1 ) )

    total = sum( statusSummary.values() )
    completed = sum( statusSummary.get( state, 0 ) for state in FTSFile.FINAL_STATES )
    self.Completeness = 100 * completed / total if total else 0

    if not full:
      return S_OK( statusSummary )

    # The order of informations is not the same for glite- and fts- !!!
    # In order: new fts-, old fts-, glite-
    realJob = len( self ) != 0
    iExptr = None
    for iExptr, exptr in enumerate( (
                   '[ ]+Source:[ ]+(\\S+)\n[ ]+Destination:[ ]+(\\S+)\n[ ]+State:[ ]+(\\S+)\n[ ]+Reason:[ ]+([\\S ]+).+?[ ]+Duration:[ ]+(\\d+)\n[ ]+Staging:[ ]+(\\d+)\n[ ]+Retries:[ ]+(\\d+)',
                   '[ ]+Source:[ ]+(\\S+)\n[ ]+Destination:[ ]+(\\S+)\n[ ]+State:[ ]+(\\S+)\n[ ]+Reason:[ ]+([\\S ]+).+?[ ]+Duration:[ ]+(\\d+)\n[ ]+Retries:[ ]+(\\d+)',
                   '[ ]+Source:[ ]+(\\S+)\n[ ]+Destination:[ ]+(\\S+)\n[ ]+State:[ ]+(\\S+)\n[ ]+Retries:[ ]+(\\d+)\n[ ]+Reason:[ ]+([\\S ]+).+?[ ]+Duration:[ ]+(\\d+)'
                   ) ):
      regExp = re.compile( exptr, re.S )
      fileInfo = re.findall( regExp, outputStr )
      if fileInfo:
        break
    if not fileInfo:
      return S_ERROR( "Error monitoring job (no regexp match)" )
    for info in fileInfo:
      if iExptr == 0:
        # version >= 3.2.30
        sourceURL, targetURL, fileStatus, reason, duration, _retries, _staging = info
      elif iExptr == 1:
        # version FTS3 < 3.2.30
        sourceURL, targetURL, fileStatus, reason, duration, _retries = info
      elif iExptr == 2:
        # version FTS2
        sourceURL, targetURL, fileStatus, _retries, reason, duration = info
      else:
        return S_ERROR( 'Error monitoring job (implement match %d)' % iExptr )
      candidateFile = None

      if not realJob:
        # This is used by the CLI monitoring of jobs in case no file was specified
        candidateFile = FTSFile()
        candidateFile.LFN = overlap( sourceURL, targetURL )
        candidateFile.SourceSURL = sourceURL
        candidateFile.Size = 0
        self +=candidateFile
      else:
        for ftsFile in self:
          if ftsFile.SourceSURL == sourceURL:
            candidateFile = ftsFile
            break
        if not candidateFile:
          continue
      # Can be uppercase for FTS3
      if not candidateFile.TargetSURL:
        candidateFile.TargetSURL = targetURL
      candidateFile.Status = fileStatus
      candidateFile.Error = reason
      candidateFile._duration = duration

      if candidateFile.Status == "Failed":
        for missingSource in self.missingSourceErrors:
          if missingSource.match( reason ):
            candidateFile.Error = "MissingSource"
      # If the staging info was present, record it
      if len( info ) > 6:
        candidateFile._staging = info[6]
    # # register successful files
    if self.Status in FTSJob.FINALSTATES:
      return self.finalize()

    return S_OK()

  def submitFTS3( self, pinTime = False ):
    """ submit fts job using FTS3 rest API """

    if self.FTSGUID:
      return S_ERROR( "FTSJob already has been submitted" )

    transfers = []

    for ftsFile in self:
      trans = fts3.new_transfer( ftsFile.SourceSURL,
                                 ftsFile.TargetSURL,
                                 checksum = 'ADLER32:%s'%ftsFile.Checksum,
                                 filesize = ftsFile.Size )
      transfers.append( trans )

    source_spacetoken = self.SourceToken if self.SourceToken else None
    dest_spacetoken = self.TargetToken if self.TargetToken else None
    copy_pin_lifetime = pinTime if pinTime else None
    bring_online = 259200 if pinTime else None

    job = fts3.new_job( transfers = transfers, overwrite = True,
                        source_spacetoken = source_spacetoken, spacetoken = dest_spacetoken,
                        bring_online = bring_online, copy_pin_lifetime = copy_pin_lifetime, retry = 3 )

    try:
      if not self._fts3context:
        self._fts3context = fts3.Context( endpoint = self.FTSServer, request_class = ftsSSLRequest, verify = False )
      context = self._fts3context
      self.FTSGUID = fts3.submit( context, job )

    except Exception as e:
      return S_ERROR( "Error at submission: %s" % e )


    self.Status = "Submitted"
    self._log = gLogger.getSubLogger( "req_%s/FTSJob-%s" % ( self.RequestID, self.FTSGUID ) , True )
    for ftsFile in self:
      ftsFile.FTSGUID = self.FTSGUID
      ftsFile.Status = "Submitted"
    return S_OK()

  def monitorFTS3( self, full = False ):
    if not self.FTSGUID:
      return S_ERROR( "FTSGUID not set, FTS job not submitted?" )

    jobStatusDict = None
    try:
      if not self._fts3context:
        self._fts3context = fts3.Context( endpoint = self.FTSServer, request_class = ftsSSLRequest, verify = False )
      context = self._fts3context
      jobStatusDict = fts3.get_job_status( context, self.FTSGUID, list_files = True )
    except Exception as e:
      return S_ERROR( "Error getting the job status %s" % e )

    self.Status = jobStatusDict['job_state'].capitalize()

    filesInfoList = jobStatusDict['files']
    statusSummary = {}
    for fileDict in filesInfoList:
      file_state = fileDict['file_state'].capitalize()
      statusSummary[file_state] = statusSummary.get( file_state, 0 ) + 1

    total = len( filesInfoList )
    completed = sum( [ statusSummary.get( state, 0 ) for state in FTSFile.FINAL_STATES ] )
    self.Completeness = 100 * completed / total

    if not full:
      return S_OK( statusSummary )

    ftsFilesPrinted = False
    for fileDict in filesInfoList:
      sourceURL = fileDict['source_surl']
      targetURL = fileDict['dest_surl']
      fileStatus = fileDict['file_state'].capitalize()
      reason = fileDict['reason']
      duration = fileDict['tx_duration']
      candidateFile = None
      for ftsFile in self:
        if ftsFile.SourceSURL == sourceURL and ftsFile.TargetSURL == targetURL :
          candidateFile = ftsFile
          break
      if candidateFile is None:
        self._log.warn( 'FTSFile not found', 'Source: %s, Target: %s' % ( sourceURL, targetURL ) )
        if not ftsFilesPrinted:
          ftsFilesPrinted = True
          if not len( self ):
            self._log.warn( 'Monitored FTS job is empty!' )
          else:
            self._log.warn( 'All FTS files are:', '\n' + '\n'.join( ['Source: %s, Target: %s' % ( ftsFile.SourceSURL, ftsFile.TargetSURL ) for ftsFile in self] ) )
      else:
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


  def monitorFTS( self, ftsVersion, command = "glite-transfer-status", full = False ):
    """ Wrapper calling the proper method for a given version of FTS"""

    if ftsVersion == "FTS2":
      return self.monitorFTS2( command = command, full = full )
    elif ftsVersion == "FTS3":
      return self.monitorFTS3( full = full )
    else:
      return S_ERROR( "monitorFTS: unknown FTS version %s" % ftsVersion )


  def submitFTS( self, ftsVersion, command = 'glite-transfer-submit', pinTime = False ):
    """ Wrapper calling the proper method for a given version of FTS"""

    if ftsVersion == "FTS2":
      return self.submitFTS2( command = command, pinTime = pinTime )
    elif ftsVersion == "FTS3":
      return self.submitFTS3( pinTime = pinTime )
    else:
      return S_ERROR( "submitFTS: unknown FTS version %s" % ftsVersion )


  def finalize( self ):
    """ register successfully transferred  files """

    if self.Status not in FTSJob.FINALSTATES:
      return S_OK()

    if not len( self ):
      return S_ERROR( "Empty job in finalize" )

    startTime = time.time()
    targetSE = StorageElement( self.TargetSE )
    toRegister = [ ftsFile for ftsFile in self if ftsFile.Status == "Finished" ]
    toRegisterDict = {}
    for ftsFile in toRegister:
      pfn = returnSingleResult( targetSE.getURL( ftsFile.LFN, protocol = 'srm' ) )
      if pfn["OK"]:
        pfn = pfn["Value"]
        toRegisterDict[ ftsFile.LFN ] = { "PFN": pfn, "SE": self.TargetSE }
      else:
        self._log.error( "Error getting SRM URL", pfn['Message'] )

    if toRegisterDict:
      self._regTotal += len( toRegisterDict )
      register = self._fc.addReplica( toRegisterDict )
      self._regTime += time.time() - startTime
      if not register["OK"]:
        self._log.error( 'Error registering replica', register['Message'] )
        for ftsFile in toRegister:
          ftsFile.Error = "AddCatalogReplicaFailed"
        return register
      register = register["Value"]
      self._regSuccess += len( register.get( 'Successful', {} ) )
      if self._regSuccess:
        self._log.info( 'Successfully registered %d replicas' % self._regSuccess )
      failedFiles = register.get( "Failed", {} )
      errorReason = {}
      for lfn, reason in failedFiles.items():
        errorReason.setdefault( str( reason ), [] ).append( lfn )
      for reason in errorReason:
        self._log.error( 'Error registering %d replicas' % len( errorReason[reason] ), reason )
      for ftsFile in toRegister:
        if ftsFile.LFN in failedFiles:
          ftsFile.Error = "AddCatalogReplicaFailed"
    else:
      statuses = set( [ftsFile.Status for ftsFile in self] )
      self._log.warn( "No replicas to register for FTSJob (%s) - Files status: '%s'" % \
                      ( self.Status, ','.join( sorted( statuses ) ) ) )

    return S_OK()

  def toSQL( self ):
    """ prepare SQL INSERT or UPDATE statement

    :return: str with SQL fragment
    """
    colVals = []
    for column, value in self.__data__.items():
      if value is not None and column not in ( "FTSJobID", "LastUpdate" ):
        colStr = "`%s`" % column
        if isinstance( value, datetime.datetime ) or isinstance( value, basestring ):
          valStr = "'%s'" % value
        else:
          valStr = str( value )
        colVals.append( ( colStr, valStr ) )
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
                        [ str( val ) if val is not None else "" for val in self.__data__.values() ] ) )
    digest["FTSFiles"] = []
    for ftsFile in self:
      fileJSON = ftsFile.toJSON()
      if not fileJSON["OK"]:
        return fileJSON
      digest["FTSFiles"].append( fileJSON["Value"] )
    return S_OK( digest )

def overlap( s1, s2 ):
  """ Method returning the common end of 2 strings """
  s = ''
  while s1 and s2:
    c1 = s1[-1]
    c2 = s2[-1]
    if c1 == c2:
      s = c1 + s
    else:
      break
    s1 = s1[:-1]
    s2 = s2[:-1]
  return s
