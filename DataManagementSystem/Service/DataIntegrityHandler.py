########################################################################
# $HeadURL $
# File: DataIntegrityHandler.py
########################################################################
""" 
:mod: DataIntegrityHandler
 
.. module: DataIntegrityHandler
:synopsis: DataIntegrityHandler is the implementation of the Data Integrity service in 
the DISET framework
"""

__RCSID__ = "$Id$"

## imports
from types import DictType, IntType, LongType, ListType, StringType
## from DIRAC
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK
from DIRAC.DataManagementSystem.DB.DataIntegrityDB import DataIntegrityDB

# This is a global instance of the DataIntegrityDB class
gDataIntegrityDB = False

def initializeDataIntegrityHandler( serviceInfo ):
  """ Check that we can connect to the DB and that the tables are properly created or updated
  """
  global gDataIntegrityDB
  gDataIntegrityDB = DataIntegrityDB()
  res = gDataIntegrityDB._connect()
  if not res['OK']:
    return res
  res = gDataIntegrityDB._checkTable()
  if not res['OK'] and not res['Message'] == 'The requested table already exist':
    return res

  return S_OK()

class DataIntegrityHandler( RequestHandler ):
  """
  .. class:: DataIntegrityHandler

  Implementation of the Data Integrity service in the DISET framework.
  """

  types_removeProblematic = [ [IntType, LongType, ListType] ]
  @staticmethod
  def export_removeProblematic( fileID ):
    """ Remove the file with the supplied FileID from the database
    """
    if type( fileID ) == ListType:
      fileIDs = fileID
    else:
      fileIDs = [int( fileID )]
    gLogger.info( "DataIntegrityHandler.removeProblematic: Attempting to remove problematic." )
    res = gDataIntegrityDB.removeProblematic( fileIDs )
    if not res['OK']:
      gLogger.error( "DataIntegrityHandler.removeProblematic: Failed to remove problematic.", res['Message'] )
    return res

  types_getProblematic = []
  @staticmethod
  def export_getProblematic():
    """ Get the next problematic to resolve from the IntegrityDB
    """
    gLogger.info( "DataIntegrityHandler.getProblematic: Getting file to resolve." )
    res = gDataIntegrityDB.getProblematic()
    if not res['OK']:
      gLogger.error( "DataIntegrityHandler.getProblematic: Failed to get problematic file to resolve.", res['Message'] )
    return res

  types_getPrognosisProblematics = [StringType]
  @staticmethod
  def export_getPrognosisProblematics( prognosis ):
    """ Get problematic files from the problematics table of the IntegrityDB
    """
    gLogger.info( "DataIntegrityHandler.getPrognosisProblematics: Getting files with %s prognosis." % prognosis )
    res = gDataIntegrityDB.getPrognosisProblematics( prognosis )
    if not res['OK']:
      gLogger.error( "DataIntegrityHandler.getPrognosisProblematics: Failed to get prognosis files.", res['Message'] )
    return res

  types_setProblematicStatus = [[IntType, LongType], StringType]
  @staticmethod
  def export_setProblematicStatus( fileID, status ):
    """ Update the status of the problematics with the provided fileID
    """
    gLogger.info( "DataIntegrityHandler.setProblematicStatus: Setting file %s status to %s." % ( fileID, status ) )
    res = gDataIntegrityDB.setProblematicStatus( fileID, status )
    if not res['OK']:
      gLogger.error( "DataIntegrityHandler.setProblematicStatus: Failed to set status.", res['Message'] )
    return res

  types_incrementProblematicRetry = [[IntType, LongType]]
  @staticmethod
  def export_incrementProblematicRetry( fileID ):
    """ Update the retry count for supplied file ID.
    """
    gLogger.info( "DataIntegrityHandler.incrementProblematicRetry: Incrementing retries for file %s." % ( fileID ) )
    res = gDataIntegrityDB.incrementProblematicRetry( fileID )
    if not res['OK']:
      gLogger.error( "DataIntegrityHandler.incrementProblematicRetry: Failed to increment retries.", res['Message'] )
    return res

  types_insertProblematic = [StringType, DictType]
  @staticmethod
  def export_insertProblematic( source, fileMetadata ):
    """ Insert problematic files into the problematics table of the IntegrityDB
    """
    gLogger.info( "DataIntegrityHandler.insertProblematic: Inserting problematic file to integrity DB." )
    res = gDataIntegrityDB.insertProblematic( source, fileMetadata )
    if not res['OK']:
      gLogger.error( "DataIntegrityHandler.insertProblematic: Failed to insert.", res['Message'] )
    return res

  types_changeProblematicPrognosis = []
  @staticmethod
  def export_changeProblematicPrognosis( fileID, newPrognosis ):
    """ Change the prognosis for the supplied file """
    gLogger.info( "DataIntegrityHandler.changeProblematicPrognosis: Changing problematic prognosis." )
    res = gDataIntegrityDB.changeProblematicPrognosis( fileID, newPrognosis )
    if not res['OK']:
      gLogger.error( "DataIntegrityHandler.changeProblematicPrognosis: Failed to update.", res['Message'] )
    return res

  types_getTransformationProblematics = [ [IntType, LongType] ]
  @staticmethod
  def export_getTransformationProblematics( transID ):
    """ Get the problematics for a given transformation """
    gLogger.info( "DataIntegrityHandler.getTransformationProblematics: Getting problematics for transformation." )
    res = gDataIntegrityDB.getTransformationProblematics( transID )
    if not res['OK']:
      gLogger.error( "DataIntegrityHandler.getTransformationProblematics: Failed.", res['Message'] )
    return res

  types_getProblematicsSummary = []
  @staticmethod
  def export_getProblematicsSummary():
    """ Get a summary from the Problematics table from the IntegrityDB
    """
    gLogger.info( "DataIntegrityHandler.getProblematicsSummary: Getting problematics summary." )
    res = gDataIntegrityDB.getProblematicsSummary()
    if res['OK']:
      for prognosis, statusDict in res['Value'].items():
        gLogger.info( "DataIntegrityHandler.getProblematicsSummary: %s." % prognosis )
        for status, count in statusDict.items():
          gLogger.info( "DataIntegrityHandler.getProblematicsSummary: \t%-10s %-10s." % ( status, str( count ) ) )
    else:
      gLogger.error( "DataIntegrityHandler.getProblematicsSummary: Failed to get summary.", res['Message'] )
    return res

  types_getDistinctPrognosis = []
  @staticmethod
  def export_getDistinctPrognosis():
    """ Get a list of the distinct prognosis from the IntegrityDB
    """
    gLogger.info( "DataIntegrityHandler.getDistinctPrognosis: Getting distinct prognosis." )
    res = gDataIntegrityDB.getDistinctPrognosis()
    if res['OK']:
      for prognosis in res['Value']:
        gLogger.info( "DataIntegrityHandler.getDistinctPrognosis: \t%s." % prognosis )
    else:
      gLogger.error( "DataIntegrityHandler.getDistinctPrognosis: Failed to get unique prognosis.", res['Message'] )
    return res
