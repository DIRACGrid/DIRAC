########################################################################
# $HeadURL$
########################################################################
""" DataIntegrityHandler is the implementation of the Data Integrity service in the DISET framework
"""
__RCSID__ = "$Id$"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, rootPath, S_OK, S_ERROR
from DIRAC.DataManagementSystem.DB.DataIntegrityDB import DataIntegrityDB
import time, os
# This is a global instance of the DataIntegrityDB class
integrityDB = False

def initializeDataIntegrityHandler( serviceInfo ):
  """ Check that we can connect to the DB and that the tables are properly created or updated
  """
  global integrityDB
  integrityDB = DataIntegrityDB()
  res = integrityDB._connect()
  if not res['OK']:
    return res
  res = integrityDB._checkTable()
  if not res['OK'] and not res['Message'] == 'The requested table already exist':
    return res

  return S_OK()

class DataIntegrityHandler( RequestHandler ):

  types_removeProblematic = [[IntType, LongType, ListType]]
  def export_removeProblematic( self, fileID ):
    """ Remove the file with the supplied FileID from the database
    """
    if type( fileID ) == types.ListType:
      fileIDs = fileID
    else:
      fileIDs = [int( fileID )]
    gLogger.info( "DataIntegrityHandler.removeProblematic: Attempting to remove problematic." )
    res = integrityDB.removeProblematic( fileIDs )
    if not res['OK']:
      gLogger.error( "DataIntegrityHandler.removeProblematic: Failed to remove problematic.", res['Message'] )
    return res

  types_getProblematic = []
  def export_getProblematic( self ):
    """ Get the next problematic to resolve from the IntegrityDB
    """
    gLogger.info( "DataIntegrityHandler.getProblematic: Attempting to get file to resolve." )
    res = integrityDB.getProblematic()
    if not res['OK']:
      gLogger.error( "DataIntegrityHandler.getProblematic: Failed to get problematic file to resolve.", res['Message'] )
    return res

  types_getPrognosisProblematics = [StringType]
  def export_getPrognosisProblematics( self, prognosis ):
    """ Get problematic files from the problematics table of the IntegrityDB
    """
    gLogger.info( "DataIntegrityHandler.getPrognosisProblematics: Attempting to get files with %s prognosis." % prognosis )
    res = integrityDB.getPrognosisProblematics( prognosis )
    if not res['OK']:
      gLogger.error( "DataIntegrityHandler.getPrognosisProblematics: Failed to get prognosis files.", res['Message'] )
    return res

  types_setProblematicStatus = [[IntType, LongType], StringType]
  def export_setProblematicStatus( self, fileID, status ):
    """ Update the status of the problematics with the provided fileID
    """
    gLogger.info( "DataIntegrityHandler.setProblematicStatus: Attempting to set file %s status to %s." % ( fileID, status ) )
    res = integrityDB.setProblematicStatus( fileID, status )
    if not res['OK']:
      gLogger.error( "DataIntegrityHandler.setProblematicStatus: Failed to set status.", res['Message'] )
    return res

  types_incrementProblematicRetry = [[IntType, LongType]]
  def export_incrementProblematicRetry( self, fileID ):
    """ Update the retry count for supplied file ID.
    """
    gLogger.info( "DataIntegrityHandler.incrementProblematicRetry: Attempting to increment retries for file %s." % ( fileID ) )
    res = integrityDB.incrementProblematicRetry( fileID )
    if not res['OK']:
      gLogger.error( "DataIntegrityHandler.incrementProblematicRetry: Failed to increment retries.", res['Message'] )
    return res

  types_insertProblematic = [StringType, DictType]
  def export_insertProblematic( self, source, fileMetadata ):
    """ Insert problematic files into the problematics table of the IntegrityDB
    """
    gLogger.info( "DataIntegrityHandler.insertProblematic: Attempting to insert problematic file to integrity DB." )
    res = integrityDB.insertProblematic( source, fileMetadata )
    if not res['OK']:
      gLogger.error( "DataIntegrityHandler.insertProblematic: Failed to insert.", res['Message'] )
    return res

  types_changeProblematicPrognosis = []
  def export_changeProblematicPrognosis( self, fileID, newPrognosis ):
    """ Change the prognosis for the supplied file """
    gLogger.info( "DataIntegrityHandler.changeProblematicPrognosis: Attempting to change problematic prognosis." )
    res = integrityDB.changeProblematicPrognosis( fileID, newPrognosis )
    if not res['OK']:
      gLogger.error( "DataIntegrityHandler.changeProblematicPrognosis: Failed to update.", res['Message'] )
    return res

  types_getTransformationProblematics = [[IntType, LongType]]
  def export_getTransformationProblematics( self, transID ):
    """ Get the problematics for a given transformation """
    gLogger.info( "DataIntegrityHandler.getTransformationProblematics: Attempting to get problematics for transformation." )
    res = integrityDB.getTransformationProblematics( transID )
    if not res['OK']:
      gLogger.error( "DataIntegrityHandler.getTransformationProblematics: Failed.", res['Message'] )
    return res

  types_getProblematicsSummary = []
  def export_getProblematicsSummary( self ):
    """ Get a summary from the Problematics table from the IntegrityDB
    """
    gLogger.info( "DataIntegrityHandler.getProblematicsSummary: Attempting to get problematics summary." )
    res = integrityDB.getProblematicsSummary()
    if res['OK']:
      for prognosis, statusDict in res['Value'].items():
        gLogger.info( "DataIntegrityHandler.getProblematicsSummary: %s." % prognosis )
        for status, count in statusDict.items():
          gLogger.info( "DataIntegrityHandler.getProblematicsSummary: \t%s %s." % ( status.ljust( 10 ), str( count ).ljust( 10 ) ) )
    else:
      gLogger.error( "DataIntegrityHandler.getProblematicsSummary: Failed to get summary.", res['Message'] )
    return res

  types_getDistinctPrognosis = []
  def export_getDistinctPrognosis( self ):
    """ Get a list of the distinct prognosis from the IntegrityDB
    """
    gLogger.info( "DataIntegrityHandler.getDistinctPrognosis: Attempting to get distinct prognosis." )
    res = integrityDB.getDistinctPrognosis()
    if res['OK']:
      for prognosis in res['Value']:
        gLogger.info( "DataIntegrityHandler.getDistinctPrognosis: \t%s." % prognosis )
    else:
      gLogger.error( "DataIntegrityHandler.getDistinctPrognosis: Failed to get unique prognosis.", res['Message'] )
    return res
