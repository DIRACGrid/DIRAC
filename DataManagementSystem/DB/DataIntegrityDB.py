########################################################################
# $HeadURL$
########################################################################
""" DataIntegrityDB class is a front-end to the Data Integrity Database. """
__RCSID__ = "$Id$"

import re, os, sys
import time, datetime
from types import *

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB

#############################################################################
class DataIntegrityDB( DB ):

  def __init__( self, maxQueueSize = 10 ):
    """ Standard Constructor
    """
    DB.__init__( self, 'DataIntegrityDB', 'DataManagement/DataIntegrityDB', maxQueueSize )

#############################################################################
  def insertProblematic( self, source, fileMetadata ):
    """ Insert the supplied file metadata into the problematics table
    """
    failed = {}
    successful = {}
    for lfn, metadata in fileMetadata.items():
      prognosis = metadata['Prognosis']
      pfn = metadata['PFN']
      storageElement = metadata['SE']
      res = self.__problematicExists( prognosis, lfn, pfn, storageElement )
      if not res['OK']:
        failed[lfn] = res['Message']
      elif res['Value']:
        successful[lfn] = 'Already exists'
      else:
        metadata['LFN'] = lfn
        req = self.__buildInsertReq( source, metadata )
        res = self._update( req )
        if res['OK']:
          successful[lfn] = True
        else:
          failed[lfn] = res['Message']
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  def __problematicExists( self, prognosis, lfn, pfn, storageElement ):
    """  Determine whether the file already exists in the problematics table.
    """
    req = "SELECT FileID FROM Problematics WHERE Prognosis ='%s' AND LFN = '%s' AND PFN = '%s' AND SE = '%s';" % ( prognosis, lfn, pfn, storageElement )
    res = self._query( req )
    if not res['OK']:
      return res
    if res['Value']:
      return S_OK( True )
    else:
      return S_OK( False )

  def __buildInsertReq( self, source, fileMetadata ):
    fields = "(Source,InsertDate,LastUpdate"
    values = "('%s',UTC_TIMESTAMP(),UTC_TIMESTAMP()" % source
    for attrName, attrVal in fileMetadata.items():
      fields = "%s,%s" % ( fields, attrName )
      values = "%s,'%s'" % ( values, attrVal )
    fields = "%s)" % fields
    values = "%s)" % values
    req = "INSERT INTO Problematics %s VALUES %s;" % ( fields, values )
    return req

#############################################################################
  def getProblematicsSummary( self ):
    """ Get a summary of the current problematics table
    """
    req = "SELECT Prognosis,Status,COUNT(*) FROM Problematics GROUP BY Prognosis,Status;"
    res = self._query( req )
    if not res['OK']:
      return res
    resDict = {}
    for prognosis, status, count in res['Value']:
      if not resDict.has_key( prognosis ):
        resDict[prognosis] = {}
      resDict[prognosis][status] = int( count )
    return S_OK( resDict )

#############################################################################
  def getDistinctPrognosis( self ):
    """ Get a list of all the current problematic types
    """
    req = "SELECT DISTINCT Prognosis from Problematics;"
    res = self._query( req )
    if not res['OK']:
      return res
    if not res['Value'][0]:
      return S_OK()
    prognosisList = []
    for prognosis in res['Value'][0]:
      prognosisList.append( prognosis )
    return S_OK( prognosisList )

#############################################################################
  def getProblematic( self ):
    """ Get the next file to resolve
    """
    req = "SELECT FileID,LFN,PFN,Size,SE,GUID,Prognosis FROM Problematics WHERE Status='New' ORDER BY LastUpdate ASC LIMIT 1;"
    res = self._query( req )
    if not res['OK']:
      return res
    if not res['Value'][0]:
      return S_OK()
    fileid, lfn, pfn, size, se, guid, prognosis = res['Value'][0]
    problematicDict = {'FileID':fileid, 'LFN':lfn, 'PFN':pfn, 'Size':size, 'SE':se, 'GUID':guid, 'Prognosis':prognosis}
    return S_OK( problematicDict )

  def getPrognosisProblematics( self, prognosis ):
    """ Get all the active files with the given problematic
    """
    req = "SELECT FileID,LFN,PFN,Size,SE,GUID,Prognosis FROM Problematics WHERE Prognosis = '%s' AND Status = 'New' ORDER BY Retries,LastUpdate;" % prognosis
    res = self._query( req )
    if not res['OK']:
      return res
    problematics = []
    for fileid, lfn, pfn, size, se, guid, prognosis in res['Value']:
      problematics.append( {'FileID':fileid, 'LFN':lfn, 'PFN':pfn, 'Size':size, 'SE':se, 'GUID':guid, 'Prognosis':prognosis} )
    return S_OK( problematics )

  def getTransformationProblematics( self, transID ):
    req = "SELECT LFN,FileID FROM Problematics WHERE Status = 'New' AND LFN LIKE '%s/%s/%s';" % ( '%', ( '%8.f' % prodID ).replace( ' ', '0' ), '%' )
    res = self._query( req )
    if not res['OK']:
      return res
    problematics = {}
    for lfn, fileID in res['Value']:
      problematics[lfn] = fileID
    return S_OK( problematics )

  def incrementProblematicRetry( self, fileID ):
    req = "UPDATE Problematics SET Retries=Retries+1, LastUpdate=UTC_TIMESTAMP() WHERE FileID = %s;" % ( fileID )
    res = self._update( req )
    return res

  def removeProblematic( self, fileID ):
    req = "DELETE FROM Problematics WHERE FileID = %d" % fileID
    res = self._update( req )
    return res

  def setProblematicStatus( self, fileID, status ):
    req = "UPDATE Problematics SET Status= '%s', LastUpdate=UTC_TIMESTAMP() WHERE FileID = %s;" % ( status, fileID )
    res = self._update( req )
    return res

  def changeProblematicPrognosis( self, fileID, newPrognosis ):
    req = "UPDATE Problematics SET Prognosis = '%s', LastUpdate=UTC_TIMESTAMP() WHERE FileID = %s;" % ( newPrognosis, fileID )
    res = self._update( req )
    return res


