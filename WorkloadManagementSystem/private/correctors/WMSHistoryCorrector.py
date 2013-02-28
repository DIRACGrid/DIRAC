########################################################################
# $HeadURL$
########################################################################
""" WMSHistory corrector for the group and ingroup shares
"""

__RCSID__ = "$Id$"

import datetime
import time as nativetime
from DIRAC.WorkloadManagementSystem.private.correctors.BaseCorrector import BaseCorrector
from DIRAC.Core.Utilities import List, Time
from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
from DIRAC  import gLogger, S_OK, S_ERROR
from DIRAC.Core.Security import CS

class WMSHistoryCorrector( BaseCorrector ):

  _GLOBAL_MAX_CORRECTION = 'MaxGlobalCorrection'
  _SLICE_TIME_SPAN = 'TimeSpan'
  _SLICE_WEIGHT = 'Weight'
  _SLICE_MAX_CORRECTION = 'MaxCorrection'

  def initialize( self ):
    self.__log = gLogger.getSubLogger( "WMSHistoryCorrector" )
    self.__reportsClient = ReportsClient()
    self.__usageHistory = {}
    self.__slices = {}
    self.__lastHistoryUpdate = 0
    self.__globalCorrectionFactor = 5
    self._fillSlices()
    return S_OK()

  def _fillSlices( self ):
    self.__log.info( "Filling time slices..." )
    self.__slices = {}
    self.__globalCorrectionFactor =self.getCSOption( self._GLOBAL_MAX_CORRECTION, 5 )
    result = self.getCSSections()
    if not result[ 'OK' ]:
      self.__log.error( "Cound not get configured time slices", result[ 'Message' ] )
      return
    timeSlices = result[ 'Value' ]
    for timeSlice in timeSlices:
      self.__slices[ timeSlice ] = {}
      for key, defaultValue in ( ( self._SLICE_TIME_SPAN, 604800 ),
                                 ( self._SLICE_WEIGHT, 1 ),
                                 ( self._SLICE_MAX_CORRECTION, 3 ) ):
        self.__slices[ timeSlice ][ key ] = self.getCSOption( "%s/%s" % ( timeSlice, key ), defaultValue )
    #Weight has to be normalized to sum 1
    weightSum = 0
    for timeSlice in self.__slices:
      weightSum += self.__slices[ timeSlice ][ self._SLICE_WEIGHT ]
    for timeSlice in self.__slices:
      self.__slices[ timeSlice ][ self._SLICE_WEIGHT ] /= float( weightSum )
    self.__log.info( "Found %s time slices" % len( self.__slices ) )

  def updateHistoryKnowledge( self ):
    updatePeriod = self.getCSOption( 'UpdateHistoryPeriod', 900 )
    now = nativetime.time()
    if self.__lastHistoryUpdate + updatePeriod > now:
      self.__log.verbose( "Skipping history update. Last update was less than %s secs ago" % updatePeriod)
      return
    self.__lastHistoryUpdate = now
    self.__log.info( "Updating history knowledge" )
    self.__usageHistory = {}
    for timeSlice in self.__slices:
      result = self._getUsageHistoryForTimeSpan( self.__slices[ timeSlice ][ self._SLICE_TIME_SPAN ],
                                                 self.getGroup() )
      if not result[ 'OK' ]:
        self.__usageHistory = {}
        self.__log.error( "Could not get history for slice", "%s: %s" % ( timeSlice, result[ 'Message' ] ) )
        return
      self.__usageHistory[ timeSlice ] = result[ 'Value' ]
      self.__log.info( "Got history for slice %s (%s entities in slice)" % ( timeSlice, len( self.__usageHistory[ timeSlice ] ) ) )
    self.__log.info( "Updated history knowledge" )

  def _getUsageHistoryForTimeSpan( self, timeSpan, groupToUse = "" ):
    reportCondition = { 'Status' : [ 'Running' ] }
    if not groupToUse:
      reportGrouping = 'UserGroup'
    else:
      reportGrouping = 'User'
      reportCondition = { 'UserGroup' : groupToUse }
    now = Time.dateTime()
    result = self.__reportsClient.getReport( 'WMSHistory', 'AverageNumberOfJobs',
                                             now - datetime.timedelta( seconds = timeSpan ), now,
                                             reportCondition, reportGrouping,
                                             { 'lastSeconds' : timeSpan } )
    if not result[ 'OK' ]:
      self.__log.error( "Cannot get history from Accounting", result[ 'Message' ] )
      return result
    data = result[ 'Value' ][ 'data' ]

    #Map the usernames to DNs
    if groupToUse:
      mappedData = {}
      for userName in data:
        result = CS.getDNForUsername( userName )
        if not result[ 'OK' ]:
          self.__log.error( "User does not have any DN assigned", "%s :%s" % ( userName, result[ 'Message' ] ) )
          continue
        for userDN in result[ 'Value' ]:
          mappedData[ userDN ] = data[ userName ]
      data = mappedData

    return S_OK( data )

  def __normalizeShares( self, entityShares ):
    totalShare = 0.0
    normalizedShares = {}
    #Normalize shares
    for entity in entityShares:
      totalShare += entityShares[ entity ]
    self.__log.verbose( "Total share for given entities is %.3f" % totalShare )
    for entity in entityShares:
      normalizedShare = entityShares[ entity ] / totalShare
      normalizedShares[ entity ] = normalizedShare
      self.__log.verbose( "Normalized share for %s: %.3f" % ( entity, normalizedShare ) )

    return normalizedShares

  def applyCorrection( self, entitiesExpectedShare ):
    #Normalize expected shares
    normalizedShares = self.__normalizeShares( entitiesExpectedShare )

    if not self.__usageHistory:
      self.__log.verbose( "No history knowledge available. Correction is 1 for all entities" )
      return entitiesExpectedShare

    entitiesSliceCorrections = dict( [ ( entity, [] ) for entity in entitiesExpectedShare ] )
    for timeSlice in self.__usageHistory:
      self.__log.verbose( "Calculating correction for time slice %s" % timeSlice )
      sliceTotal = 0.0
      sliceHistory = self.__usageHistory[ timeSlice ]
      for entity in entitiesExpectedShare:
        if entity in sliceHistory:
          sliceTotal += sliceHistory[ entity ]
          self.__log.verbose( "Usage for %s: %.3f" % ( entity, sliceHistory[ entity ] ) )
      self.__log.verbose( "Total usage for slice %.3f" % sliceTotal )
      if sliceTotal == 0.0:
        self.__log.verbose( "Slice usage is 0, skeeping slice" )
        continue
      maxSliceCorrection = self.__slices[ timeSlice ][ self._SLICE_MAX_CORRECTION ]
      minSliceCorrection = 1.0/maxSliceCorrection
      for entity in entitiesExpectedShare:
        if entity in sliceHistory:
          normalizedSliceUsage = sliceHistory[ entity ] / sliceTotal
          self.__log.verbose( "Entity %s is present in slice %s (normalized usage %.2f)" % ( entity,
                                                                                             timeSlice,
                                                                                             normalizedSliceUsage ) )
          sliceCorrectionFactor = normalizedShares[ entity ] / normalizedSliceUsage
          sliceCorrectionFactor = min( sliceCorrectionFactor, maxSliceCorrection )
          sliceCorrectionFactor = max( sliceCorrectionFactor, minSliceCorrection )
          sliceCorrectionFactor *= self.__slices[ timeSlice ][ self._SLICE_WEIGHT ]
        else:
          self.__log.verbose( "Entity %s is not present in slice %s" % ( entity, timeSlice ) )
          sliceCorrectionFactor = maxSliceCorrection
        self.__log.verbose( "Slice correction factor for entity %s is %.3f" % ( entity, sliceCorrectionFactor ) )
        entitiesSliceCorrections[ entity ].append( sliceCorrectionFactor )

    correctedEntityShare = {}
    maxGlobalCorrectionFactor = self.__globalCorrectionFactor
    minGlobalCorrectionFactor = 1.0/maxGlobalCorrectionFactor
    for entity in entitiesSliceCorrections:
      entityCorrectionFactor = 0.0
      slicesCorrections = entitiesSliceCorrections[ entity ]
      if not slicesCorrections:
        self.__log.verbose( "Entity does not have any correction %s" % entity )
        correctedEntityShare[ entity ] = entitiesExpectedShare[ entity ]
      else:
        for cF in entitiesSliceCorrections[ entity ]:
          entityCorrectionFactor += cF
        entityCorrectionFactor = min( entityCorrectionFactor, maxGlobalCorrectionFactor )
        entityCorrectionFactor = max( entityCorrectionFactor, minGlobalCorrectionFactor )
        correctedShare = entitiesExpectedShare[ entity ] * entityCorrectionFactor
        correctedEntityShare[ entity ] = correctedShare
        self.__log.verbose( "Final correction factor for entity %s is %.3f\n Final share is %.3f" % ( entity,
                                                                                                    entityCorrectionFactor,
                                                                                                    correctedShare ) )
    self.__log.verbose( "Initial shares:\n  %s" % "\n  ".join( [ "%s : %.2f" % ( en, entitiesExpectedShare[ en ] ) for en in entitiesExpectedShare ] ) )
    self.__log.verbose( "Corrected shares:\n  %s" % "\n  ".join( [ "%s : %.2f" % ( en, correctedEntityShare[ en ] ) for en in correctedEntityShare ] ) )
    return correctedEntityShare
