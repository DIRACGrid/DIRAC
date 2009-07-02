########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/private/Attic/PriorityCorrector.py,v 1.1 2009/07/02 14:59:09 acasajus Exp $
########################################################################
""" Pritority corrector for the group and ingroup shares
"""

__RCSID__ = "$Id: PriorityCorrector.py,v 1.1 2009/07/02 14:59:09 acasajus Exp $"

import datetime
from DIRAC.Core.Utilities import List, Time
from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR

class PriorityCorrector:
  
  _GLOBAL_MAX_CORRECTION = 'MaxGlobalCorrection'
  _SLICE_TIME_SPAN = 'TimeSpan'
  _SLICE_WEIGHT = 'Weight'
  _SLICE_MAX_CORRECTION = 'MaxCorrection'
  
  def __init__( self, group = False ):
    baseCSPath = "/PriorityCorrection"
    if not group:
      self.__log = gLogger.getSubLogger( "GlobalPriorityCorrector" )
      self.__baseCSPath = "%s/Global" % baseCSPath
    else:
      self.__log = gLogger.getSubLogger( "%s:PriorityCorrector" % group )
      self.__baseCSPath = "%s/Groups/%s" % ( baseCSPath, group )
    self.__group = group
    self.__reportsClient = ReportsClient()
    self.__usageHistory = {}
    self.__slices = {}
    self.__globalCorrectionFactor = 5
    self._fillSlices()
    
  def _applyHistoryCorrections( self, entityShares, baseSection = "" ):
    if baseSection not in self.__historyForCorrections or not self.__historyForCorrections[ baseSection ]:
      return entityShares
    
  def __getCSValue( self, path, defaultValue = '' ):
    return gConfig.getValue( "%s/%s" % ( self.__baseCSPath, path ), defaultValue )
    
  def _fillSlices( self ):
    self.__log.info( "Filling time slices..." )
    self.__slices = {}
    self.__globalCorrectionFactor =self.__getCSValue( self._GLOBAL_MAX_CORRECTION, 5 )
    result = gConfig.getSections( self.__baseCSPath )
    if not result[ 'OK' ]:
      self.__log.error( "Cound not get configured time slices", result[ 'Message' ] )
      return
    timeSlices = result[ 'Value' ] 
    for timeSlice in timeSlices:
      self.__slices[ timeSlice ] = {}
      for key, defaultValue in ( ( self._SLICE_TIME_SPAN, 604800 ), 
                                 ( self._SLICE_WEIGHT, 1 ), 
                                 ( self._SLICE_MAX_CORRECTION, 3 ) ):
        csPath = "%s/%s/%s" % ( self.__baseCSPath, timeSlice, key )
        self.__slices[ timeSlice ][ key ] = gConfig.getValue( csPath, defaultValue )
    #Weight has to be normalized to sum 1
    weightSum = 0
    for timeSlice in self.__slices:
      weightSum += self.__slices[ timeSlice ][ self._SLICE_WEIGHT ]
    for timeSlice in self.__slices:
      self.__slices[ timeSlice ][ self._SLICE_WEIGHT ] /= float( weightSum )
    self.__log.info( "Found %s time slices" % len( self.__slices ) )
      
  def updateUsageHistory( self ):
    self.__usageHistory = {}
    for timeSlice in self.__slices:
      result = self._getUsageHistoryForTimeSpan( self.__slices[ timeSlice ][ self._SLICE_TIME_SPAN ], 
                                                 self.__group )
      if not result[ 'OK' ]:
        self.__usageHistory = {}
        self.__log.error( "Could not get history for slice", "%s: %s" % ( timeSlice, result[ 'Message' ] ) )
        return
      self.__usageHistory[ timeSlice ] = result[ 'Value' ]
      self.__log.error( "Got history for slice %s (%s entities in slice)" % ( timeSlice, len( self.__usageHistory[ timeSlice ] ) ) )
      
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
      return result
    data = result[ 'Value' ][ 'data' ]
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
      for cF in entitiesSliceCorrections[ entity ]:
        entityCorrectionFactor += cF
      entityCorrectionFactor = min( entityCorrectionFactor, maxGlobalCorrectionFactor )
      entityCorrectionFactor = max( entityCorrectionFactor, minGlobalCorrectionFactor )
      correctedShare = entitiesExpectedShare[ entity ] * entityCorrectionFactor
      correctedEntityShare[ entity ] = correctedShare
      self.__log.verbose( "Final correction factor for entity %s is %.3f\n Final share is %.3f" % ( entity,
                                                                                                  entityCorrectionFactor,
                                                                                                  correctedShare ) )
      
    return correctedEntityShare
    