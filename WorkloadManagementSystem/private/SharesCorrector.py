########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/private/SharesCorrector.py,v 1.2 2009/07/02 17:23:39 acasajus Exp $
########################################################################
""" Pritority corrector for the group and ingroup shares
"""

__RCSID__ = "$Id: SharesCorrector.py,v 1.2 2009/07/02 17:23:39 acasajus Exp $"

import datetime
import time as nativetime
from DIRAC.Core.Utilities import List, Time
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR

class SharesCorrector:
  
  def __init__( self ):
    #self.__baseCSPath = "/Operations/Scheduling"
    self.__baseCSPath = "/SharesCorrection" 
    self.__shareCorrectors = {}
    self.__correctorsOrder = []
    
  def __getCSValue( self, path, defaultValue = '' ):
    return gConfig.getValue( "%s/%s" % ( self.__baseCSPath, path ), defaultValue )
    
  def __getCorrectorClass( self, correctorName ):
    fullCN = "%sCorrector" % correctorName
    try:
      correctorModule = __import__( "DIRAC.WorkloadManagementSystem.private.correctors.%s" % fullCN,
                                    globals(),
                                    locals(), fullCN )
      correctorClass  = getattr( correctorModule, fullCN )
    except Exception, e:
      gLogger.exception()
      return S_ERROR( "Can't import corrector %s: %s" % ( fullCN, str( e ) ) )
    return S_OK( correctorClass )
      
  def instantiateRequiredCorrectors( self ):
    correctorsToStart = self.__getCSValue( "ShareCorrectorsToStart", [] )
    self.__correctorsOrder = correctorsToStart
    gLogger.info( "Correctors requested: %s" % ", ".join( correctorsToStart ) )
    for corrector in self.__shareCorrectors:
      if corrector not in correctorsToStart:
        gLogger.info( "Stopping corrector %s" % corrector )
        del( self.__shareCorrectors[ corrector ] )
    for corrector in correctorsToStart:
      if corrector not in self.__shareCorrectors:
        gLogger.info( "Starting corrector %s" % corrector )
        result = gConfig.getSections( "%s/%s" % ( self.__baseCSPath, corrector ) )
        if not result[ 'OK' ]:
          gLogger.error( "Cannot get list of correctors to instantiate", 
                         " for corrector type %s: %s" % ( corrector, result[ 'Message' ] ) )
          continue
        groupCorrectors = result[ 'Value' ]
        self.__shareCorrectors[ corrector ] = {}
        result = self.__getCorrectorClass( corrector )
        if not result[ 'OK' ]:
          gLogger.error( "Cannot instantiate corrector", "%s %s" % ( corrector, result[ 'Message' ] ) )
          continue
        correctorClass = result[ 'Value' ]
        for groupCor in groupCorrectors:
          groupCorPath = "%s/%s/%s" % ( self.__baseCSPath, corrector, groupCor )
          groupToCorrect = self.__getCSValue( "%s/Group" % ( groupCorPath ), "" )
          if groupToCorrect:
            groupKey = "gr:%s" % groupToCorrect
          else:
            groupKey = "global"
          gLogger.info( "Instantiating group corrector %s of type %s" % ( groupToCorrect, corrector ) )
          if groupKey in self.__shareCorrectors[ corrector ]:
            gLogger.error( "There are two group correctors defined",
                           " for %s type (group %s)" % ( corrector, groupToCorrect ) )
          else:
            self.__shareCorrectors[ corrector ][ groupKey ] = correctorClass( groupCorPath, 
                                                                              groupToCorrect )
    return S_OK()
  
  def updateCorrectorsKnowledge(self):
    for corrector in self.__shareCorrectors:
      for groupTC in self.__shareCorrectors[ corrector ]:
        self.__shareCorrectors[ corrector ][ groupTC ].updateHistoryKnowledge()
        
  def update(self):
    self.instantiateRequiredCorrectors()
    self.updateCorrectorsKnowledge()

  def correctShares( self, shareDict, group = '' ):
    if group:
      groupKey = "gr:%s" % group
    else:
      groupKey = "global"
    for corrector in self.__shareCorrectors:
      if groupKey in self.__shareCorrectors[ corrector ]:
        shareDict = self.__shareCorrectors[ corrector ][ groupKey ].applyCorrection( shareDict )
    return shareDict
          
          
        
        
        
        
      
    