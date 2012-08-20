########################################################################
# $HeadURL$
########################################################################
""" Pritority corrector for the group and ingroup shares
"""

__RCSID__ = "$Id$"

import datetime
import time as nativetime
from DIRAC.Core.Utilities import List, Time
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR

class SharesCorrector:

  def __init__( self, opsHelper ):
    if not opsHelper:
      opsHelper = Operations()
    self.__opsHelper = opsHelper
    self.__log = gLogger.getSubLogger( "SharesCorrector" )
    self.__shareCorrectors = {}
    self.__correctorsOrder = []

  def __getCSValue( self, path, defaultValue = '' ):
    return self.__opsHelper.getValue( "Matching/%s" % path, defaultValue )

  def __getCorrectorClass( self, correctorName ):
    fullCN = "%sCorrector" % correctorName
    try:
      correctorModule = __import__( "DIRAC.WorkloadManagementSystem.private.correctors.%s" % fullCN,
                                    globals(),
                                    locals(), fullCN )
      correctorClass = getattr( correctorModule, fullCN )
    except Exception, e:
      self.__log.exception()
      return S_ERROR( "Can't import corrector %s: %s" % ( fullCN, str( e ) ) )
    return S_OK( correctorClass )

  def instantiateRequiredCorrectors( self ):
    correctorsToStart = self.__getCSValue( "ShareCorrectorsToStart", [] )
    self.__correctorsOrder = correctorsToStart
    self.__log.info( "Correctors requested: %s" % ", ".join( correctorsToStart ) )
    for corrector in self.__shareCorrectors:
      if corrector not in correctorsToStart:
        self.__log.info( "Stopping corrector %s" % corrector )
        del( self.__shareCorrectors[ corrector ] )
    for corrector in correctorsToStart:
      if corrector not in self.__shareCorrectors:
        self.__log.info( "Starting corrector %s" % corrector )
        result = gConfig.getSections( "%s/%s" % ( self.__baseCSPath, corrector ) )
        if not result[ 'OK' ]:
          self.__log.error( "Cannot get list of correctors to instantiate",
                         " for corrector type %s: %s" % ( corrector, result[ 'Message' ] ) )
          continue
        groupCorrectors = result[ 'Value' ]
        self.__shareCorrectors[ corrector ] = {}
        result = self.__getCorrectorClass( corrector )
        if not result[ 'OK' ]:
          self.__log.error( "Cannot instantiate corrector", "%s %s" % ( corrector, result[ 'Message' ] ) )
          continue
        correctorClass = result[ 'Value' ]
        for groupCor in groupCorrectors:
          groupPath = "%s/%s/Group" % ( corrector, groupCor )
          groupToCorrect = self.__getCSValue( groupPath, "" )
          if groupToCorrect:
            groupKey = "gr:%s" % groupToCorrect
          else:
            groupKey = "global"
          self.__log.info( "Instantiating group corrector %s (%s) of type %s" % ( groupCor,
                                                                                  groupToCorrect,
                                                                                  corrector ) )
          if groupKey in self.__shareCorrectors[ corrector ]:
            self.__log.error( "There are two group correctors defined",
                           " for %s type (group %s)" % ( corrector, groupToCorrect ) )
          else:
            groupCorPath = "/%s/%s/%s" % ( self.__baseCSPath, corrector, groupCor )
            self.__shareCorrectors[ corrector ][ groupKey ] = correctorClass( groupCorPath,
                                                                              groupToCorrect )
    return S_OK()

  def updateCorrectorsKnowledge( self ):
    for corrector in self.__shareCorrectors:
      for groupTC in self.__shareCorrectors[ corrector ]:
        self.__shareCorrectors[ corrector ][ groupTC ].updateHistoryKnowledge()

  def update( self ):
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








