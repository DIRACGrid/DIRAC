########################################################################
# $HeadURL$
########################################################################
""" Base corrector for the group and ingroup shares
"""

from DIRAC import S_OK, S_ERROR

__RCSID__ = "$Id$"

from DIRAC import S_ERROR, S_OK

class BaseCorrector( object ):

  def __init__( self, opsHelper, baseCSPath, group ):
    self.__opsHelper = opsHelper
    self.__baseCSPath = baseCSPath
    self.__group = group

  def initialize( self ):
    return S_OK()

  def getCSOption( self, opName, defValue = None ):
    return self.__opsHelper.getValue( "%s/%s" % ( self.__baseCSPath, opName ), defValue )

  def getiCSOptions( self, opName = "" ):
    return self.__opsHelper.getSections( "%s/%s" % ( self.__baseCSPath, opName ) )

  def getCSSections( self, secName = "" ):
    return self.__opsHelper.getSections( "%s/%s" % ( self.__baseCSPath, secName ) )

  def getGroup( self ):
    return self.__group

  def updateHistoryKnowledge( self ):
    return S_OK()

  def applyCorrection( self, entitiesExpectedShare ):
    return S_ERROR( "applyCorrection function has not been implemented" )
