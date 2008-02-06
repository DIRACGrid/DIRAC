# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/MonitoringSystem/private/RRDManager.py,v 1.14 2008/02/06 13:31:03 acasajus Exp $
__RCSID__ = "$Id: RRDManager.py,v 1.14 2008/02/06 13:31:03 acasajus Exp $"
import os
import os.path
import time
import md5
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceSection
from DIRAC.MonitoringSystem.private.ColorGenerator import ColorGenerator
from DIRAC.Core.Utilities import Subprocess, Time

class RRDManager:

  sizesList = [ [ 200, 50 ], [ 400, 100 ], [ 600, 150 ], [ 800, 200 ] ]

  def __init__( self, rrdLocation, graphLocation ):
    """
    Initialize RRDManager
    """
    self.rrdLocation = rrdLocation
    self.graphLocation = graphLocation
    self.rrdExec = gConfig.getValue( "%s/RRDExec" % getServiceSection( "Monitoring/Server" ), "rrdtool" )
    self.bucketTime = 60
    for path in ( self.rrdLocation, self.graphLocation ):
      try:
        os.makedirs( path )
      except:
        pass

  def getGraphLocation(self):
    """
    Set the location for graph files
    """
    return self.graphLocation

  def __exec( self, cmd ):
    """
    Execute a system command
    """
    gLogger.debug( "RRD command: %s" % cmd)
    retVal = Subprocess.shellCall( 0, cmd )
    if not retVal[ 'OK' ]:
      return retVal
    retTuple = retVal[ 'Value' ]
    if retTuple[0]:
      return S_ERROR( "Failed to execute rrdtool: %s" % ( retTuple[2] ) )
    return retVal

  def getCurrentBucketTime( self ):
    """
    Get current time "bucketized"
    """
    return self.bucketize( time.mktime( time.gmtime() ) )

  def bucketize( self, secs ):
    """
    Bucketize a time (in secs)
    """
    return ( int( secs ) / self.bucketTime ) * self.bucketTime

  def create( self, type, rrdFile ):
    """
    Create an rrd file
    """
    rrdFilePath = "%s/%s" % ( self.rrdLocation, rrdFile )
    if os.path.isfile( rrdFilePath ):
      return True
    try:
      os.makedirs( os.path.dirname( rrdFilePath ) )
    except:
      pass
    gLogger.info( "Creating rrd file %s" % rrdFile )
    cmd = "%s create '%s'" % ( self.rrdExec, rrdFilePath )
    #Start GMT(now) - 1h
    cmd += " --start %s" % ( self.getCurrentBucketTime() - 86400 )
    cmd += " --step %s" % self.bucketTime
    dst = "GAUGE"
    cf = "AVERAGE"
    cmd += " DS:value:%s:120:U:U" % dst
    # 1m res for 1 month
    cmd += " RRA:%s:0.9:1:43200" % cf
    return self.__exec( cmd ) == 0

  def __getLastUpdateTime( self, rrdFile ):
    """
    Get last update time from an rrd
    """
    cmd = "%s last %s" % ( self.rrdExec, rrdFile )
    retVal = Subprocess.shellCall( 0, cmd )
    if not retVal[ 'OK' ]:
      return retVal
    retTuple = retVal[ 'Value' ]
    if retTuple[0]:
      return S_ERROR( "Failed to fetch last update %s : %s" % ( rrdFile, retTuple[2] ) )
    return S_OK( int( retTuple[1].strip() ) )


  def update( self, rrdFile, valuesList ):
    """
    Add marks to an rrd
    """
    rrdFilePath = "%s/%s" % ( self.rrdLocation, rrdFile )
    gLogger.info( "Updating rrd file", rrdFilePath )
    retVal = self.__getLastUpdateTime( rrdFilePath )
    if retVal[ 'OK' ]:
      lastUpdateTime = retVal[ 'Value' ]
      gLogger.verbose( "Last update time is %s" % lastUpdateTime )
    cmd = "%s update %s" % ( self.rrdExec, rrdFilePath )
    rrdUpdates = []
    for entry in valuesList:
      rrdUpdates.append( "%s:%s" % entry )
    maxRRDArgs = 50
    for i in range( 0, len( rrdUpdates ), maxRRDArgs ):
      finalCmd = "%s %s" % ( cmd, " ".join( rrdUpdates[ i: i + maxRRDArgs ] ) )
      retVal = self.__exec( finalCmd )
      if not retVal[ 'OK' ]:
        gLogger.error( "Error updating %s rrd: %s" % ( rrdFile, retVal[ 'Message' ] ) )
    return S_OK()

  def __generateName( self, *args, **kwargs ):
    """
    Generate a random name
    """
    m = md5.new()
    m.update( str( args ) )
    m.update( str( kwargs ) )
    return m.hexdigest()

  def __generateRRDGraphVar( self, entryName, rrdFile, rrdType ):
    """
    Calculate the graph query in rrd lingo for an activity
    """
    if rrdType in ( "mean", "sum", "rate" ):
      varStr = "'DEF:ac%sRAW=%s/%s:value:AVERAGE'" % ( entryName, self.rrdLocation, rrdFile )
      varStr += " 'CDEF:%s=ac%sRAW,UN,0,ac%sRAW,IF'" % ( entryName, entryName, entryName )
      return varStr
    elif rrdType == "acum":
      varStr = "'DEF:ac%sRAW=%s/%s:value:AVERAGE'" % ( entryName, self.rrdLocation, rrdFile )
      varStr += " 'CDEF:ac%sNOTUN=ac%sRAW,UN,0,ac%sRAW,IF'" % ( entryName, entryName, entryName )
      varStr += " 'CDEF:%s=PREV,UN,ac%sNOTUN,PREV,ac%sNOTUN,+,IF'" % ( entryName, entryName, entryName )
      return varStr
    raise Exception( "rrdType %s is not valid" % rrdType )

  def __graphTimeComment( self ):
    return " 'COMMENT:Generated on %s GMT'" % Time.toString().replace( ":", "\:" ).split( "." )[0]

  def groupPlot( self, fromSecs, toSecs, activitiesList, stackActivities, size, graphFilename = "" ):
    """
    Generate a group plot
    """
    if not graphFilename:
      graphFilename = "%s.png" % self.__generateName( fromSecs,
                                                    toSecs,
                                                    activitiesList,
                                                    stackActivities
                                                    )
    rrdCmd = "%s graph %s/%s" % ( self.rrdExec, self.graphLocation, graphFilename )
    rrdCmd += " -s %s" % fromSecs
    rrdCmd += " -e %s" % toSecs
    rrdCmd += " -w %s" % self.sizesList[ size ][0]
    rrdCmd += " -h %s" % self.sizesList[ size ][1]
    rrdCmd += " --title '%s'" % activitiesList[ 0 ].getGroupLabel()
    colorGen = ColorGenerator()
    for idActivity in range( len( activitiesList ) ):
      activity = activitiesList[ idActivity ]
      rrdCmd += " %s" % self.__generateRRDGraphVar( idActivity, activity.getFile(), activity.getType() )
      if stackActivities:
        rrdCmd += " 'AREA:%s#%s:%s:STACK'" % ( idActivity, colorGen.getHexColor(), activity.getLabel().replace( ":", "\:" ) )
      else:
        rrdCmd += " 'LINE2:%s#%s:%s'" % ( idActivity, colorGen.getHexColor(), activity.getLabel().replace( ":", "\:" ) )
    rrdCmd += self.__graphTimeComment()
    retVal = self.__exec( rrdCmd )
    if not retVal[ 'OK' ]:
      return retVal
    return S_OK( graphFilename )

  def plot( self, fromSecs, toSecs, activity, stackActivities , size, graphFilename = ""  ):
    """
    Generate a non grouped plot
    """
    if not graphFilename:
      graphFilename = "%s.png" % self.__generateName( fromSecs,
                                                    toSecs,
                                                    activity,
                                                    stackActivities
                                                    )
    rrdCmd = "%s graph %s/%s" % ( self.rrdExec, self.graphLocation, graphFilename )
    rrdCmd += " -s %s" % fromSecs
    rrdCmd += " -e %s" % toSecs
    rrdCmd += " -w %s" % self.sizesList[ size ][0]
    rrdCmd += " -h %s" % self.sizesList[ size ][1]
    rrdCmd += " --title '%s'" % activity.getLabel()
    rrdCmd += " --vertical-label '%s'" % activity.getUnit()
    rrdCmd += " %s" % self.__generateRRDGraphVar( 0, activity.getFile(), activity.getType() )
    if stackActivities:
      rrdCmd += " 'AREA:0#FF0000::STACK'"
    else:
      rrdCmd += " 'LINE2:0#FF0000'"
    rrdCmd += self.__graphTimeComment()
    retVal = self.__exec( rrdCmd )
    if not retVal[ 'OK' ]:
      return retVal
    return S_OK( graphFilename )

  def deleteRRD( self, rrdFile ):
    try:
      os.unlink( "%s/%s" % ( self.rrdLocation, rrdFile ) )
    except Exception, e:
      gLogger.error( "Could not delete rrd file %s: %s" % ( rrdFile, str(e) ) )
