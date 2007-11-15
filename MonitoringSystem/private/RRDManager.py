import os
import time
from DIRAC import gLogger

class RRDManager:

  def __init__( self, dataPath ):
    self.rrdLocation = "%s/rrd" % dataPath
    try:
      os.makedirs( self.rrdLocation )
    except:
      pass

  def __exec( self, cmd ):
    gLogger.debug( "EXEC: %s" % cmd)
    return os.system( cmd )

  def create( self, type, rrdFile ):
    gLogger.info( "Creating rrd file %s" % rrdFile )
    rrdFilePath = "%s/%s" % ( self.rrdLocation, rrdFile )
    cmd = "rrdtool create '%s'" % rrdFilePath
    #Start GMT(now) - 1h
    cmd += " --start %s" % ( int( time.mktime( time.gmtime() ) ) - 3600 )
    cmd += " --step 60"
    if type == "mean":
      dst = "GAUGE"
      cf = "AVERAGE"
    elif type == "sum":
      dst = "GAUGE"
      cf = "AVERAGE"
    elif type == "rate":
      dst = "DERIVE"
      cf = "AVERAGE"
    else:
      gLogger.error( "Activity type %s is not known" % type )
      return 0
    cmd += " DS:value:%s:86400:0:U" % dst
    # 1m res for 1 month
    cmd += " RRA:%s:0.9:1:43200" % cf
    # 5m res for 1 year
    cmd += " RRA:%s:0.9:5:105120" % cf
    # 1h res for 10 years
    cmd += " RRA:%s:0.9:3600:87600" % cf
    return self.__exec( cmd ) == 0

  def update( self, rrdFile, valuesList ):
    rrdFilePath = "%s/%s" % ( self.rrdLocation, rrdFile )
    cmd = "rrdtool update %s" % rrdFilePath
    for entry in valuesList:
      cmd += " %s:%s" % entry
    return self.__exec( cmd ) == 0


