#!/bin/env python

"""
This is doc

"""
import DIRAC
from DIRAC import gLogger, gConfig
from DIRAC.Core.Base import Script 
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client import PathFinder 
#Script.setUsageMassage( __doc__ )
#Script.registerSwitch("")


def showChannels( channels = [] ):

  taSection = PathFinder.getAgentSection("DataManagement/TransferAgent")

  timeScale = gConfig.getOption( taSection + "/ThroughputTimescale", 3600 )
  if not timeScale["OK"]:
    gLogger.error( timeScale["Message"] )
    DIRAC.exit(1)
  timeScale = int( timeScale["Value"] )
  accFailureRate = gConfig.getOption( taSection + "/StrategyHandler/AcceptableFailureRate", 75 )
  if not accFailureRate["OK"]:
    gLogger.error( accFailureRate["Message"] )
  accFailureRate = int( accFailureRate["Value"] )  


  scInfo = " timescale = %s s\n acceptable failure rate = %s %%" % ( timeScale, accFailureRate) 
  
  transferDB = RPCClient( "DataManagement/TransferDBMonitoring" )
  channels = transferDB.getChannelQueues()
  if not channels["OK"]:
    gLogger.error( channels["Message"] )
    DIRAC.exit(1)
  channels = channels["Value"]
  bands = transferDB.getChannelObservedThroughput( timeScale ) 
  if not bands["OK"]:
    gLogger.error( bands["Message"] )
    DIRAC.exit(1)
  bands = bands["Value"]

  header =   " %2s | %-15s | %8s | %8s | %8s | %8s | %8s | %10s | %8s" % ( "Id", "Name", "Status", "Waiting",
                                                                          "Success", "Failed", "FilePut", "ThroughPut", "FailRate"  )
  dashLine = "-"*len(header)
  lineTemplate = " %2s | %-15s | %8s | %8d | %8d | %8d | %8.2f | %10.2f | %8.2f"

  printOut = []

  for id, channel in channels.items():

    status = channel["Status"]
    name = channel["ChannelName"]
    waitingFiles = channel["Files"]
    waitingSize = channel["Size"]
    failedFiles = successFiles = filePut = througPut = fRate = 0
    if id in bands:
      band = bands[id]
      failedFiles = int(band["FailedFiles"])
      successFiles = int(band["SuccessfulFiles"])
      filePut = band["Fileput"]
      throughPut = band["Throughput"]
      
      if failedFiles or successFiles:
        fRate =  100.0 * float( failedFiles ) / ( float(failedFiles) + float( successFiles) )
        if fRate > accFailureRate:
          status = "Closed"
        fRate = "%3.2f" % fRate
        
    printOut.append( lineTemplate % ( id, name, status, waitingFiles, successFiles, failedFiles, filePut, throughPut, fRate ) )

   
    
  if printOut:
    printOut = [ scInfo,  header, dashLine ] + printOut 
    for line in printOut:
      gLogger.always( line )
  


  
if __name__ == "__main__":
  Script.parseCommandLine()
  channels = Script.getPositionalArgs()
  showChannels()
