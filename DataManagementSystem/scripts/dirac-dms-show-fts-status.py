#!/bin/env python
""" Showing last hour history of FTS transfers. """
import sys
import DIRAC
from DIRAC import gLogger, gConfig, S_OK
from DIRAC.Core.Base import Script 
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client import PathFinder 

__RCSID__ = "$Id$"

colors = { "yellow" : "\033[93m%s\033[0m", 
           "red" : "\033[91m%s\033[0m" } 

gProblematic = False

def showChannels():
  """ print info about the last hour performance of FTS system  """

  global gProblematic
 
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
  accFailedFiles = gConfig.getOption( taSection + "/StrategyHandler/AcceptableFailedFiles", 5 )
  if not accFailedFiles["OK"]:
    gLogger.error( accFailedFiles["Message"] )
  accFailedFiles = int( accFailedFiles["Value"] )  
  
  scInfo = "timescale = %s s\nacc failure rate = %s %%\nacc distinct failed files = %s" % ( timeScale, 
                                                                                            accFailureRate,
                                                                                            accFailedFiles ) 
  ## db monitor
  transferDB = RPCClient( "DataManagement/TransferDBMonitoring" )
  ## get channels
  channels = transferDB.getChannelQueues()
  if not channels["OK"]:
    gLogger.error( channels["Message"] )
    DIRAC.exit(1)
  channels = channels["Value"]
  ## gend bandwidths
  bands = transferDB.getChannelObservedThroughput( timeScale ) 
  if not bands["OK"]:
    gLogger.error( bands["Message"] )
    DIRAC.exit(1)
  bands = bands["Value"]
  ## get failed files  
  badFiles = transferDB.getCountFileToFTS( timeScale, "Failed" )
  if not badFiles["OK"]:
    gLogger.error( badFiles["Message"] )
    DIRAC.exit(1)
  badFiles = badFiles["Value"] if badFiles["Value"] else {} 

  colorize = sys.stdout.isatty()
  header =   " %2s | %-15s | %8s | %8s | %8s | %8s | %8s | %12s | %8s | %8s" % ( "Id", "Name", "Status", 
                                                                                 "Waiting", "Success", "Failed", 
                                                                                 "FilePut", "ThroughPut", "FailRate", 
                                                                                 "FailedFiles" )
  dashLine = "-"*len(header)
  lineTemplate = " %2s | %-15s | %8s | %8d | %8d | %8d | %8.2f | %12.2f | %8.2f | %8d" 

  printOut = []

  for chId, channel in channels.items():

    name = channel["ChannelName"]
    color = None 
    status = channel["Status"]
    if status == "Active": 
      status = "OK"

    waitingFiles = channel["Files"]
    waitingSize = channel["Size"]
    failedFiles = successFiles = filePut = througPut = fRate = 0
    
    fFiles = 0
    if chId in badFiles:
      fFiles = int(badFiles[chId])

    if chId in bands:
      band = bands[chId]
      failedFiles = int(band["FailedFiles"])
      successFiles = int(band["SuccessfulFiles"])
      filePut = band["Fileput"]
      throughPut = band["Throughput"]
      
      if failedFiles or successFiles:
        fRate =  100.0 * float( failedFiles ) / ( float(failedFiles) + float( successFiles) )
        
        if fRate > 0 and colorize:
          color = "yellow"
          status = "Poor"

        if fRate > accFailureRate and fFiles > accFailedFiles:
          status = "Closed"
          if colorize:
            color = "red"

    if gProblematic and not fRate:
      continue            
    if colorize and color:
      line = colors[color] % lineTemplate 
    else:
      line = lineTemplate
    printOut.append( line % ( chId, name, status, 
                              waitingFiles if waitingFiles else 0, 
                              successFiles if successFiles else 0, 
                              failedFiles if failedFiles else 0, 
                              filePut if filePut else 0, 
                              throughPut if througPut else 0, 
                              fRate if fRate else 0, 
                              fFiles if fFiles else 0 ) )
      
  if printOut:
    printOut = [ scInfo,  header, dashLine ] + printOut 
    for line in printOut:
      gLogger.always( line )
  else:
    gLogger.always("Noting to display...")

def setProblematic( problematic=False ):
  """ callback for showing only problematic channels """
  global gProblematic
  gProblematic = True
  return S_OK()

## script execution  
if __name__ == "__main__":
  Script.registerSwitch( "p", "problematic", "show only problematic channels", setProblematic )
  Script.parseCommandLine()
  showChannels()
