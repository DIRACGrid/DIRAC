import signal
import os

from DIRAC.LoggingSystem.Client.Logger import gLogger

gCallbackList = []

def registerSignals():
  """
  Registers signal handlers
  """
  for sigNum in ( signal.SIGINT, signal.SIGTERM ):
    try:
      signal.signal( sigNum, execute )
    except:
      pass

def execute( exitCode, frame ):
  """
  Executes the callback list
  """
  for callback in gCallbackList:
    try:
      callback( exitCode )
    except:
      gLogger.exception( "Exception while calling callback" )
  os._exit( exitCode )

def registerExitCallback( function ):
  """
  Adds a new callback to the list
  """
  global gCallbackList
  if not function in gCallbackList:
    gCallbackList.append( function )
