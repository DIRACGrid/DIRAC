#!/usr/bin/env python
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/AccountingSystem/scripts/dirac-accounting-report-cli.py,v 1.2 2008/03/14 19:10:55 acasajus Exp $
__RCSID__ = "$Id: dirac-accounting-report-cli.py,v 1.2 2008/03/14 19:10:55 acasajus Exp $"

import cmd
import sys
import signal
import datetime
import dirac
import pprint
from DIRAC import gLogger
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities import ExitCallback, ColorCLI, List
from DIRAC.Core.DISET.RPCClient import RPCClient

Script.localCfg.addDefaultEntry( "LogLevel", "info" )
Script.parseCommandLine()

class ReportCLI( cmd.Cmd ):

  def __init__( self ):
    cmd.Cmd.__init__( self )
    self.do_connect( None )
    self.identSpace = 20
    ExitCallback.registerExitCallback( self.do_quit )
    #User friendly hack
    self.do_exit = self.do_quit
    self.do_EOF = self.do_quit
    self.initSignals()

  def initSignals( self ):
    """
    Registers signal handlers
    """
    for sigNum in ( signal.SIGINT, signal.SIGQUIT, signal.SIGKILL, signal.SIGTERM ):
      try:
        signal.signal( sigNum, self.do_quit )
      except:
        pass

  def start( self ):
    if not self.connected:
      gLogger.error( "Client is not connected" )
    try:
      self.cmdloop()
    except KeyboardInterrupt, v:
      gLogger.warn( "Received a keyboard interrupt." )
      self.do_quit( "" )

  def do_connect( self, args ):
    """
    Tries to connect to the server
        Usage: connect
    """
    gLogger.info( "Trying to connect to server" )
    self.connected = False
    self.prompt = "(%s)> " % ColorCLI.colorize( "Not connected", "red" )
    self.repClient = RPCClient( "Accounting/ReportGenerator" )
    retVal = self.repClient.ping()
    if retVal[ 'OK' ]:
      self.prompt = "(%s)> " % ColorCLI.colorize( "Connected", "green" )
      self.connected = True

  def do_quit( self, *args ):
    """
    Exits the application without sending changes to server
        Usage: quit
    """
    sys.exit( 0 )

  def printPair( self, key, value, separator=":" ):
    valueList = value.split( "\n" )
    print "%s%s%s %s" % ( key, " " * ( self.identSpace - len( key ) ), separator, valueList[0].strip() )
    for valueLine in valueList[ 1:-1 ]:
      print "%s  %s" % ( " " * self.identSpace, valueLine.strip() )

  def printComment( self, comment ):
    commentList = comment.split( "\n" )
    for commentLine in commentList[ :-1 ]:
      print "# %s" % commentLine.strip()

  def showTraceback( self ):
    import traceback
    type, value = sys.exc_info()[:2]
    print "________________________\n"
    print "Exception", type, ":", value
    traceback.print_tb( sys.exc_info()[2] )
    print "________________________\n"

  def do_help( self, args ):
    """
    Shows help information
        Usage: help <command>
        If no command is specified all commands are shown
    """
    if len( args ) == 0:
      print "\nAvailable commands:\n"
      attrList = dir( self )
      attrList.sort()
      for attribute in attrList:
        if attribute.find( "do_" ) == 0:
          self.printPair( attribute[ 3: ], getattr( self, attribute ).__doc__[ 1: ] )
          print ""
    else:
      command = args.split()[0].strip()
      try:
        obj = getattr( self, "do_%s" % command )
      except:
        print "There's no such %s command" % command
        return
      self.printPair( command, obj.__doc__[1:] )

  def do_showSummariesList( self, args ):
    """
    Get a list of available summaries
      Usage : showSummariesList
    """
    try:
      retVal = self.repClient.listSummaries()
      if not retVal[ 'OK' ]:
        gLogger.error( "Error: %s" % retVal[ 'Message' ] )
        return
      print "Available summaries:"
      for summary in retVal[ 'Value' ]:
        print  " %s" % summary
    except:
      self.showTraceback()

  def do_showViewsList( self, args ):
    """
    Get a list of available views
      Usage : do_showViewsList
    """
    try:
      retVal = self.repClient.listViews()
      if not retVal[ 'OK' ]:
        gLogger.error( "Error: %s" % retVal[ 'Message' ] )
        return
      print "Available summaries:"
      for summary in retVal[ 'Value' ]:
        print  " %s" % summary
    except:
      self.showTraceback()

  def __getDatetimeFromArg( self, dtString ):
    if len( dtString ) != 12:
      return False
    dt = datetime.datetime( year  = int( dtString[0:4] ),
                            month = int( dtString[4:6] ),
                            day   = int( dtString[6:8] ) )
    dt += datetime.timedelta( hours = int( dtString[ 8:10 ] ),
                              minutes = int( dtString[ 10:12 ] ) )
    return dt

  def do_getSummary( self, args ):
    """
    Gets a summary
      Usage : getSummary <Summary name> <startdate YYYYMMDDHHMM> <enddate YYYYMMDDHHMM> (<field name> <field value>)*
    """
    try:
      argList = List.fromChar( args, " " )
      if len( argList ) < 3:
        gLogger.error( "Missing arguments!" )
        return
      startDT = self.__getDatetimeFromArg( argList[1] )
      if not startDT:
        gLogger.error( "Start time has invalid format" )
      endDT = self.__getDatetimeFromArg( argList[2] )
      if not endDT:
        gLogger.error( "End time has invalid format" )
      gLogger.info( "Start time is %s" % startDT )
      gLogger.info( "End time is %s" % endDT )
      sumArgs = {}
      for iP in range( 3, len( argList ), 2 ):
        key = argList[ iP ]
        if key in sumArgs:
          sumArgs[ key ].append( argList[ iP + 1 ] )
        else:
          sumArgs[ key ] = [ argList[ iP + 1 ] ]
      retVal = self.repClient.generateSummary( argList[ 0 ], startDT, endDT, sumArgs )
      if not retVal[ 'OK' ]:
        gLogger.error( "Error: %s" % retVal[ 'Message' ] )
        return
      printer = pprint.PrettyPrinter(indent=4)
      printer.pprint( retVal[ 'Value' ][0] )
      for data in retVal[ 'Value' ][1]:
        printer.pprint( data )
    except:
      self.showTraceback()

  def do_plotView( self, args ):
    """
    Gets a summary
      Usage : getSummary <Summary name> <startdate YYYYMMDDHHMM> <enddate YYYYMMDDHHMM> <destLocation> (<field name> <field value>)*
    """
    try:
      argList = List.fromChar( args, " " )
      if len( argList ) < 4:
        gLogger.error( "Missing arguments!" )
        return
      startDT = self.__getDatetimeFromArg( argList[1] )
      if not startDT:
        gLogger.error( "Start time has invalid format" )
      endDT = self.__getDatetimeFromArg( argList[2] )
      if not endDT:
        gLogger.error( "End time has invalid format" )
      gLogger.info( "Start time is %s" % startDT )
      gLogger.info( "End time is %s" % endDT )
      sumArgs = {}
      for iP in range( 4, len( argList ), 2 ):
        key = argList[ iP ]
        if key in sumArgs:
          sumArgs[ key ].append( argList[ iP + 1 ] )
        else:
          sumArgs[ key ] = [ argList[ iP + 1 ] ]
      retVal = self.repClient.plotView( argList[ 0 ], startDT, endDT, sumArgs )
      if not retVal[ 'OK' ]:
        gLogger.error( "Error: %s" % retVal[ 'Message' ] )
        return
      print retVal
    except:
      self.showTraceback()

if __name__=="__main__":
    reli = ReportCLI()
    reli.start()