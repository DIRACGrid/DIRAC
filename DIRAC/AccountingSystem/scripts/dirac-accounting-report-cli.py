#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-accounting-report-cli
# Author : Adria Casajus
########################################################################
"""
  Command line interface to DIRAC Accounting ReportGenerator Service 
"""
__RCSID__ = "$Id$"

# FIXME: As it is, this one does not do much...

import cmd
import sys
import signal
import datetime
import pprint
from DIRAC import gLogger
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities import ExitCallback, ColorCLI, List
from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient

Script.localCfg.addDefaultEntry( "LogLevel", "info" )
Script.setUsageMessage('\n'.join( [ __doc__.split( '\n' )[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ...' % Script.scriptName, ] )   )
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
    """
    Start the command loop
    """
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
    retVal = ReportsClient().pingService()
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

  def __getDatetimeFromArg( self, dtString ):
    if len( dtString ) != 12:
      return False
    dt = datetime.datetime( year  = int( dtString[0:4] ),
                            month = int( dtString[4:6] ),
                            day   = int( dtString[6:8] ) )
    dt += datetime.timedelta( hours = int( dtString[ 8:10 ] ),
                              minutes = int( dtString[ 10:12 ] ) )
    return dt

if __name__=="__main__":
    reli = ReportCLI()
    reli.start()
