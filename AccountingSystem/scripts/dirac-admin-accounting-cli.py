#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-admin-accounting-cli
# Author : Adria Casajus
########################################################################
"""
  Command line administrative interface to DIRAC Accounting DataStore Service
"""
__RCSID__ = "$Id$"

import cmd
import sys
from DIRAC import gLogger
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities import ExitCallback, ColorCLI
from DIRAC.Core.DISET.RPCClient import RPCClient

Script.localCfg.addDefaultEntry( "LogLevel", "info" )
Script.setUsageMessage('\n'.join( [ __doc__.split( '\n' )[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ...' % Script.scriptName, ] )   )
Script.parseCommandLine()

class AccountingCLI( cmd.Cmd ):

  def __init__( self ):
    cmd.Cmd.__init__( self )
    self.do_connect( None )
    self.identSpace = 20
    ExitCallback.registerExitCallback( self.do_quit )
    #User friendly hack
    self.do_exit = self.do_quit
    self.do_EOF = self.do_quit

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
    acClient = RPCClient( "Accounting/DataStore" )
    retVal = acClient.ping()
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

  def do_registerType( self, args ):
    """
    Registers a new accounting type
      Usage : registerType <typeName>
      <DIRACRoot>/DIRAC/AccountingSystem/Client/Types/<typeName>
       should exist and inherit the base type
    """
    try:
      argList = args.split()
      if argList:
        typeName = argList[0].strip()
      else:
        gLogger.error( "No type name specified" )
        return
      #Try to import the type
      try:
        typeModule = __import__( "DIRAC.AccountingSystem.Client.Types.%s" % typeName,
                                  globals(),
                                  locals(), typeName )
        typeClass  = getattr( typeModule, typeName )
      except Exception, e:
        gLogger.error( "Can't load type %s: %s" % ( typeName, str(e) ) )
        return
      gLogger.info( "Loaded type %s"  % typeClass.__name__ )
      typeDef = typeClass().getDefinition()
      acClient = RPCClient( "Accounting/DataStore" )
      retVal = acClient.registerType( *typeDef )
      if retVal[ 'OK' ]:
        gLogger.info( "Type registered successfully" )
      else:
        gLogger.error( "Error: %s" % retVal[ 'Message' ] )
    except:
      self.showTraceback()

  def do_resetBucketLength( self, args ):
    """
    Set the bucket Length. Will trigger a recalculation of buckets. Can take a while.
      Usage : resetBucketLength <typeName>
      <DIRACRoot>/DIRAC/AccountingSystem/Client/Types/<typeName>
       should exist and inherit the base type
    """
    try:
      argList = args.split()
      if argList:
        typeName = argList[0].strip()
      else:
        gLogger.error( "No type name specified" )
        return
      #Try to import the type
      try:
        typeModule = __import__( "DIRAC.AccountingSystem.Client.Types.%s" % typeName,
                                  globals(),
                                  locals(), typeName )
        typeClass  = getattr( typeModule, typeName )
      except Exception, e:
        gLogger.error( "Can't load type %s: %s" % ( typeName, str(e) ) )
        return
      gLogger.info( "Loaded type %s"  % typeClass.__name__ )
      typeDef = typeClass().getDefinition()
      acClient = RPCClient( "Accounting/DataStore" )
      retVal = acClient.setBucketsLength( typeDef[0], typeDef[3] )
      if retVal[ 'OK' ]:
        gLogger.info( "Type registered successfully" )
      else:
        gLogger.error( "Error: %s" % retVal[ 'Message' ] )
    except:
      self.showTraceback()

  def do_regenerateBuckets( self, args ):
    """
    Regenerate buckets for type. Can take a while.
      Usage : regenerateBuckets <typeName>
      <DIRACRoot>/DIRAC/AccountingSystem/Client/Types/<typeName>
       should exist and inherit the base type
    """
    try:
      argList = args.split()
      if argList:
        typeName = argList[0].strip()
      else:
        gLogger.error( "No type name specified" )
        return
      #Try to import the type
      try:
        typeModule = __import__( "DIRAC.AccountingSystem.Client.Types.%s" % typeName,
                                  globals(),
                                  locals(), typeName )
        typeClass  = getattr( typeModule, typeName )
      except Exception, e:
        gLogger.error( "Can't load type %s: %s" % ( typeName, str(e) ) )
        return
      gLogger.info( "Loaded type %s"  % typeClass.__name__ )
      typeDef = typeClass().getDefinition()
      acClient = RPCClient( "Accounting/DataStore" )
      retVal = acClient.regenerateBuckets( typeDef[0] )
      if retVal[ 'OK' ]:
        gLogger.info( "Buckets recalculated!" )
      else:
        gLogger.error( "Error: %s" % retVal[ 'Message' ] )
    except:
      self.showTraceback()

  def do_showRegisteredTypes( self, args ):
    """
    Get a list of registered types
      Usage : showRegisteredTypes
    """
    try:
      acClient = RPCClient( "Accounting/DataStore" )
      retVal = acClient.getRegisteredTypes()
      if not retVal[ 'OK' ]:
        gLogger.error( "Error: %s" % retVal[ 'Message' ] )
        return
      for typeList in retVal[ 'Value' ]:
        print typeList[0]
        print " Key fields:\n  %s" %  "\n  ".join( typeList[1] )
        print " Value fields:\n  %s" %  "\n  ".join( typeList[2] )
    except:
      self.showTraceback()

  def do_showRegisteredKeyFields( self, args ):
    """
    Get a list of registered key fields in types
      Usage : showRegisteredKeyFields
    """
    try:
      acClient = RPCClient( "Accounting/DataStore" )
      retVal = acClient.getRegisteredKeyFields()
      if not retVal[ 'OK' ]:
        gLogger.error( "Error: %s" % retVal[ 'Message' ] )
        return
      for keyInfo in retVal[ 'Value' ]:
        print "%s is used by %s types" % keyInfo
    except:
      self.showTraceback()

  def do_deleteType( self, args ):
    """
    Delete a registered accounting type.
      Usage : deleteType <typeName>
      WARN! It will delete all data associated to that type! VERY DANGEROUS!
      If you screw it, you'll discover a new dimension of pain and doom! :)
    """
    try:
      argList = args.split()
      if argList:
        typeName = argList[0].strip()
      else:
        gLogger.error( "No type name specified" )
        return
      while True:
        choice = raw_input( "Are you completely sure you want to delete type %s and all it's data? yes/no [no]: " % typeName)
        choice = choice.lower()
        if choice in ( "yes", "y" ):
          break
        else:
          print "Delete aborted"
          return
      acClient = RPCClient( "Accounting/DataStore" )
      retVal = acClient.deleteType( typeName )
      if not retVal[ 'OK' ]:
        gLogger.error( "Error: %s" % retVal[ 'Message' ] )
        return
      print "Hope you meant it, because it's done"
    except:
      self.showTraceback()

  def do_compactBuckets( self, args ):
    """
    Compact buckets table
      Usage : compactBuckets
    """
    try:
      acClient = RPCClient( "Accounting/DataStore" )
      retVal = acClient.compactDB()
      if not retVal[ 'OK' ]:
        gLogger.error( "Error: %s" % retVal[ 'Message' ] )
        return
      gLogger.info( "Done" )
    except:
      self.showTraceback()

if __name__=="__main__":
    acli = AccountingCLI()
    acli.start()