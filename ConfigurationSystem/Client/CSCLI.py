########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ConfigurationSystem/Client/CSCLI.py,v 1.1 2008/04/16 19:09:02 rgracian Exp $
# File :   CSCLI.py
# Author : Adria Casajus
########################################################################
__RCSID__   = "$Id: CSCLI.py,v 1.1 2008/04/16 19:09:02 rgracian Exp $"
__VERSION__ = "$Revision: 1.1 $"

import cmd
import sys
import signal
from DIRAC.Core.Utilities.ColorCLI import colorize
from DIRAC.ConfigurationSystem.private.Modificator import Modificator
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.Core.DISET.RPCClient import RPCClient

class CSCLI( cmd.Cmd ):

  def __init__( self ):
    cmd.Cmd.__init__( self )
    self.do_connect()
    if self.connected:
      self.modificator =  Modificator ( self.rpcClient )
    else:
      self.modificator = Modificator()
    self.identSpace = 20
    self.backupFilename = "dataChanges"
    self.initSignals()
    self.modifiedData = False
    #User friendly hack
    self.do_exit = self.do_quit

  def start( self ):
    if self.connected:
      self.modificator.loadFromRemote()
      retVal = self.modificator.getCredentials()
      if not retVal[ 'OK' ]:
        print "There was an error gathering your credentials"
        print retVal[ 'Message' ]
        self.setStatus( False, False )
    try:
      self.cmdloop()
    except KeyboardInterrupt, v:
      gLogger.warn( "Received a keyboard interrupt." )
      self.do_quit( "" )

  def initSignals( self ):
    """
    Registers signal handlers
    """
    for sigNum in ( signal.SIGINT, signal.SIGQUIT, signal.SIGKILL, signal.SIGTERM ):
      try:
        signal.signal( sigNum, self.do_quit )
      except:
        pass

  def showTraceback( self ):
    import traceback
    type, value = sys.exc_info()[:2]
    print "________________________\n"
    print "Exception", type, ":", value
    traceback.print_tb( sys.exc_info()[2] )
    print "________________________\n"

  def connected( self, connected, writeEnabled ):
    self.connected = connected
    self.modifiedData = False
    self.writeEnabled = writeEnabled
    if connected:
      if writeEnabled:
        self.prompt = "(%s)-%s> " % ( self.masterURL, colorize( "Connected", "green" ) )
      else:
        self.prompt = "(%s)-%s> " % ( self.masterURL, colorize( "Connected (RO)", "yellow" ) )
    else:
      self.prompt = "(%s)-%s> " % ( self.masterURL, colorize( "Disconnected", "red" ) )


  def printPair( self, key, value, separator=":" ):
    valueList = value.split( "\n" )
    print "%s%s%s %s" % ( key, " " * ( self.identSpace - len( key ) ), separator, valueList[0].strip() )
    for valueLine in valueList[ 1:-1 ]:
      print "%s  %s" % ( " " * self.identSpace, valueLine.strip() )

  def printComment( self, comment ):
    commentList = comment.split( "\n" )
    for commentLine in commentList[ :-1 ]:
      print "# %s" % commentLine.strip()

  def do_quit( self, *args ):
    """
    Exits the application without sending changes to server
        Usage: quit
    """
    if self.modifiedData:
      print "Changes are about to be written to file for later use."
      self.do_writeToFile( self.backupFilename )
      print "Changes written to %s.cfg" % self.backupFilename
    sys.exit( 0 )

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

  def retrieveData( self ):
    if not self.connected:
      return False
    response = self.rpcClient.dumpCompressed()
    if response[ 'Status' ] == 'OK':
      self.cDataHolder.loadFromCompressedSource( response[ 'Value' ] )
      gLogger.info( "Data retrieved from server." )
      return True
    else:
      gLogger.error( "Can't retrieve updated data from server." )
      return False

  def setStatus( self, connected = True ):
    if not connected:
      self.connected( False, False )
    else:
      retVal = self.rpcClient.writeEnabled()
      if retVal[ 'OK' ]:
        if retVal[ 'Value' ] == True:
          self.connected( True, True )
        else:
          self.connected( True, False )
      else:
        print "Server returned an error: %s" % retVal[ 'Message' ]
        self.connected( True, False )

  def tryConnection( self ):
    print "Trying connection to %s" % self.masterURL
    self.rpcClient = RPCClient( self.masterURL )
    self.setStatus()

  def do_connect( self, args = False ):
    """
    Connects to localhost server in specified port.
        Usage: connect <url>
    """
    if not args:
      self.masterURL = gConfigurationData.getMasterServer()
      if self.masterURL != "unknown" and self.masterURL:
        self.tryConnection()
      else:
        self.masterURL = "unset"
        self.setStatus( False )
      return
    splitted = args.split()
    if len( splitted ) == 0:
      print "Must specify witch url to connect"
      return
    try:
      self.masterURL = splitted[0].strip()
      self.tryConnection()
    except Exception, v:
      gLogger.error( "Couldn't connect to %s (%s)" % ( self.masterURL, str(v) ) )
      self.connected( False, False )

  def do_sections( self, args ):
    """
    Shows all sections with their comments.
    If no section is specified, root is taken.
        Usage: sections <section>
    """
    try:
      argList = args.split()
      if argList:
        baseSection = argList[0].strip()
      else:
        baseSection = "/"
      if not self.modificator.existsSection( baseSection ):
        print "Section %s does not exist" % baseSection
        return
      sectionList = self.modificator.getSections( baseSection )
      if not sectionList:
        print "Section %s is empty" % baseSection
        return
      for section in sectionList:
        section = "%s/%s" % ( baseSection, section )
        self.printPair( section, self.modificator.getComment( section ) , "#" )
    except:
      self.showTraceback()

  def do_options( self, args ):
    """
    Shows all options and values of a specified section
        Usage: options <section>
    """
    try:
      argList = args.split()
      if argList:
        section = argList[0].strip()
      else:
        print "Which section?"
        return
      if not self.modificator.existsSection( section ):
        print "Section %s does not exist" % section
        return
      optionsList = self.modificator.getOptions( section )
      if not optionsList:
        print "Section %s has no options" % section
        return
      for option in optionsList:
        self.printComment( self.modificator.getComment( "%s/%s" % ( section, option ) ) )
        self.printPair( option, self.modificator.getValue( "%s/%s" % ( section, option ) ), "=" )
    except:
      self.showTraceback()

  def do_get( self, args ):
    """
    Shows value and comment for specified option in section
        Usage: get <path to option>
    """
    try:
      argList = args.split()
      if argList:
        optionPath = argList[0].strip()
      else:
        print "Which option?"
        return
      if self.modificator.existsOption( optionPath ):
        option = optionPath.split( "/" )[-1]
        self.printComment( self.modificator.getComment( optionPath ) )
        self.printPair( option, self.modificator.getValue( optionPath ), "=" )
      else:
        print "Option %s does not exist" % optionPath
    except:
      self.showTraceback()

  def do_writeToServer( self, args ):
    """
    Sends changes to server.
        Usage: writeToServer
    """
    if not self.connected:
      print "You are not connected!"
      return
    try:
      if not self.writeEnabled:
        print "This server can't receive data modifications"
        return
      assured = ""
      if not self.modifiedData:
        while True:
          choice = raw_input( "Data has not been modified, do you still want to upload changes? yes/no [no]: ")
          choice = choice.lower()
          if choice in ( "yes", "y" ):
            break
          else:
            print "Commit aborted"
            return

      choice = raw_input( "Do you really want to send changes to server? yes/no [no]: " )
      choice = choice.lower()
      if choice in ( "yes", "y" ):
        print "Uploading changes to %s (It may take some seconds)..." % self.masterURL
        response = self.modificator.commit()
        if response[ 'OK' ]:
          self.modifiedData = False
          print "Data sent to server."
          self.modificator.loadFromRemote()
        else:
          print "Error sending data, server said: %s" % response['Message']
        return
      else:
          print "Commit aborted"
    except Exception, v:
      self.showTraceback()
      print "Could not upload changes. ", v

  def do_set( self, args ):
    """
    Sets option's value
        Usage: set <optionPath> <value>...
        From second argument until the last one is considered option's value
        NOTE: If specified section does not exist it is created.
    """
    try:
      argsList = args.split()
      if len( argsList ) < 2:
        print "Must specify option and value to use"
        return
      optionPath = argsList[0].strip()
      value = " ".join( argsList[1:] ).strip()
      self.modificator.setOptionValue( optionPath, value )
      self.modifiedData = True
    except Exception, v:
      print "Cannot insert value: ", v

  def do_removeOption( self, args ):
    """
    Removes an option.
        Usage: removeOption <option>
        There can be empty sections.
    """
    try:
      argsList = args.split()
      if len( argsList ) < 1:
        print "Must specify option to delete"
        return
      optionPath = argsList[0].strip()
      choice = raw_input( "Are you sure you want to delete %s? yes/no [no]: " % optionPath )
      choice = choice.lower()
      if choice in ( "yes", "y", "true" ):
        if self.modificator.removeOption( optionPath ):
          self.modifiedData = True
        else:
          print "Can't be deleted"
      else:
        print "Aborting removal."
    except Exception, v:
      print "Error removing option, %s" % v

  def do_removeSection( self, args ):
    """
    Removes a section.
        Usage: removeSection <section>
    """
    try:
      argsList = args.split()
      if len( argsList ) < 1:
        print "Must specify section to delete"
        return
      section = argsList[0].strip()
      choice = raw_input( "Are you sure you want to delete %s? yes/no [no]: " % section )
      choice = choice.lower()
      if choice in ( "yes", "y", "true" ):
        if self.modificator.removeSection( section ):
          self.modifiedData = True
        else:
          print "Can't be deleted"
      else:
        print "Aborting removal."
    except Exception, v:
      print "Error removing section, %s" % v

  def do_setComment( self, args ):
    """
    Sets option or section's comment. Requested entry MUST exist.
        Usage: set <option/section> <comment>...
        From third argument until the last one is considered option's comment.
    """
    try:
      argsList = args.split()
      if len( argsList ) < 2:
        print "Must specify option and value to use"
        return
      entryPath = argsList[0].strip()
      value = " ".join( argsList[1:] ).strip()
      self.modificator.setComment( entryPath, value )
      self.modifiedData = True
    except Exception, v:
      print "Cannot insert comment: ", v

  def appendExtensionIfMissing( self, filename ):
    dotPosition = filename.rfind( "." )
    if dotPosition > -1:
      filename = filename[ :dotPosition ]
    return "%s.cfg" % filename

  def do_writeToFile( self, args ):
    """
    Writes modification to file for later use.
        Usage: writeToFile <filename>.cfg
        Note that if a file extension is specified, it is replaced by .cfg suffix
        If not it is added automatically
    """
    try:
      if len( args ) == 0:
        print "Filename to write must be specified!"
        return
      filename = args.split()[0].strip()
      filename = self.appendExtensionIfMissing( filename )
      self.modificator.dumpToFile( filename )
    except Exception, v:
      print "Couldn't write to file %s: %s" % ( filename, str( v ) )

  def do_readFromFile( self, args ):
    """
    Reads data from filename to be used. Actual data will be replaced!
        Usage: readFromFile <filename>.cfg
        Note that if a file extension is specified, it is replaced by .cfg suffix
        If not it is added automatically
    """
    try:
      if len( args ) == 0:
        print "Filename to read must be specified!"
        return
      filename = args.split()[0].strip()
      filename = self.appendExtensionIfMissing( filename )
      self.modificator.loadFromFile( filename )
      self.modifiedData = True
    except Exception, v:
      print "Couldn't read from file %s: %s" % ( filename, str( v ) )

  def do_mergeFromFile( self, args ):
    """
    Reads data from filename and merges it with current data.
    Data read from file has more precedence that current one.
        Usage: mergeFromFile <filename>.cfg
        Note that if a file extension is specified, it is replaced by .cfg suffix.
        If not it is added automatically
    """
    try:
      if len( args ) == 0:
        print "Filename to read must be specified!"
        return
      filename = args.split()[0].strip()
      filename = self.appendExtensionIfMissing( filename )
      self.modificator.mergeFromFile( filename )
      self.modifiedData = True
    except Exception, v:
      self.showTraceback()
      print "Couldn't read from file %s: %s" % ( filename, str( v ) )

  def do_showData( self, args ):
    """
    Shows the current modified configuration
    Usage: showData
    """
    print self.modificator

  def do_showHistory( self, args ):
    """
    Shows the last commit history
    Usage: showHistory <update limit>
    """
    try:
      argsList = args.split()
      limit = 100
      if len( argsList ) > 0:
        limit = int( argsList[0] )
      history = self.modificator.getHistory( limit )
      print "%s recent commits:" % limit
      for entry in history:
        self.printPair( entry[0], entry[1], "@" )
    except Exception, v:
      self.showTraceback()

  def do_showDiffWithServer( self, args ):
    """
    Shows diff with lastest version in server
    Usage: showDiffWithServer
    """
    try:
      diffData = self.modificator.showCurrentDiff()
      print "Diff with latest from server ( + local - remote )"
      for line in diffData:
        if line[0] in ( '-' ):
          print colorize( line, "red" )
        elif line[0] in ( '+' ):
          print colorize( line, "green" )
        elif line[0] in ( '?' ):
          print colorize( line, "yellow" ),
        else:
          print line
    except Exception, v:
      self.showTraceback()

  def do_showDiffBetweenVersions( self, args ):
    """
    Shows diff between two versions
    Usage: showDiffBetweenVersions <version 1 with spaces> <version 2 with spaces>
    """
    try:
      argsList = args.split()
      if len( argsList ) < 4:
        print "What are the two versions to compare?"
        return
      v1 = " ".join ( argsList[0:2] )
      v2 = " ".join ( argsList[2:4] )
      print "Comparing '%s' with '%s' " % ( v1, v2 )
      diffData = self.modificator.getVersionDiff( v1, v2 )
      print "Diff with latest from server ( + %s - %s )" % ( v2, v1 )
      for line in diffData:
        if line[0] in ( '-' ):
          print colorize( line, "red" )
        elif line[0] in ( '+' ):
          print colorize( line, "green" )
        elif line[0] in ( '?' ):
          print colorize( line, "yellow" ),
        else:
          print line
    except Exception, v:
      self.showTraceback()

  def do_rollbackToVersion( self, args ):
    """
    rolls back to user selected version of the configuration
    Usage: rollbackToVersion <version with spaces>>
    """
    try:
      argsList = args.split()
      if len( argsList ) < 2:
        print "What version to rollback?"
        return
      version = " ".join ( argsList[0:2] )
      choice = raw_input( "Do you really want to rollback to version %s? yes/no [no]: " % version)
      choice = choice.lower()
      if choice in ( "yes", "y" ):
        response = self.modificator.rollbackToVersion( version )
        if response[ 'OK' ]:
          self.modifiedData = False
          print "Rolled back."
          self.modificator.loadFromRemote()
        else:
          print "Error sending data, server said: %s" % response['Message']
    except Exception, v:
      self.showTraceback()

  def do_mergeWithServer( self, args ):
    """
    Shows diff with lastest version in server
    Usage: diffWithServer
    """
    try:
      choice = raw_input( "Do you want to merge with server configuration? yes/no [no]: ")
      choice = choice.lower()
      if choice in ( "yes", "y" ):
        retVal = self.modificator.mergeWithServer()
        if retVal[ 'OK' ]:
          print "Merged"
        else:
          print "There was an error: ", retVal[ 'Message' ]
      else:
        print "Merge aborted"
    except Exception, v:
      self.showTraceback()

