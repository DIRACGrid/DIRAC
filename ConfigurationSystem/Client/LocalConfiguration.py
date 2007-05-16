# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ConfigurationSystem/Client/LocalConfiguration.py,v 1.6 2007/05/16 11:31:35 acasajus Exp $
__RCSID__ = "$Id: LocalConfiguration.py,v 1.6 2007/05/16 11:31:35 acasajus Exp $"

import sys
import os
import getopt
import types

from DIRAC import gLogger
from DIRAC import S_OK, S_ERROR

from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.private.Refresher import gRefresher
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceSection, getAgentSection

class LocalConfiguration:

  def __init__( self, defaultSectionPath = "" ):
    self.currentSectionPath = defaultSectionPath
    self.mandatoryEntryList = []
    self.optionalEntryList = []
    self.commandOptionList = []
    self.__registerBasicOptions()
    self.isParsed = False
    self.componentName = "Unknown"
    self.loggingSection = "/DIRAC"

  def __getAbsolutePath( self, optionPath ):
    if optionPath[0] == "/":
      return optionPath
    else:
      return "%s/%s" % ( self.currentSectionPath, optionPath )

  def addMandatoryEntry( self, optionPath ):
    self.mandatoryEntryList.append( self.__getAbsolutePath( optionPath ) )

  def addOptionalEntry( self, optionPath, value ):
    self.optionalEntryList.append( ( self.__getAbsolutePath( optionPath ),
                                     str( value ) ) )

  def setOptionValue( self, optionPath, value ):
    gConfigurationData.setOptionInCFG( self.__getAbsolutePath( optionPath ),
                                       str( value ) )

  def __registerBasicOptions( self ):
    self.registerCmdOpt( "o:", "option=", "Option=value to add",
                         self.__setOptionByCmd  )
    self.registerCmdOpt( "s:", "section=", "Set base section for relative parsed options",
                         self.__setSectionByCmd )
    self.registerCmdOpt( "c:", "cert=", "Use server certificate to connect to Core Services",
                         self.__setUseCertByCmd )
    self.registerCmdOpt( "h", "help", "Shows this help",
                         self.__showHelp )

  def registerCmdOpt( self, shortOption, longOption, helpString, function ):
    #TODO: Can't overwrite switches (FATAL)
    self.commandOptionList.append( ( shortOption, longOption, helpString, function ) )

  def getPositionalArguments( self ):
    if not self.isParsed:
      self.__parseCommandLine()
    return self.commandArgList

  def loadUserData(self):
    if not self.isParsed:
      self.__parseCommandLine()
    try:
      for optionTuple in self.optionalEntryList:
        gConfigurationData.setOptionInCFG( optionTuple[0], optionTuple[1] )

      retVal = self.__addUserDataToConfiguration()
      gLogger.initialize( self.componentName, self.loggingSection )
      if not retVal[ 'OK' ]:
        return retVal

      retVal = self.__getRemoteConfiguration()
      if not retVal[ 'OK' ]:
        return retVal

      isMandatoryMissing = False
      for optionPath in self.mandatoryEntryList:
        if not gConfigurationData.extractOptionFromCFG( optionPath ):
          gLogger.fatal( "Missing mandatory option in the configuration", optionPath )
          isMandatoryMissing = True
      if isMandatoryMissing:
        return S_ERROR()
    except Exception, e:
      gLogger.initialize( "UNKNOWN", "/DIRAC" )
      gLogger.exception()
      return S_ERROR( str( e ) )
    return S_OK()


  def __parseCommandLine( self ):
    gLogger.debug( "Parsing command line" )
    shortOption = ""
    longOptionList = []
    for optionTuple in self.commandOptionList:
      if shortOption.find( optionTuple[0] ) < 0:
        shortOption += "%s" % optionTuple[0]
      else:
        gLog.warn( "Short option -%s has been already defined" % optionTuple[0] )
      if not optionTuple[1] in longOptionList:
        longOptionList.append( "%s" % optionTuple[1] )
      else:
        gLog.warn( "Long option --%s has been already defined" % optionTuple[1] )

    try:
      opts, args = getopt.gnu_getopt( sys.argv[1:], shortOption, longOptionList )
    except getopt.GetoptError, v:
      # print help information and exit:
      gLogger.initialize( "UNKNOWN", "/" )
      gLogger.fatal( "Error when parsing command line arguments: %s" % str( v ) )
      self.__showHelp()
      sys.exit(2)

    self.AdditionalCfgFileList = [ arg for arg in args if arg[-4:] == ".cfg" ]
    self.commandArgList = [ arg for arg in args if not arg[-4:] == ".cfg" ]
    self.parsedOptionList = opts
    self.isParsed = True

  def __addUserDataToConfiguration( self ):
    if not self.isParsed:
      self.__parseCommandLine()

    errorsList = []

    gConfigurationData.loadFile( os.path.expanduser( "~/.diracrc" ) )
    for fileName in self.AdditionalCfgFileList:
      retVal = gConfigurationData.loadFile( fileName )
      if not retVal[ 'OK' ]:
        errorsList.append( retVal[ 'Message' ] )

    for optionName, optionValue in self.parsedOptionList:
      optionName = optionName.replace( "-", "" )
      for definedOptionTuple in self.commandOptionList:
        if optionName == definedOptionTuple[0].replace( ":", "" ) or \
          optionName == definedOptionTuple[1].replace( "=", "" ):
          retVal = definedOptionTuple[3]( optionValue )
          if type( retVal ) != types.DictType:
            errorsList.append( "Callback for switch '%s' does not return S_OK or S_ERROR" % optionName )
          elif not retVal[ 'OK' ]:
            errorsList.append( retVal[ 'Message' ] )

    if len( errorsList ) > 0:
      return S_ERROR( "\n%s" % "\n".join( errorsList ) )
    return S_OK()

  def __getRemoteConfiguration( self ):
    needCSData = True
    if self.currentSectionPath == getServiceSection( "Configuration/Server" ):
      if gConfigurationData.isMaster():
        gLogger.debug( "CServer is Master!" )
        needCSData = False
      else:
        gLogger.debug( "CServer is slave" )
    if needCSData:
      retDict = gRefresher.forceRefreshConfiguration()
      if not retDict['OK']:
        gLogger.fatal( retDict[ 'Message' ] )
        return S_ERROR()

    return S_OK()

  def setConfigurationForServer( self, serviceName ):
    self.componentName = serviceName
    self.currentSectionPath = getServiceSection( serviceName )
    self.loggingSection = self.currentSectionPath
    return self.currentSectionPath

  def setConfigurationForAgent( self, agentName ):
    self.componentName = agentName
    self.currentSectionPath = getAgentSection( agentName )
    self.loggingSection = self.currentSectionPath
    return self.currentSectionPath

  def setConfigurationForScript( self, scriptName ):
    self.componentName = scriptName
    self.currentSectionPath = "/Scripts/%s" % scriptName
    self.loggingSection = self.currentSectionPath
    return self.currentSectionPath

  def __setSectionByCmd( self, value ):
    if value[0] != "/":
      return S_ERROR( "%s is not a valid section. It should start with '/'" % value )
    self.currentSectionPath = value
    return S_OK()

  def __setOptionByCmd( self, value ):
    valueList = [ sD.strip() for sD in value.split("=") if len( sD ) > 0]
    if len( valueList ) <  2:
      # FIXME: in the method above an exception is raised, check consitency
      return S_ERROR( "-o expects a option=value argument.\nFor example %s -o Port=1234" % sys.argv[0] )
    self.setOptionValue( valueList[0] , valueList[1] )
    return S_OK()

  def __setUseCertByCmd( self, value ):
    useCert = "no"
    if value.lower() in ( "y", "yes", "true" ):
      useCert = "yes"
    self.setOptionValue( "/DIRAC/Security/UseServerCertificate", useCert )
    return S_OK()

  def __showHelp( self, dummy ):
    gLogger.initialize( "UNKNOWN", "/DIRAC" )
    gLogger.info( "Usage:" )
    gLogger.info( "  %s (<options>|<cfgFile>)*" % sys.argv[0] )
    gLogger.info( "Options:" )
    for optionTuple in self.commandOptionList:
      gLogger.info( "  -%s  --%s  :  %s" % optionTuple[:3] )
    posix._exit( 0 )

