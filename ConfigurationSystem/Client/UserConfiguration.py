# $Header$
__RCSID__ = "$Id$"

import sys
import getopt
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.ConfigurationSystem.Client.ConfigurationData import g_oConfigurationData
from DIRAC.ConfigurationSystem.private.Refresher import g_oRefresher
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

class UserConfiguration:
  
  def __init__( self, sDefaultSection = "" ):
    self.sCurrentSection = sDefaultSection
    self.lMandatoryEntries = []
    self.lOptionalEntries = []
    self.lCmdOpts = []
    self.__registerBasicOptions()
    self.bParsed = False
    
  def __getAbsolutePath( self, sOption ):
    if sOption[0] == "/":
      return sOption
    else:
      return "%s/%s" % ( self.sCurrentSection, sOption )
    
  def addMandatoryEntry( self, sOption ):
    sOptionPath = self.__getAbsolutePath( sOption )
    self.lMandatoryEntries.append( sOptionPath )
  
  def addOptionalEntry( self, sOption, sValue ):
    sOptionPath = self.__getAbsolutePath( sOption )
    self.lOptionalEntries.append( ( sOptionPath, sValue ) )
    
  def setOptionValue( self, sOption, sValue ):
    g_oConfigurationData.setOptionInCFG( sOption, sValue )
    
  def __registerBasicOptions( self ):
    self.registerCmdOpt( "o", "option", "Option=value to add", self.__setOptionByCmd  )
    self.registerCmdOpt( "s", "section", "Section to add an option", self.__setSectionByCmd )
    self.registerCmdOpt( "h", "help", "Shows this help", self.__showUsage )
    
  def registerCmdOpt( self, sShort, sLong, sHelp, oFunction ):
    self.lCmdOpts.append( ( sShort, sLong, sHelp, oFunction ) )
    
  def getPositionalArguments( self ):
    if not self.bParsed:
      self.__parseCommandLine()
    return self.lPositionalCmdArgs
    
  def loadUserData(self):
    try:
      for stOption in self.lOptionalEntries:
        g_oConfigurationData.setOptionInCFG( stOption[0], stOption[1] )
      
      self.__addUserDataToConfiguration()
      
      bMissingMandatory = False
      for sOption in self.lMandatoryEntries:
        if not g_oConfigurationData.extractOptionFromCFG( sOption ):
          gLogger.fatal( "Missing mandatory option in the configuration", sOption )
          bMissingMandatory = True
      if bMissingMandatory:
        return S_ERROR()
    except Exception, e:
      gLogger.error( "Error while loading user specified configuration data", str( e ) )
      return S_ERROR()
    return S_OK()
      
    
  def __parseCommandLine( self ):
    
    sShortParams = ""
    lLongParams = []
    for stParam in self.lCmdOpts:
      if sShortParams.find( stParam[0] ) < 0:
        sShortParams += "%s:" % stParam[0]
      else:
        gLog.warn( "Short option -%s has been already defined" % stParam[0] )
      if not stParam[1] in lLongParams:
        lLongParams.append( "%s=" % stParam[1] )
      else:
        gLog.warn( "Long option --%s has been already defined" % stParam[1] )
            
    try:
      opts, args = getopt.gnu_getopt( sys.argv[1:], sShortParams, lLongParams )
    except getopt.GetoptError, v:
      # print help information and exit:
      gLog.error( "Error when parsing command line arguments: %s" % str( v ) )
      self.__showUsage()
      sys.exit(2)

    self.lAdditionalConfigurationFiles = [ sArg for sArg in args if sArg[-4:] == ".cfg" ]
    self.lPositionalCmdArgs = [ sArg for sArg in args if not sArg[-4:] == ".cfg" ]
    self.lParsedOptions = opts
    self.bParsed = True
    
  def __addUserDataToConfiguration( self ):
    if not self.bParsed:
      self.__parseCommandLine()
    for sFile in self.lAdditionalConfigurationFiles:
      g_oConfigurationData.loadFile( sFile )
    
    bDownloadData = True
    if self.sCurrentSection == "%s/Configuration" % g_oConfigurationData.getServicesPath():
      if g_oConfigurationData.isMaster():
        bDownloadData = False
    if bDownloadData:
      dRetVal = g_oRefresher.forceRefreshConfiguration()
      if not dRetVal['OK' ]:
        return dRetVal
    
    lUnknownParams = []
    for sParam, sValue in self.lParsedOptions:
      sParam = sParam.replace( "-", "" )
      for stDefinedOption in self.lCmdOpts:
        if sParam == stDefinedOption[0] or sParam == stDefinedOption[1]:
          stDefinedOption[3]( sValue )
    return S_OK()
  
  def setServerSection( self, sServer ):
    self.sCurrentSection = "%s/%s" % ( g_oConfigurationData.getServicesPath(), sServer )
    return self.sCurrentSection
          
  def __setSectionByCmd( self, sValue ):
    if sValue[0] != "/":
      raise Exception( "%s is not a valid section. It should start with '/'" % sValue)
    self.sCurrentSection = sValue
      
  def __setOptionByCmd( self, sValue ):
    lValue = [ sD.strip() for sD in sValue.split("=") if len( sD ) > 0]
    if len( lValue ) <  2:
      gLogger.error( "\t-o expects a option=value argument.\n\tFor example %s -o Port=1234" % sys.argv[0] )
      sys.exit( 1 )
    self.setOptionValue( "%s/%s" % ( self.sCurrentSection, lValue[0] ), lValue[1] )
              
  def __showUsage( self ):
    from DIRAC.Utility.Logger import gLog
    gLog.info( "Usage:" )
    gLog.info( "  %s (<options>|<cfgFile>)*" % sys.argv[0] )
    gLog.info( "Options:" )
    for stOption in self.lCmdOptions:
      gLog.info( "  -%s  --%s  :  %s" % stOption[:3] )

  
