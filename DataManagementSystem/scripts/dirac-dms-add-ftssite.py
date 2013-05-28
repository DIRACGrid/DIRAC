#!/bin/env python
""" add fts site to FTSDB """

__RCSID__ = "$Id: $"

from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'Usage:',
                                     ' %s [option|cfgfile] siteName ftsServiceURL' % Script.scriptName,
                                     'Arguments:',
                                     ' siteName: LCG site name',
                                     ' ftsService: FTS service URL' ] ) )

if __name__ == "__main__":

  from DIRAC.Core.Base.Script import parseCommandLine
  parseCommandLine()

  import DIRAC

  args = Script.getPositionalArgs()

  ftsSite = ftsServer = ""
  if not len( args ) == 2:
    Script.showHelp()
    DIRAC.exit( 0 )
  else:
    ftsSite, ftsServer = args

  import DIRAC
  from DIRAC import gLogger, gConfig
  from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getSites
  from DIRAC.DataManagementSystem.Client.FTSClient import FTSClient

  ftsClient = FTSClient()

  ftsSites = ftsClient.getFTSSiteList()
  if not ftsSites["OK"]:
    gLogger.error( "unable to read FTSSites: %s" % ftsSites["Message"] )
    DIRAC.exit( -1 )
  ftsSites = ftsSites["Value"]

  for site in ftsSites:
    if site.Name == ftsSite:
      gLogger.error( "FTSSite '%s' is present in FTSDB!!!" % ftsSite )
      DIRAC.exit( -1 )

  getSites = getSites()
  if not getSites["OK"]:
    gLogger.error( "unable to read sites defined in CS!!!" )
    DIRAC.exit( -1 )
  getSites = getSites["Value"]

  if "LCG." + ftsSite not in getSites:
    gLogger.error( "Site '%s' is not defined in CS Resources/Sites section !!!" % ( "LCG.%s" % ftsSite ) )
    DIRAC.exit( -1 )

  SEs = gConfig.getOption( "/Resources/Sites/LCG/LCG.%s/SE" % ftsSite , [] )
  if not SEs["OK"]:
    gLogger.error( "unable to read SEs attached to site LCG.%s: %s" % ftsSite )
    DIRAC.exit( -1 )
  SEs = SEs["Value"]

  newSite = FTSSite()
  newSite.Name = ftsSite
  newSite.FTSServer = ftsServer

  putSite = ftsClient.putFTSSite( newSite )
  if not putSite["OK"]:
    gLogger.error( "unable to put new FTSSite: %s" % putSite["Message"] )
    DIRAC.exit( -1 )

  gLogger.always( "FTSSite '%s' using FTS server %s and serving %s SEs created" % ( newSite.Name,
                                                                                    newSite.FTSServer,
                                                                                    SEs ) )
  DIRAC.exit( 0 )

