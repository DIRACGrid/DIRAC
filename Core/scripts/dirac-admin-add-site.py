#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-admin-add-site
# Author : Andrew C. Smith
########################################################################
"""
  Add a new DIRAC SiteName to DIRAC Configuration, including one or more CEs.
  If site is already in the CS with another name, error message will be produced.
  If site is already in the CS with the right name, only new CEs will be added.
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base                                      import Script
from DIRAC.ConfigurationSystem.Client.CSAPI               import CSAPI
from DIRAC.FrameworkSystem.Client.NotificationClient      import NotificationClient
from DIRAC.Core.Security.ProxyInfo                        import getProxyInfo
from DIRAC                                                import exit as DIRACExit, gConfig, gLogger
from DIRAC.Core.Utilities.List                            import intListToString
from DIRAC.ConfigurationSystem.Client.Helpers.Registry    import getPropertiesForGroup
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping          import getDIRACSiteName

if __name__ == "__main__":
  
  Script.setUsageMessage('\n'.join( [ __doc__.split( '\n' )[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ... DIRACSiteName GridSiteName CE [CE] ...' % Script.scriptName,
                                    'Arguments:',
                                    '  DIRACSiteName: Name of the site for DIRAC in the form GRID.LOCATION.COUNTRY (ie:LCG.CERN.ch)',
                                    '  GridSiteName: Name of the site in the Grid (ie: CERN-PROD)',
                                    '  CE: Name of the CE to be included in the site (ie: ce111.cern.ch)'] ) )
  Script.parseCommandLine( ignoreErrors = True )
  args = Script.getPositionalArgs()

  csAPI = CSAPI()
  
  if len( args ) < 3:
    Script.showHelp()
    DIRACExit( -1 )
  
  diracSiteName = args[0]
  gridSiteName = args[1]
  ces = args[2:]
  try:
    diracGridType, place, country = diracSiteName.split( '.' )
  except:
    gLogger.error( "The DIRACSiteName should be of the form GRID.LOCATION.COUNTRY for example LCG.CERN.ch" )
    DIRACExit( -1 )
  
  result = getDIRACSiteName( gridSiteName )
  newSite = True
  if result['OK']:
    if result['Value']: 
      if len( result['Value'] ) > 1:
        gLogger.notice( '%s GOC site name is associated with several DIRAC sites:' % gridSiteName )
        for i,dsite in enumerate( result['Value'] ):
          gLogger.notice( '%d: %s' % ( i, dsite ) )
        inp = raw_input( 'Enter your choice number: ' )  
        try:
          inp = int( inp )
        except:
          gLogger.error( 'You should enter an integer number' )
          DIRACExit( -1 )
        if inp >= 0 and inp < len( result['Value'] ):
          diracCSSite = result['Value'][inp]
        else:
          gLogger.error( 'Number out of range: %d' % inp ) 
          DIRACExit( -1 )
      else:
        diracCSSite = result['Value'][0]     
      if diracCSSite == diracSiteName:
        gLogger.notice( 'Site with GOC name %s is already defined as %s' % ( gridSiteName, diracSiteName ) )
        newSite = False
      else:
        gLogger.error( 'ERROR: Site with GOC name %s is already defined as %s' % ( gridSiteName, diracCSSite ) )  
        DIRACExit( -1 )
  
  cfgBase = "/Resources/Sites/%s/%s" % ( diracGridType, diracSiteName )
  change = False
  if newSite:
    gLogger.notice( "Adding new site to CS: %s" % diracSiteName )
    csAPI.setOption( "%s/Name" % cfgBase, gridSiteName )
    gLogger.notice( "Adding CEs: %s" % ','.join( ces ) )
    csAPI.setOption( "%s/CE" % cfgBase, ','.join( ces ) )
    change = True
  else:
    cesCS = set( gConfig.getValue( "%s/CE" % cfgBase, [] ) )
    ces = set( ces )
    newCEs = ces - cesCS
    if newCEs:
      cesCS = cesCS.union( ces )
      gLogger.notice( "Adding CEs %s" % ','.join( newCEs ) )
      cesCS = cesCS.union( ces )
      csAPI.modifyValue( "%s/CE" % cfgBase, ','.join( cesCS ) )
      change = True
  if change:       
    res = csAPI.commitChanges()
    if not res['OK']:
      gLogger.error( "Failed to commit changes to CS", res['Message'] )
      DIRACExit( -1 )
    else:
      if newSite:
        gLogger.notice( "Successfully added site %s to the CS with name %s and CEs: %s" % ( diracSiteName, gridSiteName, ','.join( ces ) ) )
      else:
        gLogger.notice( "Successfully added new CEs to site %s: %s" % ( diracSiteName, ','.join( newCEs ) ) )  
