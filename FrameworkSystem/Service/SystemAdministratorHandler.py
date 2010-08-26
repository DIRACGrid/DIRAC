# $HeadURL$

""" SystemAdministrator service is a tool to control and monitor the DIRAC services and agents
"""

__RCSID__ = "$Id$"

from types import *
from DIRAC import S_OK, gConfig, shellCall
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.FrameworkSystem.DB.ComponentMonitoringDB import ComponentMonitoringDB

from DIRAC.Core.Utilities import InstallTools

cmDB = None

def initializeSystemAdministratorHandler( serviceInfo ):

  global cmDB
  #try:
  #  cmDB = ComponentMonitoringDB()
  #except Exception,x:
  #  gLogger.warn('Failed to create an instance of ComponentMonitoringDB ')
  return S_OK()


class SystemAdministratorHandler( RequestHandler ):


  types_getInfo = [ ]
  def export_getInfo( self ):
    """  Get versions of the installed DIRAC software and extensions, setup of the
         local installation
    """
    return InstallTools.getInfo( gConfig.getValue( '/DIRAC/Extensions', [] ) )

  types_getSoftwareComponents = [ ]
  def export_getSoftwareComponents( self ):
    """  Get the list of all the components ( services and agents ) for which the software
         is installed on the system
    """
    return InstallTools.getSoftwareComponents( gConfig.getValue( '/DIRAC/Extensions', [] ) )

  types_getInstalledComponents = [ ]
  def export_getInstalledComponents( self ):
    """  Get the list of all the components ( services and agents ) 
         installed on the system in the runit directory
    """
    return InstallTools.getInstalledComponents()

  types_getSetupComponents = [ ]
  def export_getSetupComponents( self ):
    """  Get the list of all the components ( services and agents ) 
         set up for running with runsvdir in /opt/dirac/startup directory 
    """
    return InstallTools.getSetupComponents()

  types_getOverallStatus = []
  def export_getOverallStatus( self ):
    """  Get the complete status information for the components in the
         given list
    """
    return InstallTools.getOverallStatus( gConfig.getValue( '/DIRAC/Extensions', [] ) )

  types_getStartupComponentStatus = [ ListType ]
  def export_getStartupComponentStatus( self, componentTupleList ):
    """  Get the list of all the components ( services and agents ) 
         set up for running with runsvdir in startup directory 
    """
    return InstallTools.getStartupComponentStatus( componentTupleList )

  types_installComponent = [ StringTypes, StringTypes, StringTypes ]
  def export_installComponent( self, componentType, system, component ):
    """ Install runit directory for the specified component
    """
    return InstallTools.installComponent( system, component, gConfig.getValue( '/DIRAC/Extensions', [] ) )

  types_setupComponent = [ StringTypes, StringTypes, StringTypes ]
  def export_setupComponent( self, componentType, system, component ):
    """ Setup the specified component for running with the runsvdir daemon
        It implies installComponent
    """
    return InstallTools.setupComponent( componentType, system, component, gConfig.getValue( '/DIRAC/Extensions', [] ) )

  types_addDefaultOptionsToComponentCfg = [ StringTypes, StringTypes ]
  def export_addDefaultOptionsToComponentCfg( self, componentType, system, component ):
    """ Add default component options local component cfg
    """
    return InstallTools.addDefaultOptionsToComponentCfg( componentType, system, component, gConfig.getValue( '/DIRAC/Extensions', [] ) )

  types_unsetupComponent = [ StringTypes, StringTypes ]
  def export_unsetupComponent( self, system, component ):
    """ Removed the specified component from running with the runsvdir daemon
    """
    return InstallTools.unsetupComponent( system, component )

  types_uninstallComponent = [ StringTypes, StringTypes ]
  def export_uninstallComponent( self, system, component ):
    """ Remove runit directory for the specified component
        It implies unsetupComponent
    """
    return InstallTools.uninstallComponent( system, component )

  types_startComponent = [ StringTypes, StringTypes ]
  def export_startComponent( self, system, component ):
    """ Start the specified component, running with the runsv daemon
    """
    return InstallTools.runsvctrlComponent( system, component, 'u' )

  types_restartComponent = [ StringTypes, StringTypes ]
  def export_restartComponent( self, system, component ):
    """ Restart the specified component, running with the runsv daemon
    """
    return InstallTools.runsvctrlComponent( system, component, 't' )

  types_stopComponent = [ StringTypes, StringTypes ]
  def export_stopComponent( self, system, component ):
    """ Stop the specified component, running with the runsv daemon
    """
    return InstallTools.runsvctrlComponent( system, component, 'd' )

  types_getLogTail = [ StringTypes, StringTypes ]
  def export_getLogTail( self, system, component, length = 100 ):
    """ Get the tail of the component log file
    """
    return InstallTools.getLogTail( system, component, length )

######################################################################################
#  Database related methods
#
  types_getMySQLStatus = [ ]
  def export_getMySQLStatus( self ):
    """ Get the status of the MySQL database installation
    """
    return InstallTools.getMySQLStatus()

  types_getDatabases = [ ]
  def export_getDatabases( self, mysqlPassword=None ):
    """ Get the list of installed databases
    """
    if mysqlPassword :
      InstallTools.setMySQLPasswords(mysqlPassword)
    return InstallTools.getDatabases()

  types_getAvailableDatabases = [ ]
  def export_getAvailableDatabases( self ):
    """ Get the list of databases which software is installed in the system
    """
    return InstallTools.getAvailableDatabases( gConfig.getValue( '/DIRAC/Extensions', [] ) )

  types_installMySQL = []
  def export_installMySQL( self, mysqlPassword=None, diracPassword=None ):
    """ Install MySQL database server
    """
    
    if mysqlPassword or diracPassword:
      InstallTools.setMySQLPasswords(mysqlPassword,diracPassword)
    if InstallTools.mysqlInstalled()['OK']:
      return S_OK( 'Already installed' )

    result = InstallTools.installMySQL()
    if not result['OK']:
      return result

    return S_OK( 'Successfully installed' )

  types_installDatabase = [ StringTypes ]
  def export_installDatabase( self, dbName, mysqlPassword=None ):
    """ Install a DIRAC database named dbName
    """
    if mysqlPassword :
      InstallTools.setMySQLPasswords(mysqlPassword)
    return InstallTools.installDatabase( dbName )

  types_addDatabaseOptionsToCS = [ StringTypes, StringTypes ]
  def export_addDatabaseOptionsToCS( self, system, database, overwrite = False ):
    """ Add the section with the database options to the CS
    """
    return InstallTools.addDatabaseOptionsToCS( gConfig, system, database, overwrite )

  types_addDefaultOptionsToCS = [StringTypes, StringTypes, StringTypes]
  def export_addDefaultOptionsToCS( self, componentType, system, component, overwrite = False ):
    """ Add default component options to the global CS or to the local options
    """
    return InstallTools.addDefaultOptionsToCS( gConfig, componentType, system, component, gConfig.getValue( '/DIRAC/Extensions', [] ), overwrite )

#######################################################################################
# General purpose methods
#  
  types_updateSoftware = [ StringTypes ]
  def export_updateSoftware( self, version ):
    """ Update the local DIRAC software installation to version
    """
    result = shellCall( 0, 'update_sw.sh %s' % version )
    return result

  types_addOptionToDiracCfg = [ StringTypes, StringTypes ]
  def export_addOptionToDiracCfg( self, option, value ):
    """ Set option in the local configuration file
    """
    return InstallTools.addOptionToDiracCfg()

  types_executeCommand = [ StringTypes ]
  def export_executeCommand( self, command ):
    """ Execute a command locally and return its output
    """
    result = shellCall( 60, command )
    return result
