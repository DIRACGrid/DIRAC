"""
This Service provides functionality to access and modify the InstalledComponents database
"""

__RCSID__ = "$Id$"

import types
from DIRAC.FrameworkSystem.DB.InstalledComponentsDB import InstalledComponentsDB, Component, Host, InstalledComponent
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR

class ComponentMonitoringHandler( RequestHandler ):

  @classmethod
  def initializeHandler( cls, serviceInfo ):
    """
    Handler class initialization
    """

    ComponentMonitoringHandler.doCommit = True

    try:
      ComponentMonitoringHandler.db = InstalledComponentsDB()
    except Exception, e:
      return S_ERROR( 'Could not connect to the database: %s' % ( e ) )

    return S_OK( 'Initialization went well' )

  def __joinInstallationMatch( self, installationFields, componentFields, hostFields ):
    matchFields = installationFields
    for key in componentFields.keys():
      matchFields[ 'Component.' + key ] = componentFields[ key ]
    for key in hostFields.keys():
      matchFields[ 'Host.' + key ] = hostFields[ key ]

    return S_OK( matchFields )

  types_setCommit = [ types.BooleanType ]
  def export_setCommit( self, value ):
    """
    Sets whether or not changes should be commited to the database
    """

    ComponentMonitoringHandler.doCommit = value

    return S_OK( 'Changes set' )

  types_addComponent = [ types.DictType ]
  def export_addComponent( self, component ):
    """
    Creates a new Component object on the database
    component argument should be a dictionary with the Component fields and its values
    """

    newComponent = Component()
    newComponent.fromDict( component )

    result = ComponentMonitoringHandler.db.addComponent( newComponent )
    if not result[ 'OK' ]:
      return result

    if( ComponentMonitoringHandler.doCommit ):
      return ComponentMonitoringHandler.db.commitChanges()
    else:
      return ComponentMonitoringHandler.db.flushChanges()

  types_componentExists = [ types.DictType ]
  def export_componentExists( self, matchFields ):
    """
    Returns whether components matching the given criteria exist
    matchFields argument should be a dictionary with the fields to match
    matchFields accepts fields of the form <Field.bigger> and <Field.smaller> to filter using > and < relationships.
    """

    return ComponentMonitoringHandler.db.exists( Component, matchFields )

  types_getComponents = [ types.DictType ]
  def export_getComponents( self, matchFields, includeInstallations, includeHosts ):
    """
    Returns a list of all the Components in the database
    matchFields argument should be a dictionary with the fields to match or empty to get all the instances
    matchFields also accepts fields of the form <Field.bigger> and <Field.smaller> to filter using > and < relationships
    includeInstallations indicates whether data about the installations in which the components takes part
    is to be retrieved
    includeHosts (only if includeInstallations is set to True) indicates whether data about the host in
    which there are instances of this component is to be retrieved
    """

    result = ComponentMonitoringHandler.db.getComponents( matchFields )
    if not result[ 'OK' ]:
      return result
    result = result[ 'Value' ]

    components = []
    for component in result:
      components.append( component.toDict( includeInstallations, includeHosts )[ 'Value' ] )

    return S_OK( components )

  types_updateComponents = [ types.DictType ]
  def export_updateComponents( self, matchFields, updates ):
    """
    Updates Components objects on the database
    matchFields argument should be a dictionary with the fields to match (instances matching the fields
    will be updated) or empty to update all the instances
    matchFields also accepts fields of the form <Field.bigger> and <Field.smaller> to filter using > and < relationships
    updates argument should be a dictionary with the Component fields and their new updated values
    """

    result = ComponentMonitoringHandler.db.getComponents( matchFields )
    if not result[ 'OK' ]:
      return result

    for component in result[ 'Value' ]:
      component.fromDict( updates )

    if( ComponentMonitoringHandler.doCommit ):
      return ComponentMonitoringHandler.db.commitChanges()
    else:
      return ComponentMonitoringHandler.db.flushChanges()

  types_removeComponents = [ types.DictType ]
  def export_removeComponents( self, matchFields ):
    """
    Removes from the database components that match the given fields
    matchFields argument should be a dictionary with the fields to match or empty to remove all the instances
    matchFields also accepts fields of the form <Field.bigger> and <Field.smaller> to filter using > and < relationships
    """

    result = ComponentMonitoringHandler.db.removeComponents( matchFields )
    if not result[ 'OK' ]:
      return result

    if( ComponentMonitoringHandler.doCommit ):
      return ComponentMonitoringHandler.db.commitChanges()
    else:
      return ComponentMonitoringHandler.db.flushChanges()

  types_addHost = [ types.DictType ]
  def export_addHost( self, host ):
    """
    Creates a new Host object on the database
    host argument should be a dictionary with the Host fields and its values
    """

    newHost = Host()
    newHost.fromDict( host )

    result = ComponentMonitoringHandler.db.addHost( newHost )
    if not result[ 'OK' ]:
      return result

    if( ComponentMonitoringHandler.doCommit ):
      return ComponentMonitoringHandler.db.commitChanges()
    else:
      return ComponentMonitoringHandler.db.flushChanges()

  types_hostExists = [ types.DictType ]
  def export_hostExists( self, matchFields ):
    """
    Returns whether hosts matching the given criteria exist
    matchFields argument should be a dictionary with the fields to match
    matchFields also accepts fields of the form <Field.bigger> and <Field.smaller> to filter using > and < relationships
    """

    return ComponentMonitoringHandler.db.exists( Host, matchFields )

  types_getHosts = [ types.DictType ]
  def export_getHosts( self, matchFields, includeInstallations, includeComponents ):
    """
    Returns a list of all the Hosts in the database
    matchFields argument should be a dictionary with the fields to match or empty to get all the instances
    matchFields also accepts fields of the form <Field.bigger> and <Field.smaller> to filter using > and < relationships
    includeInstallations indicates whether data about the installations in which the host takes part
    is to be retrieved
    includeComponents (only if includeInstallations is set to True) indicates whether data about the
    components installed into this host is to be retrieved
    """

    result = ComponentMonitoringHandler.db.getHosts( matchFields )
    if not result[ 'OK' ]:
      return result
    result = result[ 'Value' ]

    hosts = []
    for host in result:
      hosts.append( host.toDict( includeInstallations, includeComponents )[ 'Value' ] )

    return S_OK( hosts )

  types_updateHosts = [ types.DictType ]
  def export_updateHosts( self, matchFields, updates ):
    """
    Updates Hosts objects on the database
    matchFields argument should be a dictionary with the fields to match (instances matching the fields
    will be updated) or empty to update all the instances
    matchFields also accepts fields of the form <Field.bigger> and <Field.smaller> to filter using > and < relationships
    updates argument should be a dictionary with the Host fields and their new updated values
    """

    result = ComponentMonitoringHandler.db.getHosts( matchFields )
    if not result[ 'OK' ]:
      return result

    for host in result[ 'Value' ]:
      host.fromDict( updates )

    if( ComponentMonitoringHandler.doCommit ):
      return ComponentMonitoringHandler.db.commitChanges()
    else:
      return ComponentMonitoringHandler.db.flushChanges()

  types_removeHosts = [ types.DictType ]
  def export_removeHosts( self, matchFields ):
    """
    Removes from the database hosts that match the given fields
    matchFields argument should be a dictionary with the fields to match or empty to remove all the instances
    matchFields also accepts fields of the form <Field.bigger> and <Field.smaller> to filter using > and < relationships
    """

    result = ComponentMonitoringHandler.db.removeHosts( matchFields )
    if not result[ 'OK' ]:
      return result

    if( ComponentMonitoringHandler.doCommit ):
      return ComponentMonitoringHandler.db.commitChanges()
    else:
      return ComponentMonitoringHandler.db.flushChanges()

  types_addInstallation = [ types.DictType ]
  def export_addInstallation( self, installation, componentDict, hostDict, forceCreate ):
    """
    Creates a new InstalledComponent object on the database
    installation argument should be a dictionary with the InstalledComponent fields and its values
    componentDict argument should be a dictionary with the Component fields and its values
    hostDict argument should be a dictionary with the Host fields and its values
    forceCreate indicates whether a new Component and/or Host should be created if the given ones do not exist
    """

    newInstallation = InstalledComponent()
    newInstallation.fromDict( installation )

    result = ComponentMonitoringHandler.db.getComponents( componentDict )
    if not result[ 'OK' ]:
      return result
    if result[ 'Value' ].count() != 1:
      if result[ 'Value' ].count() > 1:
        return S_ERROR( 'Too many Components match the criteria' )
      if result[ 'Value' ].count() < 1:
        if not forceCreate:
          return S_ERROR( 'Given component does not exist' )
        else:
          component = Component()
          component.fromDict( componentDict )
    else:
      component = result[ 'Value' ][0]

    result = ComponentMonitoringHandler.db.getHosts( hostDict )
    if not result[ 'OK' ]:
      return result
    if result[ 'Value' ].count() != 1:
      if result[ 'Value' ].count() > 1:
        return S_ERROR( 'Too many Hosts match the criteria' )
      if result[ 'Value' ].count() < 1:
        if not forceCreate:
          return S_ERROR( 'Given host does not exist' )
        else:
          host = Host()
          host.fromDict( hostDict )
    else:
      host = result[ 'Value' ][0]

    result = ComponentMonitoringHandler.db.addInstalledComponent( newInstallation, component, host, forceCreate )
    if not result[ 'OK' ]:
      return result

    if( ComponentMonitoringHandler.doCommit ):
      return ComponentMonitoringHandler.db.commitChanges()
    else:
      return ComponentMonitoringHandler.db.flushChanges()

  types_installationExists = [ types.DictType ]
  def export_installationExists( self, installationFields, componentFields, hostFields ):
    """
    Returns whether installations matching the given criteria exist
    installationFields argument should be a dictionary with the fields to match for the installation
    componentFields argument should be a dictionary with the fields to match for the component installed
    hostFields argument should be a dictionary with the fields to match for the host where the
    installation is made
    """

    matchFields = self.__joinInstallationMatch( installationFields, componentFields, hostFields )[ 'Value' ]

    return ComponentMonitoringHandler.db.exists( InstalledComponent, matchFields )

  types_getInstallations = [ types.DictType ]
  def export_getInstallations( self, installationFields, componentFields, hostFields, installationsInfo ):
    """
    Returns a list of installations matching the given criteria
    installationFields argument should be a dictionary with the fields to match for the installation
    componentFields argument should be a dictionary with the fields to match for the component installed
    hostFields argument should be a dictionary with the fields to match for the host where the
    installation is made
    installationsInfo indicates whether information about the components and host taking part in the
    installation is to be provided
    """

    matchFields = self.__joinInstallationMatch( installationFields, componentFields, hostFields )[ 'Value' ]

    result = ComponentMonitoringHandler.db.getInstalledComponents( matchFields )
    if not result[ 'OK' ]:
      return result
    result = result[ 'Value' ]

    installations = []
    for installation in result:
      installations.append( installation.toDict( installationsInfo, installationsInfo )[ 'Value' ] )

    return S_OK( installations )

  types_updateInstallations = [ types.DictType ]
  def export_updateInstallations( self, installationFields, componentFields, hostFields, updates ):
    """
    Updates installations matching the given criteria
    installationFields argument should be a dictionary with the fields to match for the installation
    componentFields argument should be a dictionary with the fields to match for the component installed
    or empty to update regardless of component
    hostFields argument should be a dictionary with the fields to match for the host where the
    installation is made or empty to update regardless of host
    updates argument should be a dictionary with the Installation fields and their new updated values
    """

    matchFields = self.__joinInstallationMatch( installationFields, componentFields, hostFields )[ 'Value' ]

    result = ComponentMonitoringHandler.db.getInstalledComponents( matchFields )
    if not result[ 'OK' ]:
      return result

    for installation in result[ 'Value' ]:
      installation.fromDict( updates )

    if( ComponentMonitoringHandler.doCommit ):
      return ComponentMonitoringHandler.db.commitChanges()
    else:
      return ComponentMonitoringHandler.db.flushChanges()

  types_removeInstallations = [ types.DictType ]
  def export_removeInstallations( self, installationFields, componentFields, hostFields ):
    """
    Removes installations matching the given criteria
    installationFields argument should be a dictionary with the fields to match for the installation
    componentFields argument should be a dictionary with the fields to match for the component installed
    hostFields argument should be a dictionary with the fields to match for the host where the
    installation is made
    """

    matchFields = self.__joinInstallationMatch( installationFields, componentFields, hostFields )[ 'Value' ]

    result = ComponentMonitoringHandler.db.removeInstalledComponents( matchFields )
    if not result[ 'OK' ]:
      return result

    if( ComponentMonitoringHandler.doCommit ):
      return ComponentMonitoringHandler.db.commitChanges()
    else:
      return ComponentMonitoringHandler.db.flushChanges()

  types_commit = [ ]
  def export_commit( self ):
    """
    Commit all the unsaved changes to the database
    """
    return ComponentMonitoringHandler.db.commitChages()
