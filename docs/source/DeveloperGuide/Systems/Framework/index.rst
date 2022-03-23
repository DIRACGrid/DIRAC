.. contents:: Table of contents
   :depth: 3

================================
Framework Overview
================================

Information regarding use of the DIRAC Framework to build new components

.. toctree::

  stableconns/index.rst

Static Component Monitoring
===========================

The Component Monitoring system takes care of logging information about the components that have been installed and uninstalled in different hosts, like the date or author of the change.
The following figure illustrates how different components from this system communicate with each other:

.. image:: /_static/Systems/FS/InteractionsDiagram.png
   :alt: Interaction between components.
   :align: center

All of the static information is stored in a MySQL database, InstalledComponentsDB. This database contains 3 tables, as illustrated below:

.. image:: /_static/Systems/FS/InstalledComponentsDB.png
   :alt: InstalledComponentsDB schema.
   :align: center

The InstalledComponentsDB.py file in the Framework system defines all the tables and their relationships using SQLAlchemy, as well as functions to access and modify the values in the database.
The following code shows the definition of the 'Component' class::

  class Component( Base ):
    __tablename__ = 'Components'
    __table_args__ = {
      'mysql_engine': 'InnoDB',
      'mysql_charset': 'utf8'
    }

    componentID = Column( 'ComponentID', Integer, primary_key = True )
    system = Column( 'System', String( 32 ), nullable = False )
    module = Column( 'Module', String( 32 ), nullable = False )
    cType = Column( 'Type', String( 32 ), nullable = False )

    def __init__( self, system = null(), module = null(), cType = null() ):
      self.system = system
      self.module = module
      self.cType = cType
      self.installationList = []

As can be seen, it is fairly easy to define a new class/table. The only thing that might seem off is the self.installationList field, as it has not been 'declared' before. This field acts as a back reference for the InstalledComponent table ( it is a list of all the installations the component is associated to, i.e., a list of InstalledComponent objects ).
This reference is completed in the InstalledComponent class definition with the addition of the 'installationComponent' field::

  class InstalledComponent( Base ):
    """
    This class defines the schema of the InstalledComponents table in the
    InstalledComponentsDB database
    """

    __tablename__ = 'InstalledComponents'
    __table_args__ = {
      'mysql_engine': 'InnoDB',
      'mysql_charset': 'utf8'
    }

    componentID = Column( 'ComponentID',
                          Integer,
                          ForeignKey( 'Components.ComponentID' ),
                          primary_key = True )
    hostID = Column( 'HostID',
                      Integer,
                      ForeignKey( 'Hosts.HostID' ),
                      primary_key = True )
    instance = Column( 'Instance',
                        String( 32 ),
                        primary_key = True )
    installationTime = Column( 'InstallationTime',
                                DateTime,
                                primary_key = True )
    unInstallationTime = Column( 'UnInstallationTime',
                                  DateTime )
    installedBy = Column( 'InstalledBy', String( 32 ) )
    unInstalledBy = Column( 'UnInstalledBy', String( 32 ) )
    installationComponent = relationship( 'Component',
                                          backref = 'installationList' )

Although we are using MySQL here, it is possible to switch to other SQLAlchemy-supported databases by changing the URI of the database in the initialization methods to point to the one being used instead.

For instance, from::

   self.engine = create_engine( 'mysql://%s:%s@%s:%s/%s' %
                    ( self.user, self.password, self.host, self.port, self.db ),
                    pool_recycle = 3600, echo_pool = True
                    )

to::

   engine = create_engine( 'sqlite:////route/to/my/db.db' , pool_recycle = 3600, echo_pool = True, echo = True )

The ComponentMonitoring service acts as an interface between the client side and the functionalities provided by InstalledComponentsDB ( accessing and modifying the database ). Clients to this service are created to modify the database or access its data.
The MonitoringUtilities module provides the functionality needed to store or delete monitoring entries from the database::

  from DIRAC.FrameworkSystem.Utilities import MonitoringUtilities

  # Add an entry to the DB for the SysAdmin service
  result = MonitoringUtilities.monitorInstallation( 'service', 'Framework', 'SystemAdministrator' )
  if not result[ 'OK' ]:
    print 'Something went wrong'


Dynamic Component Monitoring
============================

This system takes care of managing monitoring information of DIRAC component. It is based on ElasticSearch database. It is based on MonitoringSystem.
The information is collected by the __storeProfiling periodic task on the SystemAdministartor. The task is disabled by default.
The MonitoringReporter is used to propagate the DB whith the collected values.
