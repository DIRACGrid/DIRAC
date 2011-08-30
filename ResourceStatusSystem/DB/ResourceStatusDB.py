"""
The ResourcesStatusDB module contains a couple of exception classes, and a
class to interact with the ResourceStatus DB.
"""

import datetime
#from types import *

from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import getDIRACSiteName

from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSException, InvalidRes, InvalidStatus

from DIRAC.ResourceStatusSystem.Utilities.Utils import where, convertTime
from DIRAC.ResourceStatusSystem import ValidRes, ValidStatus, ValidSiteType, \
    ValidResourceType, ValidServiceType

#############################################################################

class RSSDBException( RSSException ):
  """
  DB exception
  """

  def __init__( self, message = "" ):
    self.message = message
    RSSException.__init__( self, message )

  def __str__( self ):
    return "Exception in the RSS DB: " + repr( self.message )

#############################################################################

class NotAllowedDate( RSSException ):
  """
  Exception that signals a not allowed date
  """

  def __init__( self, message = "" ):
    self.message = message
    RSSException.__init__( self, message )

  def __str__( self ):
    return "Not allowed date in the RSS DB: " + repr( self.message )

#############################################################################

class ResourceStatusDB:
  """
  The ResourcesStatusDB class is a front-end to the Resource Status Database.

  The simplest way to instantiate an object of type :class:`ResourceStatusDB`
  is simply by calling

   >>> rsDB = ResourceStatusDB()

  This way, it will use the standard :mod:`DIRAC.Core.Base.DB`.
  But there's the possibility to use other DB classes.
  For example, we could pass custom DB instantiations to it,
  provided the interface is the same exposed by :mod:`DIRAC.Core.Base.DB`.

   >>> AnotherDB = AnotherDBClass()
   >>> rsDB = ResourceStatusDB(DBin = AnotherDB)

  Alternatively, for testing purposes, you could do:

   >>> from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
   >>> mockDB = Mock()
   >>> rsDB = ResourceStatusDB(DBin = mockDB)

  Or, if you want to work with a local DB, providing it's mySQL:

   >>> rsDB = ResourceStatusDB(DBin = ['UserName', 'Password'])

  """


  def __init__( self, *args, **kwargs ):

    if len( args ) == 1:
      if isinstance( args[ 0 ], str ):
#        systemInstance=args[0]
        maxQueueSize = 10
      if isinstance( args[ 0 ], int ):
        maxQueueSize = args[ 0 ]
#        systemInstance='Default'
    elif len( args ) == 2:
#      systemInstance=args[0]
      maxQueueSize = args[ 1 ]
    elif len( args ) == 0:
#      systemInstance='Default'
      maxQueueSize = 10

    if 'DBin' in kwargs.keys():
      DBin = kwargs[ 'DBin' ]
      if isinstance( DBin, list ):
        from DIRAC.Core.Utilities.MySQL import MySQL
        self.db = MySQL( 'localhost', DBin[ 0 ], DBin[ 1 ], 'ResourceStatusDB' )
      else:
        self.db = DBin
    else:
      from DIRAC.Core.Base.DB import DB
      self.db = DB( 'ResourceStatusDB', 'ResourceStatus/ResourceStatusDB', maxQueueSize )

#  def __init__(self, DBin=None, systemInstance='Default', maxQueueSize=10):
#
#    if not isinstance(DBin, Mock):
#      from DIRAC.Core.Base.DB import DB
#      self.db = DB('ResourceStatusDB','ResourceStatus/ResourceStatusDB',maxQueueSize)
#    else:
#      self.db = DBin
#    self.lock = threading.Lock()


#############################################################################

#############################################################################
# Monitored functions
#############################################################################

#############################################################################

  def getMonitoredsList( self, granularity, paramsList = None, siteName = None,
                         serviceName = None, resourceName = None, storageElementName = None,
                         status = None, siteType = None, resourceType = None,
                         serviceType = None, countries = None, gridSiteName = None ):
    """
    Get Present Sites /Services / Resources / StorageElements lists.

    :params:
      :attr:`granularity`: a ValidRes

      :attr:`paramsList`: a list of parameters can be entered. If not given,
      a custom list is used.

      :attr:`siteName`, `serviceName`, `resourceName`, `storageElementName`:
      a string or a list representing the site/service/resource/storageElement name.
      If not given, fetch all.

      :attr:`status`: a string or a list representing the status. If not given, fetch all.

      :attr:`siteType`: a string or a list representing the site type.
      If not given, fetch all.

      :attr:`serviceType`: a string or a list representing the service type.
      If not given, fetch all.

      :attr:`resourceType`: a string or a list representing the resource type.
      If not given, fetch all.

      :attr:`countries`: a string or a list representing the countries extensions.
      If not given, fetch all.

      :attr:`gridSiteName`: a string or a list representing the grid site name.
      If not given, fetch all.

      See :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils` for these parameters.

    :return:
      list of monitored paramsList's values
    """

    #get the parameters of the query

    getInfo = []

    if granularity in ( 'Site', 'Sites' ):
      DBname = 'SiteName'
      DBtable = 'PresentSites'
      getInfo = getInfo + [ 'SiteName', 'SiteType', 'GridSiteName' ]
    elif granularity in ( 'Service', 'Services' ):
      DBname = 'ServiceName'
      DBtable = 'PresentServices'
      getInfo = getInfo + [ 'SiteName', 'SiteType', 'ServiceName', 'ServiceType' ]
    elif granularity in ( 'Resource', 'Resources' ):
      DBname = 'ResourceName'
      DBtable = 'PresentResources'
      getInfo = getInfo + [ 'SiteType', 'ResourceName', 'ResourceType', 'ServiceType', 'GridSiteName' ]
    elif granularity in ( 'StorageElementRead', 'StorageElementsRead' ):
      DBname = 'StorageElementName'
      DBtable = 'PresentStorageElementsRead'
      getInfo = getInfo + [ 'StorageElementReadName', 'GridSiteName' ]
    elif granularity in ( 'StorageElementWrite', 'StorageElementsWrite' ):
      DBname = 'StorageElementName'
      DBtable = 'PresentStorageElementsWrite'
      getInfo = getInfo + [ 'StorageElementReadName', 'GridSiteName' ]
    else:
      raise InvalidRes, where( self, self.getMonitoredsList )

    #paramsList
    if ( paramsList == None or paramsList == [] ):
      params = DBname + ', Status, FormerStatus, DateEffective, LastCheckTime '
    else:
      if type( paramsList ) is not type([]):
        paramsList = [ paramsList ]
      params = ','.join( [ x.strip()+' ' for x in paramsList ] )

    #siteName
    if 'SiteName' in getInfo:
      if ( siteName == None or siteName == [] ):
        r = "SELECT SiteName FROM PresentSites"
        resQuery = self.db._query( r )
        if not resQuery[ 'OK' ]:
          raise RSSDBException, where( self, self.getMonitoredsList ) + resQuery[ 'Message' ]
        if not resQuery[ 'Value' ]:
          siteName = []
        siteName = [ x[0] for x in resQuery[ 'Value' ] ]
        siteName = ','.join( [ '"'+x.strip()+'"' for x in siteName ] )
      else:
        if type( siteName ) is not type( [] ):
          siteName = [ siteName ]
        siteName = ','.join( [ '"'+x.strip()+'"' for x in siteName ] )

    #gridSiteName
    if 'GridSiteName' in getInfo:
      if ( gridSiteName == None or gridSiteName == [] ):
        r = "SELECT GridSiteName FROM GridSites"
        resQuery = self.db._query( r )
        if not resQuery[ 'OK' ]:
          raise RSSDBException, where( self, self.getMonitoredsList ) + resQuery[ 'Message' ]
        if not resQuery[ 'Value' ]:
          gridSiteName = []
        gridSiteName = [ x[0] for x in resQuery[ 'Value' ] ]
        gridSiteName = ','.join( [ '"'+x.strip()+'"' for x in gridSiteName ] )
      else:
        if type( gridSiteName ) is not type( [] ):
          gridSiteName = [ gridSiteName ]
        gridSiteName = ','.join( [ '"'+x.strip()+'"' for x in gridSiteName ] )

    #serviceName
    if 'ServiceName' in getInfo:
      if ( serviceName == None or serviceName == [] ):
        r = "SELECT ServiceName FROM PresentServices"
        resQuery = self.db._query( r )
        if not resQuery[ 'OK' ]:
          raise RSSDBException, where( self, self.getMonitoredsList ) + resQuery[ 'Message' ]
        if not resQuery[ 'Value' ]:
          serviceName = []
        serviceName = [ x[0] for x in resQuery[ 'Value' ] ]
        serviceName = ','.join( [ '"'+x.strip()+'"' for x in serviceName ] )
      else:
        if type( serviceName ) is not type( [] ):
          serviceName = [ serviceName ]
        serviceName = ','.join( [ '"'+x.strip()+'"' for x in serviceName ] )

    #resourceName
    if 'ResourceName' in getInfo:
      if ( resourceName == None or resourceName == [] ):
        r = "SELECT ResourceName FROM PresentResources"
        resQuery = self.db._query( r )
        if not resQuery[ 'OK' ]:
          raise RSSDBException, where( self, self.getMonitoredsList ) + resQuery[ 'Message' ]
        if not resQuery[ 'Value' ]:
          resourceName = []
        resourceName = [ x[0] for x in resQuery[ 'Value' ] ]
        resourceName = ','.join( [ '"'+x.strip()+'"' for x in resourceName ] )
      else:
        if type( resourceName ) is not type( [] ):
          resourceName = [ resourceName ]
        resourceName = ','.join( [ '"'+x.strip()+'"' for x in resourceName ] )

    #storageElementReadName
    if 'StorageElementReadName' in getInfo:
      if ( storageElementName == None or storageElementName == [] ):
        r = "SELECT StorageElementName FROM PresentStorageElementsRead"
        resQuery = self.db._query( r )
        if not resQuery[ 'OK' ]:
          raise RSSDBException, where( self, self.getMonitoredsList ) + resQuery[ 'Message' ]
        if not resQuery[ 'Value' ]:
          storageElementName = []
        storageElementName = [ x[0] for x in resQuery[ 'Value' ] ]
        storageElementName = ','.join( [ '"'+x.strip()+'"' for x in storageElementName ] )
      else:
        if type( storageElementName ) is not type( [] ):
          storageElementName = [ storageElementName ]
        storageElementName = ','.join( [ '"'+x.strip()+'"' for x in storageElementName ] )

    #storageElementWriteName
    if 'StorageElementWriteName' in getInfo:
      if ( storageElementName == None or storageElementName == [] ):
        r = "SELECT StorageElementName FROM PresentStorageElementsWrite"
        resQuery = self.db._query( r )
        if not resQuery[ 'OK' ]:
          raise RSSDBException, where( self, self.getMonitoredsList ) + resQuery[ 'Message' ]
        if not resQuery[ 'Value' ]:
          storageElementName = []
        storageElementName = [ x[0] for x in resQuery[ 'Value' ] ]
        storageElementName = ','.join( [ '"'+x.strip()+'"' for x in storageElementName ] )
      else:
        if type( storageElementName ) != list:
          storageElementName = [ storageElementName ]
        storageElementName = ','.join( [ '"'+x.strip()+'"' for x in storageElementName ] )

    #status
    if ( status == None or status == [] ):
      status = ValidStatus
    else:
      if type( status ) is not type( [] ):
        status = [ status ]
    status = ','.join( [ '"'+x.strip()+'"' for x in status ] )

    #siteType
    if 'SiteType' in getInfo:
      if ( siteType == None or siteType == [] ):
        siteType = ValidSiteType
      else:
        if type( siteType ) is not type( [] ):
          siteType = [ siteType ]
      siteType = ','.join( [ '"'+x.strip()+'"' for x in siteType ] )

    #serviceType
    if 'ServiceType' in getInfo:
      if ( serviceType == None or serviceType == [] ):
        serviceType = ValidServiceType
      else:
        if type( serviceType ) is not type( [] ):
          serviceType = [ serviceType ]
      serviceType = ','.join( [ '"'+x.strip()+'"' for x in serviceType ] )

    #resourceType
    if 'ResourceType' in getInfo:
      if ( resourceType == None or resourceType == [] ):
        resourceType = ValidResourceType
      else:
        if type( resourceType ) is not type( [] ):
          resourceType = [ resourceType ]
      resourceType = ','.join( [ '"'+x.strip()+'"' for x in resourceType ] )

    #countries
    if ( countries == None or countries == [] ):
      countries = self.getCountries( granularity )
    else:
      if type( countries ) is not type( [] ):
        countries = [ countries ]
    if countries == None:
      countries = " '%%'"
    else:
      str_ = ' OR %s LIKE ' %DBname
      countries = str_.join( [ '"%.'+x.strip()+'"' for x in countries ] )


    #storageElementType
#    if 'StorageElementType' in getInfo:
#      if (storageElementType == None or storageElementType == []):
#        storageElementType = ValidStorageElementType
#      else:
#        if type(storageElementType) is not type([]):
#          storageElementType = [storageElementType]
#      storageElementType = ','.join(['"'+x.strip()+'"' for x in storageElementType])


    #query construction
    #base
    req = "SELECT %s FROM %s WHERE" %( params, DBtable )
    #what "names"
    if 'SiteName' in getInfo:
      if siteName != [] and siteName != None and siteName is not None and siteName != '':
        req = req + " SiteName IN (%s) AND" %( siteName )
    if 'GridSiteName' in getInfo:
      if gridSiteName != [] and gridSiteName != None and gridSiteName is not None and gridSiteName != '':
        req = req + " GridSiteName IN (%s) AND" %( gridSiteName )
    if 'ServiceName' in getInfo:
      if serviceName != [] and serviceName != None and serviceName is not None and serviceName != '':
        req = req + " ServiceName IN (%s) AND" %( serviceName )
    if 'ResourceName' in getInfo:
      if resourceName != [] and resourceName != None and resourceName is not None and resourceName != '':
        req = req + " ResourceName IN (%s) AND" %( resourceName )
    if 'StorageElementReadName' in getInfo:
      if storageElementName != [] and storageElementName != None and storageElementName is not None and storageElementName != '':
        req = req + " StorageElementName IN (%s) AND" %( storageElementName )
    # Added for keeping consistency with 5 granularities
    if 'StorageElementWriteName' in getInfo:
      if storageElementName != [] and storageElementName != None and storageElementName is not None and storageElementName != '':
        req = req + " StorageElementName IN (%s) AND" %( storageElementName )

    #status
    req = req + " Status IN (%s)" % ( status )
    #types
    if 'SiteType' in getInfo:
      req = req + " AND SiteType IN (%s)" % ( siteType )
    if 'ServiceType' in getInfo:
      req = req + " AND ServiceType IN (%s)" % ( serviceType )
    if 'ResourceType' in getInfo:
      req = req + " AND ResourceType IN (%s)" % ( resourceType )
#    if 'StorageElementType' in getInfo:
#      req = req + " WHERE StorageElementName LIKE \'%" + "%s\'" %(storageElementType)
    if granularity not in ( 'StorageElementRead', 'StorageElementsRead', 'StorageElementWrite', 'StorageElementsWrite' ):
      req = req + " AND (%s LIKE %s)" % ( DBname, countries )

    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.getMonitoredsList ) + resQuery[ 'Message' ]
    if not resQuery[ 'Value' ]:
      return []
    list_ = []
    list_ = [ x for x in resQuery[ 'Value' ] ]
    return list_


#############################################################################

  def getMonitoredsStatusWeb( self, granularity, selectDict, _sortList, startItem, maxItems ):
    """
    Get present sites status list, for the web.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsList`
    and :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsHistory`

    Example of parameters:

    :params:
      :attr:`selectDict`: { 'SiteName':['XX', ...] , 'ExpandSiteHistory': ['XX', ...],
      'Status': ['XX', ...]}
      and equivalents for the other monitoreds

      :attr:`sortList`

      :attr:`startItem`

      :attr:`maxItems`

    :return: {
      :attr:`ParameterNames`: ['SiteName', 'Tier', 'GridType', 'Country',
      'Status', 'DateEffective', 'FormerStatus', 'Reason', 'StatusInTheMask'],

      :attr:'Records': [[], [], ...],

      :attr:'TotalRecords': X,

      :attr:'Extras': {}

      }
    """

    if granularity in ( 'Site', 'Sites' ):
      paramNames = [ 'SiteName', 'Tier', 'GridType', 'Country',
                     'Status', 'DateEffective', 'FormerStatus', 'Reason' ]
      paramsList = [ 'SiteName', 'SiteType', 'Status', 'DateEffective',
                     'FormerStatus', 'Reason' ]
    elif granularity in ( 'Service', 'Services' ):
      paramNames = [ 'ServiceName', 'ServiceType', 'Site', 'Country', 'Status',
                     'DateEffective', 'FormerStatus', 'Reason' ]
      paramsList = [ 'ServiceName', 'ServiceType', 'SiteName', 'Status',
                     'DateEffective', 'FormerStatus', 'Reason' ]
    elif granularity in ( 'Resource', 'Resources' ):
      paramNames = [ 'ResourceName', 'ServiceType', 'SiteName', 'ResourceType',
                     'Country', 'Status', 'DateEffective', 'FormerStatus', 'Reason' ]
      paramsList = [ 'ResourceName', 'ServiceType', 'SiteName', 'GridSiteName', 'ResourceType',
                     'Status', 'DateEffective', 'FormerStatus', 'Reason' ]
    elif granularity in ( 'StorageElementRead', 'StorageElementsRead' ):
      paramNames = [ 'StorageElementName', 'ResourceName', 'SiteName',
                     'Country', 'Status', 'DateEffective', 'FormerStatus', 'Reason' ]
      paramsList = [ 'StorageElementName', 'ResourceName', 'GridSiteName', 'Status',
                     'DateEffective', 'FormerStatus', 'Reason' ]
    elif granularity in ( 'StorageElementWrite', 'StorageElementsWrite' ):
      paramNames = [ 'StorageElementName', 'ResourceName', 'SiteName',
                     'Country', 'Status', 'DateEffective', 'FormerStatus', 'Reason' ]
      paramsList = [ 'StorageElementName', 'ResourceName', 'GridSiteName', 'Status',
                     'DateEffective', 'FormerStatus', 'Reason' ]
    else:
      raise InvalidRes, where( self, self.getMonitoredsStatusWeb )

#    resultDict = {}
    records                = []

    sites_select           = []
    services_select        = []
    resources_select       = []
    storageElements_select = []
    status_select          = []
    siteType_select        = []
    serviceType_select     = []
    resourceType_select    = []
    countries_select       = []
#    expand_site_history = ''
#    expand_service_history = ''
#    expand_resource_history = ''
#    expand_storageElements_history = ''

    #specify SiteName
    if selectDict.has_key( 'SiteName' ):
      sites_select = selectDict[ 'SiteName' ]
      if type( sites_select ) is not list:
        sites_select = [ sites_select ]
      del selectDict[ 'SiteName' ]

    #specify ServiceName
    if selectDict.has_key( 'ServiceName' ):
      services_select = selectDict[ 'ServiceName' ]
      if type( services_select ) is not list:
        services_select = [ services_select ]
      del selectDict[ 'ServiceName' ]

    #ResourceName
    if selectDict.has_key( 'ResourceName' ):
      resources_select = selectDict[ 'ResourceName' ]
      if type( resources_select ) is not list:
        resources_select = [ resources_select ]
      del selectDict[ 'ResourceName' ]

    #StorageElementName
    if selectDict.has_key( 'StorageElementName' ):
      storageElements_select = selectDict[ 'StorageElementName' ]
      if type( storageElements_select ) is not list:
        storageElements_select = [ storageElements_select ]
      del selectDict[ 'StorageElementName' ]

    #Status
    if selectDict.has_key( 'Status' ):
      status_select = selectDict[ 'Status' ]
      if type( status_select ) is not list:
        status_select = [ status_select ]
      del selectDict[ 'Status' ]

    #SiteType
    if selectDict.has_key( 'SiteType' ):
      siteType_select = selectDict[ 'SiteType' ]
      if type( siteType_select ) is not list:
        siteType_select =  [siteType_select ]
      del selectDict[ 'SiteType' ]

    #ServiceType
    if selectDict.has_key( 'ServiceType' ):
      serviceType_select = selectDict[ 'ServiceType' ]
      if type( serviceType_select ) is not list:
        serviceType_select = [ serviceType_select ]
      del selectDict[ 'ServiceType' ]

    #ResourceType
    if selectDict.has_key( 'ResourceType' ):
      resourceType_select = selectDict[ 'ResourceType' ]
      if type( resourceType_select ) is not list:
        resourceType_select = [ resourceType_select ]
      del selectDict[ 'ResourceType' ]

    #Countries
    if selectDict.has_key( 'Countries' ):
      countries_select = selectDict[ 'Countries' ]
      if type( countries_select ) is not list:
        countries_select = [ countries_select ]
      del selectDict[ 'Countries' ]

    #ExpandSiteHistory
    if selectDict.has_key( 'ExpandSiteHistory' ):
      paramsList = [ 'SiteName', 'Status', 'Reason', 'DateEffective' ]
      sites_select = selectDict[ 'ExpandSiteHistory' ]
      if type( sites_select ) is not list:
        sites_select = [ sites_select ]
      sitesHistory = self.getMonitoredsHistory( granularity, paramsList = paramsList,
                                               name = sites_select )
      # sitesHistory is a list of tuples
      for site in sitesHistory:
        record = []
        record.append( site[ 0  ] ) #SiteName
        record.append( None ) #Tier
        record.append( None ) #GridType
        record.append( None ) #Country
        record.append( site[ 1 ] ) #Status
        record.append( site[ 3 ].isoformat(' ') ) #DateEffective
        record.append( None ) #FormerStatus
        record.append( site[ 2 ] ) #Reason
        records.append( record )

    #ExpandServiceHistory
    elif selectDict.has_key( 'ExpandServiceHistory' ):
      paramsList = [ 'ServiceName', 'Status', 'Reason', 'DateEffective' ]
      services_select = selectDict[ 'ExpandServiceHistory' ]
      if type( services_select ) is not list:
        services_select = [ services_select ]
      servicesHistory = self.getMonitoredsHistory( granularity, paramsList = paramsList,
                                                   name = services_select )
      # servicesHistory is a list of tuples
      for service in servicesHistory:
        record = []
        record.append( service[ 0 ] ) #ServiceName
        record.append( None ) #ServiceType
        record.append( None ) #Site
        record.append( None ) #Country
        record.append( service[ 1 ] ) #Status
        record.append( service[ 3 ].isoformat(' ') ) #DateEffective
        record.append( None ) #FormerStatus
        record.append( service[ 2 ] ) #Reason
        records.append( record )

    #ExpandResourceHistory
    elif selectDict.has_key( 'ExpandResourceHistory' ):
      paramsList = [ 'ResourceName', 'Status', 'Reason', 'DateEffective' ]
      resources_select = selectDict[ 'ExpandResourceHistory' ]
      if type( resources_select ) is not list:
        resources_select = [ resources_select ]
      resourcesHistory = self.getMonitoredsHistory( granularity, paramsList = paramsList,
                                                    name = resources_select )
      # resourcesHistory is a list of tuples
      for resource in resourcesHistory:
        record = []
        record.append( resource[ 0 ] ) #ResourceName
        record.append( None ) #ServiceName
        record.append( None ) #SiteName
        record.append( None ) #ResourceType
        record.append( None ) #Country
        record.append( resource[ 1 ] ) #Status
        record.append( resource[ 3 ].isoformat(' ')) #DateEffective
        record.append( None ) #FormerStatus
        record.append( resource[ 2 ]) #Reason
        records.append( record )

    #ExpandStorageElementHistory
    elif selectDict.has_key( 'ExpandStorageElementHistory' ):
      paramsList = [ 'StorageElementName', 'Status', 'Reason', 'DateEffective' ]
      storageElements_select = selectDict[ 'ExpandStorageElementHistory' ]
      if type( storageElements_select ) is not list:
        storageElements_select = [ storageElements_select ]
      storageElementsHistory = self.getMonitoredsHistory( granularity, paramsList = paramsList,
                                                          name = storageElements_select )
      # storageElementsHistory is a list of tuples
      for storageElement in storageElementsHistory:
        record = []
        record.append( storageElement[ 0 ] ) #StorageElementName
        record.append( None ) #ResourceName
        record.append( None ) #SiteName
        record.append( None ) #Country
        record.append( storageElement[ 1 ] ) #Status
        record.append( storageElement[ 3 ].isoformat(' ')) #DateEffective
        record.append( None ) #FormerStatus
        record.append( storageElement[ 2 ] ) #Reason
        records.append( record )

    else:
      if granularity in ('Site', 'Sites'):
        sitesList = self.getMonitoredsList(granularity,
                                           paramsList = paramsList,
                                           siteName = sites_select,
                                           status = status_select,
                                           siteType = siteType_select,
                                           countries = countries_select)
        for site in sitesList:
          record   = []
          gridType = ( site[ 0 ] ).split( '.' ).pop(0)
          country  = ( site[ 0 ] ).split( '.' ).pop()

          record.append( site[ 0 ] ) #SiteName
          record.append( site[ 1 ] ) #Tier
          record.append( gridType ) #GridType
          record.append( country ) #Country
          record.append( site[ 2 ] ) #Status
          record.append( site[ 3 ].isoformat(' ') ) #DateEffective
          record.append( site[ 4 ] ) #FormerStatus
          record.append( site[ 5 ] ) #Reason
          records.append( record )

      elif granularity in ( 'Service', 'Services' ):
        servicesList = self.getMonitoredsList( granularity,
                                               paramsList = paramsList,
                                               serviceName = services_select,
                                               siteName = sites_select,
                                               status = status_select,
                                               siteType = siteType_select,
                                               serviceType = serviceType_select,
                                               countries = countries_select )
        for service in servicesList:
          record  = []
          country = ( service[ 0 ] ).split( '.' ).pop()

          record.append( service[ 0 ] ) #ServiceName
          record.append( service[ 1 ] ) #ServiceType
          record.append( service[ 2 ] ) #Site
          record.append( country ) #Country
          record.append( service[ 3 ] ) #Status
          record.append( service[ 4 ].isoformat(' ') ) #DateEffective
          record.append( service[ 5 ] ) #FormerStatus
          record.append( service[ 6 ] ) #Reason
          records.append( record )

      elif granularity in ( 'Resource', 'Resources' ):
        if sites_select == []:
          sites_select = self.getMonitoredsList( 'Site',
                                                 paramsList = [ 'SiteName' ] )
          sites_select = [ x[ 0 ] for x in sites_select ]

        gridSites_select = self.getMonitoredsList( 'Site',
                                                   paramsList = [ 'GridSiteName' ],
                                                   siteName = sites_select )
        gridSites_select = [ x[ 0 ] for x in gridSites_select ]

        resourcesList = self.getMonitoredsList( granularity,
                                                paramsList = paramsList,
                                                resourceName = resources_select,
                                                status = status_select,
                                                siteType = siteType_select,
                                                resourceType = resourceType_select,
                                                countries = countries_select,
                                                gridSiteName = gridSites_select )

        for resource in resourcesList:
          DIRACsite = resource[ 2 ]

          if DIRACsite == None:
            GridSiteName = resource[ 3 ]  #self.getGridSiteName(granularity, resource[0])
            DIRACsites = getDIRACSiteName( GridSiteName )
            if not DIRACsites[ 'OK' ]:
              raise RSSDBException, 'Error executing getDIRACSiteName'
            DIRACsites = DIRACsites[ 'Value' ]
            DIRACsite_comp = ''
            for DIRACsite in DIRACsites:
              if DIRACsite not in sites_select:
                continue
              DIRACsite_comp = DIRACsite + ' ' + DIRACsite_comp

            record  = []
            country = ( resource[ 0 ] ).split( '.' ).pop()

            record.append( resource[ 0 ] ) #ResourceName
            record.append( resource[ 1 ] ) #ServiceType
            record.append( DIRACsite_comp ) #SiteName
            record.append( resource[ 4 ] ) #ResourceType
            record.append( country ) #Country
            record.append( resource[ 5 ] ) #Status
            record.append( resource[ 6 ].isoformat(' ') ) #DateEffective
            record.append( resource[ 7 ] ) #FormerStatus
            record.append( resource[ 8 ] ) #Reason
            records.append( record )

          else:
            if DIRACsite not in sites_select:
              continue
            record  = []
            country = ( resource[ 0 ] ).split( '.' ).pop()

            record.append( resource[ 0 ] ) #ResourceName
            record.append( resource[ 1 ] ) #ServiceType
            record.append( DIRACsite ) #SiteName
            record.append( resource[ 4 ] ) #ResourceType
            record.append( country ) #Country
            record.append( resource[ 5 ] ) #Status
            record.append( resource[ 6 ].isoformat(' ') ) #DateEffective
            record.append( resource[ 7 ] ) #FormerStatus
            record.append( resource[ 8 ] ) #Reason
            records.append( record )


      elif granularity in ( 'StorageElementRead', 'StorageElementsRead' ):
        if sites_select == []:
          sites_select = self.getMonitoredsList( 'Site',
                                                paramsList = [ 'SiteName' ] )
          sites_select = [ x[ 0 ] for x in sites_select ]

        gridSites_select = self.getMonitoredsList( 'Site',
                                                   paramsList = [ 'GridSiteName' ],
                                                   siteName = sites_select )
        gridSites_select = [ x[ 0 ] for x in gridSites_select ]

        storageElementsList = self.getMonitoredsList( granularity,
                                                      paramsList = paramsList,
                                                      storageElementName = storageElements_select,
                                                      status = status_select,
                                                      countries = countries_select,
                                                      gridSiteName = gridSites_select )

#      paramNames = ['StorageElementName', 'ResourceName', 'SiteName', 'Country',
#                    'Status', 'DateEffective', 'FormerStatus', 'Reason']
#      paramsList = ['StorageElementName', 'ResourceName', 'GridSiteName', 'Status',
#                    'DateEffective', 'FormerStatus', 'Reason']

        for storageElement in storageElementsList:
          DIRACsites = getDIRACSiteName( storageElement[ 2 ] )
          if not DIRACsites[ 'OK' ]:
            raise RSSDBException, 'Error executing getDIRACSiteName'
          DIRACsites = DIRACsites[ 'Value' ]
          DIRACsite_comp = ''
          for DIRACsite in DIRACsites:
            if DIRACsite not in sites_select:
              continue
            DIRACsite_comp = DIRACsite + ' ' + DIRACsite_comp
          record  = []
          country = ( storageElement[ 1 ] ).split( '.' ).pop()

          record.append( storageElement[ 0 ] ) #StorageElementName
          record.append( storageElement[ 1 ] ) #ResourceName
          record.append( DIRACsite_comp ) #SiteName
          record.append( country ) #Country
          record.append( storageElement[ 3 ] ) #Status
          record.append( storageElement[ 4 ].isoformat(' ') ) #DateEffective
          record.append( storageElement[ 5 ] ) #FormerStatus
          record.append( storageElement[ 6 ] ) #Reason
          records.append( record )

      elif granularity in ( 'StorageElementWrite', 'StorageElementsWrite' ):
        if sites_select == []:
          sites_select = self.getMonitoredsList( 'Site',
                                                 paramsList = ['SiteName'] )
          sites_select = [ x[ 0 ] for x in sites_select ]

        gridSites_select = self.getMonitoredsList( 'Site',
                                                   paramsList = [ 'GridSiteName' ],
                                                   siteName = sites_select )
        gridSites_select = [ x[ 0 ] for x in gridSites_select ]

        storageElementsList = self.getMonitoredsList( granularity,
                                                      paramsList = paramsList,
                                                      storageElementName = storageElements_select,
                                                      status = status_select,
                                                      countries = countries_select,
                                                      gridSiteName = gridSites_select )

#      paramNames = ['StorageElementName', 'ResourceName', 'SiteName', 'Country',
#                    'Status', 'DateEffective', 'FormerStatus', 'Reason']
#      paramsList = ['StorageElementName', 'ResourceName', 'GridSiteName', 'Status',
#                    'DateEffective', 'FormerStatus', 'Reason']

        for storageElement in storageElementsList:
          DIRACsites = getDIRACSiteName( storageElement[ 2 ] )
          if not DIRACsites[ 'OK' ]:
            raise RSSDBException, 'Error executing getDIRACSiteName'
          DIRACsites = DIRACsites[ 'Value' ]
          DIRACsite_comp = ''
          for DIRACsite in DIRACsites:
            if DIRACsite not in sites_select:
              continue
            DIRACsite_comp = DIRACsite + ' ' + DIRACsite_comp

          record  = []
          country = ( storageElement[ 1 ] ).split( '.' ).pop()

          record.append( storageElement[ 0 ] ) #StorageElementName
          record.append( storageElement[ 1 ] ) #ResourceName
          record.append( DIRACsite_comp ) #SiteName
          record.append( country ) #Country
          record.append( storageElement[ 3 ] ) #Status
          record.append( storageElement[ 4 ].isoformat(' ')) #DateEffective
          record.append( storageElement[ 5 ] ) #FormerStatus
          record.append( storageElement[ 6 ] ) #Reason
          records.append( record )


    finalDict = {}
    finalDict[ 'TotalRecords' ]   = len( records )
    finalDict[ 'ParameterNames' ] = paramNames

    # Return all the records if maxItems == 0 or the specified number otherwise
    if maxItems:
      finalDict[ 'Records' ] = records[ startItem:startItem+maxItems ]
    else:
      finalDict[ 'Records' ] = records

    finalDict[ 'Extras' ] = None

    return finalDict


#############################################################################

  def getMonitoredsHistory( self, granularity, paramsList = None, name = None,
                            presentAlso = True, order = 'ASC', limit = None ):
    """
    Get history of sites / services / resources / storageElements in a list
    (a site name can be specified)

    :params:
      :attr:`granularity`: a ValidRes

      :attr:`paramsList`: A list of parameters can be entered. If not, a custom list is used.

      :attr:`name`: list of strings. If not given, fetches the complete list
    """

    if paramsList is not None:
      if type( paramsList ) is not type( [] ):
        paramsList = [ paramsList ]
      params = ','.join( [ x.strip()+' ' for x in paramsList ] )

    if granularity in ( 'Site', 'Sites' ):
      if ( paramsList == None or paramsList == [] ):
        params = 'SiteName, Status, Reason, DateCreated, DateEffective, DateEnd, TokenOwner '
      DBtable  = 'SitesHistory'
      DBtableP = 'Sites'
      DBname   = 'SiteName'
      DBid     = 'SitesHistoryID'
    elif granularity in ( 'Service', 'Services' ):
      if ( paramsList == None or paramsList == [] ):
        params = 'ServiceName, Status, Reason, DateCreated, DateEffective, DateEnd, TokenOwner '
      DBtable  = 'ServicesHistory'
      DBtableP = 'Services'
      DBname   = 'ServiceName'
      DBid     = 'ServicesHistoryID'
    elif granularity in ( 'Resource', 'Resources' ):
      if ( paramsList == None or paramsList == [] ):
        params = 'ResourceName, Status, Reason, DateCreated, DateEffective, DateEnd, TokenOwner '
      DBtable  = 'ResourcesHistory'
      DBtableP = 'Resources'
      DBname   = 'ResourceName'
      DBid     = 'ResourcesHistoryID'
    elif granularity in ( 'StorageElementRead', 'StorageElementsRead' ):
      if ( paramsList == None or paramsList == [] ):
        params = 'StorageElementName, Status, Reason, DateCreated, DateEffective, DateEnd, TokenOwner '
      DBtable  = 'StorageElementsReadHistory'
      DBtableP = 'StorageElementsRead'
      DBname   = 'StorageElementName'
      DBid     = 'StorageElementsHistoryID'
    elif granularity in ( 'StorageElementWrite', 'StorageElementsWrite' ):
      if ( paramsList == None or paramsList == [] ):
        params = 'StorageElementName, Status, Reason, DateCreated, DateEffective, DateEnd, TokenOwner '
      DBtable  = 'StorageElementsWriteHistory'
      DBtableP = 'StorageElementsWrite'
      DBname   = 'StorageElementName'
      DBid     = 'StorageElementsHistoryID'
    else:
      raise InvalidRes, where( self, self.getMonitoredsHistory )


    #take history data
    if ( name == None or name == [] ):
      req = "SELECT %s FROM %s ORDER BY %s, %s" %( params, DBtable, DBname, DBid )
    else:
      if type( name ) is not type( [] ):
        nameM = [ name ]
      else:
        nameM = name
      nameM = ','.join( [ '"'+x.strip()+'"' for x in nameM ] )
      req = "SELECT %s FROM %s WHERE %s IN (%s) ORDER BY %s" % ( params, DBtable, DBname,
                                                                 nameM, DBid )
      if order == 'DESC':
        req = req + " DESC"
      if limit is not None:
        req = req + " LIMIT %s" %str( limit )
    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.getMonitoredsHistory ) + resQuery[ 'Message' ]
    if not resQuery[ 'Value' ]:
      return []
#    list_h = []
    list_h = [ x for x in resQuery[ 'Value' ] ]

    if presentAlso:
      #take present data
      if ( name == None or name == [] ):
        req = "SELECT %s FROM %s ORDER BY %s" %( params, DBtableP, DBname )
      else:
        if type( name ) is not type( [] ):
          nameM = [ name ]
        else:
          nameM = name
        nameM = ','.join( [ '"'+x.strip()+'"' for x in nameM ] )
        req = "SELECT %s FROM %s WHERE %s IN (%s)" % ( params, DBtableP, DBname, nameM )

      resQuery = self.db._query( req )
      if not resQuery[ 'OK' ]:
        raise RSSDBException, where( self, self.getMonitoredsHistory ) + resQuery[ 'Message' ]
      if not resQuery[ 'Value' ]:
        return []
      list_p = []
      list_p = [ x for x in resQuery[ 'Value' ] ]

      list_ = list_h + list_p
    else:
      list_ = list_h

    return list_

#############################################################################

  def setLastMonitoredCheckTime( self, granularity, name ):
    """
    Set to utcnow() LastCheckTime of table Sites /Services /Resources / StorageElements

    :params:
      :attr:`granularity`: a ValidRes

      :attr:`name`: string
    """

    DBtable, DBname = self.__DBchoice( granularity )

    req = "UPDATE %s SET LastCheckTime = UTC_TIMESTAMP() WHERE " %( DBtable )
    req = req + "%s = '%s' AND DateEffective <= UTC_TIMESTAMP();" % ( DBname, name )
    resUpdate = self.db._update( req )

    if not resUpdate[ 'OK' ]:
      raise RSSDBException, where( self, self.setLastMonitoredCheckTime ) + resUpdate[ 'Message' ]

#############################################################################

  def setMonitoredReason( self, granularity, name, reason, tokenOwner ):
    """
    Set new reason to name.

    :params:
      :attr:`granularity`: a ValidRes

      :attr:`name`: string, name or res

      :attr:`reason`: string, reason

      :attr:`tokenOwner`: string, who's making this change
      (RS_SVC if it's the service itslef)
    """

    DBtable, DBname = self.__DBchoice( granularity )

    req = "UPDATE %s SET Reason = '%s', " %( DBtable, reason )
    req = req + "TokenOwner = '%s' WHERE %s = '%s';"  %( tokenOwner, DBname, name )
    resUpdate = self.db._update( req )

    if not resUpdate[ 'OK' ]:
      raise RSSDBException, where( self, self.setMonitoredReason ) + resUpdate[ 'Message' ]

#############################################################################

#############################################################################
# Site functions
#############################################################################

#############################################################################

  def setSiteStatus( self, siteName, status, reason, tokenOwner ):
    """
    Set a Site status, effective from now, with no ending

    :params:
      :attr:`siteName`: string

      :attr:`status`: string. Possibilities:
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`reason`: string

      :attr:`tokenOwner`: string. For the service itself: `RS_SVC`
    """

    req = "SELECT SiteType, GridSiteName FROM Sites WHERE SiteName = '%s' " %( siteName )
    req = req + "AND DateEffective < UTC_TIMESTAMP();"
    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.setSiteStatus ) + resQuery[ 'Message' ]
    if not resQuery[ 'Value' ]:
      return None

    siteType = resQuery[ 'Value' ][ 0 ][ 0 ]
    gridSiteName = resQuery[ 'Value' ][ 0 ][ 1 ]

    self.addOrModifySite( siteName, siteType, gridSiteName, status, reason,
                          datetime.datetime.utcnow().replace( microsecond = 0 ), tokenOwner,
                          datetime.datetime( 9999, 12, 31, 23, 59, 59 ) )

#############################################################################

  def addOrModifySite( self, siteName, siteType, gridSiteName, status,
                       reason, dateEffective, tokenOwner, dateEnd ):
    """
    Add or modify a site to the Sites table.

    :params:
      :attr:`siteName`: string - name of the site (DIRAC name)

      :attr:`siteType`: string - ValidSiteType:
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`. LHCb Tier of the site

      :attr:`gridSiteName`: string - name of the site in GOC DB

      :attr:`status`: string - ValidStatus:
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`reason`: string - free

      :attr:`dateEffective`: datetime.datetime - date from which the site status is effective

      :attr:`tokenOwner`: string - free

      :attr:`dateEnd`: datetime.datetime - date from which the site status ends to be effective
    """

    dateCreated, dateEffective = self.__addOrModifyInit( dateEffective, dateEnd, status )

    #check if the site is already there
    query = "SELECT SiteName FROM Sites WHERE SiteName='%s'" % siteName
    resQuery = self.db._query( query )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.addOrModifySite ) + resQuery[ 'Message' ]

    if resQuery[ 'Value' ]:
      if dateEffective <= ( dateCreated + datetime.timedelta( minutes=2 ) ):
        #site modification, effective in less than 2 minutes
        self.setDateEnd( 'Site', siteName, dateEffective )
        self.transact2History( 'Site', siteName, dateEffective )
      else:
        self.setDateEnd( 'Site', siteName, dateEffective )
    else:
      if status in ( 'Active', 'Probing', 'Bad' ):
        oldStatus = 'Banned'
      else:
        oldStatus = 'Active'
      self._addSiteHistoryRow( siteName, oldStatus, reason, dateCreated, dateEffective,
                               datetime.datetime.utcnow().replace( microsecond = 0 ).isoformat( ' ' ),
                               tokenOwner )

    #in any case add a row to present Sites table
    self._addSiteRow( siteName, siteType, gridSiteName, status, reason,
                     dateCreated, dateEffective, dateEnd, tokenOwner )

#############################################################################

  def _addSiteRow( self, siteName, siteType, gridSiteName, status, reason,
                   dateCreated, dateEffective, dateEnd, tokenOwner ):
    """
    Add a new site row in Sites table

    :params:
      :attr:`siteName`: string - name of the site (DIRAC name)

      :attr:`siteType`: string - ValidSiteType: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`gridSiteName`: string - name of the site in GOC DB

      :attr:`status`: string - ValidStatus: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`reason`: string - free

      :attr:`dateCreated`: datetime.datetime or string - date when which the site row is created

      :attr:`dateEffective`: datetime.datetime or string - date from which the site status is effective

      :attr:`dateEnd`: datetime.datetime or string - date from which the site status ends to be effective

      :attr:`tokenOwner`: string - free
    """

    dateCreated, dateEffective, dateEnd = self.__usualChecks( dateCreated, dateEffective, dateEnd, status )

    req = "INSERT INTO Sites (SiteName, SiteType, GridSiteName, Status, Reason, "
    req = req + "DateCreated, DateEffective, DateEnd, TokenOwner, TokenExpiration) "
    req = req + "VALUES ('%s', '%s', '%s', " % ( siteName, siteType, gridSiteName )
    req = req + "'%s', '%s', '%s', " %( status, reason, dateCreated )
    req = req + "'%s', '%s', '%s', '9999-12-31 23:59:59');" %( dateEffective, dateEnd, tokenOwner )

    resUpdate = self.db._update( req )
    if not resUpdate[ 'OK' ]:
      raise RSSDBException, where( self, self._addSiteRow ) + resUpdate[ 'Message' ]

#############################################################################

  def _addSiteHistoryRow( self, siteName, status, reason, dateCreated, dateEffective,
                          dateEnd, tokenOwner ):
    """
    Add an old site row in the SitesHistory table

    :params:
      :attr:`siteName`: string - name of the site (DIRAC name)

      :attr:`status`: string - ValidStatus:
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`reason`: string - free

      :attr:`dateCreated`: datetime.datetime or string - date when which the site row is created

      :attr:`dateEffective`: datetime.datetime or string - date from which the site status is effective

      :attr:`dateEnd`: datetime.datetime or string - date from which the site status
      ends to be effective

      :attr:`tokenOwner`: string - free
    """

    if not isinstance( dateCreated, basestring ):
      dateCreated = dateCreated.isoformat( ' ' )
    if not isinstance( dateEffective, basestring ):
      dateEffective = dateEffective.isoformat( ' ' )
    if not isinstance( dateEnd, basestring ):
      dateEnd = dateEnd.isoformat( ' ' )

    req = "INSERT INTO SitesHistory (SiteName, Status, Reason, DateCreated,"
    req = req + " DateEffective, DateEnd, TokenOwner) "
    req = req + "VALUES ('%s', '%s', '%s', '%s', " % ( siteName, status, reason, dateCreated )
    req = req + "'%s', '%s', '%s');" % ( dateEffective, dateEnd, tokenOwner )
    resUpdate = self.db._update( req )
    if not resUpdate[ 'OK' ]:
      raise RSSDBException, where( self, self._addSiteHistoryRow ) + resUpdate[ 'Message' ]


#############################################################################

  def removeSite( self, siteName ):
    """
    Completely remove a site from the Sites, SitesHistory tables.
    Also, remove its services and CEs.

    :params:
      :attr:`siteName`: string
    """

    gridSiteName = self.getGridSiteName( 'Site', siteName )

    DIRACSiteNames = [x[0] for x in self.getMonitoredsList( 'Site', 'GridSiteName',
                                                           gridSiteName = gridSiteName )]

    if len( DIRACSiteNames ) == 1:
      self.removeResource( gridSiteName = gridSiteName )
    else:
      self.removeResource( siteName = siteName )

    self.removeService( siteName = siteName )

    req = "DELETE from Sites WHERE SiteName = '%s';" %siteName
    resDel = self.db._update( req )
    if not resDel[ 'OK' ]:
      raise RSSDBException, where( self, self.removeSite ) + resDel[ 'Message' ]

    req = "DELETE from SitesHistory WHERE SiteName = '%s';" %siteName
    resDel = self.db._update( req )
    if not resDel[ 'OK' ]:
      raise RSSDBException, where( self, self.removeSite ) + resDel[ 'Message' ]

#############################################################################

#############################################################################
# Service functions
#############################################################################

#############################################################################

  def setServiceStatus( self, serviceName, status, reason, tokenOwner ):
    """
    Set a Service status, effective from now, with no ending

    :params:
      :attr:`serviceName`: string

      :attr:`status`: string. Possibilities:
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`reason`: string

      :attr:`tokenOwner`: string. For the service itself: `RS_SVC`
    """

    req = "SELECT ServiceType, SiteName FROM Services WHERE ServiceName = "
    req = req + "'%s' AND DateEffective < UTC_TIMESTAMP();" %( serviceName )

    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.setServiceStatus ) + resQuery[ 'Message' ]
    if not resQuery[ 'Value' ]:
      return None

    serviceType = resQuery[ 'Value' ][ 0 ][ 0 ]
    siteName = resQuery[ 'Value' ][ 0 ][ 1 ]

    self.addOrModifyService( serviceName, serviceType, siteName, status, reason,
                             datetime.datetime.utcnow().replace( microsecond = 0 ), tokenOwner,
                             datetime.datetime( 9999, 12, 31, 23, 59, 59 ) )

#############################################################################

  def addOrModifyService( self, serviceName, serviceType, siteName, status, reason,
                          dateEffective, tokenOwner, dateEnd ):
    """
    Add or modify a service to the Services table.

    :params:
      :attr:`serviceName`: string - name of the service (DIRAC name)

      :attr:`serviceType`: string - ValidServiceType:
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`siteName`: string - DIRAC site name

      :attr:`status`: string - ValidStatus:
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`reason`: string - free

      :attr:`dateEffective`: datetime.datetime - date from which the service status is effective

      :attr:`tokenOwner`: string - free

      :attr:`dateEnd`: datetime.datetime - date from which the service status ends to be effective
    """

    dateCreated, dateEffective = self.__addOrModifyInit( dateEffective, dateEnd, status )

    #check if the service is already there
    query = "SELECT ServiceName FROM Services WHERE ServiceName = '%s'" % serviceName
    resQuery = self.db._query( query )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.addOrModifyService ) + resQuery[ 'Message' ]

    if resQuery[ 'Value' ]:
      if dateEffective <= ( dateCreated + datetime.timedelta( minutes=2 ) ):
        #service modification, effective in less than 2 minutes
        self.setDateEnd( 'Service', serviceName, dateEffective )
        self.transact2History( 'Service', serviceName, dateEffective )
      else:
        self.setDateEnd( 'Service', serviceName, dateEffective )
    else:
      if status in ( 'Active', 'Probing', 'Bad' ):
        oldStatus = 'Banned'
      else:
        oldStatus = 'Active'
      self._addServiceHistoryRow( serviceName, oldStatus, reason, dateCreated, dateEffective,
                                  datetime.datetime.utcnow().replace(microsecond = 0).isoformat(' '),
                                  tokenOwner )

    #in any case add a row to present Services table
    self._addServiceRow( serviceName, serviceType, siteName, status, reason,
                         dateCreated, dateEffective, dateEnd, tokenOwner )
#    serviceRow = "Added %s --- %s " %(serviceName, dateEffective)
#    return serviceRow

#############################################################################

  def _addServiceRow( self, serviceName, serviceType, siteName, status, reason,
                      dateCreated, dateEffective, dateEnd, tokenOwner ):
    """
    Add a new service row in Services table

    :params:
      :attr:`serviceName`: string - name of the service (DIRAC name)

      :attr:`serviceType`: string - ValidServiceType:
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`siteName`: string - DIRAC site name

      :attr:`status`: string - ValidStatus:
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`reason`: string - free

      :attr:`dateCreated`: datetime.datetime or string -
      date when which the service row is created

      :attr:`dateEffective`: datetime.datetime or string -
      date from which the service status is effective

      :attr:`dateEnd`: datetime.datetime or string -
      date from which the service status ends to be effective

      :attr:`tokenOwner`: string - free
    """

    dateCreated, dateEffective, dateEnd = self.__usualChecks( dateCreated, dateEffective, dateEnd, status )

    req = "INSERT INTO Services (ServiceName, ServiceType, SiteName, Status, Reason, "
    req = req + "DateCreated, DateEffective, DateEnd, TokenOwner, TokenExpiration) "
    req = req + "VALUES ('%s', '%s', '%s', " % ( serviceName, serviceType, siteName )
    req = req + "'%s', '%s', '%s', '%s'" %( status, reason, dateCreated, dateEffective )
    req = req + ", '%s', '%s', '9999-12-31 23:59:59');" %( dateEnd, tokenOwner )

    resUpdate = self.db._update( req )
    if not resUpdate[ 'OK' ]:
      raise RSSDBException, where( self, self._addServiceRow ) + resUpdate[ 'Message' ]

#############################################################################

  def _addServiceHistoryRow( self, serviceName, status, reason, dateCreated,
                             dateEffective, dateEnd, tokenOwner ):
    """
    Add an old service row in the ServicesHistory table

    :params:
      :attr:`serviceName`: string - name of the service (DIRAC name)

      :attr:`status`: string - ValidStatus:
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`reason`: string - free

      :attr:`dateCreated`: datetime.datetime or string -
      date when which the service row is created

      :attr:`dateEffective`: datetime.datetime or string -
      date from which the service status is effective

      :attr:`dateEnd`: datetime.datetime or string -
      date from which the service status ends to be effective

      :attr:`tokenOwner`: string - free

    """

    dateCreated, dateEffective, dateEnd = self.__usualChecks( dateCreated, dateEffective, dateEnd, status )

    req = "INSERT INTO ServicesHistory (ServiceName, Status, Reason, DateCreated,"
    req = req + " DateEffective, DateEnd, TokenOwner) "
    req = req + "VALUES ('%s', '%s', '%s', " % ( serviceName, status, reason )
    req = req + "'%s', '%s', '%s', '%s');" %( dateCreated, dateEffective, dateEnd, tokenOwner )
    resUpdate = self.db._update( req )
    if not resUpdate[ 'OK' ]:
      raise RSSDBException, where( self, self._addServiceHistoryRow ) + resUpdate[ 'Message' ]

#############################################################################

  def removeService( self, serviceName = None, siteName = None ):
    """
    Completely remove a service from the Services and ServicesHistory tables

    :params:
      :attr:`serviceName`: string
    """

    if serviceName != None:

      req = "DELETE from Services WHERE ServiceName = '%s';" % ( serviceName )
      resDel = self.db._update( req )
      if not resDel[ 'OK' ]:
        raise RSSDBException, where( self, self.removeService ) + resDel[ 'Message' ]

      req = "DELETE from ServicesHistory WHERE ServiceName = '%s';" % ( serviceName )
      resDel = self.db._update( req )
      if not resDel[ 'OK' ]:
        raise RSSDBException, where( self, self.removeService ) + resDel[ 'Message' ]

    if siteName != None:

      self.removeResource( siteName = siteName )

      req = "DELETE from Services WHERE SiteName = '%s';" % ( siteName )
      resDel = self.db._update( req )
      if not resDel[ 'OK' ]:
        raise RSSDBException, where( self, self.removeService ) + resDel[ 'Message' ]


#############################################################################

#############################################################################
# Resource functions
#############################################################################

#############################################################################

  def getResources( self, resourceName = None, resourceType = None, serviceType = None,
                    siteName = None, gridSiteName = None, status = None, reason = None,
                    tokenOwner = None ):

    req  = "SELECT ResourceName, ResourceType, ServiceType, SiteName, GridSiteName, Status, Reason, TokenOwner"
    req += " FROM Resources"

    whereConds = []
    if resourceName is not None:
      whereConds.append( " ResourceName = '%s'" % resourceName )
    if resourceType is not None:
      whereConds.append( " ResourceType = '%s'" % resourceType )
    if serviceType is not None:
      whereConds.append( " ServiceType = '%s'" % serviceType )
    if siteName is not None:
      whereConds.append( " SiteName = '%s'" % siteName )
    if gridSiteName is not None:
      whereConds.append( " GridSiteName = '%s'" % gridSiteName )
    if status is not None:
      whereConds.append( " Status = '%s'" % status )
    if reason is not None:
      whereConds.append( " Reason = '%s'" % reason )
    if tokenOwner is not None:
      whereConds.append( " TokenOwner = '%s'" % tokenOwner )

    if whereConds:
      req += " WHERE " + " AND".join( whereConds )#.replace( " AND", ",", len( whereConds ) - 2 )

    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getResources) + resQuery['Message']
    if not resQuery['Value']:
      return []

    return resQuery['Value']

  def setResourceStatus( self, resourceName, status, reason, tokenOwner ):

    """
    Set a Resource status, effective from now, with no ending

    :params:
      :attr:`resourceName`: string

      :attr:`status`: string. Possibilities:
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`reason`: string

      :attr:`tokenOwner`: string. For the service itself: `RS_SVC`
    """

    req = "SELECT ResourceType, ServiceType, SiteName, GridSiteName FROM Resources WHERE "
    req = req + "ResourceName = '%s' AND DateEffective < UTC_TIMESTAMP();" %( resourceName )
    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.setResourceStatus ) + resQuery[ 'Message' ]
    if not resQuery[ 'Value' ]:
      return None

    resourceType = resQuery[ 'Value' ][ 0 ][ 0 ]
    serviceType  = resQuery[ 'Value' ][ 0 ][ 1 ]
    siteName     = resQuery[ 'Value' ][ 0 ][ 2 ]
    gridSiteName = resQuery[ 'Value' ][ 0 ][ 3 ]

    self.addOrModifyResource( resourceName, resourceType, serviceType, siteName, gridSiteName,
                              status, reason, datetime.datetime.utcnow().replace( microsecond = 0 ),
                              tokenOwner, datetime.datetime( 9999, 12, 31, 23, 59, 59 ) )

#############################################################################

  def addOrModifyResource( self, resourceName, resourceType, serviceType, siteName,
                           gridSiteName, status, reason, dateEffective, tokenOwner, dateEnd ):
    """
    Add or modify a resource to the Resources table.

    :params:
      :attr:`resourceName`: string - name of the resource (DIRAC name)

      :attr:`resourceType`: string - ValidResourceType:
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`serviceType`: string - ValidServiceType:
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`siteName`: string - name of the site (DIRAC name). Can be NULL.

      :attr:`gridSiteName`: string - name of the site in GOC DB. Can be NULL.

      :attr:`status`: string - ValidStatus:
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`reason`: string - free

      :attr:`dateEffective`: datetime.datetime - date from which the resource status is effective

      :attr:`tokenOwner`: string - free

      :attr:`dateEnd`: datetime.datetime - date from which the resource status ends to be effective
    """

    dateCreated, dateEffective = self.__addOrModifyInit( dateEffective, dateEnd, status )

    #check if the resource is already there
    query = "SELECT ResourceName FROM Resources WHERE ResourceName = '%s'" % ( resourceName )
    resQuery = self.db._query( query )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.addOrModifyResource ) + resQuery[ 'Message' ]

    if resQuery[ 'Value' ]:
      #site modification, effective from now
      if dateEffective <= ( dateCreated + datetime.timedelta( minutes=2 ) ):
        self.setDateEnd( 'Resource', resourceName, dateEffective )
        self.transact2History( 'Resource', resourceName, dateEffective )
      else:
        self.setDateEnd( 'Resource', resourceName, dateEffective )
    else:
      if status in ( 'Active', 'Probing', 'Bad' ):
        oldStatus = 'Banned'
      else:
        oldStatus = 'Active'
      self._addResourcesHistoryRow( resourceName, oldStatus, reason, dateCreated, dateEffective,
                                    datetime.datetime.utcnow().replace( microsecond = 0 ).isoformat( ' ' ),
                                    tokenOwner )

    #in any case add a row to present Sites table
    self._addResourcesRow( resourceName, resourceType, serviceType, siteName, gridSiteName,
                           status, reason, dateCreated, dateEffective, dateEnd, tokenOwner )

#############################################################################

  def _addResourcesRow( self, resourceName, resourceType, serviceType, siteName, gridSiteName,
                        status, reason, dateCreated, dateEffective, dateEnd, tokenOwner ):
    """
    Add a new resource row in Resources table

    :params:
      :attr:`resourceName`: string - name of the resource (DIRAC name)

      :attr:`resourceType`: string - ValidResourceType:
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`serviceType`: string - ValidServiceType:
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`siteName`: string - name of the site (DIRAC name). Can be NULL.

      :attr:`gridSiteName`: string - name of the site in GOC DB. Can be NULL.

      :attr:`status`: string - ValidStatus:
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`reason`: string - free

      :attr:`dateCreated`: datetime.datetime  or string - date when which the resource row is created

      :attr:`dateEffective`: datetime.datetime or string - date from which the resource status
      is effective

      :attr:`dateEnd`: datetime.datetime  or string - date from which the resource status
      ends to be effective

      :attr:`tokenOwner`: string - free
    """

    dateCreated, dateEffective, dateEnd = self.__usualChecks( dateCreated, dateEffective, dateEnd, status )

    if siteName is None:
      siteName = 'NULL'
    if gridSiteName is None:
      gridSiteName = 'NULL'

    req = "INSERT INTO Resources (ResourceName, ResourceType, ServiceType, SiteName, GridSiteName, "
    req = req + "Status, Reason, DateCreated, DateEffective, DateEnd, TokenOwner, TokenExpiration) "
    req = req + "VALUES ('%s', '%s', '%s', " %( resourceName, resourceType, serviceType )
    if siteName == 'NULL':
      req = req + "%s, " %siteName
    else:
      req = req + "'%s', " %siteName
    if gridSiteName == 'NULL':
      req = req + "%s, " %gridSiteName
    else:
      req = req + "'%s', " %gridSiteName
    req = req + "'%s', '%s', '%s', " %( status, reason, dateCreated )
    req = req + "'%s', '%s', '%s', '9999-12-31 23:59:59');" %( dateEffective, dateEnd, tokenOwner )

    resUpdate = self.db._update( req )
    if not resUpdate[ 'OK' ]:
      raise RSSDBException, where( self, self._addResourcesRow ) + resUpdate[ 'Message' ]


#############################################################################

  def _addResourcesHistoryRow( self, resourceName, status, reason, dateCreated,
                               dateEffective, dateEnd, tokenOwner ):
    """
    Add an old resource row in the ResourcesHistory table

    :params:
      :attr:`resourceName`: string - name of the resource (DIRAC name)

      :attr:`status`: string - ValidStatus:
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`reason`: string - free

      :attr:`dateCreated`: datetime.datetime  or string - date when which the resource row is created

      :attr:`dateEffective`: datetime.datetime  or string - date from which the resource status
      is effective

      :attr:`dateEnd`: datetime.datetime  or string - date from which the resource status ends
      to be effective

      :attr:`tokenOwner`: string - free
    """

    dateCreated, dateEffective, dateEnd = self.__usualChecks( dateCreated, dateEffective, dateEnd, status )

    req = "INSERT INTO ResourcesHistory (ResourceName, "
    req = req + " Status, Reason, DateCreated,"
    req = req + " DateEffective, DateEnd, TokenOwner) "
    req = req + "VALUES ('%s', " % ( resourceName )
    req = req + "'%s', '%s', '%s', " %( status, reason, dateCreated )
    req = req + "'%s', '%s', '%s');" %( dateEffective, dateEnd, tokenOwner )

    resUpdate = self.db._update( req )
    if not resUpdate[ 'OK' ]:
      raise RSSDBException, where( self, self._addResourcesHistoryRow ) + resUpdate[ 'Message' ]

#############################################################################

  def removeResource( self, resourceName = None, siteName = None, gridSiteName = None ):
    """
    Completely remove a resource from the Resources and ResourcesHistory tables.
    Also, remove the SEs of an SRM endpont.
    """

    if resourceName != None:
      self.removeStorageElement( resourceName = resourceName, access = 'Read' )
      self.removeStorageElement( resourceName = resourceName, access = 'Write' )

      req = "DELETE from Resources WHERE ResourceName = '%s';" % ( resourceName )
      resDel = self.db._update( req )
      if not resDel[ 'OK' ]:
        raise RSSDBException, where( self, self.removeResource ) + resDel[ 'Message' ]

      req = "DELETE from ResourcesHistory WHERE ResourceName = '%s';" % ( resourceName )
      resDel = self.db._update( req )
      if not resDel[ 'OK' ]:
        raise RSSDBException, where( self, self.removeResource ) + resDel[ 'Message' ]

    if siteName != None:

      req = "DELETE from Resources WHERE SiteName = '%s';" % ( siteName )
      resDel = self.db._update( req )
      if not resDel[ 'OK' ]:
        raise RSSDBException, where( self, self.removeResource ) + resDel[ 'Message' ]

    if gridSiteName != None:

      req = "DELETE from Resources WHERE GridSiteName = '%s';" % ( gridSiteName )
      resDel = self.db._update( req )
      if not resDel[ 'OK' ]:
        raise RSSDBException, where( self, self.removeResource ) + resDel[ 'Message' ]

#############################################################################

  def setMonitoredToBeChecked( self, monitoreds, granularity, name ):
    """
    Set LastCheckTime to 0 to monitored(s)

    :params:
      :attr:`monitoreds`: string, or a list of strings where each is a ValidRes:
      which granularity has to be set to be checked

      :attr:`granularity`: string, a ValidRes: from who this set comes

      :attr:`name`: string, name of Site or Resource
    """

    if type( monitoreds ) is not list:
      monitoreds = [ monitoreds ]

    for monitored in monitoreds:

      if monitored in ( 'Site', 'Sites' ):
        siteName = self.getGeneralName( name, granularity, monitored )
        siteName = ','.join( [ '"'+x.strip()+'"' for x in siteName ] )

        req = "UPDATE Sites SET LastCheckTime = '00000-00-00 00:00:00'"
        req = req + " WHERE SiteName IN (%s);" %( siteName )

      elif monitored in ( 'Service', 'Services' ):

        if granularity in ( 'Site', 'Sites' ):
          serviceName = self.getMonitoredsList( 'Service', paramsList = [ 'ServiceName' ],
                                                siteName = name )
          if type( serviceName ) is not list:
            serviceName = [ serviceName ]
          if serviceName == []:
            raise RSSDBException, where( self, self.setMonitoredToBeChecked ) + " No services for site %s" %name
          else:
            serviceName = [ x[0] for x in serviceName ]
            serviceName = ','.join( [ '"'+x.strip()+'"' for x in serviceName ] )
            req = "UPDATE Services SET LastCheckTime = '00000-00-00 00:00:00'"
            req = req + " WHERE ServiceName IN (%s);" %( serviceName )
        else:
          serviceName = self.getGeneralName( name, granularity, monitored )
          serviceName = ','.join( [ '"'+x.strip()+'"' for x in serviceName ] )

          req = "UPDATE Services SET LastCheckTime = '00000-00-00 00:00:00'"
          req = req + " WHERE ServiceName IN (%s);" %( serviceName )

      elif monitored in ( 'Resource', 'Resources' ):

        if granularity in ( 'Site', 'Sites' ):
          resourceName = self.getMonitoredsList( 'Resource', paramsList = [ 'ResourceName' ],
                                                 siteName = name )
          if type( resourceName ) is not list:
            resourceName = [ resourceName ]
          if resourceName == []:
            raise RSSDBException, where( self, self.setMonitoredToBeChecked ) + " No resources for site %s" %name
          else:
            resourceName = [ x[0] for x in resourceName ]
            resourceName = ','.join( [ '"'+x.strip()+'"' for x in resourceName ] )
            req = "UPDATE Resources SET LastCheckTime = '00000-00-00 00:00:00'"
            req = req + " WHERE ResourceName IN (%s);" %( resourceName )

        elif granularity in ( 'Service', 'Services' ):

          resourceName = self.getMonitoredsList( 'Resource', paramsList = [ 'ResourceName' ],
                                                 serviceName = name )
          if type( resourceName ) is not list:
            resourceName = [ resourceName ]
          if resourceName == []:
            raise RSSDBException, where( self, self.setMonitoredToBeChecked ) + " No resources for service %s" %name
          else:
            resourceName = [ x[0] for x in resourceName ]
            resourceName = ','.join( [ '"'+x.strip()+'"' for x in resourceName ] )
            req = "UPDATE Resources SET LastCheckTime = '00000-00-00 00:00:00'"
            req = req + " WHERE ResourceName IN (%s);" %( resourceName )


        elif granularity in ( 'StorageElementRead', 'StorageElementsRead' ):
          resourceName = self.getGeneralName( name, granularity, monitored )
          resourceName = ','.join( [ '"'+x.strip()+'"' for x in resourceName ] )

          req = "UPDATE Resources SET LastCheckTime = '00000-00-00 00:00:00'"
          req = req + " WHERE ResourceName IN (%s);" %( resourceName )

        elif granularity in ( 'StorageElementWrite', 'StorageElementsWrite'):
          resourceName = self.getGeneralName( name, granularity, monitored )
          resourceName = ','.join( [ '"'+x.strip()+'"' for x in resourceName ] )

          req = "UPDATE Resources SET LastCheckTime = '00000-00-00 00:00:00'"
          req = req + " WHERE ResourceName IN (%s);" %( resourceName )

      # Put read and write together here... too much fomr copy/paste
      elif monitored in ('StorageElementRead', 'StorageElementsRead', 'StorageElementWrite', 'StorageElementsWrite'):

        if monitored in ('StorageElementRead', 'StorageElementsRead'):
          SEtable = 'StorageElementsRead'
        elif monitored in ('StorageElementWrite', 'StorageElementsWrite'):
          SEtable = 'StorageElementsWrite'

        if granularity in ('Site', 'Sites'):
          SEName = self.getMonitoredsList( monitored, paramsList = [ 'StorageElementName' ],
                                           siteName = name )
          if type( SEName ) is not list:
            SEName = [ SEName ]
          if SEName == []:
            pass
#            raise RSSDBException, where(self, self.setMonitoredToBeChecked) + "No storage elements for site %s" %name
          else:
            SEName = [ x[0] for x in SEName ]
            SEName = ','.join( [ '"'+x.strip()+'"' for x in SEName ] )
            req = "UPDATE %s SET LastCheckTime = '00000-00-00 00:00:00'" % SEtable
            req = req + " WHERE StorageElementName IN (%s);" %( SEName )

        elif granularity in ( 'Resource', 'Resources' ):
          SEName = self.getMonitoredsList( monitored, paramsList = [ 'StorageElementName' ],
                                           resourceName = name )
          if type( SEName ) is not list:
            SEName = [ SEName ]
          if SEName == []:
            pass
#            raise RSSDBException, where(self, self.setMonitoredToBeChecked) + "No storage elements for resource %s" %name
          else:
            SEName = [ x[0] for x in SEName ]
            SEName = ','.join( [ '"'+x.strip()+'"' for x in SEName ] )
            req = "UPDATE %s SET LastCheckTime = '00000-00-00 00:00:00'" % SEtable
            req = req + " WHERE StorageElementName IN (%s);" %( SEName )

        elif granularity in ( 'Service', 'Services' ):
          SEName = self.getMonitoredsList( monitored, paramsList = [ 'StorageElementName' ],
                                           siteName = name.split('@').pop() )
          if type( SEName ) is not list:
            SEName = [ SEName ]
          if SEName == []:
            pass
#            raise RSSDBException, where(self, self.setMonitoredToBeChecked) + "No storage elements for service %s" %name
          else:
            SEName = [ x[0] for x in SEName ]
            SEName = ','.join( [ '"'+x.strip()+'"' for x in SEName ] )
            req = "UPDATE %s SET LastCheckTime = '00000-00-00 00:00:00'" % SEtable
            req = req + " WHERE StorageElementName IN (%s);" %( SEName )


      resUpdate = self.db._update( req )

      if not resUpdate[ 'OK' ]:
        raise RSSDBException, where( self, self.setMonitoredToBeChecked ) + resUpdate[ 'Message' ]


#############################################################################

  def getResourceStats( self, granularity, name ):
    """
    Returns simple statistics of active, probing, bad and banned resources of a site or service;

    :params:
      :attr:`granularity`: string: site or service

      :attr:`name`: string - name of site or service

    :returns:
      { 'Active':xx, 'Probing':yy, 'Bad':vv 'Banned':zz, 'Total':xyvz }
    """

    res = {'Active':0, 'Probing':0, 'Bad':0, 'Banned':0, 'Total':0}


    if granularity in ( 'Site', 'Sites' ):
      name   = self.getGridSiteName( granularity, name )
      DBname = 'GridSiteName'

    elif granularity in ( 'Service', 'Services' ):
      serviceType = name.split( '@' )[ 0 ]
      name        = name.split( '@' )[ 1 ]
      if serviceType == 'Computing':
        DBname = 'SiteName'

      else:
        name = self.getGridSiteName( 'Site', name )
        DBname = 'GridSiteName'


    req = "SELECT Status, COUNT(*) "
    req = req + "FROM Resources WHERE %s = '%s' " %( DBname, name )
    if granularity in ( 'Service', 'Services' ) and serviceType != 'Computing':
      req = req + "AND ServiceType = '%s' " %serviceType
    req = req + "GROUP BY Status"
    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.getResourceStats ) + resQuery[ 'Message' ]
    else:
      for x in resQuery[ 'Value' ]:
        res[ x[ 0 ] ] = int( x[ 1 ] )

    res[ 'Total' ] = sum( res.values() )

    return res

#############################################################################

  def getStorageElementsStats( self, granularity, name, access ):
    """
    Returns simple statistics of active, probing, bad and banned resources of a site or resource;

    :params:
      :attr:`granularity`: string: site or resource

      :attr:`name`: string - name of site or resource

      :attr:`access`: string: Read or Write

    :returns:
      { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
    """

    if access == 'Read':
      SEtable = 'StorageElementsRead'
    elif access == 'Write':
      SEtable = 'StorageElementsWrite'
    else:
      raise RSSException, where( self, self.getStorageElementsStats ) + 'Invalid access mode'

    res = {'Active':0, 'Probing':0, 'Bad':0, 'Banned':0, 'Total':0}

    if granularity in ( 'Site', 'Sites' ):
#      gridSiteName = self.getGridSiteName(granularity, name)
      req = "SELECT Status, COUNT(*)"
      req = req + " FROM %s WHERE GridSiteName = '%s' GROUP BY Status" % ( SEtable, name )
    elif granularity in ('Resource', 'Resources'):
      req = "SELECT Status, COUNT(*)"
      req = req + " FROM %s WHERE ResourceName = '%s' GROUP BY Status" % ( SEtable, name )
    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.getStorageElementsStats ) + resQuery[ 'Message' ]
    else:
      for x in resQuery[ 'Value' ]:
        res[ x[ 0 ] ] = int( x[ 1 ] )

    res[ 'Total' ] = sum( res.values() )

    return res

#############################################################################

#############################################################################
# StorageElement functions
#############################################################################

#############################################################################

  def setStorageElementStatus( self, storageElementName, status, reason, tokenOwner, access ):
    """
    Set a StorageElement status, effective from now, with no ending

    :params:
      :attr:`storageElementName`: string

      :attr:`status`: string. Possibilities:
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`reason`: string

      :attr:`tokenOwner`: string. For the service itself: `RS_SVC`

      :attr:`access`: string Read or Write
    """

    if access == 'Read':
      SEtable = 'StorageElementsRead'
    elif access == 'Write':
      SEtable = 'StorageElementsWrite'
    else:
      raise RSSException, where( self, self.setStorageElementStatus ) + 'Invalid access mode'

    req = "SELECT ResourceName, GridSiteName FROM %s WHERE StorageElementName = " % SEtable
    req = req + "'%s' AND DateEffective < UTC_TIMESTAMP();" %( storageElementName )
    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.setStorageElementStatus ) + resQuery[ 'Message' ]
    if not resQuery[ 'Value' ]:
      return None

    resourceName = resQuery[ 'Value' ][ 0 ][ 0 ]
    gridSiteName = resQuery[ 'Value' ][ 0 ][ 1 ]

    self.addOrModifyStorageElement( storageElementName, resourceName, gridSiteName, status,
                                    reason, datetime.datetime.utcnow().replace( microsecond = 0 ),
                                    tokenOwner, datetime.datetime( 9999, 12, 31, 23, 59, 59 ),
                                    access )

#############################################################################

  def addOrModifyStorageElement( self, storageElementName, resourceName, gridSiteName,
                                 status, reason, dateEffective, tokenOwner, dateEnd, access ):
    """
    Add or modify a storageElement to the StorageElements table.

    :params:
      :attr:`storageElementName`: string - name of the storageElement

      :attr:`resourceName`: string - name of the node

      :attr:`gridSiteName`: string - name of the site in GOC DB

      :attr:`status`: string - ValidStatus:
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`reason`: string - free

      :attr:`dateEffective`: datetime.datetime -
      date from which the storageElement status is effective

      :attr:`tokenOwner`: string - free

      :attr:`dateEnd`: datetime.datetime -
      date from which the storageElement status ends to be effective

      :attr:`access`: string - Read or Write
    """

    if access == 'Read':
      SEtable     = 'StorageElementsRead'
      granularity = 'StorageElementRead'
    elif access == 'Write':
      SEtable = 'StorageElementsWrite'
      granularity = 'StorageElementWrite'
    else:
      raise RSSException, where( self, self.addOrModifyStorageElement ) + 'Invalid access mode'

    dateCreated, dateEffective = self.__addOrModifyInit( dateEffective, dateEnd, status )

    #check if the storageElement is already there
    query = "SELECT StorageElementName FROM %s WHERE " % SEtable
    query = query + "StorageElementName='%s'" % storageElementName
    resQuery = self.db._query( query )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.addOrModifyStorageElement ) + resQuery[ 'Message' ]

    if resQuery[ 'Value' ]:
      if dateEffective <= ( dateCreated + datetime.timedelta( minutes=2 ) ):
        #storageElement modification, effective in less than 2 minutes
        self.setDateEnd( granularity, storageElementName, dateEffective )
        self.transact2History( granularity, storageElementName, dateEffective )
      else:
        self.setDateEnd( granularity, storageElementName, dateEffective )
    else:
      if status in ( 'Active', 'Probing', 'Bad' ):
        oldStatus = 'Banned'
      else:
        oldStatus = 'Active'
      self._addStorageElementHistoryRow( storageElementName, oldStatus, reason, dateCreated, dateEffective,
                                         datetime.datetime.utcnow().replace( microsecond = 0 ).isoformat( ' ' ),
                                         tokenOwner, access )

    #in any case add a row to present StorageElements table
    self._addStorageElementRow( storageElementName, resourceName, gridSiteName, status,
                                reason, dateCreated, dateEffective, dateEnd, tokenOwner,
                                access )

#############################################################################

  def _addStorageElementRow( self, storageElementName, resourceName, gridSiteName, status,
                             reason, dateCreated, dateEffective, dateEnd, tokenOwner, access ):
    """
    Add a new storageElement row in StorageElements table

    :params:
      :attr:`storageElementName`: string - name of the storageElement

      :attr:`resourceName`: string - name of the resource

      :attr:`gridSiteName`: string - name of the site (GOC DB name)

      :attr:`status`: string - ValidStatus:
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`reason`: string - free

      :attr:`dateCreated`: datetime.datetime or string - date when which the storageElement
      row is created

      :attr:`dateEffective`: datetime.datetime or string - date from which the storageElement
      status is effective

      :attr:`dateEnd`: datetime.datetime or string - date from which the storageElement status
      ends to be effective

      :attr:`tokenOwner`: string - free

      :attr:`access`: string - Read or Write
    """

    if access == 'Read':
      SEtable = 'StorageElementsRead'
    elif access == 'Write':
      SEtable = 'StorageElementsWrite'
    else:
      raise RSSException, where( self, self._addStorageElementRow ) + 'Invalid access mode'

    dateCreated, dateEffective, dateEnd = self.__usualChecks( dateCreated, dateEffective, dateEnd, status )

    if gridSiteName is None:
      gridSiteName = 'NULL'

    req = "INSERT INTO %s (StorageElementName, ResourceName, GridSiteName, " % SEtable
    req = req + "Status, Reason, DateCreated, DateEffective, DateEnd, TokenOwner, TokenExpiration) "
    req = req + "VALUES ('%s', '%s', " %( storageElementName, resourceName )
    if gridSiteName == 'NULL':
      req = req + "%s, " %gridSiteName
    else:
      req = req + "'%s', " %gridSiteName
    req = req + "'%s', '%s', '%s', " %( status, reason, dateCreated, )
    req = req + "'%s', '%s', '%s', '9999-12-31 23:59:59');" %( dateEffective, dateEnd, tokenOwner )

    resUpdate = self.db._update( req )
    if not resUpdate[ 'OK' ]:
      raise RSSDBException, where( self, self._addStorageElementRow ) + resUpdate[ 'Message' ]

#############################################################################

  def _addStorageElementHistoryRow( self, storageElementName, status,
                                    reason, dateCreated, dateEffective, dateEnd, tokenOwner, access ):
    """
    Add an old storageElement row in the StorageElementsHistory table

    :params:
      :attr:`storageElementName`: string - name of the storageElement

      :attr:`status`: string - ValidStatus:
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`reason`: string - free

      :attr:`dateCreated`: datetime.datetime or string -
      date when which the storageElement row is created

      :attr:`dateEffective`: datetime.datetime or string -
      date from which the storageElement status is effective

      :attr:`dateEnd`: datetime.datetime or string -
      date from which the storageElement status ends to be effective

      :attr:`tokenOwner`: string - free

      :attr:`access`: string - Read or Write
    """

    if access == 'Read':
      SEtable = 'StorageElementsReadHistory'
    elif access == 'Write':
      SEtable = 'StorageElementsWriteHistory'
    else:
      raise RSSException, where( self, self._addStorageElementHistoryRow ) + 'Invalid access mode'

    dateCreated, dateEffective, dateEnd = self.__usualChecks( dateCreated, dateEffective, dateEnd, status )

    req = "INSERT INTO %s (StorageElementName, " % SEtable
    req = req + "Status, Reason, DateCreated, DateEffective, DateEnd, TokenOwner) "
    req = req + "VALUES ('%s', '%s', " % ( storageElementName, status )
    req = req + "'%s', '%s', '%s', " %( reason, dateCreated, dateEffective )
    req = req + "'%s', '%s');" %( dateEnd, tokenOwner )

    resUpdate = self.db._update( req )
    if not resUpdate[ 'OK' ]:
      raise RSSDBException, where( self, self._addStorageElementHistoryRow ) + resUpdate[ 'Message' ]


#############################################################################

  def removeStorageElement( self, storageElementName = None, resourceName = None, access = None ):
    """
    Completely remove a storageElement from the StorageElements
    and StorageElementsHistory tables

    :params:
      :attr:`storageElementName`: string

      :attr:`resourceName`: string

      :attr:`access`: string - Read or Write
    """

    if access == 'Read':
      SEtable = 'StorageElementsRead'
    elif access == 'Write':
      SEtable = 'StorageElementsWrite'
    else:
      raise RSSException, where( self, self.removeStorageElement ) + 'Invalid access mode'

    if storageElementName != None:
      req = "DELETE from %s " % SEtable
      req = req + "WHERE StorageElementName = '%s';" % ( storageElementName )
      resDel = self.db._update( req )
      if not resDel[ 'OK' ]:
        raise RSSDBException, where( self, self.removeStorageElement ) + resDel[ 'Message' ]

      req = "DELETE from %sHistory" % SEtable
      req = req + " WHERE StorageElementName = '%s';" % ( storageElementName )
      resDel = self.db._update( req )
      if not resDel[ 'OK' ]:
        raise RSSDBException, where( self, self.removeStorageElement ) + resDel[ 'Message' ]

    if resourceName != None:
      req = "DELETE from %s WHERE ResourceName = '%s';" % ( SEtable, resourceName )
      resDel = self.db._update( req )
      if not resDel[ 'OK' ]:
        raise RSSDBException, where( self, self.removeStorageElement ) + resDel[ 'Message' ]

#############################################################################

#############################################################################
# GENERAL functions
#############################################################################

#############################################################################

  def addType( self, granularity, type_, description = '' ):
    """
    Add a site, service or resource type
    (T1, Computing, CE (different types also), SE, ...)

    :params:
      :attr:`granularity`: string - 'Site', 'Service', 'Resource'

      :attr:`serviceType`: string

      :attr:`description`: string, optional
    """

    DBtype, DBtable = self.__DBchoiceType( granularity )

    req = "INSERT INTO %s (%s, Description)" %( DBtable, DBtype )
    req = req + "VALUES ('%s', '%s');" % ( type_, description )

    resUpdate = self.db._update( req )
    if not resUpdate[ 'OK' ]:
      raise RSSDBException, where( self, self.addType ) + resUpdate[ 'Message' ]

#############################################################################

  def addOrModifyGridSite( self, name, tier ):
    """
    Add or modify a Grid Site to the GridSites table.

    :params:
      :attr:`name`: string - name of the site in GOC DB

      :attr:`tier`: string - tier of the site
    """

    if tier not in ValidSiteType:
      raise RSSDBException, "Not the right SiteType"

    req = "SELECT GridSiteName, GridTier FROM GridSites "
    req = req + "WHERE GridSiteName = '%s'" %( name )
    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.addOrModifyGridSite ) + resQuery[ 'Message' ]

    if resQuery[ 'Value' ]:
      req = "UPDATE GridSites SET GridTier = '%s' WHERE GridSiteName = '%s'" %( tier, name )

      resUpdate = self.db._update( req )
      if not resUpdate[ 'OK' ]:
        raise RSSDBException, where( self, self.addOrModifyGridSite ) + resUpdate[ 'Message' ]
    else:
      req = "INSERT INTO GridSites (GridSiteName, GridTier) VALUES ('%s', '%s')" %( name, tier )

      resUpdate = self.db._update( req )
      if not resUpdate[ 'OK' ]:
        raise RSSDBException, where( self, self.addOrModifyGridSite ) + resUpdate[ 'Message' ]

#############################################################################

  def getGridSitesList( self, paramsList = None, gridSiteName = None, gridTier = None ):
    """
    Get grid site lists.

    :params:
      :attr:`paramsList`: a list of parameters can be entered. If not given,
      a custom list is used.

      :attr:`gridSiteName` grid site name. If not given, fetch all.

      :attr:`gridTier`: a string or a list representing the site type.
      If not given, fetch all.

    :return:
      list of gridSites paramsList's values
    """

    #paramsList
    if (paramsList == None or paramsList == []):
      params = 'GridSiteName, GridTier'
    else:
      if type( paramsList ) is not type( [] ):
        paramsList = [ paramsList ]
      params = ','.join( [ x.strip()+' ' for x in paramsList ] )

    #gridSiteName
    if ( gridSiteName == None or gridSiteName == [] ):
      r = "SELECT GridSiteName FROM GridSites"
      resQuery = self.db._query( r )
      if not resQuery[ 'OK' ]:
        raise RSSDBException, where( self, self.getMonitoredsList )+resQuery[ 'Message' ]
      if not resQuery[ 'Value' ]:
        gridSiteName = []
      gridSiteName = [ x[0] for x in resQuery['Value'] ]
      gridSiteName = ','.join( [ '"'+x.strip()+'"' for x in gridSiteName ] )
    else:
      if type( gridSiteName ) is not type( [] ):
        gridSiteName = [ gridSiteName ]
      gridSiteName = ','.join( [ '"'+x.strip()+'"' for x in gridSiteName ] )

    #gridTier
    if ( gridTier == None or gridTier == [] ):
      gridTier = ValidSiteType
    else:
      if type( gridTier ) is not type([]):
        gridTier = [ gridTier ]
    gridTier = ','.join( [ '"'+x.strip()+'"' for x in gridTier ] )

    #query construction
    req = "SELECT %s FROM GridSites WHERE" %( params )
    if gridSiteName != [] and gridSiteName != None and gridSiteName is not None and gridSiteName != '':
      req = req + " GridSiteName IN (%s) " %( gridSiteName )
    req = req + " AND GridTier IN (%s)" % ( gridTier )

    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.getMonitoredsList )+resQuery[ 'Message' ]
    if not resQuery[ 'Value' ]:
      return []
    list_ = []
    list_ = [ x for x in resQuery[ 'Value' ] ]
    return list_

##############################################################################

  def removeRow( self, fromWhere, name, dateEffective = None ):
    """
    Remove a row from one of the tables

    :params:
      :attr:`fromWhere`: string, a ValidRes
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`name`: string

      :attr:`dateEffective`: string or datetime.datetime
    """

    if dateEffective is not None:
      if not isinstance( dateEffective, basestring ):
        dateEffective = dateEffective.isoformat( ' ' )

    DBtable, DBname = self.__DBchoice( fromWhere )

    req = "DELETE from %s WHERE %s = '%s'" % ( DBtable, DBname, name )
    if dateEffective is not None:
      req = req + " AND DateEffective = '%s'" %( dateEffective )
    resDel = self.db._update( req )
    if not resDel[ 'OK' ]:
      raise RSSDBException, where( self, self.removeRow ) + resDel[ 'Message' ]

#############################################################################

  def getTypesList( self, granularity, type_=None ):
    """
    Get list of site, resource, service types with description

    :Params:
      :attr:`type`: string, the type.
    """

    DBtype, DBtable = self.__DBchoiceType(granularity)

    if type_ == None:
      req = "SELECT %s FROM %s" %( DBtype, DBtable )
    else:
      req = "SELECT %s, Description FROM %s " %( DBtype, DBtable )
      req = req + "WHERE %s = '%s'" % ( DBtype, type_ )

    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.getTypesList ) + resQuery[ 'Message' ]
    if not resQuery[ 'Value' ]:
      return []
#    typeList = []
    typeList = [ x[0] for x in resQuery[ 'Value' ] ]
    return typeList

#############################################################################

  def removeType( self, granularity, type_ ):
    """
    Remove a type from the DB

    :params:
      :attr:`type`: string, a type (see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`)
    """

    DBtype, DBtable = self.__DBchoiceType( granularity )

    req = "DELETE from %s WHERE %s = '%s';" % ( DBtable, DBtype, type_ )
    resDel = self.db._update( req )
    if not resDel[ 'OK' ]:
      raise RSSDBException, where( self, self.removeType ) + resDel[ 'Message' ]

#############################################################################

  def getStatusList( self ):
    """
    Get list of status with no descriptions.
    """

    req = "SELECT Status from Status"

    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.getStatusList ) + resQuery[ 'Message' ]
    if not resQuery[ 'Value' ]:
      return []
    l = [ x[0] for x in resQuery[ 'Value' ] ]
    return l

#############################################################################

  def getGeneralName( self, name, from_g, to_g ):
    """
    Get name of res, of granularity `from_g`, to the name of res with granularity `to_g`

    For a StorageElement, get the Site name, or the Service name, or the Resource name.
    For a Resource, get the Site name, or the Service name.
    For a Service name, get the Site name

    :params:
      :attr:`name`: a string with a name

      :attr:`from_g`: a string with a valid granularity
      (see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`)

      :attr:`to_g`: a string with a valid granularity
      (see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`)

    :return:
      a string with the resulting name
    """

    if from_g in ( 'Service', 'Services' ):
      req = "SELECT SiteName FROM Services WHERE ServiceName = '%s'" %( name )

    elif from_g in ( 'Resource', 'Resources' ):
      reqType = "SELECT ServiceType FROM Resources WHERE ResourceName = '%s'" %( name )
      resQuery = self.db._query( reqType )
      if not resQuery[ 'OK' ]:
        raise RSSDBException, where( self, self.getGeneralName ) + resQuery[ 'Message' ]
      serviceType = resQuery[ 'Value' ][ 0 ][ 0 ]

      if serviceType == 'Computing':
        req = "SELECT SiteName FROM Resources WHERE ResourceName = '%s'" %( name )
      else:
        req = "SELECT SiteName FROM Sites WHERE GridSiteName = "
        req = req + "(SELECT GridSiteName FROM Resources WHERE ResourceName = '%s')" %( name )

    elif from_g in ( 'StorageElementRead', 'StorageElementsRead' ):

      if to_g in ( 'Resource', 'Resources' ):
        req = "SELECT ResourceName FROM StorageElementsRead WHERE StorageElementName = '%s'" % ( name )
      else:
        req = "SELECT SiteName FROM Sites WHERE GridSiteName = "
        req = req + "(SELECT GridSiteName FROM StorageElementsRead WHERE StorageElementName = '%s')" % ( name )

        if to_g in ( 'Service', 'Services' ):
          serviceType = 'Storage'

    elif from_g in ( 'StorageElementWrite', 'StorageElementsWrite' ):

      if to_g in ( 'Resource', 'Resources' ):
        req = "SELECT ResourceName FROM StorageElementsWrite WHERE StorageElementName = '%s'" % ( name )
      else:
        req = "SELECT SiteName FROM Sites WHERE GridSiteName = "
        req = req + "(SELECT GridSiteName FROM StorageElementsWrite WHERE StorageElementName = '%s')" % ( name )

        if to_g in ( 'Service', 'Services' ):
          serviceType = 'Storage'
    else:
      raise ValueError

    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.getGeneralName ) + resQuery[ 'Message' ]
    if not resQuery[ 'Value' ]:
      return []
    newNames = [ x[0] for x in resQuery[ 'Value' ] ]

    if to_g in ( 'Service', 'Services' ):
      return [ serviceType + '@' + x for x in newNames ]
    else:
      return newNames

#############################################################################

  def getGridSiteName( self, granularity, name ):

    DBtable, DBname = self.__DBchoice( granularity )

    req = "SELECT GridSiteName FROM %s WHERE %s = '%s'" %( DBtable, DBname, name )

    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.getGridSiteName ) + resQuery[ 'Message' ]
    if not resQuery[ 'Value' ]:
      return []

    return resQuery[ 'Value' ][ 0 ][ 0 ]

#############################################################################

  def getEndings( self, table ):
    """ get list of rows from table(s) that end to be effective
    """

    #getting primary key for table
    req = "SELECT k.column_name FROM information_schema.table_constraints t "
    req = req + "JOIN information_schema.key_column_usage k "
    req = req + "USING(constraint_name,table_schema,table_name) "
    req = req + "WHERE t.constraint_type='PRIMARY KEY' "
    req = req + "AND t.table_schema='ResourceStatusDB' AND t.table_name='%s';" %( table )
    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.getEndings )
    else:
      PKList = [ x[0] for x in resQuery[ 'Value' ]]
      if len( PKList ) == 1:
        req = "SELECT %s FROM %s " %( PKList[0], table )
        req = req + "WHERE TIMESTAMP(DateEnd) < UTC_TIMESTAMP();"
      elif len( PKList ) == 2:
        req = "SELECT %s, %s FROM %s " %( PKList[0], PKList[1], table )
        req = req + "WHERE TIMESTAMP(DateEnd) < UTC_TIMESTAMP();"
      elif len( PKList ) == 3:
        req = "SELECT %s, %s, %s FROM %s " %( PKList[0], PKList[1], PKList[2], table )
        req = req + "WHERE TIMESTAMP(DateEnd) < UTC_TIMESTAMP();"
      resQuery = self.db._query( req )
      if not resQuery[ 'OK' ]:
        raise RSSDBException, where( self, self.getEndings ) + resQuery[ 'Message' ]
      else:
        list_ = []
        list_ = [ int(x[0]) for x in resQuery['Value'] ]
        return list_


#############################################################################

  def getPeriods( self, granularity, name, status, hours = None, days = None ):
    """
    Get list of periods of times when a ValidRes was in ValidStatus
    (see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`)
    for a total of `hours` or `days`.
    :params:
      :attr:`granularity`: string - ValidRes

      :attr:`name`: string - name

      :attr:`status`: string - ValidStatus

      :attr:`hours`: integer

      :attr:`days`: integer

    :return:
      list of periods of time as tuples in string format
    """

    if days is not None:
      hours = 24*days

    hours = datetime.timedelta( hours = hours )

    if granularity in ( 'Site', 'Sites' ):
      req = "SELECT DateEffective FROM Sites WHERE SiteName = '%s' AND DateEffective < UTC_TIMESTAMP() AND Status = '%s'" %( name, status )
    elif granularity in ( 'Service', 'Services' ):
      req = "SELECT DateEffective FROM Services WHERE ServiceName = '%s' AND DateEffective < UTC_TIMESTAMP() AND Status = '%s'" %( name, status )
    elif granularity in ( 'Resource', 'Resources' ):
      req = "SELECT DateEffective FROM Resources WHERE ResourceName = '%s' AND DateEffective < UTC_TIMESTAMP() AND Status = '%s'" %( name, status )
    elif granularity in ( 'StorageElementRead', 'StorageElementsRead' ):
      req = "SELECT DateEffective FROM StorageElementsRead WHERE StorageElementName = '%s' AND DateEffective < UTC_TIMESTAMP() AND Status = '%s'" %( name, status )
    elif granularity in ( 'StorageElementWrite', 'StorageElementsWrite' ):
      req = "SELECT DateEffective FROM StorageElementsWrite WHERE StorageElementName = '%s' AND DateEffective < UTC_TIMESTAMP() AND Status = '%s'" %( name, status )

    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.getPeriods ) + resQuery[ 'Message' ]
    else:
      if resQuery[ 'Value' ] == '':
        return None
      elif resQuery[ 'Value' ] == ():
        #actual status is not what was requested
        periods = []
        timeInStatus = datetime.timedelta(0)
      else:
        #actual status is what was requested
        effFrom = resQuery[ 'Value' ][ 0 ][ 0 ]
        timeInStatus = datetime.datetime.utcnow().replace( microsecond = 0 ) - effFrom

        if timeInStatus > hours:
          return [((datetime.datetime.utcnow().replace(microsecond = 0)-hours).isoformat(' '), datetime.datetime.utcnow().replace(microsecond = 0).isoformat(' '))]

        periods = [(effFrom.isoformat(' '), datetime.datetime.utcnow().replace(microsecond = 0).isoformat(' '))]

      if granularity in ('Site', 'Sites'):
        req = "SELECT DateEffective, DateEnd FROM SitesHistory WHERE "
        req = req + "SiteName = '%s' AND Status = '%s'" %(name, status)
      elif granularity in ('Resource', 'Resources'):
        req = "SELECT DateEffective, DateEnd FROM ResourcesHistory WHERE "
        req = req + "ResourceName = '%s' AND Status = '%s'" %(name, status)
      elif granularity in ('Service', 'Services'):
        req = "SELECT DateEffective, DateEnd FROM ServicesHistory WHERE "
        req = req + "ServiceName = '%s' AND Status = '%s'" %(name, status)
      elif granularity in ('StorageElementRead', 'StorageElementsRead'):
        req = "SELECT DateEffective, DateEnd FROM StorageElementsReadHistory "
        req = req + "WHERE StorageElementName = '%s' AND Status = '%s'" %(name, status)
      elif granularity in ('StorageElementWrite', 'StorageElementsWrite'):
        req = "SELECT DateEffective, DateEnd FROM StorageElementsWriteHistory "
        req = req + "WHERE StorageElementName = '%s' AND Status = '%s'" %(name, status)

      resQuery = self.db._query(req)
      if not resQuery['OK']:
        raise RSSDBException, where(self, self.getPeriods) + resQuery['Message']
      else:
        for x in range(len(resQuery['Value'])):
          i = len(resQuery['Value']) - x
          effFrom = resQuery['Value'][i-1][0]
          effTo = resQuery['Value'][i-1][1]
          oldTimeInStatus = timeInStatus
          timeInStatus = timeInStatus + (effTo - effFrom)
          if timeInStatus > hours:
            periods.append(((effTo - (hours - oldTimeInStatus)).isoformat(' '),
                            effTo.isoformat(' ')))
            return periods
          else:
            periods.append((effFrom.isoformat(' '), effTo.isoformat(' ')))
      return periods

#############################################################################

  def getTablesWithHistory( self ):
    """
    Get list of tables with associated an history table
    """

    tablesList=[]
    req = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
    req = req + "WHERE TABLE_SCHEMA = 'ResourceStatusDB' AND TABLE_NAME LIKE \"%History\"";
    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.getTablesWithHistory ) + resQuery[ 'Message' ]
    else:
      HistoryTablesList = [ x[0] for x in resQuery[ 'Value' ] ]
      for x in HistoryTablesList:
        tablesList.append( x[0:len(x)-7] )
      return tablesList

#############################################################################

  def getServiceStats( self, siteName ):
    """
    Returns simple statistics of active, probing, bad and banned services of a site;

    :params:
      :attr:`siteName`: string - a site name

    :returns:
      { 'Active':xx, 'Probing':yy, 'Bad':vv, 'Banned':zz, 'Total':xyz }
    """

    res = {'Active':0, 'Probing':0, 'Bad':0,'Banned':0, 'Total':0}

    req = "SELECT Status, COUNT(*) FROM Services WHERE SiteName = '%s' GROUP BY Status" %siteName
    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.getServiceStats ) + resQuery[ 'Message' ]
    else:
      for x in resQuery[ 'Value' ]:
        res[x[0]] = int(x[1])

    res['Total'] = sum( res.values() )

    return res

#############################################################################


  def transact2History( self, *args ):
    """
    Transact a row from a Sites or Service or Resources table to history.
    Does not do a real transaction in terms of DB.

    :params:
      :attr: a tuple with info on what to transact

    Examples of possible way to call it:

    >>> trasact2History(('Site', 'LCG.CERN.ch',
          datetime.datetime.utcnow().replace(microsecond = 0).isoformat(' '))
          - the date is the DateEffective parameter
        trasact2History(('Site', 523)) - the number if the SiteID
        trasact2History(('Service', 'Computing@LCG.CERN.ch', 'LCG.CERN.ch',
          datetime.datetime.utcnow().replace(microsecond = 0).isoformat(' '))
          - the date is the DateEffective parameter
        trasact2History(('Service', 523)) - the number if the ServiceID
        trasact2History(('Resource', 'srm-lhcb.cern.ch', 'Computing@LCG.CERN.ch',
          'LCG.CERN.ch', datetime.datetime.utcnow().replace(microsecond = 0).isoformat(' '))
          - the date is the DateEffective parameter
        trasact2History(('Resource', 523)) - the number if the ResourceID
        trasact2History(('StorageElement', 'CERN-RAW', 'srm-lhcb.cern.ch',
          'LCG.CERN.ch', datetime.datetime.utcnow().replace(microsecond = 0).isoformat(' '))
          - the date is the DateEffective parameter
        trasact2History(('StorageElement', 523)) - the number if the StorageElementID
    """

    if args[ 0 ] in ('Site', 'Sites'):
      #get row to be put in history Sites table
      if len( args ) == 3:
        req = "SELECT Status, Reason, DateCreated, "
        req = req + "DateEffective, DateEnd, TokenOwner from Sites "
        req = req + "WHERE (SiteName='%s' AND DateEffective < '%s');" % ( args[ 1 ], args[ 2 ] )
        resQuery = self.db._query( req )

        if not resQuery[ 'OK' ]:
          raise RSSDBException, where( self, self.transact2History ) + resQuery[ 'Message' ]
        if not resQuery[ 'Value' ]:
          return None
        oldStatus        = resQuery[ 'Value' ][ 0 ][ 0 ]
        oldReason        = resQuery[ 'Value' ][ 0 ][ 1 ]
        oldDateCreated   = resQuery[ 'Value' ][ 0 ][ 2 ]
        oldDateEffective = resQuery[ 'Value' ][ 0 ][ 3 ]
        oldDateEnd       = resQuery[ 'Value' ][ 0 ][ 4 ]
        oldTokenOwner    = resQuery[ 'Value' ][ 0 ][ 5 ]

        #start "transaction" to history -- should be better to use a real transaction
        self._addSiteHistoryRow( args[ 1 ], oldStatus, oldReason, oldDateCreated,
                                 oldDateEffective, oldDateEnd, oldTokenOwner )
        self.removeRow( args[ 0 ], args[ 1 ], oldDateEffective )

      elif len( args ) == 2:
        req = "SELECT SiteName, Status, Reason, DateCreated, "
        req = req + "DateEffective, DateEnd, TokenOwner from Sites "
        req = req + "WHERE (SiteID='%s');" % ( args[ 1 ] )
        resQuery = self.db._query( req )

        if not resQuery[ 'OK' ]:
          raise RSSDBException, where( self, self.transact2History ) + resQuery[ 'Message' ]
        if not resQuery[ 'Value' ]:
          return None

        siteName         = resQuery[ 'Value' ][ 0 ][ 0 ]
        oldStatus        = resQuery[ 'Value' ][ 0 ][ 1 ]
        oldReason        = resQuery[ 'Value' ][ 0 ][ 2 ]
        oldDateCreated   = resQuery[ 'Value' ][ 0 ][ 3 ]
        oldDateEffective = resQuery[ 'Value' ][ 0 ][ 4 ]
        oldDateEnd       = resQuery[ 'Value' ][ 0 ][ 5 ]
        oldTokenOwner    = resQuery[ 'Value' ][ 0 ][ 6 ]

        #start "transaction" to history -- should be better to use a real transaction
        self._addSiteHistoryRow( siteName, oldStatus, oldReason, oldDateCreated,
                                 oldDateEffective, oldDateEnd, oldTokenOwner )
        self.removeRow( args[ 0 ], siteName, oldDateEffective )


    if args[ 0 ] in ( 'Service', 'Services' ):
      #get row to be put in history Services table
      if len( args ) == 3:
        req = "SELECT Status, Reason, DateCreated, "
        req = req + "DateEffective, DateEnd, TokenOwner from Services "
        req = req + "WHERE (ServiceName='%s' AND DateEffective < '%s');" % ( args[ 1 ], args[ 2 ] )
        resQuery = self.db._query( req )

        if not resQuery[ 'OK' ]:
          raise RSSDBException, where( self, self.transact2History ) + resQuery[ 'Message' ]
        if not resQuery[ 'Value' ]:
          return None

        oldStatus        = resQuery[ 'Value' ][ 0 ][ 0 ]
        oldReason        = resQuery[ 'Value' ][ 0 ][ 1 ]
        oldDateCreated   = resQuery[ 'Value' ][ 0 ][ 2 ]
        oldDateEffective = resQuery[ 'Value' ][ 0 ][ 3 ]
        oldDateEnd       = resQuery[ 'Value' ][ 0 ][ 4 ]
        oldTokenOwner    = resQuery[ 'Value' ][ 0 ][ 5 ]

        #start "transaction" to history -- should be better to use a real transaction
        self._addServiceHistoryRow( args[ 1 ], oldStatus, oldReason,
                                    oldDateCreated, oldDateEffective, oldDateEnd,
                                    oldTokenOwner )
        self.removeRow( args[ 0 ], args[ 1 ], oldDateEffective )

      elif len( args ) == 2:
        req = "SELECT ServiceName, Status, Reason, DateCreated, "
        req = req + "DateEffective, DateEnd, TokenOwner from Services "
        req = req + "WHERE (ServiceID='%s');" % ( args[ 1 ] )
        resQuery = self.db._query( req )

        if not resQuery[ 'OK' ]:
          raise RSSDBException, where( self, self.transact2History ) + resQuery[ 'Message' ]
        if not resQuery[ 'Value' ]:
          return None

        serviceName      = resQuery[ 'Value' ][ 0 ][ 0 ]
        oldStatus        = resQuery[ 'Value' ][ 0 ][ 1 ]
        oldReason        = resQuery[ 'Value' ][ 0 ][ 2 ]
        oldDateCreated   = resQuery[ 'Value' ][ 0 ][ 3 ]
        oldDateEffective = resQuery[ 'Value' ][ 0 ][ 4 ]
        oldDateEnd       = resQuery[ 'Value' ][ 0 ][ 5 ]
        oldTokenOwner    = resQuery[ 'Value' ][ 0 ][ 6 ]

        #start "transaction" to history -- should be better to use a real transaction
        self._addServiceHistoryRow( serviceName, oldStatus, oldReason,
                                    oldDateCreated, oldDateEffective, oldDateEnd,
                                    oldTokenOwner )
        self.removeRow( args[ 0 ], serviceName, oldDateEffective )


    if args[ 0 ] in ( 'Resource', 'Resources' ):
      if len( args ) == 3:
        req = "SELECT Status, Reason, DateCreated, "
        req = req + "DateEffective, DateEnd, TokenOwner from Resources "
        req = req + "WHERE (ResourceName='%s' AND DateEffective < '%s' );" % ( args[ 1 ], args[ 2 ] )
        resQuery = self.db._query( req )

        if not resQuery[ 'OK' ]:
          raise RSSDBException, where( self, self.transact2History ) + resQuery[ 'Message' ]
        if not resQuery[ 'Value' ]:
          return None

        oldStatus        = resQuery[ 'Value' ][ 0 ][ 0 ]
        oldReason        = resQuery[ 'Value' ][ 0 ][ 1 ]
        oldDateCreated   = resQuery[ 'Value' ][ 0 ][ 2 ]
        oldDateEffective = resQuery[ 'Value' ][ 0 ][ 3 ]
        oldDateEnd       = resQuery[ 'Value' ][ 0 ][ 4 ]
        oldTokenOwner    = resQuery[ 'Value' ][ 0 ][ 5 ]

        self._addResourcesHistoryRow( args[ 1 ], oldStatus, oldReason,
                                      oldDateCreated, oldDateEffective, oldDateEnd,
                                      oldTokenOwner )
        self.removeRow( args[ 0 ], args[ 1 ], oldDateEffective )

      elif len( args ) == 2:
        req = "SELECT ResourceName, Status, Reason, DateCreated, "
        req = req + "DateEffective, DateEnd, TokenOwner from Resources "
        req = req + "WHERE (ResourceID='%s');" % ( args[ 1 ] )
        resQuery = self.db._query( req )

        if not resQuery[ 'OK' ]:
          raise RSSDBException, where( self, self.transact2History ) + resQuery[ 'Message' ]
        if not resQuery[ 'Value' ]:
          return None

        resourceName     = resQuery[ 'Value' ][ 0 ][ 0 ]
        oldStatus        = resQuery[ 'Value' ][ 0 ][ 1 ]
        oldReason        = resQuery[ 'Value' ][ 0 ][ 2 ]
        oldDateCreated   = resQuery[ 'Value' ][ 0 ][ 3 ]
        oldDateEffective = resQuery[ 'Value' ][ 0 ][ 4 ]
        oldDateEnd       = resQuery[ 'Value' ][ 0 ][ 5 ]
        oldTokenOwner    = resQuery[ 'Value' ][ 0 ][ 6 ]

        #start "transaction" to history -- should be better to use a real transaction
        self._addResourcesHistoryRow( resourceName, oldStatus,
                                      oldReason, oldDateCreated, oldDateEffective,
                                      oldDateEnd, oldTokenOwner )
        self.removeRow( args[ 0 ], resourceName, oldDateEffective )

    if args[ 0 ] in ( 'StorageElementRead', 'StorageElementsRead' ):
      if len( args ) == 3:
        req = "SELECT Status, Reason, DateCreated, "
        req = req + "DateEffective, DateEnd, TokenOwner from StorageElementsRead "
        req = req + "WHERE (StorageElementName='%s' AND DateEffective < '%s' );" % ( args[ 1 ], args[ 2 ] )
        resQuery = self.db._query( req )

        if not resQuery[ 'OK' ]:
          raise RSSDBException, where( self, self.transact2History ) + resQuery[ 'Message' ]
        if not resQuery[ 'Value' ]:
          return None

        oldStatus        = resQuery[ 'Value' ][ 0 ][ 0 ]
        oldReason        = resQuery[ 'Value' ][ 0 ][ 1 ]
        oldDateCreated   = resQuery[ 'Value' ][ 0 ][ 2 ]
        oldDateEffective = resQuery[ 'Value' ][ 0 ][ 3 ]
        oldDateEnd       = resQuery[ 'Value' ][ 0 ][ 4 ]
        oldTokenOwner    = resQuery[ 'Value' ][ 0 ][ 5 ]

        self._addStorageElementHistoryRow( args[ 1 ], oldStatus, oldReason,
                                           oldDateCreated, oldDateEffective, oldDateEnd,
                                           oldTokenOwner, 'Read' )
        self.removeRow( args[ 0 ], args[ 1 ], oldDateEffective )

      elif len( args ) == 2:
        req = "SELECT StorageElementName, Status, Reason, "
        req = req + "DateCreated, DateEffective, DateEnd, TokenOwner from StorageElementsRead "
        req = req + "WHERE (StorageElementID='%s');" % ( args[ 1 ] )
        resQuery = self.db._query( req )

        if not resQuery[ 'OK' ]:
          raise RSSDBException, where( self, self.transact2History ) + resQuery[ 'Message' ]
        if not resQuery[ 'Value' ]:
          return None

        storageElementName = resQuery[ 'Value' ][ 0 ][ 0 ]
        oldStatus          = resQuery[ 'Value' ][ 0 ][ 1 ]
        oldReason          = resQuery[ 'Value' ][ 0 ][ 2 ]
        oldDateCreated     = resQuery[ 'Value' ][ 0 ][ 3 ]
        oldDateEffective   = resQuery[ 'Value' ][ 0 ][ 4 ]
        oldDateEnd         = resQuery[ 'Value' ][ 0 ][ 5 ]
        oldTokenOwner      = resQuery[ 'Value' ][ 0 ][ 6 ]

        #start "transaction" to history -- should be better to use a real transaction
        self._addStorageElementHistoryRow( storageElementName, oldStatus, oldReason, oldDateCreated,
                                           oldDateEffective, oldDateEnd, oldTokenOwner, 'Read' )
        self.removeRow( args[ 0 ], storageElementName, oldDateEffective )

    if args[ 0 ] in ( 'StorageElementWrite', 'StorageElementsWrite' ):
      if len( args ) == 3:
        req = "SELECT Status, Reason, DateCreated, "
        req = req + "DateEffective, DateEnd, TokenOwner from StorageElementsWrite "
        req = req + "WHERE (StorageElementName='%s' AND DateEffective < '%s' );" % ( args[ 1 ], args[ 2 ] )
        resQuery = self.db._query( req )

        if not resQuery[ 'OK' ]:
          raise RSSDBException, where( self, self.transact2History ) + resQuery[ 'Message' ]
        if not resQuery[ 'Value' ]:
          return None

        oldStatus        = resQuery[ 'Value' ][ 0 ][ 0 ]
        oldReason        = resQuery[ 'Value' ][ 0 ][ 1 ]
        oldDateCreated   = resQuery[ 'Value' ][ 0 ][ 2 ]
        oldDateEffective = resQuery[ 'Value' ][ 0 ][ 3 ]
        oldDateEnd       = resQuery[ 'Value' ][ 0 ][ 4 ]
        oldTokenOwner    = resQuery[ 'Value' ][ 0 ][ 5 ]

        self._addStorageElementHistoryRow( args[ 1 ], oldStatus, oldReason,
                                           oldDateCreated, oldDateEffective, oldDateEnd,
                                           oldTokenOwner, 'Write' )
        self.removeRow( args[ 0 ], args[ 1 ], oldDateEffective )

      elif len( args ) == 2:
        req = "SELECT StorageElementName, Status, Reason, "
        req = req + "DateCreated, DateEffective, DateEnd, TokenOwner from StorageElementsWrite "
        req = req + "WHERE (StorageElementID='%s');" % ( args[ 1 ] )
        resQuery = self.db._query( req )

        if not resQuery[ 'OK' ]:
          raise RSSDBException, where( self, self.transact2History ) + resQuery[ 'Message' ]
        if not resQuery[ 'Value' ]:
          return None

        storageElementName = resQuery[ 'Value' ][ 0 ][ 0 ]
        oldStatus          = resQuery[ 'Value' ][ 0 ][ 1 ]
        oldReason          = resQuery[ 'Value' ][ 0 ][ 2 ]
        oldDateCreated     = resQuery[ 'Value' ][ 0 ][ 3 ]
        oldDateEffective   = resQuery[ 'Value' ][ 0 ][ 4 ]
        oldDateEnd         = resQuery[ 'Value' ][ 0 ][ 5 ]
        oldTokenOwner      = resQuery[ 'Value' ][ 0 ][ 6 ]

        #start "transaction" to history -- should be better to use a real transaction
        self._addStorageElementHistoryRow( storageElementName, oldStatus, oldReason, oldDateCreated,
                                           oldDateEffective, oldDateEnd, oldTokenOwner, 'Write' )
        self.removeRow( args[ 0 ], storageElementName, oldDateEffective )


#############################################################################

  def setDateEnd( self, granularity, name, dateEffective ):
    """
    Set date end, for a Site or for a Resource

    :params:
      :attr:`granularity`: a ValidRes.

      :attr:`name`: string, the name of the ValidRes

      :attr:`dateEffective`: a datetime.datetime
    """

    DBtable, DBname = self.__DBchoice( granularity )

    query = "UPDATE %s SET DateEnd = '%s' " % ( DBtable, dateEffective )
    query = query + "WHERE %s = '%s' AND DateEffective < '%s'" %( DBname, name, dateEffective )
    resUpdate = self.db._update( query )
    if not resUpdate[ 'OK' ]:
      raise RSSDBException, where( self, self.setDateEnd ) + resUpdate[ 'Message' ]

#############################################################################

  #usata solo nell'handler
  def addStatus( self, status, description='' ):
    """
    Add a status.

    :params:
      :attr:`status`: string - a new status

      :attr:`description`: string - optional description
    """

    req = "INSERT INTO Status (Status, Description)"
    req = req + "VALUES ('%s', '%s');" % ( status, description )

    resUpdate = self.db._update( req )
    if not resUpdate[ 'OK' ]:
      raise RSSDBException, where( self, self.addStatus ) + resUpdate[ 'Message' ]

#############################################################################

  #usata solo nell'handler
  def removeStatus( self, status ):
    """
    Remove a status from the Status table.

    :params:
      :attr:`status`: string - status
    """

    req = "DELETE from Status WHERE Status = '%s';" % ( status )
    resDel = self.db._update( req )
    if not resDel[ 'OK' ]:
      raise RSSDBException, where( self, self.removeStatus ) + resDel[ 'Message' ]

#############################################################################

  def getCountries( self, granularity ):
    """
    Get countries of resources in granularity

    :params:
      :attr:`granularity`: string - a ValidRes
    """

    DBtable, DBname = self.__DBchoice( granularity )

    if granularity in ( 'StorageElementRead', 'StorageElementsRead', 'StorageElementWrite', 'StorageElementsWrite' ):
      DBname = "SiteName"
      DBtable = "Sites"

    req = "SELECT %s FROM %s" %( DBname, DBtable )
    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.getCountries ) + resQuery[ 'Message' ]
    if not resQuery[ 'Value' ]:
      return None

    countries = []

    for name in resQuery[ 'Value' ]:
      country = name[0].split('.').pop()
      if country not in countries:
        countries.append( country )

    return countries

#############################################################################

  def unique( self, table, ID ):
    """
    Check if a ValidRes is unique.

    :params:
      :attr:`table`: string of the table name

      :attr:`ID`: integer
    """

#    DBtable, DBname = self.__DBchoice(table)
    DBname = self.__DBchoice( table )[ 1 ]
    DBid = table.rstrip( 's' ) + 'ID'

    req = "SELECT COUNT(*) FROM %s WHERE %s = (SELECT %s " %( table, DBname, DBname )
    req = req + " FROM %s WHERE %s = '%d');" % ( table, DBid, ID )

    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.unique ) + resQuery[ 'Message' ]
    else:
      n = int( resQuery[ 'Value' ][ 0 ][ 0 ] )
      if n == 1 :
        return True
      else:
        return False

#############################################################################

  def getTokens( self, granularity, name = None, dateExpiration = None ):
    """
    Get tokens, either by name, those expiring or expired

    :params:
      :attr:`granularity`: a ValidRes

      :attr:`granularity`: optional name of the res

      :attr:`dateExpiration`: optional, datetime.datetime - date from which to consider
    """

    DBtable, DBname = self.__DBchoice( granularity )

    req = "SELECT %s, TokenOwner, TokenExpiration FROM %s WHERE " % ( DBname, DBtable )
    if name is not None:
      req = req + "%s = '%s' " % ( DBname, name )
      if dateExpiration is not None:
        req = req + "AND "
    if dateExpiration is not None:
      req = req + "TokenExpiration < '%s'" % ( dateExpiration )

    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.getTokens ) + resQuery[ 'Message' ]
    if not resQuery[ 'Value' ]:
      return []
#    tokenList = []
    tokenList = [ x for x in resQuery[ 'Value' ] ]
    return tokenList


#############################################################################

  def setToken( self, granularity, name, newTokenOwner, dateExpiration ):
    """
    (re)Set token properties.
    """

    DBtable, DBname = self.__DBchoice( granularity )

    query = "UPDATE %s SET TokenOwner = '%s', TokenExpiration" % ( DBtable, newTokenOwner )
    query = query + " = '%s' WHERE %s = '%s'" %( dateExpiration, DBname, name )
    resUpdate = self.db._update( query )
    if not resUpdate[ 'OK' ]:
      raise RSSDBException, where( self, self.setToken ) + resUpdate[ 'Message' ]


#############################################################################

  def whatIs( self, name ):
    """
    Find which is the granularity of name.
    """

    for g in ValidRes:
      DBtable, DBname = self.__DBchoice( g )
      req = "SELECT %s FROM %s WHERE %s = '%s'" %( DBname, DBtable, DBname, name )
      resQuery = self.db._query( req )
      if not resQuery[ 'OK' ]:
        raise RSSDBException, where( self, self.whatIs ) + resQuery[ 'Message' ]
      if not resQuery[ 'Value' ]:
        continue
      else:
        return g

    return 'Unknown'

#############################################################################

  def getStuffToCheck( self, granularity, checkFrequency = None, maxN = None, name = None ):
    """
    Get Sites, Services, Resources, StorageElements to be checked using Present-x views.

    :params:
      :attr:`granularity`: a ValidRes

      :attr:`checkFrequecy': dictonary. Frequency of active sites/resources checking in minutes.

      :attr:`maxN`: integer - maximum number of lines in output
    """

    if checkFrequency is not None:
      T0activeCheckFrequecy  = checkFrequency[ 'T0_ACTIVE_CHECK_FREQUENCY' ]
      T0probingCheckFrequecy = checkFrequency[ 'T0_PROBING_CHECK_FREQUENCY' ]
      T0badCheckFrequecy     = checkFrequency[ 'T0_BAD_CHECK_FREQUENCY' ]
      T0bannedCheckFrequecy  = checkFrequency[ 'T0_BANNED_CHECK_FREQUENCY' ]
      T1activeCheckFrequecy  = checkFrequency[ 'T1_ACTIVE_CHECK_FREQUENCY' ]
      T1probingCheckFrequecy = checkFrequency[ 'T1_PROBING_CHECK_FREQUENCY' ]
      T1badCheckFrequecy     = checkFrequency[ 'T1_BAD_CHECK_FREQUENCY' ]
      T1bannedCheckFrequecy  = checkFrequency[ 'T1_BANNED_CHECK_FREQUENCY' ]
      T2activeCheckFrequecy  = checkFrequency[ 'T2_ACTIVE_CHECK_FREQUENCY' ]
      T2probingCheckFrequecy = checkFrequency[ 'T2_PROBING_CHECK_FREQUENCY' ]
      T2badCheckFrequecy     = checkFrequency[ 'T2_BAD_CHECK_FREQUENCY' ]
      T2bannedCheckFrequecy  = checkFrequency[ 'T2_BANNED_CHECK_FREQUENCY' ]

      now = datetime.datetime.utcnow().replace(microsecond = 0)

      T0dateToCheckFromActive  = ( now - datetime.timedelta(minutes=T0activeCheckFrequecy)).isoformat(' ')
      T0dateToCheckFromProbing = ( now - datetime.timedelta(minutes=T0probingCheckFrequecy)).isoformat(' ')
      T0dateToCheckFromBad     = ( now - datetime.timedelta(minutes=T0badCheckFrequecy)).isoformat(' ')
      T0dateToCheckFromBanned  = ( now - datetime.timedelta(minutes=T0bannedCheckFrequecy)).isoformat(' ')
      T1dateToCheckFromActive  = ( now - datetime.timedelta(minutes=T1activeCheckFrequecy)).isoformat(' ')
      T1dateToCheckFromProbing = ( now - datetime.timedelta(minutes=T1probingCheckFrequecy)).isoformat(' ')
      T1dateToCheckFromBad     = ( now - datetime.timedelta(minutes=T1badCheckFrequecy)).isoformat(' ')
      T1dateToCheckFromBanned  = ( now - datetime.timedelta(minutes=T1bannedCheckFrequecy)).isoformat(' ')
      T2dateToCheckFromActive  = ( now - datetime.timedelta(minutes=T2activeCheckFrequecy)).isoformat(' ')
      T2dateToCheckFromProbing = ( now - datetime.timedelta(minutes=T2probingCheckFrequecy)).isoformat(' ')
      T2dateToCheckFromBad     = ( now - datetime.timedelta(minutes=T2badCheckFrequecy)).isoformat(' ')
      T2dateToCheckFromBanned  = ( now - datetime.timedelta(minutes=T2bannedCheckFrequecy)).isoformat(' ')

    if granularity in ( 'Site', 'Sites' ):
      req = "SELECT SiteName, Status, FormerStatus, SiteType, TokenOwner FROM PresentSites"
    elif granularity in ( 'Service', 'Services' ):
      req = "SELECT ServiceName, Status, FormerStatus, SiteType, ServiceType, TokenOwner FROM PresentServices"
    elif granularity in ( 'Resource', 'Resources' ):
      req = "SELECT ResourceName, Status, FormerStatus, SiteType, ResourceType, TokenOwner FROM PresentResources"
    elif granularity in ( 'StorageElementRead', 'StorageElementsRead' ):
      req = "SELECT StorageElementName, Status, FormerStatus, SiteType, TokenOwner FROM PresentStorageElementsRead"
    elif granularity in ( 'StorageElementWrite', 'StorageElementsWrite' ):
      req = "SELECT StorageElementName, Status, FormerStatus, SiteType, TokenOwner FROM PresentStorageElementsWrite"
    else:
      raise InvalidRes, where(self, self.getStuffToCheck)
    if name is None:
      if checkFrequency is not None:
        req = req + " WHERE"
        req = req + " (Status = 'Active' AND SiteType = 'T0' AND LastCheckTime < '%s') OR" %( T0dateToCheckFromActive )
        req = req + " (Status = 'Probing' AND SiteType = 'T0' AND LastCheckTime < '%s') OR" %( T0dateToCheckFromProbing )
        req = req + " (Status = 'Bad' AND SiteType = 'T0' AND LastCheckTime < '%s') OR" %( T0dateToCheckFromBad )
        req = req + " (Status = 'Banned' AND SiteType = 'T0' AND LastCheckTime < '%s') OR" %( T0dateToCheckFromBanned )
        req = req + " (Status = 'Active' AND SiteType = 'T1' AND LastCheckTime < '%s') OR" %( T1dateToCheckFromActive )
        req = req + " (Status = 'Probing' AND SiteType = 'T1' AND LastCheckTime < '%s') OR" %( T1dateToCheckFromProbing )
        req = req + " (Status = 'Bad' AND SiteType = 'T1' AND LastCheckTime < '%s') OR" %( T1dateToCheckFromBad )
        req = req + " (Status = 'Banned' AND SiteType = 'T1' AND LastCheckTime < '%s') OR" %( T1dateToCheckFromBanned )
        req = req + " (Status = 'Active' AND SiteType = 'T2' AND LastCheckTime < '%s') OR" %( T2dateToCheckFromActive )
        req = req + " (Status = 'Probing' AND SiteType = 'T2' AND LastCheckTime < '%s') OR" %( T2dateToCheckFromProbing )
        req = req + " (Status = 'Bad' AND SiteType = 'T2' AND LastCheckTime < '%s') OR" %( T2dateToCheckFromBad )
        req = req + " (Status = 'Banned' AND SiteType = 'T2' AND LastCheckTime < '%s')" %( T2dateToCheckFromBanned )
        req = req + " ORDER BY LastCheckTime"
    else:
      req = req + " WHERE"
      if granularity in ( 'Site', 'Sites' ):
        req = req + " SiteName = '%s'" %name
      elif granularity in ( 'Service', 'Services' ):
        req = req + " ServiceName = '%s'" %name
      elif granularity in ( 'Resource', 'Resources' ):
        req = req + " ResourceName = '%s'" %name
      elif granularity in ( 'StorageElementRead', 'StorageElementsRead', 'StorageElementWrite', 'StorageElementsWrite' ):
        req = req + " StorageElementName = '%s'" %name
    if maxN != None:
      req = req + " LIMIT %d" %maxN

    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.getStuffToCheck ) + resQuery[ 'Message' ]
    if not resQuery[ 'Value' ]:
      return []
#    stuffList = []
    stuffList = [ x for x in resQuery[ 'Value' ]]

    return stuffList

#############################################################################

  def rankRes( self, granularity, days, startingDate = None ):
    """
    Construct the rank of a ValidRes, based on the time it's been Active, Probing, Bad
    (see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`)

    :params:
      :attr:`granularity`: string, a ValidRes

      :attr:`days`: integer, amount of days in the past to look at

      :attr:`startingDate`: datetime.datetime or string - optional date to start from
    """

    if granularity not in ValidRes:
      raise InvalidRes, where( self, self.rankRes )

    if startingDate is not None:
      if isinstance( startingDate, basestring ):
        startingDate = datetime.datetime.strptime( startingDate, '%Y-%m-%d %H:%M:%S' )
    else:
      startingDate = datetime.datetime.utcnow().replace( microsecond = 0 )

    dateToCheckFrom = startingDate - datetime.timedelta( days = days )

    if granularity in ( 'Site', 'Sites' ):
      resList = self.getMonitoredsList( granularity, paramsList = [ 'SiteName' ] )
    if granularity in ( 'Service', 'Services' ):
      resList = self.getMonitoredsList( granularity, paramsList = [ 'ServiceName' ] )
    if granularity in ( 'Resource', 'Resources' ):
      resList = self.getMonitoredsList( granularity, paramsList = [ 'ResourceName' ] )
    if granularity in ( 'StorageElementRead', 'StorageElementsRead' ):
      resList = self.getMonitoredsList( granularity, paramsList = [ 'StorageElementName' ] )
    if granularity in ( 'StorageElementWrite', 'StorageElementsWrite' ):
      resList = self.getMonitoredsList( granularity, paramsList = [ 'StorageElementName' ] )

    rankList        = []
    activeRankList  = []
    probingRankList = []
    badRankList     = []

    for res in resList:

      periodsActive = self.getPeriods( granularity, res[0], 'Active', None, days )
      periodsActive = [ [ datetime.datetime.strptime(period[0], '%Y-%m-%d %H:%M:%S'),
                         datetime.datetime.strptime(period[1], '%Y-%m-%d %H:%M:%S') ] for period in periodsActive ]

      for p in periodsActive:
        if p[1] < dateToCheckFrom:
          periodsActive.remove(p)
        elif p[0] < dateToCheckFrom:
          p[0] = dateToCheckFrom

      activePeriodsLength = [ x[1]-x[0] for x in periodsActive ]
      activePeriodsLength = [ convertTime(x) for x in activePeriodsLength ]
      activeRankList.append(( res, sum(activePeriodsLength) ))



      periodsProbing = self.getPeriods( granularity, res[0], 'Probing', None, days )
      periodsProbing = [ [ datetime.datetime.strptime(period[0], '%Y-%m-%d %H:%M:%S'),
                          datetime.datetime.strptime(period[1], '%Y-%m-%d %H:%M:%S') ] for period in periodsProbing ]

      for p in periodsProbing:
        if p[1] < dateToCheckFrom:
          periodsProbing.remove(p)
        elif p[0] < dateToCheckFrom:
          p[0] = dateToCheckFrom

      probingPeriodsLength = [ x[1]-x[0] for x in periodsProbing ]
      probingPeriodsLength = [ convertTime(x) for x in probingPeriodsLength ]
      probingRankList.append(( res, sum(probingPeriodsLength) ))

      rankList.append( ( res[0], sum(activePeriodsLength) + sum(probingPeriodsLength)/2 ) )



      periodsBad = self.getPeriods(granularity, res[0], 'Bad', None, days)
      periodsBad = [ [ datetime.datetime.strptime(period[0], '%Y-%m-%d %H:%M:%S'),
                      datetime.datetime.strptime(period[1], '%Y-%m-%d %H:%M:%S') ] for period in periodsBad ]

      for p in periodsBad:
        if p[1] < dateToCheckFrom:
          periodsBad.remove(p)
        elif p[0] < dateToCheckFrom:
          p[0] = dateToCheckFrom

      badPeriodsLength = [ x[1]-x[0] for x in periodsBad ]
      badPeriodsLength = [ convertTime(x) for x in badPeriodsLength ]
      badRankList.append(( res, sum(badPeriodsLength) ))

      rankList.append( ( res[0],
                         sum(activePeriodsLength) + sum(probingPeriodsLength) + sum(badPeriodsLength)/2 ) )

    activeRankList  = sorted(activeRankList, key=lambda x:(x[1], x[0]))
    probingRankList = sorted(probingRankList, key=lambda x:(x[1], x[0]))
    badRankList     = sorted(badRankList, key=lambda x:(x[1], x[0]))
    rankList        = sorted(rankList, key=lambda x:(x[1], x[0]))

    rank = {'WeightedRank':rankList, 'ActivesRank':activeRankList,
            'ProbingsRank':probingRankList,
            'BadsRank':badRankList}

    return rank

#############################################################################

  def __DBchoice( self, granularity ):

    if granularity in ( 'Site', 'Sites' ):
      DBtable = 'Sites'
      DBname  = 'SiteName'
    elif granularity in ( 'Service', 'Services' ):
      DBtable = 'Services'
      DBname  = 'ServiceName'
    elif granularity in ( 'Resource', 'Resources' ):
      DBtable = 'Resources'
      DBname  = 'ResourceName'
    elif granularity in ('StorageElementRead', 'StorageElementsRead'):
      DBtable = 'StorageElementsRead'
      DBname  = 'StorageElementName'
    elif granularity in ('StorageElementWrite', 'StorageElementsWrite'):
      DBtable = 'StorageElementsWrite'
      DBname  = 'StorageElementName'
#    elif granularity in ('Cache', 'ClientsCache', 'ClientCache'):
#      DBtable = 'ClientsCache'
#      DBname = 'Name'
    else:
      raise InvalidRes, where( self, self.__DBchoice )

    return ( DBtable, DBname )

#############################################################################

  def __DBchoiceType( self, granularity ):

    if granularity in ( 'Site', 'Sites' ):
      DBtype  = 'SiteType'
      DBtable = 'SiteTypes'
    elif granularity in ( 'Service', 'Services' ):
      DBtype  = 'ServiceType'
      DBtable = 'ServiceTypes'
    elif granularity in ( 'Resource', 'Resources' ):
      DBtype  = 'ResourceType'
      DBtable = 'ResourceTypes'
    else:
      raise InvalidRes, where( self, self.__DBchoiceType )

    return ( DBtype, DBtable )

#############################################################################

  def __usualChecks( self, dateCreated, dateEffective, dateEnd, status ):

    if not isinstance( dateCreated, basestring ):
      dateCreated = dateCreated.isoformat( ' ' )
    if not isinstance( dateEffective, basestring ):
      dateEffective = dateEffective.isoformat( ' ' )
    if not isinstance( dateEnd, basestring ):
      dateEnd = dateEnd.isoformat( ' ' )
    if status not in ValidStatus:
      raise InvalidStatus, where( self, self.__usualChecks )

    return ( dateCreated, dateEffective, dateEnd )

#############################################################################

  def __addOrModifyInit( self, dateEffective, dateEnd, status ):

    dateCreated = datetime.datetime.utcnow().replace( microsecond = 0 )
    if dateEffective < dateCreated:
      dateEffective = dateCreated
    if dateEnd < dateEffective:
      raise NotAllowedDate, where( self, self.__addOrModifyInit )
    if status not in ValidStatus:
      raise InvalidStatus, where( self, self.__addOrModifyInit )

    return ( dateCreated, dateEffective )

#############################################################################
