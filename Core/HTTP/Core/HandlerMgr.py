
import os
import re
import imp
import inspect
import collections

from DIRAC import S_OK, S_ERROR, rootPath, gLogger
from DIRAC.Core.HTTP.Lib import Conf
from DIRAC.Core.HTTP.Lib.WebHandler import WebHandler, WebSocketHandler
from DIRAC.Core.HTTP.Core.CoreHandler import CoreHandler
from DIRAC.Core.HTTP.Core.StaticHandler import StaticHandler
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Core.Utilities.DIRACSingleton import DIRACSingleton
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals


class HandlerMgr( object ):
  __metaclass__ = DIRACSingleton

  def __init__( self, baseURL="/", sysService=None ):
    self.__baseURL = baseURL.strip( "/" )
    self.__sysService = sysService or []
    if type(self.__sysService) is not list:
      self.__sysService = self.__sysService.replace(' ','').split(',')
    self.__routes = []
    self.__handlers = []
    self.__setupGroupRE = r"(?:/s:([\w-]*)/g:([\w.-]*))?"
    self.__shySetupGroupRE = r"(?:/s:(?:[\w-]*)/g:(?:[\w.-]*))?"
    self.log = gLogger.getSubLogger( "Routing" )

  def getPaths( self, dirName ):
    """
    Get lists of paths for all installed and enabled extensions
    """
    pathList = []
    for extName in CSGlobals.getCSExtensions():
      if extName.rfind( "DIRAC" ) != len( extName ) - 5:
        extName = "%sDIRAC" % extName
      try:
        modFile, modPath, desc = imp.find_module( extName )
        #to match in the real root path to enabling module web extensions (static, templates...)
        realModPath = os.path.realpath( modPath )
      except ImportError:
        continue
      staticPath = os.path.join( realModPath, "WebApp", dirName )
      if os.path.isdir( staticPath ):
        pathList.append( staticPath )
    return pathList

  def __calculateRoutes( self ):
    """
    Load all handlers and generate the routes
    """
    ol = ObjectLoader()
    # origin = self.__sysService
    self.__handlers = collections.OrderedDict()

    for origin in self.__sysService:
      
      result = ol.getObjects( origin, parentClass = WebHandler, recurse = True )
      if not result[ 'OK' ]:
       return result
      
      self.__handlers = collections.OrderedDict(list(self.__handlers.items()) + list(result[ 'Value' ].items()))
 
    staticPaths = self.getPaths( "static" )
    self.log.verbose( "Static paths found:\n - %s" % "\n - ".join( staticPaths ) )
    self.__routes = []

    # Add some standard paths for static files
    statDirectories = Conf.getStaticDirs() 
    self.log.info( "The following static directories are used:%s" % str( statDirectories ) )
    for stdir in statDirectories:
      pattern = '/%s/(.*)' % stdir
      self.__routes.append( ( pattern, StaticHandler, dict( pathList = ['%s/webRoot/www/%s' % ( rootPath, stdir ) ] ) ) )
      self.log.debug( " - Static route: %s" % pattern )

    for pattern in ( ( r"/static/(.*)", r"/(favicon\.ico)", r"/(robots\.txt)" ) ):
      pattern = r"%s%s" % ( self.__shySetupGroupRE, pattern )
      if self.__baseURL:
        pattern = "/%s%s" % ( self.__baseURL, pattern )
      self.__routes.append( ( pattern, StaticHandler, dict( pathList = staticPaths ) ) )
      self.log.debug( " - Static route: %s" % pattern )
    for hn in self.__handlers:
      self.log.info( "Found handler %s" % hn  )
      handler = self.__handlers[ hn ]
      #CHeck it has AUTH_PROPS
      if type( handler.AUTH_PROPS ) == None:
        return S_ERROR( "Handler %s does not have AUTH_PROPS defined. Fix it!" % hn )
      #Get the root for the handler
      if handler.LOCATION:
        handlerRoute = handler.LOCATION  #.strip("/")
      else:
        handlerRoute = hn[ len( origin ): ].replace( ".", "/" ).replace( "Handler", "" )
      #Add the setup group RE before
      baseRoute = self.__setupGroupRE
      #IF theres a base url like /DIRAC add it
      if self.__baseURL:
        baseRoute = "/%s%s" % ( self.__baseURL, baseRoute )
      #Set properly the LOCATION after calculating where it is with helpers to add group and setup later
      handler.LOCATION = handlerRoute
      handler.PATH_RE = re.compile( "%s(%s/.*)" % ( baseRoute, handlerRoute ) )
      handler.URLSCHEMA = "/%s%%(setup)s%%(group)s%%(location)s/%%(action)s" % ( self.__baseURL )
      if issubclass( handler, WebSocketHandler ):
        handler.PATH_RE = re.compile( "%s(%s)" % ( baseRoute, handlerRoute ) )
        route = "%s(%s)" % ( baseRoute, handlerRoute )
        self.__routes.append( ( route, handler ) )
        self.log.verbose( " - WebSocket %s -> %s" % ( handlerRoute, hn ) )
        self.log.debug( "  * %s" % route )
        continue
      #Look for methods that are exported
      for mName, mObj in inspect.getmembers( handler ):
        if inspect.ismethod( mObj ) and mName.find( "web_" ) == 0:
          if mName == "web_index":
            #Index methods have the bare url
            self.log.verbose( " - Route %s -> %s.web_index" % ( handlerRoute, hn ) )
            route = "%s(%s/)" % ( baseRoute, handlerRoute )
            self.__routes.append( ( route, handler ) )
            self.__routes.append( ( "%s(%s)" % ( baseRoute, handlerRoute ), CoreHandler, dict( action = 'addSlash' ) ) )
          else:
            #Normal methods get the method appended without web_
            self.log.verbose( " - Route %s/%s ->  %s.%s" % ( handlerRoute, mName[4:], hn, mName ) )
            route = "%s(%s/%s)" % ( baseRoute, handlerRoute, mName[4:] )
            self.__routes.append( ( route, handler ) )
          self.log.debug( "  * %s" % route )
    #Send to root
    self.__routes.append( ( "%s(/?)" % self.__setupGroupRE, CoreHandler, dict( action = "sendToRoot" ) ) )
    if self.__baseURL:
      self.__routes.append( ( "/%s%s()" % ( self.__baseURL, self.__setupGroupRE ),
                              CoreHandler, dict( action = "sendToRoot" ) ) )
    return S_OK()

  def getHandlers( self ):
    if not self.__handlers:
      result = self.__calculateRoutes()
      if not result[ 'OK' ]:
        return result
    return S_OK( self.__handlers )

  def getRoutes( self ):
    if not self.__routes:
      result = self.__calculateRoutes()
      if not result[ 'OK' ]:
        return result
    return S_OK( self.__routes )
