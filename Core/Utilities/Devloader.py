
import sys
import os
import types
from DIRAC import gLogger
from DIRAC.Core.Utilities.DIRACSingleton import DIRACSingleton
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getInstalledExtensionPaths

try:
  from watchdog.observers import Observer
  from watchdog.events import FileSystemEventHandler
  watchdogEnabled = True
except:
  watchdogEnabled = False

class Devloader( object ):
  __metaclass__ = DIRACSingleton

  def __init__( self ):
    self.__log = gLogger.getSubLogger( "Devloader" )
    self.__observers = []

  @property
  def enabled( self ):
    return watchdogEnabled

  def bootstrap( self, modules = False ):
    if not watchdogEnabled:
      return False

    if type( modules ) in types.StringTypes:
      modules = [ modules ]
    elif modules:
      modules = list( modules )

    if modules:
      for obs in self.__observers:
        if obs in modules:
          modules.remove( obs )

    if not modules and self.__observers:
      return True

    exts = getInstalledExtensionPaths()
    if modules:
      exts = dict([ ( k, exts[k] ) for k in exts if k in modules ])

    exts = dict([ ( k, exts[k] ) for k in exts if k not in self.__observers ])

    if not exts:
      return True

    class Restarter( FileSystemEventHandler ):
      def __init__( self, log, observer ):
        self.__log = log
        self.__observer = observer
      def on_any_event( event ):
        self.__log.always( "File system changed (%s %s). Restarting..." % ( event.event_type, event.src_path ) )
        self.__observer.unschedule_all()
        python = sys.executable
        os.execl(python, python, * sys.argv)

    for extName in exts:
      extPath = exts[ extName ]
      self.__log.notice( "Starting reload watchdog for %s" % extPath )
      obs = Observer()
      restarter = Restarter( self.__log, obs )
      obs.schedule( restarter, extPath, recursive = True )
      self.__observers.append( obs )
      obs.start()



