"""
Profiling class for updated information on process status
"""

__RCSID__ = "$Id$"

import psutil
import datetime
from DIRAC import S_OK, S_ERROR

class Profiler:
  """
  Class for profiling both general stats about a machine and individual processes.
  Every instance of this class is associated to a single process.
  """

  def __init__( self, pid = None ):
    """
    :param str pid: PID of the process to be profiled
    """
    self.__setProcess( pid )

  def __setProcess( self, pid = None ):
    """
    Set the current process
    :param str pid: PID of the process to be profiled
    """
    if pid:
      self.process = psutil.Process( int( pid ) )
    else:
      self.process = psutil.Process()

  def pid( self ):
    """
    Returns the process PID
    """
    return S_OK( self.process.pid )

  def status( self ):
    """
    Returns the process status
    """
    try:
      result = self.process.status()
    except Exception as e:
      return S_ERROR( e )

    return S_OK( result )

  def runningTime( self ):
    """
    Returns the uptime of the process
    """
    try:
      start = datetime.datetime.fromtimestamp( self.process.create_time() )
      result = ( datetime.datetime.now() - start ).total_seconds()
    except Exception as e:
      return S_ERROR( e )

    return S_OK( result )

  def memoryUsage( self ):
    """
    Returns the memory usage of the process in MB
    """
    try:
      result = self.process.get_memory_info()[0] / float( 2 ** 20 )
    except Exception as e:
      return S_ERROR( e )

    return S_OK( result )

  def numThreads( self ):
    """
    Returns the number of threads the process is using
    """
    try:
      result = self.process.num_threads()
    except Exception as e:
      return S_ERROR( e )

    return S_OK( result )

  def cpuUsage( self ):
    """
    Returns the percentage of cpu used by the process
    """
    try:
      result = self.process.cpu_percent()
    except Exception as e:
      return S_ERROR( e )

    return S_OK( result )

  def getAllProcessData( self ):
    """
    Returns data available about a process
    """
    data = {}

    data[ 'datetime' ] = datetime.datetime.utcnow()
    data[ 'stats' ] = {}

    result = self.pid()
    if result[ 'OK' ]:
      data[ 'stats' ][ 'pid' ] = result[ 'Value' ]

    result = self.status()
    if result[ 'OK' ]:
      data[ 'stats' ][ 'status' ] = result[ 'Value' ]

    result = self.runningTime()
    if result[ 'OK' ]:
      data[ 'stats' ][ 'runningTime' ] = result[ 'Value' ]

    result = self.memoryUsage()
    if result[ 'OK' ]:
      data[ 'stats' ][ 'memoryUsage' ] = result[ 'Value' ]

    result = self.numThreads()
    if result[ 'OK' ]:
      data[ 'stats' ][ 'threads' ] = result[ 'Value' ]

    result = self.cpuUsage()
    if result[ 'OK' ]:
      data[ 'stats' ][ 'cpuUsage' ] = result[ 'Value' ]

    return S_OK( data )
