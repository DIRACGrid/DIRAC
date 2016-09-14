"""
Profiling class for updated information on process status
"""

__RCSID__ = "$Id$"

import psutil
import datetime
from DIRAC import gLogger, S_OK, S_ERROR

class Profiler( object ):
  """
  Class for profiling both general stats about a machine and individual processes.
  Every instance of this class is associated to a single process by using its PID.
  Calls to the different methods of the class will return the current state of the process.
  """

  def __init__( self, pid = None ):
    """
    :param str pid: PID of the process to be profiled
    """
    if pid:
      try:
        self.process = psutil.Process( int( pid ) )
      except psutil.NoSuchProcess as e:
        gLogger.error( 'No such process: %s' % e )
    else:
      self.process = None

  def pid( self ):
    """
    Returns the process PID
    """
    if self.process:
      return S_OK( self.process.pid )
    else:
      gLogger.error( 'No PID of process to profile' )
      return S_ERROR( 'No PID of process to profile' )

  def status( self ):
    """
    Returns the process status
    """
    if self.process:
      try:
        result = self.process.status()
      except psutil.NoSuchProcess as e:
        gLogger.error( 'No such process: %s' % e )
        return S_ERROR( 'No such process: %s' % e )
      except psutil.ZombieProcess as e:
        gLogger.error( 'Zombie process: %s' % e )
        return S_ERROR( 'Zombie process: %s' % e )
      except psutil.AccessDenied as e:
        gLogger.error( 'Access denied: %s' % e )
        return S_ERROR( 'Access denied: %s' % e )
      except Exception as e:
        gLogger.error( e )
        return S_ERROR( e )

      return S_OK( result )
    else:
      gLogger.error( 'No PID of process to profile' )
      return S_ERROR( 'No PID of process to profile' )

  def runningTime( self ):
    """
    Returns the uptime of the process
    """
    if self.process:
      try:
        start = datetime.datetime.fromtimestamp( self.process.create_time() )
        result = ( datetime.datetime.now() - start ).total_seconds()
      except psutil.NoSuchProcess as e:
        gLogger.error( 'No such process: %s' % e )
        return S_ERROR( 'No such process: %s' % e )
      except psutil.ZombieProcess as e:
        gLogger.error( 'Zombie process: %s' % e )
        return S_ERROR( 'Zombie process: %s' % e )
      except psutil.AccessDenied as e:
        gLogger.error( 'Access denied: %s' % e )
        return S_ERROR( 'Access denied: %s' % e )
      except Exception as e:
        gLogger.error( e )
        return S_ERROR( e )

      return S_OK( result )
    else:
      gLogger.error( 'No PID of process to profile' )
      return S_ERROR( 'No PID of process to profile' )

  def memoryUsage( self ):
    """
    Returns the memory usage of the process in MB
    """
    if self.process:
      try:
        # Information is returned in bytes and converted to MB
        result = self.process.get_memory_info()[0] / float( 2 ** 20 )
      except psutil.NoSuchProcess as e:
        gLogger.error( 'No such process: %s' % e )
        return S_ERROR( 'No such process: %s' % e )
      except psutil.ZombieProcess as e:
        gLogger.error( 'Zombie process: %s' % e )
        return S_ERROR( 'Zombie process: %s' % e )
      except psutil.AccessDenied as e:
        gLogger.error( 'Access denied: %s' % e )
        return S_ERROR( 'Access denied: %s' % e )
      except Exception as e:
        gLogger.error( e )
        return S_ERROR( e )

      return S_OK( result )
    else:
      gLogger.error( 'No PID of process to profile' )
      return S_ERROR( 'No PID of process to profile' )

  def numThreads( self ):
    """
    Returns the number of threads the process is using
    """
    if self.process:
      try:
        result = self.process.num_threads()
      except psutil.NoSuchProcess as e:
        gLogger.error( 'No such process: %s' % e )
        return S_ERROR( 'No such process: %s' % e )
      except psutil.ZombieProcess as e:
        gLogger.error( 'Zombie process: %s' % e )
        return S_ERROR( 'Zombie process: %s' % e )
      except psutil.AccessDenied as e:
        gLogger.error( 'Access denied: %s' % e )
        return S_ERROR( 'Access denied: %s' % e )
      except Exception as e:
        gLogger.error( e )
        return S_ERROR( e )

      return S_OK( result )
    else:
      gLogger.error( 'No PID of process to profile' )
      return S_ERROR( 'No PID of process to profile' )

  def cpuUsage( self ):
    """
    Returns the percentage of cpu used by the process
    """
    if self.process:
      try:
        result = self.process.cpu_percent()
      except psutil.NoSuchProcess as e:
        gLogger.error( 'No such process: %s' % e )
        return S_ERROR( 'No such process: %s' % e )
      except psutil.ZombieProcess as e:
        gLogger.error( 'Zombie process: %s' % e )
        return S_ERROR( 'Zombie process: %s' % e )
      except psutil.AccessDenied as e:
        gLogger.error( 'Access denied: %s' % e )
        return S_ERROR( 'Access denied: %s' % e )
      except Exception as e:
        gLogger.error( e )
        return S_ERROR( e )

      return S_OK( result )
    else:
      gLogger.error( 'No PID of process to profile' )
      return S_ERROR( 'No PID of process to profile' )

  def getAllProcessData( self ):
    """
    Returns data available about a process
    """
    data = {}

    data['datetime'] = datetime.datetime.utcnow()
    data['stats'] = {}

    result = self.pid()
    if result['OK']:
      data['stats']['pid'] = result['Value']

    result = self.status()
    if result['OK']:
      data['stats']['status'] = result['Value']

    result = self.runningTime()
    if result['OK']:
      data['stats']['runningTime'] = result['Value']

    result = self.memoryUsage()
    if result['OK']:
      data['stats']['memoryUsage'] = result['Value']

    result = self.numThreads()
    if result['OK']:
      data['stats']['threads'] = result['Value']

    result = self.cpuUsage()
    if result['OK']:
      data['stats']['cpuUsage'] = result['Value']

    return S_OK( data )
