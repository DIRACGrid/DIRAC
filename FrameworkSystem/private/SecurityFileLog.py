
import os
import Queue
import threading
from DIRAC import gLogger, S_OK, S_ERROR

class SecurityFileLog( threading.Thread ):

  def __init__( self, basePath ):
    self.__basePath = basePath
    self.__messagesQueue = Queue.Queue()
    self.__requiredFields = ( 'timestamp',
                              'success',
                              'sourceIP',
                              'sourcePort',
                              'sourceIdentity',
                              'destinationIP',
                              'destinationPort',
                              'destinationService',
                              'action' )
    threading.Thread.__init__( self )
    self.setDaemon( True )
    self.start()

  def run(self):
    while True:
      secMsg = self.__messagesQueue.get()
      msgTime = secMsg[ 0 ]
      path = "%s/%s/%s" % ( self.__basePath, msgTime.year, msgTime.month )
      try:
        os.makedirs( path )
      except:
        pass
      logFile = "%s/%s%s%s.security.log.csv" % ( path, msgTime.year, msgTime.month, msgTime.day )
      if not os.path.isfile( logFile ):
        fd = file( logFile, "w" )
        fd.write( "Time, Success, Source IP, Source Port, source Identity, destinationIP, destinationPort, destinationService, action\n" )
      else:
        fd = file( logFile, "a" )
      fd.write( "%s\n" % ", ".join( [ str( item ) for item in secMsg ] ) )
      fd.close()


  def logAction( self, msg ):
    if len( msg ) != len( self.__requiredFields ):
      return S_ERROR( "Mismatch in the msg size, it should be %s and it's %s" % ( len( self.__requiredFields ),
                                                                                  len( msg ) ) )
    self.__messagesQueue.put( msg )
    return S_OK()
