"""  This class fills the main and auxiliary tables of the Logging Database.
    It provides the functions:
        FillMesssageRepository()
"""
import re, os, sys, string
from random import randrange
from time import localtime,strftime
from dirac import DIRAC

from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from types import *
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities.Time import dateTime, toString, hour, second, minute
from DIRAC.LoggingSystem.private.Message import tupleToMessage
from DIRAC.LoggingSystem.DB.SystemLoggingDB import SystemLoggingDB

DIRAC.gLogger.initialize('fill_DB','/testSectionDebug')

class MessageLoggingDB_fill(SystemLoggingDB):
  fixedMessages = []
  systemNames = []
  subSystemNames = []
  clientIPs = []
  sites = []
  users = []
    
  def __CreateAuxiliaryLists(self):
    """ This function is used to fill with template values the auxiliary list
    """
    for i in range(1,7):
      self.fixedMessages.append( 'error message %s' % i)

    for i in range(1,6):
      self.systemNames.append( 'system %s' % i )

    for i in range(1,6):
      self.subSystemNames.append( 'subsystem %s' % i )

    for i in range(1,21):
      self.clientIPs.append( '%s.%s.%s.%s' % ( randrange(2,255),
                                               randrange(2,255),
                                               randrange(2,255),
                                               randrange(2,255) ) )
    for i in range(1,6):
      self.sites.append( 'site %s' % i )
      
    groups={0:'lhcbsgm',1:'lhcbprod',2:'lhcb'}

    for i in range(0,3):
      for j in range(1,3+i*2):
        self.users.append( [ 'user%s' % j, '%s' % groups[i] ] )

  def FillMessageRepository(self):
    """This function fills the MessageRepository with random values.
       It could be useful to test performance of the database.
    """
    self.__CreateAuxiliaryLists()
    LogLevels = [ 'ALWAYS' , 'INFO', 'VERB', 'DEBUG', 'WARN',
                  'ERROR', 'EXCEPT', 'FATAL' ]
    initialDate=dateTime()

    for i in range(1,800):
      limitDate = toString( initialDate - randrange(0,1680) * hour -
                            randrange( 0, 60) * minute -
                            randrange( 0, 60) * second )
      message = tupleToMessage ( [ self.systemNames[ randrange( 0, 5 ) ],
                          LogLevels[ randrange( 0, 8 ) ], limitDate,
                          self.fixedMessages[ randrange( 0, 6 ) ],
                          'variable text %s' % randrange( 0, 6 ), '',
                          self.subSystemNames[ randrange( 0, 5 ) ],
                          self.sites[ randrange( 0, 5 ) ] ] )
      userId = randrange( 0, 12 )
      result = self.insertMessageIntoDB( message, self.users[ userId ][ 0 ],
                                         self.users[ userId ][ 1 ],
                                         self.clientIPs[ randrange( 0, 20 ) ] )
      if not result['OK']:
        print result['Value']
        
DBfill=MessageLoggingDB_fill()
DBfill.FillMessageRepository()
