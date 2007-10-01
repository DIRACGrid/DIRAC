import re, os, sys, string
from random import randrange
from time import localtime,strftime

from dirac import DIRAC

from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from types import *
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB


Loglevel = { -30: 'FATAL' , -20: 'ERROR', -10: 'WARN', 0: 'DEBUG',
             10: 'VERB', 20:'INFO',30:'ALWAYS'}

class MsgLoggingDB_fill(DB):

  def __init__(self, maxQueueSize=10):
    DB.__init__(self,'MsgLoggingDB','Logging/MsgLoggingDB',maxQueueSize)

  def FillAuxiliaryTables(self):
    for i in range(-30,40,10):
      cmd = 'INSERT INTO LogLevels VALUES ( %s, %s )' % (i,Loglevel[i])
      #print cmd
      self._update( cmd )

    for i in range(1,7):
      cmd = 'INSERT INTO FixtxtmsgTable (FixtxtString) VALUES ( "error message %s" )' % i
      #print cmd
      self._update( cmd )

    for i in range(1,6):
      cmd = 'INSERT INTO System (SystemName) VALUES ( "system %s" )' % i
      #print cmd
      self._update( cmd )

    for i in range(1,6):
      cmd = 'INSERT INTO SubSystem (SubSystemName) VALUES ( "subsystem %s" )' % i
      #print cmd
      self._update( cmd )

    for i in range(1,8):
      cmd = 'INSERT INTO Frame (FrameName) VALUES ( "frame %s" )' % i
      #print cmd
      self._update( cmd )

    for i in range(1,21):
      cmd = 'INSERT INTO ClientIPs (ClientIPNumberString) VALUES ( "%s.%s.%s.%s" )' % (randrange(2,255),randrange(2,255),randrange(2,255),randrange(2,255))
      #print cmd
      self._update( cmd )

    groups={0:'lhcbsgm',1:'lhcbprod',2:'lhcb'}

    for i in range(0,3):
      for j in range(1,3+i*2):
        cmd = 'INSERT INTO UserDNs (OwnerDN,OwnerGroup) VALUES ( "user%s", %s )' % (j,groups[i])
        #print cmd
        self._update( cmd )

  def DateTable(self):
    for i in range(1,800):
      cmd = 'INSERT INTO DateStamps VALUES (STR_TO_DATE("%s",GET_FORMAT(DATETIME,"ISO")),"variable text %s",%s,%s,%s,%s,%s,%s,%s)' % (strftime('%Y-%m-%d %H:%M:%S',localtime(randrange(1187617568,1188222011))),
                                                                                            randrange(1,7),randrange(1,13),randrange(1,21),randrange(-30,40,10),
                                                                                            randrange(1,7),randrange(1,6),randrange(1,6),randrange(1,8))
      #print cmd
      self._update(cmd)
    
DBfill=MsgLoggingDB_fill()
#DBfill.FillAuxiliaryTables()
DBfill.DateTable()
