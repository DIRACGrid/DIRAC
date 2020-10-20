from DIRAC.Core.Tornado.Client.TornadoClient import TornadoClient
from DIRAC.Core.DISET.RPCClient import RPCClient
from time import time
from random import randint
import sys


class Transaction(object):
  def __init__(self):
    # If we want we can force to use dirac
    if len(sys.argv) > 2 and sys.argv[2].lower() == 'dirac':
      self.client = RPCClient('Framework/UserDirac')
    else:
      self.client = TornadoClient('Framework/User')
    return

  def run(self):
    s = "Chaine 1"
    s2 = "Chaine %d" % randint(0, 42)

    newUser = self.client.addUser(s)
    userID = int(newUser['Value'])
    User = self.client.getUserName(userID)
    self.client.editUser(userID, s2)
    User = self.client.getUserName(userID)
