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
    self.client.ping()
