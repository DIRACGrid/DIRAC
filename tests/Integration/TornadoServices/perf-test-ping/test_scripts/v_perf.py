"""
 This test is designed to check protocol/server performances

 Configuration and how to run it are explained in the documentation
"""

from DIRAC.Core.Tornado.Client.RPCClientSelector import RPCClientSelector
from time import time
from random import randint
import sys
import os


class Transaction(object):
  def __init__(self):
    # If we want we can force to use another service name (testing multiple services for example)
    if len(sys.argv) > 2:
      self.client = RPCClientSelector(sys.argv[2])
    else:
      self.client = RPCClientSelector('Framework/User')
    return

  def run(self):
    assert (self.client.ping()['OK']), 'error'
