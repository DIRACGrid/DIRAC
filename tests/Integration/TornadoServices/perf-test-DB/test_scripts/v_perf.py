"""
 This test is designed to check protocol/server performances with a database

 Configuration and how to run it are explained in the documentation
"""

from DIRAC.Core.Tornado.Client.RPCClientSelector import RPCClientSelector
from time import time
from random import random
import sys
import os


class Transaction(object):
  def __init__(self):
    self.client = RPCClientSelector('Framework/User', timeout=30)
    return

  def run(self):
    # Generate random name
    s = str(int(random() * 100))
    s2 = str(int(random() * 100))
    service = self.client

    # Create a user
    newUser = service.addUser(s)
    userID = int(newUser['Value'])

    # Check if user exist and name is correct
    User = service.getUserName(userID)
    assert (User['OK']), 'Error in getting user'
    assert (User['Value'] == s), 'Error on insertion'

    # Check if update work
    service.editUser(userID, s2)
    User = service.getUserName(userID)
    assert (User['Value'] == s2), 'Error on update'
