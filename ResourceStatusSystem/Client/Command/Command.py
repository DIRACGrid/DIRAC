""" The Command class is a simple base class for all the commands
    for interacting with the clients
"""

class Command:
  
  def __init__(self):
    pass
  
  # method to be extended by sub(real) commands
  def doCommand(self, args):
    pass