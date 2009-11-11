""" The Policy class is a simple base class for all the policies
"""

class PolicyBase:
  
  def __init__(self):
    pass
  
  # method to be extended by sub(real) policies
  def evaluate(self):
    pass