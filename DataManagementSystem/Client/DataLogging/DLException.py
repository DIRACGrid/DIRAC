'''
Created on May 19, 2015

@author: Corentin Berger
'''

class DLException( Exception ):
  """ Specific Exception for the data logging system"""

  def __init__( self, value ):
    self.value = value

  def __str__( self ):
    return str( self.value )


class NoLogException( DLException ):
  """ Specific exception when a method is not in the list of method to log"""
  def __init__( self, value ):
    self.value = value

  def __str__( self ):
    return str( self.value )
