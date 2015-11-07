########################################################################
# File: FileTestCase.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/01/17 14:00:52
########################################################################

""" :mod: FileTestCase 
    =======================
     
    .. module: FileTestCase
    :synopsis: Test case for DIRAC.Core.Utilities.File module
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com
    
    Test case for DIRAC.Core.Utilities.File module
"""

__RCSID__ = "$Id $"

##
# @file FileTestCase.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/01/17 14:01:18
# @brief Definition of FileTestCase class.

## imports 
import unittest
import os
import re

# sut
from DIRAC.Core.Utilities.File import checkGuid, makeGuid, getSize, getMD5ForFiles, getCommonPath

########################################################################
class FileTestCase(unittest.TestCase):

  """
  .. class:: FileTestCase
	
  Test case for DIRAC.Core.Utilities.File module.

  """

  def setUp(self):
    """ test case setup
    .. function:: FileTestCase.setUp(self)
    :param self: "Me, myself and Irene"
    """
    self.filesList =  [ os.path.abspath(".") + os.sep + x for x in os.listdir( "." ) ]
  
  def tearDown( self ):
    """ remove filesList """
    self.filesList = None

  def testCheckGuid( self ):
    """ checkGuid tests """
    # empty string
    guid = ""
    self.assertEqual( checkGuid( guid ), False, "empty guid" )

    # wrong lenght in a 1st field
    guid = '012345678-0123-0123-0123-0123456789AB'
    self.assertEqual( checkGuid( guid ), False, 'wrong lenght in 1st field' )
    guid = '0123456-0123-0123-0123-0123456789AB'
    self.assertEqual( checkGuid( guid ), False, 'wrong lenght in 1st field')

    # wrong lenght in a 2nd field
    guid = '01234567-01234-0123-0123-0123456789AB'
    self.assertEqual( checkGuid( guid ), False, 'wrong lenght in 2nd field' )
    guid = '01234567-012-0123-0123-0123456789AB'
    self.assertEqual( checkGuid( guid ), False, 'wrong lenght in 2nd field' )

    # wrong lenght in a 3rd field
    guid = '01234567-0123-01234-0123-0123456789AB'
    self.assertEqual( checkGuid( guid ), False, 'wrong lenght in 3rd field' )
    guid = '01234567-0123-012-0123-0123456789AB'
    self.assertEqual( checkGuid( guid ), False, 'wrong lenght in 3rd field')

    # wrong lenght in a 4th field
    guid = '01234567-0123-0123-01234-0123456789AB'
    self.assertEqual( checkGuid( guid ), False, 'wrong lenght in 4th field' )
    guid = '01234567-0123-0123-012-0123456789AB'
    self.assertEqual( checkGuid( guid ), False, 'wrong lenght in 4th field' )

    # wrong lenght in a 5th field
    guid = '01234567-0123-0123-0123-0123-0123456789ABC'
    self.assertEqual( checkGuid( guid ), False, 'wrong lenght in 5th field' )
    guid = '01234567-0123-0123-0123-0123-0123456789A'
    self.assertEqual( checkGuid( guid ), False, 'wrong lenght in 5th field' )

    # small caps
    guid = '01234567-9ABC-0DEF-0123-456789ABCDEF'.lower()
    self.assertEqual( checkGuid( guid ), True, "small caps in guid, zut!" )

    # wrong characters not in [0-9A-F] 
    guid = 'NEEDMORE-SPAM-SPAM-SPAM-SPAMWITHEGGS'
    self.assertEqual( checkGuid( guid ), True, "wrong set of characters, zut!" )

    # normal operation
    guid = '01234567-9ABC-0DEF-0123-456789ABCDEF'
    self.assertEqual( checkGuid( guid ), True, "proper GUID" )

  def testMakeGuid( self ):
    """ makeGuid tests """
    # no filename - fake guid produced
    self.assertEqual( checkGuid( makeGuid() ), True , "fake guid for inexisting file" )
    # using this python file
    self.assertEqual( checkGuid( makeGuid( os.path.abspath(__file__) ) ), True, "guid for FileTestCase.py file" )
    

  def testGetSize( self ):
    """ getSize tests """
    # non existing file
    self.assertEqual( getSize("/spam/eggs/eggs"), -1, "inexisting file" )
    # file unreadable
    self.assertEqual( getSize('/root/.login'), -1 , "unreadable file")
    

  def testGetMD5ForFiles( self ):
    """ getMD5ForFiles tests """
  
    md5sum = getMD5ForFiles( self.filesList )
    reMD5 = re.compile( "^[0-9a-fA-F]+$" )
    self.assert_( reMD5.match( md5sum) != None )
    # OK for python 2.7
    # self.assertRegexpMatches( md5sum, reMD5, "regexp doesn't match" )

  def testGetCommonPath( self ):
    """ getCommonPath tests """
    # empty list
    self.assertEqual( getCommonPath( [] ), "" )

    # this folder and its parent
    filesList = [ os.path.abspath("..") + os.sep + x for x in os.listdir("..") ] + self.filesList
    self.assertEqual( getCommonPath( filesList ), os.path.abspath("..") )

# test case execution
if __name__ == "__main__":
  TESTLOADER = unittest.TestLoader()
  SUITE = TESTLOADER.loadTestsFromTestCase( FileTestCase )      
  unittest.TextTestRunner(verbosity=3).run( SUITE )
