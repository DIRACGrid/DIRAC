########################################################################
# $HeadURL $
# File: PfnTestCase.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/12/14 15:07:01
########################################################################

""" :mod: PfnTestCase 
    =======================
 
    .. module: PfnTestCase
    :synopsis: test case for Pfn module
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    test case for Pfn module
"""

__RCSID__ = "$Id $"

##
# @file PfnTestCase.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/12/14 15:07:12
# @brief Definition of PfnTestCase class.

## imports 
import unittest
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse
import random, string, timeit




########################################################################
class PfnTests( unittest.TestCase ):
  """
  .. class:: PfnTests
  
  """

  def setUp( self ):
    self.pfns =  {
      None : { "OK" : False, "Message" : "wrong 'pfn' argument value in function call, expected non-empty string, got None"},
      "" : { "OK" : False, "Message" : "wrong 'pfn' argument value in function call, expected non-empty string, got "},
      "/a/b/c" : { 'OK': True, 'Value': {'Protocol': '', 'WSUrl': '', 'FileName': 'c', 'Host': '', 'Path': '/a/b', 'Port': ''} },
      "proto:/a/b/c" : {'OK': True, 'Value': {'Protocol': 'proto', 'WSUrl': '', 'FileName': 'c', 'Host': '', 'Path': '/a/b', 'Port': ''}},
      "proto://host/a/b/c" : {'OK': True, 'Value': {'Protocol': 'proto', 'WSUrl': '', 'FileName': 'c', 'Host': 'host', 'Path': '/a/b', 'Port': ''}},
      "proto://host:port/a/b/c" : {'OK': True, 'Value': {'Protocol': 'proto', 'WSUrl': '', 'FileName': 'c', 'Host': 'host', 'Path': '/a/b', 'Port': 'port'}},
      "proto://host:port/wsurl?=/a/b/c" : {'OK': True, 'Value': {'Protocol': 'proto', 'WSUrl': '/wsurl?=', 'FileName': 'c', 'Host': 'host', 'Path': '/a/b', 'Port': 'port'}},
      "proto://host:port/wsurl?blah=/a/b/c" : {'OK': True, 'Value': {'Protocol': 'proto', 'WSUrl': '/wsurl?blah=', 'FileName': 'c', 'Host': 'host', 'Path': '/a/b', 'Port': 'port'}}}
    
  def test_01_parse( self ):
    """ pfnparse and pfnparse_old

    :param self: self reference
    """
 
    for pfn, result in self.pfns.items():
      self.assertEqual( pfnparse( pfn ), result )

  def test_02_unparse( self ):
    """ pfnunparse and pfnunparse_old
     
    :param self: self reference
    """
    for pfn, result in self.pfns.items():
      if result["OK"]:
        self.assertEqual( pfnunparse( result["Value"] ), { "OK" : True, "Value" : pfn } )
    self.assertEqual( pfnunparse( None ), 
                      {'Message': "pfnunparse: wrong type fot pfnDict argument, expected a dict, got <type 'NoneType'>", 'OK': False} )  
    self.assertEqual( pfnunparse( "Path" ), 
                      {'Message': "pfnunparse: wrong type fot pfnDict argument, expected a dict, got <type 'str'>", 'OK': False} )  
   

## test execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase( PfnTests )     
  unittest.TextTestRunner(verbosity=3).run(suite)

