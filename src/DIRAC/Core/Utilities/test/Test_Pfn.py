########################################################################
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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id $"

# #
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/12/14 15:07:12

# # imports
import unittest

# sut
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse



########################################################################
class PfnTests( unittest.TestCase ):
  """
  .. class:: PfnTests

  """

  def setUp( self ):
    self.default_pfns = {
      None : {'Errno': 0, 'Message': "wrong 'pfn' argument value in function call, expected non-empty string, got <type 'NoneType'>", 'OK': False},
      "" : { "OK" : False, 'Errno': 0, "Message" : "wrong 'pfn' argument value in function call, expected non-empty string, got <type 'NoneType'>"},
      "/a/b/c" : { 'OK': True, 'Value': {'Protocol': '', 'WSUrl': '', 'FileName': 'c', 'Host': '', 'Path': '/a/b', 'Port': ''} },
      "proto:/a/b/c" : {'OK': True, 'Value': {'Protocol': 'proto', 'WSUrl': '', 'FileName': 'c', 'Host': '', 'Path': '/a/b', 'Port': ''}},
      "proto://host/a/b/c" : {'OK': True, 'Value': {'Protocol': 'proto', 'WSUrl': '', 'FileName': 'c', 'Host': 'host', 'Path': '/a/b', 'Port': ''}},
      "proto://host:port/a/b/c" : {'OK': True, 'Value': {'Protocol': 'proto', 'WSUrl': '', 'FileName': 'c', 'Host': 'host', 'Path': '/a/b', 'Port': 'port'}},
      "proto://host:port//a/b/c?SvcClass=toto" : {'OK': True, 'Value': {'Protocol': 'proto', 'WSUrl': '', 'FileName': 'c', 'Host': 'host', 'Path': '//a/b', 'Port': 'port', 'Options' : 'SvcClass=toto'}},
      "proto://host:port/a/b/c?SvcClass=toto" : {'OK': True, 'Value': {'Protocol': 'proto', 'WSUrl': '', 'FileName': 'c', 'Host': 'host', 'Path': '/a/b', 'Port': 'port', 'Options' : 'SvcClass=toto'}},

      }


    # We keep some standard non srm specific url that used to be supported, just for backward compatibility
    self.srm_pfns = {
      None : {'Errno': 0, 'Message': "wrong 'pfn' argument value in function call, expected non-empty string, got <type 'NoneType'>", 'OK': False},
      "" : { "OK" : False, 'Errno': 0, "Message" : "wrong 'pfn' argument value in function call, expected non-empty string, got <type 'NoneType'>"},
      "/a/b/c" : { 'OK': True, 'Value': {'Protocol': '', 'WSUrl': '', 'FileName': 'c', 'Host': '', 'Path': '/a/b', 'Port': ''} },
      "proto:/a/b/c" : {'OK': True, 'Value': {'Protocol': 'proto', 'WSUrl': '', 'FileName': 'c', 'Host': '', 'Path': '/a/b', 'Port': ''}},
      "proto://host/a/b/c" : {'OK': True, 'Value': {'Protocol': 'proto', 'WSUrl': '', 'FileName': 'c', 'Host': 'host', 'Path': '/a/b', 'Port': ''}},
      "proto://host:port/a/b/c" : {'OK': True, 'Value': {'Protocol': 'proto', 'WSUrl': '', 'FileName': 'c', 'Host': 'host', 'Path': '/a/b', 'Port': 'port'}},
      "proto://host:port/wsurl?=/a/b/c" : {'OK': True, 'Value': {'Protocol': 'proto', 'WSUrl': '/wsurl?=', 'FileName': 'c', 'Host': 'host', 'Path': '/a/b', 'Port': 'port'}},
      "proto://host:port/wsurl?blah=/a/b/c" : {'OK': True, 'Value': {'Protocol': 'proto', 'WSUrl': '/wsurl?blah=', 'FileName': 'c', 'Host': 'host', 'Path': '/a/b', 'Port': 'port'}},
      }

  def test_01_srm_parse( self ):
    """ pfnparse and pfnparse_old

    :param self: self reference
    """

    for pfn, result in self.srm_pfns.items():
      parseResult = pfnparse( pfn )
      self.assertEqual( parseResult['OK'], result['OK'] )
      if result['OK']:
        self.assertEqual( parseResult['Value'], result['Value'] )


  def test_02_default_parse( self ):
    """ pfnparse and pfnparse_old

    :param self: self reference
    """

    for pfn, result in self.default_pfns.items():
      parseResult = pfnparse( pfn, srmSpecific = False )
      self.assertEqual( parseResult['OK'], result['OK'] )
      if result['OK']:
        self.assertEqual( parseResult['Value'], result['Value'] )



  def test_03_srm_unparse( self ):
    """ pfnunparse and pfnunparse_old

    :param self: self reference
    """
    for pfn, result in self.srm_pfns.items():
      if result["OK"]:
        unparseResult = pfnunparse( result["Value"] )
        self.assertEqual( unparseResult, { "OK" : True, "Value" : pfn } )
    self.assertEqual( pfnunparse( None )['OK'], False )
    self.assertEqual( pfnunparse( "Path" )['OK'], False )

  def test_03_default_unparse( self ):
    """ pfnunparse and pfnunparse_old

    :param self: self reference
    """
    for pfn, result in self.default_pfns.items():
      if result["OK"]:
        unparseResult = pfnunparse( result["Value"], srmSpecific = False )
        self.assertEqual( unparseResult, { "OK" : True, "Value" : pfn } )
    self.assertEqual( pfnunparse( None )['OK'], False )
    self.assertEqual( pfnunparse( "Path" )['OK'], False )


# # test execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  suite = testLoader.loadTestsFromTestCase( PfnTests )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )
