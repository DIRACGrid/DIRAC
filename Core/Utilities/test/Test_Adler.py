########################################################################
# File: AdlerTestCase.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/02/11 09:08:19
########################################################################

""" :mod: AdlerTestCase
    =======================

    .. module: AdlerTestCase
    :synopsis: test case for DIRAC.Core.Utilities.Adler module
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    test case for DIRAC.Core.Utilities.Adler module
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

##
# @file AdlerTestCase.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/02/11 09:08:37
# @brief Definition of AdlerTestCase class.

# imports
import os
import unittest
import string
import tempfile
# from zlib import adler32
import zlib

# from DIRAC
from DIRAC.Core.Utilities import Adler

__RCSID__ = "$Id$"

########################################################################


class AdlerTestCase(unittest.TestCase):

  """
  .. class:: AdlerTestCase
  test case for DIRAC.Core.Utilities.Adler module
  """

  def setUp(self):
    self.emptyAdler = hex(zlib.adler32(b"") & 0xffffffff)[2:]
    self.lettersAdler = hex(zlib.adler32(string.ascii_letters.encode()) & 0xffffffff)[2:]

  def testStringAdler(self):
    """ stringAdler tests """
    # no arguments supplied - TypeError
    try:
      Adler.stringAdler()  # pylint: disable=no-value-for-parameter
    except Exception as error:
      self.assertEqual(isinstance(error, TypeError), True)
    # wrong argument type
    self.assertEqual(Adler.stringAdler([]), False)
    # empty string
    self.assertEqual(int(Adler.stringAdler("")), int(self.emptyAdler))
    # all letters
    self.assertEqual(Adler.stringAdler(string.ascii_letters), self.lettersAdler)

  def testConversion(self):
    """ intAdlerToHex and hexAdlerToInt tests """
    # no arguments
    try:
      Adler.intAdlerToHex()  # pylint: disable=no-value-for-parameter
    except Exception as error:
      self.assertEqual(isinstance(error, TypeError), True)
    # wrong type of arg (should it really print out to stdout)
    self.assertEqual(Adler.intAdlerToHex("a"), False)
    # normal operation
    self.assertEqual(int(Adler.intAdlerToHex(1)),
                     Adler.hexAdlerToInt(Adler.intAdlerToHex(1)))
    self.assertEqual(Adler.hexAdlerToInt("0x01"),
                     int(Adler.intAdlerToHex(Adler.hexAdlerToInt("0x01"))))

  def testFileAdler(self):
    """ fileAdler tests """
    # no args
    try:
      Adler.fileAdler()  # pylint: disable=no-value-for-parameter
    except Exception as error:
      self.assertEqual(isinstance(error, TypeError), True)
    # read-protected file
    self.assertEqual(Adler.fileAdler("/root/.login"), False)
    # inexisting file
    self.assertEqual(Adler.fileAdler("Stone/Dead/Norwegian/Blue/Parrot/In/Camelot"), False)
    # normal operation
    fd, path = tempfile.mkstemp("_adler32", "norewgian_blue")
    self.assertEqual(int(Adler.fileAdler(path)), int(self.emptyAdler))
    os.write(fd, string.ascii_letters.encode())
    self.assertEqual(Adler.fileAdler(path), self.lettersAdler)

  def testCompareAdler(self):
    """ compareAdler tests """
    # same adlers
    self.assertEqual(Adler.compareAdler(Adler.stringAdler(""), Adler.stringAdler("")), True)
    # diff adlers
    self.assertEqual(Adler.compareAdler(Adler.stringAdler(""), Adler.stringAdler(string.ascii_letters)), False)


# test suite execution
if __name__ == "__main__":
  TESTLOADER = unittest.TestLoader()
  SUITE = TESTLOADER.loadTestsFromTestCase(AdlerTestCase)
  unittest.TextTestRunner(verbosity=3).run(SUITE)
