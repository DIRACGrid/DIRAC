""" This is a test of the creation of the pilot wrapper
"""

# pylint: disable=protected-access, invalid-name

import unittest

from DIRAC.WorkloadManagementSystem.Utilities.PilotWrapper import pilotWrapperScript

class PilotWrapperTestCase( unittest.TestCase ):
  """ Base class for the Agents test cases
  """
  def setUp( self ):
    pass

  def tearDown( self ):
    pass


class PilotWrapperTestCaseCreation( PilotWrapperTestCase ):

  def test_script(self):
    """ test script creation
    """

    mStringList = []
    for moduleName in ['aa', 'bb']:
      mString = """with open('%s', "w") as fd:\n    fd.write(bz2.decompress(base64.b64decode(\"\"\"%s\"\"\")))""" % \
                (moduleName, "something")
      mStringList.append(mString)
    pilotFilesString = '\n  '.join(mStringList)

    res = pilotWrapperScript(
        install = 'dirac-install',
        pilotFilesString = pilotFilesString,
        proxyFlag = 'False')

    print res
    # no assert as it makes little sense

#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( PilotWrapperTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( PilotWrapperTestCaseCreation ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#

