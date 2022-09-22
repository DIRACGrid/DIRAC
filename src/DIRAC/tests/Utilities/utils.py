""" few utilities
"""
import contextlib
import os
import shutil
import tempfile

# pylint: disable=missing-docstring


def cleanTestDir():
    for fileIn in os.listdir("."):
        if "Local" in fileIn and os.path.isdir(fileIn):
            shutil.rmtree(fileIn)
    for fileToRemove in ["std.out", "std.err"]:
        try:
            os.remove(fileToRemove)
        except OSError:
            continue


def find_all(name, path, directory=None):
    """Simple files finder"""
    result = []
    for root, _dirs, files in os.walk(path):
        if name in files:
            result.append(os.path.join(root, name))
    result = [os.path.abspath(p) for p in result]
    if directory:
        if directory not in os.getcwd():
            return [x for x in result if directory in x]
    return result


class MatchStringWith(str):
    """helper class to match sub strings in a mock.assert_called_with

    >>> myMock.log.error.assert_called_with( MatchStringWith('error mess') )
    """

    def __eq__(self, other):
        return self in str(other)


@contextlib.contextmanager
def generateDIRACConfig(cfgContent, testCfgFileName):
    """Utility to have a locally loaded DIRAC config for a test.

    To use it:

    .. code-block :: python

        from DIRAC.tests.Utilities.utils import generateDIRACConfig

        CFG_CONTENT = \"\"\"
        Resources
        {
            StorageElements
            {

            }
        }
        \"\"\"

        @pytest.fixture(scope="module", autouse=True)
        def loadCS():
            \"\"\" Load the CFG_CONTENT as a DIRAC Configuration for this module \"\"\"
            with generateDIRACConfig(CFG_CONTENT, "myConfig.cfg"):
                yield


    :param str cfgContent: the content of the CS you want




    """

    from diraccfg import CFG
    from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
    from DIRAC.ConfigurationSystem.private.ConfigurationClient import ConfigurationClient

    testCfgFilePath = os.path.join(tempfile.gettempdir(), testCfgFileName)
    with open(testCfgFilePath, "w") as f:
        f.write(cfgContent)
    # Load the configuration
    ConfigurationClient(fileToLoadList=[testCfgFilePath])  # we replace the configuration by our own one.

    yield

    try:
        os.remove(testCfgFilePath)
    except OSError:
        pass
    # SUPER UGLY: one must recreate the CFG objects of gConfigurationData
    # not to conflict with other tests that might be using a local dirac.cfg
    gConfigurationData.localCFG = CFG()
    gConfigurationData.remoteCFG = CFG()
    gConfigurationData.mergedCFG = CFG()
    gConfigurationData.generateNewVersion()
