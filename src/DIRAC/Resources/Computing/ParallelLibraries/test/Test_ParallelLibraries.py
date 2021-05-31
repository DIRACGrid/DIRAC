""" Test ParallelLibraries
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import pytest
import os
from six.moves import reload_module

from DIRAC import S_OK, gLogger


gLogger.setLevel('DEBUG')

executableContent = """
#!/bin/bash

echo "hello world"
"""

srunOutput = """
1: line1
1: line2
2: line1
1: line3
3: line1
2: line2
3: line2
2: line3
3: line3
"""

srunExpected1 = """
# On node 1

 line1
 line2
 line3
"""


srunExpected2 = """
# On node 3

 line1
 line2
 line3
"""


srunExpected3 = """
# On node 2

 line1
 line2
 line3
"""


srunExpected = [srunExpected1, srunExpected2, srunExpected3]


@pytest.fixture
def generateParallelLibrary(parallelLibrary, parameters):
  """ Instantiate the requested Parallel Library
  """
  # Instantiate an object from parallelLibrary class
  parallelLibraryPath = 'DIRAC.Resources.Computing.ParallelLibraries.%s' % parallelLibrary
  plugin = __import__(parallelLibraryPath, globals(), locals(), [parallelLibrary])  # pylint: disable=unused-variable
  # Need to be reloaded to update the mock within the module, else, it will reuse the one when loaded the first time
  reload_module(plugin)
  parallelLibraryStr = 'plugin.%s(%s)' % (parallelLibrary, parameters)
  return eval(parallelLibraryStr)


@pytest.mark.parametrize("parallelLibrary, parameters, expectedFile, expectedContent", [
    ('Srun', "workingDirectory='.'", './srunExec.sh', 'srun'),
])
def test_generateWrapper(generateParallelLibrary, parallelLibrary, parameters, expectedFile, expectedContent):
  """ Test generateWrapper()
  """
  parallelLibraryInstance = generateParallelLibrary

  executableFile = 'executableFile.sh'
  with open(executableFile, 'w') as f:
    f.write(executableContent)

  res = parallelLibraryInstance.generateWrapper(executableFile)
  # Make sure a wrapper file has been generated and is executable
  assert res == expectedFile
  assert os.access(res, os.R_OK | os.X_OK)

  with open(res, 'r') as f:
    wrapperContent = f.read()
  # Make sure the wrapper contains important keywords and the executable filepath
  assert expectedContent in wrapperContent
  assert executableFile in wrapperContent

  os.remove(executableFile)
  os.remove(res)


@pytest.mark.parametrize("parallelLibrary, parameters, outputContent, expectedContent", [
    ('Srun', "workingDirectory='.'", srunOutput, srunExpected),
])
def test_processOutput(generateParallelLibrary, parallelLibrary, parameters, outputContent, expectedContent):
  """ Test processOutput()
  """
  parallelLibraryInstance = generateParallelLibrary

  # We remove the '\n' at the beginning/end of the file because there are not present in reality
  outputContent = outputContent.strip()
  # We only remove the '\n' at the beginning because processOutput adds a '\n' at the end
  expectedContent = [i.lstrip() for i in expectedContent]

  # In this case, we pass output content as a string to processOutput()
  # It returns a string
  output, error = parallelLibraryInstance.processOutput(outputContent, outputContent, isFile=False)
  for srunLines in expectedContent:
    assert srunLines in output

  outputFile = 'output.txt'
  with open(outputFile, 'w') as f:
    f.write(outputContent)

  errorFile = 'error.txt'
  with open(errorFile, 'w') as f:
    f.write(outputContent)

  # In this case, we pass output content as a file to processOutput()
  # It returns a filename, which should be the same as the parameter
  output, error = parallelLibraryInstance.processOutput(outputFile, errorFile, isFile=True)
  assert output == outputFile

  with open(outputFile, 'r') as f:
    wrapperContent = f.read()
  for srunLines in expectedContent:
    assert srunLines in wrapperContent

  os.remove(outputFile)
  os.remove(errorFile)
