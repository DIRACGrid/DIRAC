""" Integration tests for CS helpers. It expects to find a proper structure in the CS
"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()


from DIRAC.ConfigurationSystem.Client.Helpers import Resources

# Assumes CS structure containing:
#
# Resources
# {
#   Sites
#   {
#     DIRAC
#     {
#       DIRAC.Jenkins.ch
#       {
#         Name = aNameWhatSoEver
#         CEs
#         {
#           jenkins.cern.ch
#           {
#           }
#         }
#       }
#     }
#   }
# }


def test_ResourcesGetters():
  res = Resources.getSites()
  assert res['OK'] is True, res['Message']
  assert res['Value'] == ['DIRAC.Jenkins.ch'], res['Value']

  res = Resources.getSiteCEMapping()
  assert res['OK'] is True, res['Message']
  assert res['Value'] == {'DIRAC.Jenkins.ch': ['jenkins.cern.ch']}, res['Value']

  res = Resources.getCESiteMapping()
  assert res['OK'] is True, res['Message']
  assert res['Value'] == {'jenkins.cern.ch': 'DIRAC.Jenkins.ch'}, res['Value']

  res = Resources.getCESiteMapping('jenkins.cern.ch')
  assert res['OK'] is True, res['Message']
  assert res['Value'] == {'jenkins.cern.ch': 'DIRAC.Jenkins.ch'}, res['Value']

  res = Resources.getCESiteMapping('not-here')
  assert res['OK'] is True, res['Message']
  assert res['Value'] == {}, res['Value']
