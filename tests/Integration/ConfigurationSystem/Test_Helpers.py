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

  res = Resources.getSitesCEsMapping()
  assert res['OK'] is True, res['Message']
  assert res['Value'] == {'DIRAC.Jenkins.ch': ['jenkins.cern.ch']}, res['Value']

  res = Resources.getCESiteMapping()
  assert res['OK'] is True, res['Message']
  assert res['Value'] == {'jenkins.cern.ch': 'DIRAC.Jenkins.ch'}, res['Value']
