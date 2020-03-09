from DIRAC.ConfigurationSystem.Client.Helpers import Resources

# Assumes CS structure be like:
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
  assert res['OK'] is True
  assert res['Value'] == ['DIRAC.Jenkins.ch']

  res = Resources.getSitesCEsMapping()
  print res
  assert res['OK'] is True
  assert res['Value'] == {'DIRAC.Jenkins.ch': ['jenkins.cern.ch']}

  res = Resources.getCESiteMapping()
  print res
  assert res['OK'] is True
  assert res['Value'] == {'jenkins.cern.ch': 'DIRAC.Jenkins.ch'}
