"""
    Integration test to test if Tornado can run multiple services and client can find url.


    You must have 2 separate tornadohandlers and this services must be in dirac.cfg like normal service
    Only change: dips:// became https://  and Protocol = dips became Protocol = https

    ```
    Services
      {
        User
        {
          Protocol = https
        }
        DummyTornado
        {
          Protocol = https
        }
      }
    ```

    ```
    URLs
      {
        User = https://MrBoincHost/Framework/User #Port not precised because we use default one
        DummyTornado = https://MrBoincHost/Framework/DummyTornado 
      }
    ```
"""
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.TornadoServices.Client.TornadoClient import TornadoClient



def test_service_user():
  service = TornadoClient('Framework/User')
  ping = service.ping()
  assert ping['OK'] == True
  assert ping['Value']['name'] == 'Framework/User'

def test_service_dummy():
  service = TornadoClient('Framework/DummyTornado')
  ping = service.ping()
  assert ping['OK'] == True
  assert ping['Value']['name'] == 'Framework/DummyTornado'